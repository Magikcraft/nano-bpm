//! **Minimal llama-GGUF metadata scanner** — reads a model's capability card without loading it.
//!
//! llama.cpp models ship as GGUF files whose header carries typed key/value metadata and the
//! tensor directory. ProcessOS scans that header (a few KB of reads; the weights are never
//! touched) to answer two questions the cockpit needs *before* a model is launched:
//!
//! 1. **Does this model support MTP (multi-token prediction)?** MTP-capable GGUFs (GLM-4.5+,
//!    DeepSeek-V3+, Qwen3.6+ family conversions) carry the `{arch}.nextn_predict_layers` KV key
//!    and/or `nextn` tensors (`blk.{n}.nextn.eh_proj`, …) — llama.cpp's canonical naming for the
//!    built-in prediction heads its `--mtp` (self-)speculative decoding uses. A model whose
//!    conversion stripped those tensors cannot run MTP, no matter what its source config claimed.
//! 2. **Can model B draft for model A?** Speculative decoding requires target and draft to agree
//!    on the token space. Mirroring llama.cpp's own `common/speculative` checks, we compare the
//!    tokenizer family (`tokenizer.ggml.model` / `.pre`), BOS/EOS ids, and vocab size (within
//!    llama.cpp's tolerance of [`SPEC_VOCAB_MAX_SIZE_DIFFERENCE`] entries).
//!
//! Only the header is parsed: magic/version, the KV section (scalars + string values are kept,
//! arrays are skipped except for their length — the `tokenizer.ggml.tokens` count *is* the vocab
//! size), and the tensor-info directory (names only). Dependency-free by design, like the rest of
//! the crate's infrastructure.

use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::path::Path;

/// llama.cpp allows the draft and target vocab sizes to differ by at most this many entries
/// (`SPEC_VOCAB_MAX_SIZE_DIFFERENCE` in `common/speculative.cpp`).
pub const SPEC_VOCAB_MAX_SIZE_DIFFERENCE: u64 = 128;

/// Refuse to allocate strings longer than this while parsing (a corrupt/hostile header would
/// otherwise ask for gigabytes). Real GGUF metadata strings are well under this.
const MAX_STR_LEN: u64 = 16 * 1024 * 1024;
/// Sanity caps on the declared KV / tensor counts (real models: tens of KVs, thousands of tensors).
const MAX_KV_COUNT: u64 = 1 << 20;
const MAX_TENSOR_COUNT: u64 = 1 << 22;

/// A scalar metadata value we retain (arrays are skipped; only their length may be recorded).
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
}

impl Value {
    fn as_u64(&self) -> Option<u64> {
        match self {
            Value::U64(v) => Some(*v),
            Value::I64(v) => u64::try_from(*v).ok(),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
}

/// What a GGUF header scan learned about a model. All fields are best-effort (`None` when the
/// key is absent) — [`GgufScan::mtp_capable`] and [`speculator_compatible`] interpret them.
#[derive(Clone, Debug, Default)]
pub struct GgufScan {
    /// `general.architecture`, e.g. `glm4moe`, `qwen3moe`, `llama`.
    pub arch: Option<String>,
    /// `general.name` — the human-facing model name baked in at conversion.
    pub name: Option<String>,
    /// `{arch}.context_length`.
    pub context_length: Option<u64>,
    /// Vocab size: the `tokenizer.ggml.tokens` array length, else `{arch}.vocab_size`.
    pub vocab_size: Option<u64>,
    /// `tokenizer.ggml.model` — the tokenizer family (`gpt2`, `llama`, …).
    pub tokenizer_model: Option<String>,
    /// `tokenizer.ggml.pre` — the pre-tokenizer variant.
    pub tokenizer_pre: Option<String>,
    /// `tokenizer.ggml.bos_token_id` / `eos_token_id`.
    pub bos_token_id: Option<u64>,
    pub eos_token_id: Option<u64>,
    /// `{arch}.nextn_predict_layers` — how many MTP (NextN) prediction layers the model carries.
    pub nextn_predict_layers: Option<u64>,
    /// How many tensors in the directory are NextN/MTP tensors (`…nextn.…`).
    pub nextn_tensor_count: usize,
    /// Total tensors in the directory.
    pub tensor_count: u64,
}

impl GgufScan {
    /// Whether the model carries MTP prediction heads llama.cpp's `--mtp` can use: the
    /// `{arch}.nextn_predict_layers` KV says so, or NextN tensors are physically present. Both are
    /// checked because some conversions carry the tensors without the KV (and a KV without the
    /// tensors — a stripped quant — would be a lie we'd catch via the tensor directory).
    pub fn mtp_capable(&self) -> bool {
        self.nextn_predict_layers.unwrap_or(0) > 0 || self.nextn_tensor_count > 0
    }

    /// Scan the GGUF file at `path` (header + KV metadata + tensor names; weights untouched).
    pub fn read(path: &Path) -> Result<Self, String> {
        let file =
            std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        Self::parse(&mut BufReader::new(file)).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Parse a GGUF stream. Split from [`Self::read`] so tests can feed synthetic bytes.
    fn parse<R: Read>(r: &mut R) -> Result<Self, String> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)
            .map_err(|e| format!("read magic: {e}"))?;
        if &magic != b"GGUF" {
            return Err("not a GGUF file (bad magic)".to_string());
        }
        let version = read_u32(r)?;
        if !(2..=3).contains(&version) {
            return Err(format!("unsupported GGUF version {version} (want 2 or 3)"));
        }
        let tensor_count = read_u64(r)?;
        let kv_count = read_u64(r)?;
        if kv_count > MAX_KV_COUNT || tensor_count > MAX_TENSOR_COUNT {
            return Err(format!(
                "implausible header (kv={kv_count}, tensors={tensor_count}) — corrupt file?"
            ));
        }

        // KV section: keep scalars/strings, skip array bodies but record array lengths.
        let mut kvs: BTreeMap<String, Value> = BTreeMap::new();
        let mut array_lens: BTreeMap<String, u64> = BTreeMap::new();
        for _ in 0..kv_count {
            let key = read_string(r)?;
            let ty = read_u32(r)?;
            if ty == TY_ARRAY {
                array_lens.insert(key, skip_array(r)?);
            } else if let Some(v) = read_scalar(r, ty)? {
                kvs.insert(key, v);
            }
        }

        // Tensor directory: names only (dims/type/offset are skipped per entry).
        let mut nextn_tensor_count = 0usize;
        for _ in 0..tensor_count {
            let name = read_string(r)?;
            if name.contains("nextn.") {
                nextn_tensor_count += 1;
            }
            let n_dims = read_u32(r)?;
            if n_dims > 8 {
                return Err(format!("implausible tensor rank {n_dims} — corrupt file?"));
            }
            skip(r, n_dims as u64 * 8 + 4 + 8)?; // dims (u64 each) + ggml type (u32) + offset (u64)
        }

        let arch = kvs
            .get("general.architecture")
            .and_then(Value::as_str)
            .map(str::to_string);
        let arch_key = |suffix: &str| -> Option<&Value> {
            arch.as_deref()
                .and_then(|a| kvs.get(&format!("{a}.{suffix}")))
        };
        Ok(Self {
            name: kvs
                .get("general.name")
                .and_then(Value::as_str)
                .map(str::to_string),
            context_length: arch_key("context_length").and_then(Value::as_u64),
            vocab_size: array_lens
                .get("tokenizer.ggml.tokens")
                .copied()
                .or_else(|| arch_key("vocab_size").and_then(Value::as_u64)),
            tokenizer_model: kvs
                .get("tokenizer.ggml.model")
                .and_then(Value::as_str)
                .map(str::to_string),
            tokenizer_pre: kvs
                .get("tokenizer.ggml.pre")
                .and_then(Value::as_str)
                .map(str::to_string),
            bos_token_id: kvs
                .get("tokenizer.ggml.bos_token_id")
                .and_then(Value::as_u64),
            eos_token_id: kvs
                .get("tokenizer.ggml.eos_token_id")
                .and_then(Value::as_u64),
            nextn_predict_layers: arch_key("nextn_predict_layers")
                .and_then(Value::as_u64)
                .or_else(|| {
                    // Fallback: any `*.nextn_predict_layers` key, in case `general.architecture`
                    // is absent or the key is namespaced unexpectedly.
                    kvs.iter()
                        .find(|(k, _)| k.ends_with(".nextn_predict_layers"))
                        .and_then(|(_, v)| v.as_u64())
                }),
            nextn_tensor_count,
            tensor_count,
            arch,
        })
    }
}

/// Whether `draft` can speculate for `target` under llama.cpp's speculative-decoding rules:
/// same tokenizer family and pre-tokenizer, same BOS/EOS, and vocab sizes within
/// [`SPEC_VOCAB_MAX_SIZE_DIFFERENCE`]. Fields both sides omit are not comparable and are treated
/// as unverifiable — an **error**, because "we couldn't check" must not read as "compatible".
/// The same model drafting for itself (identical scans) always passes.
pub fn speculator_compatible(target: &GgufScan, draft: &GgufScan) -> Result<(), String> {
    fn require<T: PartialEq + std::fmt::Debug>(
        what: &str,
        t: &Option<T>,
        d: &Option<T>,
    ) -> Result<(), String> {
        match (t, d) {
            (Some(a), Some(b)) if a == b => Ok(()),
            (Some(a), Some(b)) => Err(format!(
                "{what} differs between target ({a:?}) and draft ({b:?})"
            )),
            _ => Err(format!(
                "cannot verify {what}: missing from the GGUF metadata of {}",
                if t.is_none() {
                    "the target"
                } else {
                    "the draft"
                }
            )),
        }
    }

    require(
        "tokenizer family (tokenizer.ggml.model)",
        &target.tokenizer_model,
        &draft.tokenizer_model,
    )?;
    // `tokenizer.ggml.pre` is optional in older conversions; only compare when both declare it.
    if let (Some(a), Some(b)) = (&target.tokenizer_pre, &draft.tokenizer_pre) {
        if a != b {
            return Err(format!(
                "pre-tokenizer differs between target ({a}) and draft ({b})"
            ));
        }
    }
    require("BOS token id", &target.bos_token_id, &draft.bos_token_id)?;
    require("EOS token id", &target.eos_token_id, &draft.eos_token_id)?;
    match (target.vocab_size, draft.vocab_size) {
        (Some(t), Some(d)) => {
            let diff = t.abs_diff(d);
            if diff > SPEC_VOCAB_MAX_SIZE_DIFFERENCE {
                return Err(format!(
                    "vocab sizes differ by {diff} (target {t}, draft {d}) — more than llama.cpp's \
                     speculative-decoding tolerance of {SPEC_VOCAB_MAX_SIZE_DIFFERENCE}"
                ));
            }
            Ok(())
        }
        _ => Err("cannot verify vocab size: missing from the GGUF metadata".to_string()),
    }
}

// ── wire-format readers ──────────────────────────────────────────────────────

const TY_UINT8: u32 = 0;
const TY_INT8: u32 = 1;
const TY_UINT16: u32 = 2;
const TY_INT16: u32 = 3;
const TY_UINT32: u32 = 4;
const TY_INT32: u32 = 5;
const TY_FLOAT32: u32 = 6;
const TY_BOOL: u32 = 7;
const TY_STRING: u32 = 8;
const TY_ARRAY: u32 = 9;
const TY_UINT64: u32 = 10;
const TY_INT64: u32 = 11;
const TY_FLOAT64: u32 = 12;

fn read_u32<R: Read>(r: &mut R) -> Result<u32, String> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(|e| format!("read u32: {e}"))?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(|e| format!("read u64: {e}"))?;
    Ok(u64::from_le_bytes(b))
}

fn read_string<R: Read>(r: &mut R) -> Result<String, String> {
    let len = read_u64(r)?;
    if len > MAX_STR_LEN {
        return Err(format!("string length {len} exceeds cap — corrupt file?"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)
        .map_err(|e| format!("read string: {e}"))?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Read one scalar value of `ty`. Returns `None` for a valid-but-unretained value (there are
/// none today — every scalar type maps to a [`Value`]) and errors on an unknown type id, since we
/// cannot know its width and the stream would desynchronise.
fn read_scalar<R: Read>(r: &mut R, ty: u32) -> Result<Option<Value>, String> {
    let v = match ty {
        TY_UINT8 => Value::U64(read_bytes::<1, R>(r)?[0] as u64),
        TY_INT8 => Value::I64(read_bytes::<1, R>(r)?[0] as i8 as i64),
        TY_UINT16 => Value::U64(u16::from_le_bytes(read_bytes::<2, R>(r)?) as u64),
        TY_INT16 => Value::I64(i16::from_le_bytes(read_bytes::<2, R>(r)?) as i64),
        TY_UINT32 => Value::U64(read_u32(r)? as u64),
        TY_INT32 => Value::I64(i32::from_le_bytes(read_bytes::<4, R>(r)?) as i64),
        TY_FLOAT32 => Value::F64(f32::from_le_bytes(read_bytes::<4, R>(r)?) as f64),
        TY_BOOL => Value::Bool(read_bytes::<1, R>(r)?[0] != 0),
        TY_STRING => Value::Str(read_string(r)?),
        TY_UINT64 => Value::U64(read_u64(r)?),
        TY_INT64 => Value::I64(i64::from_le_bytes(read_bytes::<8, R>(r)?)),
        TY_FLOAT64 => Value::F64(f64::from_le_bytes(read_bytes::<8, R>(r)?)),
        other => return Err(format!("unknown GGUF value type {other}")),
    };
    Ok(Some(v))
}

fn read_bytes<const N: usize, R: Read>(r: &mut R) -> Result<[u8; N], String> {
    let mut b = [0u8; N];
    r.read_exact(&mut b).map_err(|e| format!("read: {e}"))?;
    Ok(b)
}

/// Skip an array value (element type + count already positioned at), returning its length.
/// String arrays are skipped element-by-element (each carries its own length); fixed-width
/// element bodies are skipped in one hop. Nested arrays recurse.
fn skip_array<R: Read>(r: &mut R) -> Result<u64, String> {
    let elem_ty = read_u32(r)?;
    let count = read_u64(r)?;
    match elem_ty {
        TY_STRING => {
            for _ in 0..count {
                let len = read_u64(r)?;
                if len > MAX_STR_LEN {
                    return Err(format!("string length {len} exceeds cap — corrupt file?"));
                }
                skip(r, len)?;
            }
        }
        TY_ARRAY => {
            for _ in 0..count {
                skip_array(r)?;
            }
        }
        _ => {
            let width: u64 = match elem_ty {
                TY_UINT8 | TY_INT8 | TY_BOOL => 1,
                TY_UINT16 | TY_INT16 => 2,
                TY_UINT32 | TY_INT32 | TY_FLOAT32 => 4,
                TY_UINT64 | TY_INT64 | TY_FLOAT64 => 8,
                other => return Err(format!("unknown GGUF array element type {other}")),
            };
            skip(r, count.saturating_mul(width))?;
        }
    }
    Ok(count)
}

fn skip<R: Read>(r: &mut R, n: u64) -> Result<(), String> {
    std::io::copy(&mut r.take(n), &mut std::io::sink())
        .map_err(|e| format!("skip {n} bytes: {e}"))
        .and_then(|copied| {
            if copied == n {
                Ok(())
            } else {
                Err(format!(
                    "truncated file (wanted {n} more bytes, got {copied})"
                ))
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny in-memory GGUF writer mirroring the reader, so tests exercise real wire bytes.
    struct W(Vec<u8>);

    impl W {
        fn new(tensor_count: u64, kv_count: u64) -> Self {
            let mut b = Vec::new();
            b.extend_from_slice(b"GGUF");
            b.extend_from_slice(&3u32.to_le_bytes());
            b.extend_from_slice(&tensor_count.to_le_bytes());
            b.extend_from_slice(&kv_count.to_le_bytes());
            W(b)
        }
        fn s(&mut self, s: &str) {
            self.0.extend_from_slice(&(s.len() as u64).to_le_bytes());
            self.0.extend_from_slice(s.as_bytes());
        }
        fn kv_str(&mut self, k: &str, v: &str) {
            self.s(k);
            self.0.extend_from_slice(&TY_STRING.to_le_bytes());
            self.s(v);
        }
        fn kv_u32(&mut self, k: &str, v: u32) {
            self.s(k);
            self.0.extend_from_slice(&TY_UINT32.to_le_bytes());
            self.0.extend_from_slice(&v.to_le_bytes());
        }
        fn kv_str_array(&mut self, k: &str, items: &[&str]) {
            self.s(k);
            self.0.extend_from_slice(&TY_ARRAY.to_le_bytes());
            self.0.extend_from_slice(&TY_STRING.to_le_bytes());
            self.0
                .extend_from_slice(&(items.len() as u64).to_le_bytes());
            for i in items {
                self.s(i);
            }
        }
        fn kv_f32_array(&mut self, k: &str, items: &[f32]) {
            self.s(k);
            self.0.extend_from_slice(&TY_ARRAY.to_le_bytes());
            self.0.extend_from_slice(&TY_FLOAT32.to_le_bytes());
            self.0
                .extend_from_slice(&(items.len() as u64).to_le_bytes());
            for i in items {
                self.0.extend_from_slice(&i.to_le_bytes());
            }
        }
        fn tensor(&mut self, name: &str) {
            self.s(name);
            self.0.extend_from_slice(&2u32.to_le_bytes()); // n_dims
            self.0.extend_from_slice(&8u64.to_le_bytes());
            self.0.extend_from_slice(&8u64.to_le_bytes());
            self.0.extend_from_slice(&0u32.to_le_bytes()); // ggml type
            self.0.extend_from_slice(&0u64.to_le_bytes()); // offset
        }
    }

    /// A representative MTP-capable model header (GLM-ish): arch KV + nextn tensors.
    fn mtp_model(vocab: usize) -> GgufScan {
        let tokens: Vec<String> = (0..vocab).map(|i| format!("t{i}")).collect();
        let token_refs: Vec<&str> = tokens.iter().map(String::as_str).collect();
        let mut w = W::new(3, 9);
        w.kv_str("general.architecture", "glm4moe");
        w.kv_str("general.name", "GLM Test");
        w.kv_u32("glm4moe.context_length", 131072);
        w.kv_u32("glm4moe.nextn_predict_layers", 1);
        w.kv_str("tokenizer.ggml.model", "gpt2");
        w.kv_str("tokenizer.ggml.pre", "glm4");
        w.kv_u32("tokenizer.ggml.bos_token_id", 1);
        w.kv_u32("tokenizer.ggml.eos_token_id", 2);
        w.kv_str_array("tokenizer.ggml.tokens", &token_refs);
        w.tensor("blk.0.attn_q.weight");
        w.tensor("blk.92.nextn.eh_proj.weight");
        w.tensor("blk.92.nextn.embed_tokens.weight");
        GgufScan::parse(&mut w.0.as_slice()).expect("parse ok")
    }

    /// A plain model without MTP heads and a different tokenizer.
    fn plain_model() -> GgufScan {
        let mut w = W::new(1, 7);
        w.kv_str("general.architecture", "llama");
        w.kv_u32("llama.context_length", 8192);
        w.kv_str("tokenizer.ggml.model", "llama");
        w.kv_u32("tokenizer.ggml.bos_token_id", 1);
        w.kv_u32("tokenizer.ggml.eos_token_id", 2);
        w.kv_str_array("tokenizer.ggml.tokens", &["a", "b", "c"]);
        w.kv_f32_array("tokenizer.ggml.scores", &[0.0, 1.0, 2.0]);
        w.tensor("blk.0.attn_q.weight");
        GgufScan::parse(&mut w.0.as_slice()).expect("parse ok")
    }

    #[test]
    fn scans_mtp_metadata_and_tensors() {
        let scan = mtp_model(16);
        assert_eq!(scan.arch.as_deref(), Some("glm4moe"));
        assert_eq!(scan.name.as_deref(), Some("GLM Test"));
        assert_eq!(scan.context_length, Some(131072));
        assert_eq!(scan.vocab_size, Some(16));
        assert_eq!(scan.tokenizer_model.as_deref(), Some("gpt2"));
        assert_eq!(scan.nextn_predict_layers, Some(1));
        assert_eq!(scan.nextn_tensor_count, 2);
        assert_eq!(scan.tensor_count, 3);
        assert!(scan.mtp_capable());
    }

    #[test]
    fn plain_model_is_not_mtp_capable() {
        let scan = plain_model();
        assert_eq!(scan.vocab_size, Some(3));
        assert_eq!(scan.nextn_predict_layers, None);
        assert_eq!(scan.nextn_tensor_count, 0);
        assert!(!scan.mtp_capable());
    }

    #[test]
    fn nextn_tensors_alone_imply_mtp() {
        // A conversion that kept the NextN tensors but not the KV still reads as capable.
        let mut w = W::new(1, 1);
        w.kv_str("general.architecture", "deepseek2");
        w.tensor("blk.61.nextn.shared_head.head.weight");
        let scan = GgufScan::parse(&mut w.0.as_slice()).unwrap();
        assert!(scan.mtp_capable());
    }

    #[test]
    fn same_model_is_self_compatible() {
        let a = mtp_model(16);
        let b = mtp_model(16);
        assert!(speculator_compatible(&a, &b).is_ok());
    }

    #[test]
    fn vocab_within_tolerance_is_compatible() {
        let a = mtp_model(1000);
        let b = mtp_model(1000 + SPEC_VOCAB_MAX_SIZE_DIFFERENCE as usize);
        assert!(speculator_compatible(&a, &b).is_ok());
        let c = mtp_model(1000 + SPEC_VOCAB_MAX_SIZE_DIFFERENCE as usize + 1);
        let err = speculator_compatible(&a, &c).unwrap_err();
        assert!(err.contains("vocab sizes differ"), "{err}");
    }

    #[test]
    fn tokenizer_family_mismatch_is_rejected() {
        let err = speculator_compatible(&mtp_model(16), &plain_model()).unwrap_err();
        assert!(err.contains("tokenizer family"), "{err}");
    }

    #[test]
    fn unverifiable_metadata_is_rejected_not_assumed() {
        // A header with no tokenizer metadata at all must not read as compatible.
        let mut w = W::new(0, 1);
        w.kv_str("general.architecture", "llama");
        let bare = GgufScan::parse(&mut w.0.as_slice()).unwrap();
        let err = speculator_compatible(&mtp_model(16), &bare).unwrap_err();
        assert!(err.contains("cannot verify"), "{err}");
    }

    #[test]
    fn rejects_bad_magic_and_version() {
        assert!(GgufScan::parse(&mut &b"GGML\x03\x00\x00\x00"[..])
            .unwrap_err()
            .contains("bad magic"));
        let mut w = W::new(0, 0);
        w.0[4] = 1; // version 1
        assert!(GgufScan::parse(&mut w.0.as_slice())
            .unwrap_err()
            .contains("unsupported GGUF version"));
    }

    #[test]
    fn truncated_file_errors_cleanly() {
        let mut w = W::new(0, 2);
        w.kv_str("general.architecture", "llama");
        // Declared 2 KVs but only wrote 1 — the reader must error, not hang or panic.
        assert!(GgufScan::parse(&mut w.0.as_slice()).is_err());
    }
}
