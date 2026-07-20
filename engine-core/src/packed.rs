//! Opt-in msgpack codec for the `variables` payload.
//!
//! The create/job `variables` map (`HashMap<String, Value>`) is the largest and
//! most serialization-heavy part of a command. In a Raft cluster every replica
//! decodes it, and the whole payload dominates per-create CPU (see the
//! throughput-ceiling analysis: 50 KB payloads cap creates at ~11.8k/s, ~4x
//! lower than 1 KB). Zeebe sidesteps this by keeping variables as an **opaque
//! msgpack document** in the log and only decoding the specific variables a FEEL
//! expression actually reads.
//!
//! This module provides that binary encoding for nanobpmn. It is deliberately
//! gated behind the `msgpack` feature so the default (mobile/wasm) engine build
//! stays dependency-free; only hosts that carry packed payloads (the server)
//! enable it.
//!
//! JSON remains the compatibility default across the wire and the Raft log; a
//! packed payload is an opt-in fast path that a client encodes once and the
//! cluster carries verbatim.

use std::collections::HashMap;

use crate::model::Value;

/// Encode a variables map into a compact, self-describing msgpack document.
///
/// The encoding is the natural `rmp-serde` representation of
/// `HashMap<String, Value>`: a msgpack map of string keys to `Value` nodes.
/// It round-trips losslessly through [`unpack_variables`].
pub fn pack_variables(variables: &HashMap<String, Value>) -> Vec<u8> {
    // `to_vec_named` keeps the `Value` enum variants tagged by name (rather than
    // by positional index), so the encoding is stable across reorderings of the
    // `Value` enum and legible to non-Rust encoders that follow the same shape.
    rmp_serde::to_vec_named(variables)
        .expect("Value/HashMap always msgpack-encodes (no non-string keys, no NaN)")
}

/// Decode a msgpack variables document produced by [`pack_variables`] (or a
/// conformant external encoder) back into a variables map.
pub fn unpack_variables(bytes: &[u8]) -> Result<HashMap<String, Value>, PackError> {
    rmp_serde::from_slice(bytes).map_err(|e| PackError(e.to_string()))
}

/// A msgpack decode failure: a malformed or non-conformant packed payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackError(pub String);

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed packed variables: {}", self.0)
    }
}

impl std::error::Error for PackError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("name".into(), Value::Str("north-wind".into()));
        m.insert("amount".into(), Value::Int(42));
        m.insert("ratio".into(), Value::Double(1.5));
        m.insert("active".into(), Value::Bool(true));
        m.insert("missing".into(), Value::Null);
        m.insert(
            "items".into(),
            Value::List(vec![Value::Int(1), Value::Str("two".into())]),
        );
        let mut nested = std::collections::BTreeMap::new();
        nested.insert("k".into(), Value::Str("v".into()));
        m.insert("ctx".into(), Value::Map(nested));
        m
    }

    #[test]
    fn round_trips() {
        let vars = sample();
        let packed = pack_variables(&vars);
        let back = unpack_variables(&packed).expect("decodes");
        assert_eq!(vars, back);
    }

    #[test]
    fn empty_round_trips() {
        let vars: HashMap<String, Value> = HashMap::new();
        let packed = pack_variables(&vars);
        assert_eq!(unpack_variables(&packed).unwrap(), vars);
    }

    #[test]
    fn msgpack_is_smaller_than_json() {
        // A large string payload: msgpack should be no larger than JSON and
        // avoids per-field escaping/parsing cost. This is the whole point.
        let mut vars = HashMap::new();
        vars.insert("blob".into(), Value::Str("x".repeat(50_000)));
        let packed = pack_variables(&vars);
        let json = serde_json::to_vec(&vars).unwrap();
        assert!(
            packed.len() <= json.len(),
            "packed {} json {}",
            packed.len(),
            json.len()
        );
        assert_eq!(unpack_variables(&packed).unwrap(), vars);
    }

    #[test]
    fn rejects_garbage() {
        assert!(unpack_variables(&[0xff, 0x00, 0x13, 0x37]).is_err());
    }
}
