//! **Semantic-annotation sidecar** — persistent, per-revision store of the
//! accepted `SemanticAnnotations` for a process model.
//!
//! Slice 5 of ADR 0002. Slices 1-3 infer annotations and let a human accept
//! or reject them in the Semantics panel; slice 4 lands the `nano:*`
//! extension namespace so annotations can survive a BPMN round-trip. This
//! module gives the accepted set a durable home:
//!
//! ```text
//! workspaces/<ws>/processes/<proc>/annotations.json
//! ```
//!
//! Shape: a `revisions` map keyed by the SHA-256 of the process's
//! `model.bpmn` bytes, plus a `current` pointer at the entry that matches the
//! model on disk *right now*. Historic entries stay in place so a user can
//! revert to an older model and still see the annotations they curated for
//! it.
//!
//! Revision keys are content-addressable (SHA-256 hex), so:
//! * We never need to bump a version counter by hand.
//! * Two workspaces that arrive at byte-identical models share nothing on
//!   disk, but *would* if we ever built a shared sidecar cache.
//! * Whitespace changes count as a new revision — deliberate: the semantic
//!   pass runs against exactly the bytes the engine will parse.
//!
//! Provenance carries who last wrote each entry (`inferred` for the
//! algorithmic pass, `human` for accept/reject decisions, `mixed` for a
//! save that merged both, `llm` reserved for the future semantics agent).
//! We keep the field open-ended (a `String`) rather than an enum because
//! the pipeline in slices 6-8 will grow new sources and we don't want the
//! sidecar format to gate them.
//!
//! # File format
//! ```json
//! {
//!   "revisions": {
//!     "<hex-sha256>": {
//!       "annotations": { … SemanticAnnotations … },
//!       "provenance": "human",
//!       "updatedAt": "2026-07-08T05:00:00Z"
//!     }
//!   },
//!   "current": "<hex-sha256>"
//! }
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::layout::SemanticAnnotations;

/// One entry in the sidecar: the annotations that apply at a particular
/// revision of the model, plus a little bookkeeping.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationRevision {
    /// The accepted annotations for this revision.
    pub annotations: SemanticAnnotations,
    /// Where the annotations came from. Free-form so slices 6-8 can add
    /// new sources (e.g. `"llm"`, `"telemetry"`) without a schema break.
    /// Common values today: `"inferred"`, `"human"`, `"mixed"`.
    #[serde(default = "default_provenance")]
    pub provenance: String,
    /// RFC-3339 UTC timestamp of the last write. Written by the server;
    /// clients should not set this.
    #[serde(default)]
    pub updated_at: String,
}

fn default_provenance() -> String {
    "inferred".to_string()
}

/// The full sidecar document.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationsSidecar {
    /// One entry per revision (SHA-256 hex of `model.bpmn` bytes).
    #[serde(default)]
    pub revisions: BTreeMap<String, AnnotationRevision>,
    /// The revision that matches the *current* `model.bpmn`. When the
    /// model is rewritten, callers should update this pointer to the new
    /// hash (creating an entry lazily if none existed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
}

impl AnnotationsSidecar {
    /// Look up the entry that matches a revision key. Returns `None` when
    /// the revision has never been annotated (fresh model, or the model
    /// changed and no one has re-annotated yet).
    pub fn revision(&self, key: &str) -> Option<&AnnotationRevision> {
        self.revisions.get(key)
    }

    /// Write (or replace) an entry at `revision`, and point `current` at
    /// it. `updated_at` is stamped by this method so the sidecar's clock
    /// stays server-side.
    pub fn upsert(
        &mut self,
        revision: &str,
        annotations: SemanticAnnotations,
        provenance: impl Into<String>,
    ) {
        self.revisions.insert(
            revision.to_string(),
            AnnotationRevision {
                annotations,
                provenance: provenance.into(),
                updated_at: now_utc_iso(),
            },
        );
        self.current = Some(revision.to_string());
    }
}

/// Compute the content-addressable revision key for a BPMN model.
///
/// SHA-256 is overkill for collision resistance at this scale (dozens of
/// revisions per process), but its determinism across platforms and Rust
/// versions is the property we actually care about — a `DefaultHasher`
/// digest is *not* guaranteed stable across releases.
pub fn revision_key(model_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(model_bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut out, "{byte:02x}").expect("write to String");
    }
    out
}

fn now_utc_iso() -> String {
    // A tiny RFC-3339 formatter so we don't pull in `chrono` for one field.
    // Millisecond precision is plenty for a revision timestamp.
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = dur.as_secs();
    let millis = dur.subsec_millis();

    // Days since 1970-01-01, then civil calendar via Howard Hinnant's algorithm.
    let days = (total_secs / 86_400) as i64;
    let secs_of_day = (total_secs % 86_400) as u32;
    let (hh, mm, ss) = (
        secs_of_day / 3600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60,
    );
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{millis:03}Z")
}

/// Howard Hinnant's `civil_from_days` (public-domain), inlined so we don't
/// need a date crate for one timestamp field. Given days since 1970-01-01,
/// returns `(year, month, day)`.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u32; // [0..146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0..399]
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0..365]
    let mp = (5 * doy + 2) / 153; // [0..11]
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_key_is_stable_and_content_addressable() {
        let a = revision_key(b"<bpmn:definitions/>");
        let b = revision_key(b"<bpmn:definitions/>");
        let c = revision_key(b"<bpmn:definitions></bpmn:definitions>");
        assert_eq!(a, b, "identical bytes must hash the same");
        assert_ne!(a, c, "different bytes must hash differently");
        assert_eq!(a.len(), 64, "sha256 hex is 64 chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()), "hex only: {a}");
    }

    #[test]
    fn sidecar_upsert_records_provenance_and_updates_current() {
        let mut side = AnnotationsSidecar::default();
        assert!(side.current.is_none());
        assert!(side.revision("abc").is_none());

        side.upsert("abc", SemanticAnnotations::default(), "human");
        assert_eq!(side.current.as_deref(), Some("abc"));
        let entry = side.revision("abc").expect("just inserted");
        assert_eq!(entry.provenance, "human");
        assert!(
            !entry.updated_at.is_empty(),
            "server should stamp updatedAt"
        );

        side.upsert("def", SemanticAnnotations::default(), "inferred");
        assert_eq!(side.current.as_deref(), Some("def"));
        assert!(
            side.revision("abc").is_some(),
            "historic entry stays put after a newer revision arrives"
        );
    }

    #[test]
    fn sidecar_round_trips_through_json_with_two_revisions() {
        let mut side = AnnotationsSidecar::default();
        side.upsert("11", SemanticAnnotations::default(), "inferred");
        side.upsert("22", SemanticAnnotations::default(), "human");

        let json = serde_json::to_string(&side).unwrap();
        let round: AnnotationsSidecar = serde_json::from_str(&json).unwrap();
        assert_eq!(round.current.as_deref(), Some("22"));
        assert_eq!(round.revisions.len(), 2);
        assert_eq!(
            round.revision("11").map(|e| e.provenance.clone()),
            Some("inferred".into())
        );
        assert_eq!(
            round.revision("22").map(|e| e.provenance.clone()),
            Some("human".into())
        );
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        // 1970-01-01 → day 0
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2000-03-01 → day 11017 (leap-day-skip landmark for Hinnant's algorithm)
        assert_eq!(civil_from_days(11017), (2000, 3, 1));
        // 2026-07-08 → day 20642
        assert_eq!(civil_from_days(20642), (2026, 7, 8));
    }

    #[test]
    fn sidecar_round_trips_cost_and_time_annotations() {
        // Slice 6: the Semantics Workbench authors per-element Cost and Time
        // objects. Persisting them through the sidecar is exactly the shape
        // the workbench sends over the wire, so this test doubles as a
        // contract check for the /annotations endpoint payload.
        use crate::layout::{Cost, Time};
        let mut ann = SemanticAnnotations::default();
        ann.costs.insert(
            "Task_Review".into(),
            Cost {
                value: 0.42,
                currency: Some("USD".into()),
                per: Some("invocation".into()),
            },
        );
        ann.times.insert(
            "Task_Review".into(),
            Time {
                p50_ms: Some(1200),
                p99_ms: Some(4800),
                source: Some("telemetry".into()),
            },
        );
        let mut side = AnnotationsSidecar::default();
        side.upsert("rev1", ann, "human");

        let json = serde_json::to_string(&side).unwrap();
        let round: AnnotationsSidecar = serde_json::from_str(&json).unwrap();
        let entry = round.revision("rev1").expect("revision persisted");
        let cost = entry.annotations.costs.get("Task_Review").unwrap();
        assert_eq!(cost.value, 0.42);
        assert_eq!(cost.currency.as_deref(), Some("USD"));
        assert_eq!(cost.per.as_deref(), Some("invocation"));
        let time = entry.annotations.times.get("Task_Review").unwrap();
        assert_eq!(time.p50_ms, Some(1200));
        assert_eq!(time.p99_ms, Some(4800));
        assert_eq!(time.source.as_deref(), Some("telemetry"));
    }

    #[test]
    fn sidecar_parses_pre_slice6_documents_missing_cost_and_time() {
        // Sidecars written by slice 5 don't have `costs`/`times` fields. The
        // schema uses `#[serde(default, skip_serializing_if = "…is_empty")]`
        // so old documents must load cleanly with empty maps.
        let old = r#"{"revisions":{"r1":{"annotations":{"flows":[],"clusters":[],"roles":{}},"provenance":"human","updatedAt":"2026-07-08T00:00:00.000Z"}},"current":"r1"}"#;
        let side: AnnotationsSidecar = serde_json::from_str(old).expect("legacy sidecar loads");
        let entry = side.revision("r1").unwrap();
        assert!(entry.annotations.costs.is_empty());
        assert!(entry.annotations.times.is_empty());
    }
}
