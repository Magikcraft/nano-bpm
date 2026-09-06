//! Canonical lease issuance and persistence-only legacy decoding.

use crate::state::Key;

/// Issuance uses the replicated key allocator; public callers compare opaque strings only.
pub(crate) fn issue(key: Key) -> String {
    format!("nano-lease:{key:016x}")
}

/// Recover the allocator high-water mark from a persisted activation.
pub(crate) fn issued_key(token: &str) -> Key {
    token
        .strip_prefix("nano-lease:")
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .unwrap_or(0)
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PersistedToken {
    String(String),
    Legacy(u64),
}

#[cfg(feature = "serde")]
impl PersistedToken {
    fn into_string(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Legacy(0) => String::new(),
            Self::Legacy(key) => issue(key),
        }
    }
}

/// Only persisted state/event fields use this decoder; commands require strings.
#[cfg(feature = "serde")]
pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    use serde::Deserialize;
    PersistedToken::deserialize(deserializer).map(PersistedToken::into_string)
}

#[cfg(feature = "serde")]
pub(crate) fn deserialize_optional<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    use serde::Deserialize;
    Option::<PersistedToken>::deserialize(deserializer).map(|value| {
        value
            .map(PersistedToken::into_string)
            .filter(|value| !value.is_empty())
    })
}
