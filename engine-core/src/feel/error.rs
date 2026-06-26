//! The FEEL parse/evaluation error type.

/// A FEEL parse or evaluation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeelError(pub String);

impl std::fmt::Display for FeelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FEEL error: {}", self.0)
    }
}

impl std::error::Error for FeelError {}
