#[allow(clippy::module_inception)]
mod config;
mod error;

#[cfg(test)]
mod config_test;

pub use config::Config;
pub(crate) use config::RuntimeConfig;
pub use config::set_snapshot_logs_multiplier_permille;
pub use config::SnapshotPolicy;
pub use error::ConfigError;
