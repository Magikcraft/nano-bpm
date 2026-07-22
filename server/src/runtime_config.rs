//! Runtime observability configuration (ADR 0035).
//!
//! Resolves the runtime-controllable observability surface — today just whether
//! the Prometheus `GET /metrics` endpoint is served — from four layers, highest
//! precedence first:
//!
//! ```text
//! CLI flag  >  config file  >  environment variable  >  built-in default
//! ```
//!
//! The gateway is otherwise configured entirely through environment variables;
//! this adds a *config file* and *flags* specifically for the observability
//! toggles operators asked to control at deploy time (e.g. turning `/metrics`
//! off on a hardened node without a rebuild). The parser is intentionally
//! lenient: unknown flags and unparseable/missing files warn and are ignored, so
//! a newer config never hard-fails an older binary and vice versa.
//!
//! The config file is YAML (to match the Kubernetes ecosystem operators deploy
//! into), read once at startup — never on any request path. Example:
//!
//! ```yaml
//! observability:
//!   metrics: "off"      # on (default) | off — the Prometheus /metrics endpoint
//! ```

/// The resolved observability configuration for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilityConfig {
    /// Serve the Prometheus `GET /metrics` endpoint. Default `true`. When
    /// `false` the route is not registered at all (it 404s), removing the
    /// endpoint from the attack surface.
    pub metrics: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self { metrics: true }
    }
}

impl ObservabilityConfig {
    /// Resolves the effective config from CLI flags, an optional config file,
    /// environment variables, and built-in defaults (in that precedence).
    pub fn resolve() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let config_path = config_path(&args);
        Self::resolve_from(
            std::env::var("NANOBPMN_METRICS").ok().as_deref(),
            config_path.as_deref().and_then(read_file_warn),
            &args,
        )
    }

    /// Pure resolver, factored out for unit testing. `env_metrics` is the raw
    /// `NANOBPMN_METRICS` value (if set); `file` is the config file's contents
    /// (if a readable file was configured); `args` are the CLI args after the
    /// program name.
    fn resolve_from(env_metrics: Option<&str>, file: Option<String>, args: &[String]) -> Self {
        let mut cfg = Self::default();

        // Layer 1 (lowest above default): environment variable.
        if let Some(v) = env_metrics.and_then(parse_onoff) {
            cfg.metrics = v;
        }

        // Layer 2: config file (overrides env).
        if let Some(text) = file.as_deref()
            && let Some(v) = metrics_from_yaml(text)
        {
            cfg.metrics = v;
        }

        // Layer 3 (highest): CLI flags (override file). Accepts both
        // `--metrics <on|off>` and the convenience `--no-metrics`.
        if let Some(v) = metrics_flag(args) {
            cfg.metrics = v;
        }

        cfg
    }
}

/// Extracts `--config <path>` (or falls back to `NANOBPMN_CONFIG`). A value
/// glued with `=` (`--config=path`) is also accepted.
fn config_path(args: &[String]) -> Option<String> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if let Some(rest) = a.strip_prefix("--config") {
            if let Some(inline) = rest.strip_prefix('=') {
                return Some(inline.to_string());
            }
            if rest.is_empty()
                && let Some(next) = it.next()
            {
                return Some(next.clone());
            }
        }
    }
    std::env::var("NANOBPMN_CONFIG")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Reads a config file, warning (not failing) if it can't be read — the file is
/// optional and lenient by design.
fn read_file_warn(path: &str) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(e) => {
            eprintln!("warning: could not read config file '{path}': {e} (ignoring)");
            None
        }
    }
}

/// Resolves the metrics toggle from CLI flags. Returns `None` if neither flag is
/// present. `--no-metrics` forces off; `--metrics <on|off>` sets explicitly. An
/// unrecognized `--metrics` value warns and is ignored.
fn metrics_flag(args: &[String]) -> Option<bool> {
    let mut result = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--no-metrics" => result = Some(false),
            "--metrics" => {
                if let Some(v) = it.next() {
                    match parse_onoff(v) {
                        Some(b) => result = Some(b),
                        None => eprintln!(
                            "warning: unrecognized --metrics value '{v}' (expected on|off)"
                        ),
                    }
                }
            }
            other => {
                if let Some(v) = other.strip_prefix("--metrics=") {
                    match parse_onoff(v) {
                        Some(b) => result = Some(b),
                        None => eprintln!(
                            "warning: unrecognized --metrics value '{v}' (expected on|off)"
                        ),
                    }
                }
            }
        }
    }
    result
}

/// Reads `observability.metrics` from a YAML config document, tolerating either a
/// YAML boolean (`metrics: false`) or a string (`metrics: "off"`). Returns `None`
/// when the key is absent or the document doesn't parse.
fn metrics_from_yaml(text: &str) -> Option<bool> {
    let doc: serde_yaml::Value = match serde_yaml::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("warning: could not parse config file as YAML: {e} (ignoring)");
            return None;
        }
    };
    let node = doc.get("observability")?.get("metrics")?;
    match node {
        serde_yaml::Value::Bool(b) => Some(*b),
        serde_yaml::Value::String(s) => parse_onoff(s),
        _ => None,
    }
}

/// Parses a permissive on/off toggle: `on/off`, `true/false`, `yes/no`, `1/0`
/// (case/whitespace-insensitive). Returns `None` for anything else.
fn parse_onoff(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" | "enabled" => Some(true),
        "off" | "false" | "no" | "0" | "disabled" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn default_is_metrics_on() {
        let cfg = ObservabilityConfig::resolve_from(None, None, &[]);
        assert!(cfg.metrics);
    }

    #[test]
    fn env_can_disable_metrics() {
        let cfg = ObservabilityConfig::resolve_from(Some("off"), None, &[]);
        assert!(!cfg.metrics);
    }

    #[test]
    fn file_overrides_env() {
        let file = "observability:\n  metrics: \"on\"\n".to_string();
        let cfg = ObservabilityConfig::resolve_from(Some("off"), Some(file), &[]);
        assert!(cfg.metrics, "file (on) must override env (off)");
    }

    #[test]
    fn flag_overrides_file_and_env() {
        let file = "observability:\n  metrics: on\n".to_string();
        let cfg =
            ObservabilityConfig::resolve_from(Some("on"), Some(file), &args(&["--metrics", "off"]));
        assert!(!cfg.metrics, "flag (off) must override file+env (on)");
    }

    #[test]
    fn no_metrics_flag_disables() {
        let cfg = ObservabilityConfig::resolve_from(None, None, &args(&["--no-metrics"]));
        assert!(!cfg.metrics);
    }

    #[test]
    fn glued_flag_value() {
        let cfg = ObservabilityConfig::resolve_from(None, None, &args(&["--metrics=off"]));
        assert!(!cfg.metrics);
    }

    #[test]
    fn yaml_bool_form() {
        assert_eq!(
            metrics_from_yaml("observability:\n  metrics: false\n"),
            Some(false)
        );
        assert_eq!(
            metrics_from_yaml("observability:\n  metrics: true\n"),
            Some(true)
        );
    }

    #[test]
    fn yaml_missing_key_is_none() {
        assert_eq!(metrics_from_yaml("other:\n  x: 1\n"), None);
        assert_eq!(
            metrics_from_yaml("observability:\n  console: studio\n"),
            None
        );
    }

    #[test]
    fn unrecognized_values_ignored() {
        // A bad env value falls through to the default (on).
        let cfg = ObservabilityConfig::resolve_from(Some("maybe"), None, &[]);
        assert!(cfg.metrics);
        // A bad flag value leaves the resolved layer unchanged (file wins).
        let file = "observability:\n  metrics: off\n".to_string();
        let cfg =
            ObservabilityConfig::resolve_from(None, Some(file), &args(&["--metrics", "banana"]));
        assert!(!cfg.metrics);
    }

    #[test]
    fn config_path_from_flag_and_glued() {
        assert_eq!(
            config_path(&args(&["--config", "/x.yaml"])),
            Some("/x.yaml".to_string())
        );
        assert_eq!(
            config_path(&args(&["--config=/y.yaml"])),
            Some("/y.yaml".to_string())
        );
    }
}
