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
//!   console: "observe"  # studio (default) | observe (read-only) | off (headless)
//!   consolePeers:       # non-empty => run as a standalone off-cluster console
//!     - "http://10.0.0.11:8080"
//! ```

/// How the embedded web console is served at runtime (ADR 0035 §C). Independent
/// of the ADR 0034 *build* profile: this is a thin runtime gate over whatever
/// console bundle was compiled in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsoleProfile {
    /// Headless: the console router is not mounted at all (SPA + `/console/api/*`
    /// and the marketing/docs pages all 404).
    Off,
    /// Operator subset: the observability views are served, but authoring
    /// (mutating) API requests are refused — a runtime read-only gate.
    Observe,
    /// Full behaviour: the complete studio IDE + authoring API (the default).
    #[default]
    Studio,
}

/// The resolved observability configuration for this process.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// Serve the Prometheus `GET /metrics` endpoint. Default `true`. When
    /// `false` the route is not registered at all (it 404s), removing the
    /// endpoint from the attack surface.
    pub metrics: bool,
    /// How the embedded web console is served (off | observe | studio). Default
    /// `studio`.
    #[cfg_attr(not(feature = "console"), allow(dead_code))]
    pub console: ConsoleProfile,
    /// Peer base URLs for the standalone off-cluster console (ADR 0035 §B). When
    /// non-empty, the process runs as an engine-less console that scrapes these
    /// peers instead of serving the engine. Empty = normal (engine) mode.
    #[cfg_attr(not(feature = "console"), allow(dead_code))]
    pub standalone_console_peers: Vec<String>,
}

impl ObservabilityConfig {
    /// Whether this process should run as a standalone off-cluster console.
    #[cfg_attr(not(feature = "console"), allow(dead_code))]
    pub fn is_standalone_console(&self) -> bool {
        !self.standalone_console_peers.is_empty()
    }

    /// Whether the console router should be mounted at all (`off` => headless).
    #[cfg_attr(not(feature = "console"), allow(dead_code))]
    pub fn console_enabled(&self) -> bool {
        self.console != ConsoleProfile::Off
    }

    /// Whether the console is in the read-only `observe` gate (authoring API
    /// requests must be refused).
    #[cfg_attr(not(feature = "console"), allow(dead_code))]
    pub fn console_read_only(&self) -> bool {
        self.console == ConsoleProfile::Observe
    }
}

impl ObservabilityConfig {
    /// The default: metrics on, console in full studio mode, engine
    /// (non-standalone) mode.
    fn defaults() -> Self {
        Self {
            metrics: true,
            console: ConsoleProfile::default(),
            standalone_console_peers: Vec::new(),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self::defaults()
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
            std::env::var("NANOBPMN_CONSOLE").ok().as_deref(),
            std::env::var("NANOBPMN_CONSOLE_STANDALONE").ok().as_deref(),
            config_path.as_deref().and_then(read_file_warn),
            &args,
        )
    }

    /// Pure resolver, factored out for unit testing. `env_metrics` /
    /// `env_console` / `env_peers` are the raw `NANOBPMN_METRICS` /
    /// `NANOBPMN_CONSOLE` / `NANOBPMN_CONSOLE_STANDALONE` values (if set); `file`
    /// is the config file's contents (if a readable file was configured); `args`
    /// are the CLI args after the program name.
    fn resolve_from(
        env_metrics: Option<&str>,
        env_console: Option<&str>,
        env_peers: Option<&str>,
        file: Option<String>,
        args: &[String],
    ) -> Self {
        let mut cfg = Self::default();

        // --- metrics toggle (flag > file > env > default) -------------------
        if let Some(v) = env_metrics.and_then(parse_onoff) {
            cfg.metrics = v;
        }
        if let Some(text) = file.as_deref()
            && let Some(v) = metrics_from_yaml(text)
        {
            cfg.metrics = v;
        }
        // Accepts both `--metrics <on|off>` and the convenience `--no-metrics`.
        if let Some(v) = metrics_flag(args) {
            cfg.metrics = v;
        }

        // --- console profile (flag > file > env > default) ------------------
        if let Some(p) = env_console.and_then(parse_console_profile) {
            cfg.console = p;
        }
        if let Some(text) = file.as_deref()
            && let Some(p) = console_from_yaml(text)
        {
            cfg.console = p;
        }
        if let Some(p) = console_flag(args) {
            cfg.console = p;
        }

        // --- standalone console peers (flag > file > env > default) ---------
        if let Some(p) = env_peers.map(parse_csv_urls).filter(|p| !p.is_empty()) {
            cfg.standalone_console_peers = p;
        }
        if let Some(text) = file.as_deref()
            && let Some(p) = peers_from_yaml(text).filter(|p| !p.is_empty())
        {
            cfg.standalone_console_peers = p;
        }
        if let Some(p) = peers_flag(args).filter(|p| !p.is_empty()) {
            cfg.standalone_console_peers = p;
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

/// Parses a console profile: `off` (aliases `none`, `headless`, `disabled`),
/// `observe` (aliases `observer`, `operator`, `readonly`, `read-only`), or
/// `studio` (aliases `full`, `ide`, `on`). Case/whitespace-insensitive. Returns
/// `None` for anything else.
fn parse_console_profile(v: &str) -> Option<ConsoleProfile> {
    match v.trim().to_ascii_lowercase().as_str() {
        "off" | "none" | "headless" | "disabled" | "false" | "no" => Some(ConsoleProfile::Off),
        "observe" | "observer" | "operator" | "readonly" | "read-only" => {
            Some(ConsoleProfile::Observe)
        }
        "studio" | "full" | "ide" | "on" | "true" | "yes" => Some(ConsoleProfile::Studio),
        _ => None,
    }
}

/// Resolves the console profile from CLI flags: `--console <off|observe|studio>`
/// (also glued with `=`). An unrecognized value warns and is ignored. Returns
/// `None` if the flag is absent.
fn console_flag(args: &[String]) -> Option<ConsoleProfile> {
    let mut result = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let raw = match a.as_str() {
            "--console" => it.next().map(String::as_str),
            other => other.strip_prefix("--console="),
        };
        if let Some(v) = raw {
            match parse_console_profile(v) {
                Some(p) => result = Some(p),
                None => eprintln!(
                    "warning: unrecognized --console value '{v}' (expected off|observe|studio)"
                ),
            }
        }
    }
    result
}

/// Reads `observability.console` (a string) from a YAML config document. Returns
/// `None` when the key is absent or the document doesn't parse.
fn console_from_yaml(text: &str) -> Option<ConsoleProfile> {
    let doc: serde_yaml::Value = serde_yaml::from_str(text).ok()?;
    let node = doc.get("observability")?.get("console")?;
    node.as_str().and_then(parse_console_profile)
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

/// Splits a comma- or whitespace-separated list of peer base URLs, trimming
/// blanks and trailing slashes. `"http://a:8080, http://b:8080"` → two entries.
fn parse_csv_urls(v: &str) -> Vec<String> {
    v.split([',', ' ', '\t', '\n'])
        .map(|s| s.trim().trim_end_matches('/'))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Resolves the standalone-console peer list from CLI flags: repeated
/// `--console-peer <url>` and/or `--console-standalone <csv>` (also glued with
/// `=`). Returns `None` if no such flag is present.
fn peers_flag(args: &[String]) -> Option<Vec<String>> {
    let mut peers: Vec<String> = Vec::new();
    let mut seen = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let value = match a.as_str() {
            "--console-peer" | "--console-standalone" => it.next().map(String::as_str),
            other => other
                .strip_prefix("--console-peer=")
                .or_else(|| other.strip_prefix("--console-standalone=")),
        };
        if let Some(v) = value {
            seen = true;
            peers.extend(parse_csv_urls(v));
        }
    }
    seen.then_some(peers)
}

/// Reads `observability.consolePeers` (a YAML sequence) or `consoleStandalone`
/// (a scalar CSV) from a config document. Returns `None` when neither is present.
fn peers_from_yaml(text: &str) -> Option<Vec<String>> {
    let doc: serde_yaml::Value = serde_yaml::from_str(text).ok()?;
    let obs = doc.get("observability")?;
    if let Some(serde_yaml::Value::Sequence(seq)) = obs.get("consolePeers") {
        return Some(
            seq.iter()
                .filter_map(|v| v.as_str())
                .flat_map(parse_csv_urls)
                .collect(),
        );
    }
    if let Some(serde_yaml::Value::String(s)) = obs.get("consoleStandalone") {
        return Some(parse_csv_urls(s));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn default_is_metrics_on() {
        let cfg = ObservabilityConfig::resolve_from(None, None, None, None, &[]);
        assert!(cfg.metrics);
        assert!(!cfg.is_standalone_console());
        assert_eq!(cfg.console, ConsoleProfile::Studio);
        assert!(cfg.console_enabled());
        assert!(!cfg.console_read_only());
    }

    #[test]
    fn env_can_disable_metrics() {
        let cfg = ObservabilityConfig::resolve_from(Some("off"), None, None, None, &[]);
        assert!(!cfg.metrics);
    }

    #[test]
    fn file_overrides_env() {
        let file = "observability:\n  metrics: \"on\"\n".to_string();
        let cfg = ObservabilityConfig::resolve_from(Some("off"), None, None, Some(file), &[]);
        assert!(cfg.metrics, "file (on) must override env (off)");
    }

    #[test]
    fn flag_overrides_file_and_env() {
        let file = "observability:\n  metrics: on\n".to_string();
        let cfg = ObservabilityConfig::resolve_from(
            Some("on"),
            None,
            None,
            Some(file),
            &args(&["--metrics", "off"]),
        );
        assert!(!cfg.metrics, "flag (off) must override file+env (on)");
    }

    #[test]
    fn no_metrics_flag_disables() {
        let cfg =
            ObservabilityConfig::resolve_from(None, None, None, None, &args(&["--no-metrics"]));
        assert!(!cfg.metrics);
    }

    #[test]
    fn glued_flag_value() {
        let cfg =
            ObservabilityConfig::resolve_from(None, None, None, None, &args(&["--metrics=off"]));
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
        let cfg = ObservabilityConfig::resolve_from(Some("maybe"), None, None, None, &[]);
        assert!(cfg.metrics);
        // A bad flag value leaves the resolved layer unchanged (file wins).
        let file = "observability:\n  metrics: off\n".to_string();
        let cfg = ObservabilityConfig::resolve_from(
            None,
            None,
            None,
            Some(file),
            &args(&["--metrics", "banana"]),
        );
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

    // --- console profile ----------------------------------------------------

    #[test]
    fn console_profile_parses_aliases() {
        assert_eq!(parse_console_profile("off"), Some(ConsoleProfile::Off));
        assert_eq!(parse_console_profile("HEADLESS"), Some(ConsoleProfile::Off));
        assert_eq!(
            parse_console_profile(" observe "),
            Some(ConsoleProfile::Observe)
        );
        assert_eq!(
            parse_console_profile("read-only"),
            Some(ConsoleProfile::Observe)
        );
        assert_eq!(
            parse_console_profile("studio"),
            Some(ConsoleProfile::Studio)
        );
        assert_eq!(parse_console_profile("banana"), None);
    }

    #[test]
    fn console_profile_env() {
        let cfg = ObservabilityConfig::resolve_from(None, Some("observe"), None, None, &[]);
        assert_eq!(cfg.console, ConsoleProfile::Observe);
        assert!(cfg.console_enabled());
        assert!(cfg.console_read_only());
    }

    #[test]
    fn console_profile_off_disables() {
        let cfg = ObservabilityConfig::resolve_from(None, Some("off"), None, None, &[]);
        assert_eq!(cfg.console, ConsoleProfile::Off);
        assert!(!cfg.console_enabled());
    }

    #[test]
    fn console_profile_precedence() {
        let file = "observability:\n  console: observe\n".to_string();
        // env only.
        let env_only = ObservabilityConfig::resolve_from(None, Some("off"), None, None, &[]);
        assert_eq!(env_only.console, ConsoleProfile::Off);
        // file overrides env.
        let file_over =
            ObservabilityConfig::resolve_from(None, Some("off"), None, Some(file.clone()), &[]);
        assert_eq!(file_over.console, ConsoleProfile::Observe);
        // flag overrides file + env.
        let flag_over = ObservabilityConfig::resolve_from(
            None,
            Some("off"),
            None,
            Some(file),
            &args(&["--console", "studio"]),
        );
        assert_eq!(flag_over.console, ConsoleProfile::Studio);
    }

    #[test]
    fn console_flag_glued_and_bad_value() {
        let cfg =
            ObservabilityConfig::resolve_from(None, None, None, None, &args(&["--console=off"]));
        assert_eq!(cfg.console, ConsoleProfile::Off);
        // Bad flag value is ignored -> default studio.
        let cfg = ObservabilityConfig::resolve_from(
            None,
            None,
            None,
            None,
            &args(&["--console", "banana"]),
        );
        assert_eq!(cfg.console, ConsoleProfile::Studio);
    }

    // --- standalone console peers -------------------------------------------

    #[test]
    fn standalone_peers_from_env_csv() {
        let cfg = ObservabilityConfig::resolve_from(
            None,
            None,
            Some("http://a:8080, http://b:8080/"),
            None,
            &[],
        );
        assert!(cfg.is_standalone_console());
        assert_eq!(
            cfg.standalone_console_peers,
            vec!["http://a:8080".to_string(), "http://b:8080".to_string()],
            "CSV split + trailing slash trimmed"
        );
    }

    #[test]
    fn standalone_peers_from_yaml_sequence() {
        let file = "observability:\n  consolePeers:\n    - http://a:8080\n    - http://b:8080\n"
            .to_string();
        let cfg = ObservabilityConfig::resolve_from(None, None, None, Some(file), &[]);
        assert_eq!(cfg.standalone_console_peers.len(), 2);
    }

    #[test]
    fn standalone_peers_precedence() {
        let file = "observability:\n  consolePeers:\n    - http://file:8080\n".to_string();
        // env only.
        let env_only =
            ObservabilityConfig::resolve_from(None, None, Some("http://env:8080"), None, &[]);
        assert_eq!(env_only.standalone_console_peers, vec!["http://env:8080"]);
        // file overrides env.
        let file_over = ObservabilityConfig::resolve_from(
            None,
            None,
            Some("http://env:8080"),
            Some(file.clone()),
            &[],
        );
        assert_eq!(file_over.standalone_console_peers, vec!["http://file:8080"]);
        // flag overrides file + env.
        let flag_over = ObservabilityConfig::resolve_from(
            None,
            None,
            Some("http://env:8080"),
            Some(file),
            &args(&["--console-peer", "http://flag:8080"]),
        );
        assert_eq!(flag_over.standalone_console_peers, vec!["http://flag:8080"]);
    }
}
