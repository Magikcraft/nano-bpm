//! **Python escape hatch** — run model-authored analysis code over the dataset's
//! flattened tables, for hypotheses the read-only SQL tool can't express (distribution
//! fitting, changepoint/seasonal decomposition, clustering).
//!
//! This deliberately executes **arbitrary code** — it is a *trusted-operator* tool, not
//! a security boundary — with pragmatic rails: a private temp workdir, a wall-clock
//! timeout (the child is killed if it overruns), and an output-size cap. It is **off by
//! default** and only offered to the model when an investigation explicitly enables it.
//!
//! The data is handed over as CSV (DuckDB writes it natively, no `pyarrow` needed) and a
//! **dependency-tolerant preamble** exposes the three tables as `pandas` DataFrames and a
//! `duckdb` connection when those libraries are present, degrading to stdlib `csv`
//! row-dicts otherwise — so the tool is useful even on a bare interpreter.

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Configuration for the Python runner.
#[derive(Debug, Clone)]
pub struct PyConfig {
    /// Interpreter to invoke. Point this at a venv with `pandas`/`duckdb`/`scipy` for
    /// full power; a bare `python3` still works (stdlib fallback).
    pub python_bin: String,
    /// Wall-clock budget; the child is killed past it.
    pub timeout: Duration,
    /// Max characters of combined stdout+stderr returned to the model.
    pub max_output: usize,
}

impl Default for PyConfig {
    fn default() -> Self {
        Self {
            python_bin: "python3".to_string(),
            timeout: Duration::from_secs(20),
            max_output: 8_000,
        }
    }
}

impl PyConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        let python_bin = std::env::var("PROCESSOS_PYTHON")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(d.python_bin);
        let timeout = std::env::var("PROCESSOS_PYTHON_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.timeout);
        let max_output = std::env::var("PROCESSOS_PYTHON_MAX_OUTPUT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(d.max_output);
        Self {
            python_bin,
            timeout,
            max_output,
        }
    }
}

/// The instructions handed to the model about the Python environment it gets.
pub const PYTHON_TOOL_DOC: &str = "\
Run a short Python 3 script over the dataset (for analysis the SQL tool can't express: \
distribution fitting, changepoint/seasonal decomposition, clustering). A preamble runs \
first and gives you three tables loaded from CSV:\n\
  jobs, instances, incidents  — pandas DataFrames if HAVE_PANDAS else lists of dict rows\n\
  con                         — a duckdb connection over the same tables if HAVE_DUCKDB else None\n\
Same columns as the SQL schema (jobs has queue_ms/service_ms/job_type/hour/dow/is_weekend, \
etc.). You MUST print() your findings — only stdout/stderr come back. No network. There is \
a wall-clock timeout. Prefer pandas/numpy/scipy/duckdb when available; the preamble sets \
HAVE_PANDAS / HAVE_DUCKDB flags so you can branch.";

/// Build the preamble that loads the CSVs (rich if libs are present, stdlib otherwise).
fn preamble() -> &'static str {
    r#"import os, sys, csv, json, math, statistics
def _load_csv(name):
    with open(name + '.csv', newline='') as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k, v in list(r.items()):
            if v == '':
                r[k] = None
            else:
                try:
                    r[k] = int(v)
                except ValueError:
                    try:
                        r[k] = float(v)
                    except ValueError:
                        pass
    return rows
try:
    import pandas as pd
    jobs = pd.read_csv('jobs.csv'); instances = pd.read_csv('instances.csv'); incidents = pd.read_csv('incidents.csv')
    HAVE_PANDAS = True
except Exception:
    jobs = _load_csv('jobs'); instances = _load_csv('instances'); incidents = _load_csv('incidents')
    HAVE_PANDAS = False
try:
    import duckdb
    con = duckdb.connect()
    for _t in ('jobs','instances','incidents'):
        con.execute("CREATE TABLE " + _t + " AS SELECT * FROM read_csv_auto('" + _t + ".csv')")
    HAVE_DUCKDB = True
except Exception:
    con = None; HAVE_DUCKDB = False
# ---- model code below ----
"#
}

/// Run `code` against the CSVs in `data_dir`, returning combined (capped) output.
///
/// `data_dir` must already contain `{jobs,instances,incidents}.csv`. The harness script
/// and scratch run in that directory (it is the model's cwd).
pub fn run_python(cfg: &PyConfig, data_dir: &Path, code: &str) -> Result<String, String> {
    let harness_path = data_dir.join("_harness.py");
    {
        let mut f =
            File::create(&harness_path).map_err(|e| format!("write harness: {e}"))?;
        f.write_all(preamble().as_bytes())
            .and_then(|_| f.write_all(code.as_bytes()))
            .map_err(|e| format!("write harness: {e}"))?;
    }

    let out_path = data_dir.join("_stdout.txt");
    let err_path = data_dir.join("_stderr.txt");
    let stdout = File::create(&out_path).map_err(|e| format!("stdout file: {e}"))?;
    let stderr = File::create(&err_path).map_err(|e| format!("stderr file: {e}"))?;

    let mut child = Command::new(&cfg.python_bin)
        .arg("_harness.py")
        .current_dir(data_dir)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .map_err(|e| {
            format!(
                "failed to start python ('{}'): {e}. Set PROCESSOS_PYTHON to a usable interpreter.",
                cfg.python_bin
            )
        })?;

    let deadline = Instant::now() + cfg.timeout;
    let timed_out = loop {
        match child.try_wait() {
            Ok(Some(_status)) => break false,
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break true;
                }
                std::thread::sleep(Duration::from_millis(40));
            }
            Err(e) => return Err(format!("waiting on python: {e}")),
        }
    };

    let out = std::fs::read_to_string(&out_path).unwrap_or_default();
    let err = std::fs::read_to_string(&err_path).unwrap_or_default();

    let mut combined = String::new();
    if !out.trim().is_empty() {
        combined.push_str(&out);
    }
    if !err.trim().is_empty() {
        if !combined.is_empty() {
            combined.push('\n');
        }
        combined.push_str("[stderr]\n");
        combined.push_str(&err);
    }
    if timed_out {
        combined.push_str(&format!(
            "\n[timed out after {}s — process killed]",
            cfg.timeout.as_secs()
        ));
    }
    if combined.trim().is_empty() {
        combined.push_str("[no output — remember to print() your findings]");
    }

    Ok(cap(combined, cfg.max_output))
}

fn cap(mut s: String, max: usize) -> String {
    if s.len() > max {
        s.truncate(max);
        s.push_str("\n…[output truncated]");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python_available() -> bool {
        Command::new("python3")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn write_csv(dir: &Path, name: &str, body: &str) {
        std::fs::write(dir.join(format!("{name}.csv")), body).unwrap();
    }

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("pyrunner-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn runs_stdlib_code_and_captures_stdout() {
        if !python_available() {
            return;
        }
        let dir = tmpdir("stdout");
        write_csv(&dir, "jobs", "job_type,queue_ms\ncredit-check,100\ncredit-check,900\n");
        write_csv(&dir, "instances", "instance_key\n1\n");
        write_csv(&dir, "incidents", "kind\n");
        let cfg = PyConfig::default();
        let out = run_python(
            &cfg,
            &dir,
            "vals = [int(r['queue_ms']) for r in jobs] if not HAVE_PANDAS else list(jobs['queue_ms'])\n\
             print('rows', len(jobs)); print('max', max(vals))\n",
        )
        .expect("run");
        assert!(out.contains("rows 2"), "got: {out}");
        assert!(out.contains("max 900"), "got: {out}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn surfaces_python_errors_on_stderr() {
        if !python_available() {
            return;
        }
        let dir = tmpdir("err");
        write_csv(&dir, "jobs", "job_type\n");
        write_csv(&dir, "instances", "k\n");
        write_csv(&dir, "incidents", "k\n");
        let out = run_python(&PyConfig::default(), &dir, "raise ValueError('boom')\n")
            .expect("run returns Ok with stderr");
        assert!(out.contains("ValueError") && out.contains("boom"), "got: {out}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn enforces_timeout() {
        if !python_available() {
            return;
        }
        let dir = tmpdir("timeout");
        write_csv(&dir, "jobs", "job_type\n");
        write_csv(&dir, "instances", "k\n");
        write_csv(&dir, "incidents", "k\n");
        let cfg = PyConfig {
            timeout: Duration::from_millis(300),
            ..PyConfig::default()
        };
        let start = Instant::now();
        let out = run_python(&cfg, &dir, "import time\ntime.sleep(10)\n").expect("run");
        assert!(out.contains("timed out"), "got: {out}");
        assert!(start.elapsed() < Duration::from_secs(5), "should not wait the full sleep");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_interpreter_is_a_clear_error() {
        let dir = tmpdir("nopy");
        write_csv(&dir, "jobs", "job_type\n");
        write_csv(&dir, "instances", "k\n");
        write_csv(&dir, "incidents", "k\n");
        let cfg = PyConfig {
            python_bin: "definitely-not-a-real-python-xyz".into(),
            ..PyConfig::default()
        };
        let err = run_python(&cfg, &dir, "print(1)").unwrap_err();
        assert!(err.contains("PROCESSOS_PYTHON"), "got: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
