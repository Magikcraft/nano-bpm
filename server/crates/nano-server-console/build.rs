//! Stamps the console crate's reported gateway version via the shared
//! [`nano_version_stamp`] helper, so `env!("NANOBPM_VERSION")` here derives from
//! the same source of truth as the gateway binary (ADR 0064) — no drift.

fn main() {
    nano_version_stamp::emit();
}
