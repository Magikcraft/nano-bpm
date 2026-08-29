//! Stamps the gateway binary's reported version. The derivation lives in the
//! shared [`nano_version_stamp`] build helper (ADR 0064) so the binary and the
//! extracted `nano-server-console` crate stamp `NANOBPM_VERSION` from the same
//! single source of truth and can never drift.

fn main() {
    nano_version_stamp::emit();
}
