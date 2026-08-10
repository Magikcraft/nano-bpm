# Noise filter for openapi-generator-cli output.
#
# The rust-axum generator is extremely chatty: on our spec it emits ~2400 log
# lines, of which ~2350 are the *same* two benign, generator-internal shapes:
#
#   [main] WARN  ...AbstractRustCodegen - <ident> cannot be used as a
#                <field/variable|model|parameter> name. Renamed to <ident>
#   [main] INFO  ...TemplateManager - writing file <path>   (per-file progress)
#
# plus the donation banner. Those are expected and auto-handled; they bury the
# ~25 lines that actually matter (real WARNs, generator notes, our own summary).
#
# This filter HIDES only those exact known-noise shapes and PASSES EVERYTHING
# ELSE THROUGH UNTOUCHED — so any *new* message (a new WARN shape, an ERROR, a
# new INFO) is never clobbered and always surfaces. The full, unfiltered output
# is additionally teed to `raw` (a logfile) so nothing is ever truly discarded,
# and a one-line summary reports how many lines were hidden.
#
# Usage: ... 2>&1 | awk -f openapi-log-filter.awk -v raw=<logfile> [-v verbose=1]
#   verbose=1 (or GEN_VERBOSE in the caller) prints every line unfiltered.

BEGIN { renamed = 0; wrote = 0; banner = 0 }

{ if (raw != "") print $0 >> raw }              # full audit trail, always
verbose != "" { print; next }                    # --verbose: show everything

# Benign identifier sanitization: "<x> cannot be used as a <kind> name. Renamed to <y>"
/^\[main\] WARN .*AbstractRustCodegen - .+ cannot be used as a .+ name\. Renamed to .+$/ {
    renamed++
    next
}

# Per-file write/skip progress from the template manager.
/^\[main\] INFO .*TemplateManager - writing file / { wrote++; next }
/^\[main\] INFO .*TemplateManager - Skipped /      { wrote++; next }

# Donation banner (border rule + the three known text lines).
/^#####/                                                          { banner++; next }
/^# (Thanks for using OpenAPI|We appreciate your support|https:\/\/opencollective)/ { banner++; next }

{ print }

END {
    hidden = renamed + wrote + banner
    if (hidden > 0) {
        msg = "openapi-generator: hid " hidden " expected noise line(s) ["   \
              renamed " identifier-rename warning(s), "                       \
              wrote " file-write log(s), "                                    \
              banner " banner line(s)]"
        if (raw != "") msg = msg " — full log: " raw
        msg = msg " (set GEN_VERBOSE=1 to show all)"
        print msg > "/dev/stderr"
    }
}
