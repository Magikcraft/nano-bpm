//! Canonical ISO-8601 duration parsing (L0 leaf: depends on nothing internal).
//!
//! Historically two hand-rolled parsers existed and had already drifted:
//!
//! - `bpmn::parse_iso8601_duration` — milliseconds, unsigned, weeks supported,
//!   **no** fractional seconds.
//! - `feel::temporal::DtDuration::parse` — nanoseconds, signed, fractional
//!   seconds supported, **no** weeks.
//!
//! The concrete symptom: `PT1.5S` was a valid FEEL duration but a silently
//! `None` BPMN timer.
//!
//! This module is the single source of truth. The ISO-8601 grammar is parsed
//! once, into nanoseconds, and the per-layer width (ms vs ns) and the policy
//! decisions (sign? weeks? fractions?) become **explicit, documented choices**
//! via [`DurationForm`]. The two callers delegate here; their historical
//! behavioural differences are preserved as distinct `DurationForm` presets
//! ([`DurationForm::BPMN_TIMER`], [`DurationForm::FEEL_DAYTIME`]).
//!
//! Accepted/rejected forms are aligned with the Camunda/Zeebe timer-expression
//! parsing at `camunda/camunda`: the date-portion years (`Y`) and months (`M`)
//! are calendar-ambiguous and rejected; weeks (`W`), days (`D`), hours (`H`),
//! minutes (`M`) and seconds (`S`) are accepted.

const NANOS_PER_SEC: i128 = 1_000_000_000;
const NANOS_PER_MILLI: i128 = 1_000_000;
const SECS_PER_DAY: i128 = 86_400;
const SECS_PER_HOUR: i128 = 3_600;
const SECS_PER_MINUTE: i128 = 60;
const DAYS_PER_WEEK: i128 = 7;

/// Which optional ISO-8601 duration forms a caller accepts.
///
/// The core grammar is shared; these three flags are the documented points at
/// which BPMN timers and FEEL day-time durations historically diverged.
#[derive(Clone, Copy, Debug)]
pub struct DurationForm {
    /// Accept a leading `+`/`-` sign (FEEL: yes; BPMN timers: no).
    pub allow_sign: bool,
    /// Accept the `nW` weeks field in the date part (BPMN timers: yes; FEEL: no).
    pub allow_weeks: bool,
    /// Accept a fractional seconds field, e.g. `PT1.5S` (FEEL: yes; BPMN: no).
    pub allow_fraction: bool,
}

impl DurationForm {
    /// BPMN timer policy: unsigned, weeks allowed, **no** fractional seconds.
    pub const BPMN_TIMER: DurationForm = DurationForm {
        allow_sign: false,
        allow_weeks: true,
        allow_fraction: false,
    };

    /// FEEL day-time-duration policy: signed, **no** weeks, fractional seconds.
    pub const FEEL_DAYTIME: DurationForm = DurationForm {
        allow_sign: true,
        allow_weeks: false,
        allow_fraction: true,
    };
}

/// Parses an ISO-8601 duration (e.g. `PT5S`, `P1DT6H30M`, `P1W`) into a signed
/// total of **nanoseconds**, applying `form`.
///
/// Supports weeks (when allowed), days, hours, minutes and seconds. The
/// date-portion years/months are calendar-ambiguous and always rejected.
/// Returns `None` if the string is not a recognisable duration, or if the
/// total overflows `i128`.
pub fn parse_duration_nanos(raw: &str, form: DurationForm) -> Option<i128> {
    let (neg, body) = if form.allow_sign {
        strip_sign(raw)
    } else {
        (false, raw)
    };
    let body = body.strip_prefix('P')?;
    if body.is_empty() {
        return None;
    }
    let (date_part, time_part) = match body.find('T') {
        Some(idx) => (&body[..idx], &body[idx + 1..]),
        None => (body, ""),
    };

    let mut nanos: i128 = 0;
    let mut saw_unit = false;
    let mut num = String::new();

    // Date part: `D` always, `W` when allowed. `Y`/`M` are calendar-ambiguous.
    for c in date_part.chars() {
        match c {
            '0'..='9' => num.push(c),
            'W' if form.allow_weeks => {
                nanos = add_unit(nanos, &num, DAYS_PER_WEEK * SECS_PER_DAY * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            'D' => {
                nanos = add_unit(nanos, &num, SECS_PER_DAY * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            _ => return None,
        }
    }
    if !num.is_empty() {
        return None;
    }

    // Time part: `H`, `M`, `S`; `S` may carry a fraction when allowed.
    for c in time_part.chars() {
        match c {
            '0'..='9' => num.push(c),
            '.' if form.allow_fraction => num.push(c),
            'H' => {
                nanos = add_unit(nanos, &num, SECS_PER_HOUR * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            'M' => {
                nanos = add_unit(nanos, &num, SECS_PER_MINUTE * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            'S' => {
                nanos = add_seconds(nanos, &num, form.allow_fraction)?;
                num.clear();
                saw_unit = true;
            }
            _ => return None,
        }
    }

    // Trailing digits without a unit, or no units at all, are invalid.
    if !num.is_empty() || !saw_unit {
        return None;
    }
    Some(if neg { -nanos } else { nanos })
}

/// BPMN timer helper: parses an ISO-8601 duration into whole **milliseconds**
/// under [`DurationForm::BPMN_TIMER`]. Leading/trailing whitespace is trimmed.
/// Returns `None` if the value is not a recognisable duration or does not fit
/// in `u64` milliseconds.
pub fn parse_duration_millis(raw: &str) -> Option<u64> {
    let nanos = parse_duration_nanos(raw.trim(), DurationForm::BPMN_TIMER)?;
    u64::try_from(nanos / NANOS_PER_MILLI).ok()
}

/// BPMN timer helper: parses an ISO-8601 repeating interval (a BPMN `timeCycle`,
/// e.g. `R/PT1H` or `R5/PT1H`) into the interval in **milliseconds**. The `Rn`
/// repetition-count prefix is accepted but ignored (the engine repeats
/// unboundedly). A bare duration without the `R[n]/` prefix is also accepted.
/// Returns `None` if the interval portion is not a recognisable duration.
pub fn parse_cycle_millis(raw: &str) -> Option<u64> {
    let s = raw.trim();
    let interval = match s.split_once('/') {
        Some((repeat, interval)) if repeat.starts_with('R') => interval,
        Some(_) => return None,
        None => s,
    };
    parse_duration_millis(interval)
}

/// Accumulates an integer-valued unit (`num` × `factor` nanoseconds). Returns
/// `None` on an empty/invalid number or on `i128` overflow.
fn add_unit(nanos: i128, num: &str, factor: i128) -> Option<i128> {
    if num.is_empty() {
        return None;
    }
    let value: i128 = num.parse().ok()?;
    nanos.checked_add(value.checked_mul(factor)?)
}

/// Accumulates a seconds field, which may carry a fraction when `allow_fraction`.
/// Returns `None` on an empty/invalid number or on `i128` overflow.
fn add_seconds(nanos: i128, num: &str, allow_fraction: bool) -> Option<i128> {
    if num.is_empty() {
        return None;
    }
    if allow_fraction {
        let secs: f64 = num.parse().ok()?;
        nanos.checked_add((secs * NANOS_PER_SEC as f64).round() as i128)
    } else {
        let value: i128 = num.parse().ok()?;
        nanos.checked_add(value.checked_mul(NANOS_PER_SEC)?)
    }
}

/// Strips an optional leading `+`/`-` sign, returning `(is_negative, rest)`.
fn strip_sign(s: &str) -> (bool, &str) {
    if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bpmn_millis_basic() {
        assert_eq!(parse_duration_millis("PT5S"), Some(5_000));
        assert_eq!(parse_duration_millis("PT1M"), Some(60_000));
        assert_eq!(parse_duration_millis("PT2H"), Some(7_200_000));
        assert_eq!(parse_duration_millis("P1D"), Some(86_400_000));
        assert_eq!(parse_duration_millis("P1W"), Some(604_800_000));
        assert_eq!(parse_duration_millis("P1DT6H30M"), Some(109_800_000));
        assert_eq!(parse_duration_millis("  PT5S  "), Some(5_000));
    }

    #[test]
    fn bpmn_millis_rejects() {
        // Empty / missing units.
        assert_eq!(parse_duration_millis("P"), None);
        assert_eq!(parse_duration_millis("PT"), None);
        assert_eq!(parse_duration_millis("5S"), None);
        assert_eq!(parse_duration_millis("PT5"), None);
        // Calendar-ambiguous date fields.
        assert_eq!(parse_duration_millis("P1Y"), None);
        assert_eq!(parse_duration_millis("P1M"), None);
        // BPMN policy: no sign, no fractions.
        assert_eq!(parse_duration_millis("-PT5S"), None);
        assert_eq!(parse_duration_millis("PT1.5S"), None);
    }

    #[test]
    fn bpmn_cycle() {
        assert_eq!(parse_cycle_millis("R/PT1H"), Some(3_600_000));
        assert_eq!(parse_cycle_millis("R5/PT1H"), Some(3_600_000));
        assert_eq!(parse_cycle_millis("PT1H"), Some(3_600_000));
        assert_eq!(parse_cycle_millis("X/PT1H"), None);
    }

    #[test]
    fn feel_nanos_fractions_and_sign() {
        let f = DurationForm::FEEL_DAYTIME;
        // The drift symptom: PT1.5S is valid under the FEEL policy.
        assert_eq!(parse_duration_nanos("PT1.5S", f), Some(1_500_000_000));
        assert_eq!(parse_duration_nanos("PT1M", f), Some(60_000_000_000));
        assert_eq!(parse_duration_nanos("P1DT2H", f), Some(93_600_000_000_000));
        assert_eq!(parse_duration_nanos("-PT1S", f), Some(-1_000_000_000));
        assert_eq!(parse_duration_nanos("+PT1S", f), Some(1_000_000_000));
        // FEEL policy: weeks are not allowed.
        assert_eq!(parse_duration_nanos("P1W", f), None);
    }

    #[test]
    fn policy_divergence_is_explicit() {
        // Same input, two policies, two answers — the documented divergences.
        assert_eq!(parse_duration_nanos("PT1.5S", DurationForm::BPMN_TIMER), None);
        assert_eq!(
            parse_duration_nanos("PT1.5S", DurationForm::FEEL_DAYTIME),
            Some(1_500_000_000)
        );
        assert_eq!(
            parse_duration_nanos("P1W", DurationForm::BPMN_TIMER),
            Some(604_800_000_000_000)
        );
        assert_eq!(parse_duration_nanos("P1W", DurationForm::FEEL_DAYTIME), None);
        assert_eq!(parse_duration_nanos("-PT1S", DurationForm::BPMN_TIMER), None);
        assert_eq!(
            parse_duration_nanos("-PT1S", DurationForm::FEEL_DAYTIME),
            Some(-1_000_000_000)
        );
    }

    #[test]
    fn millis_overflow_is_none() {
        // Enormous day count overflows u64 milliseconds.
        assert_eq!(parse_duration_millis("P100000000000000000D"), None);
    }
}
