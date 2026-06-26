//! FEEL temporal types: date, time, date-and-time, and the two duration
//! flavours (years-and-months, days-and-time), plus ranges.
//!
//! Everything here is dependency-free. Calendar arithmetic uses the proleptic
//! Gregorian day-number algorithm (Howard Hinnant's `days_from_civil`), so we
//! can add/subtract durations and diff dates without pulling in `chrono`.

use std::cmp::Ordering;

/// A calendar date (proleptic Gregorian), no time component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Date {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

/// A wall-clock time, optionally carrying a UTC offset (seconds) and/or a zone
/// id (only the offset participates in arithmetic; the zone id round-trips for
/// rendering — we do not embed a timezone database).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Time {
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub nano: u32,
    pub offset_seconds: Option<i32>,
    pub zone: Option<String>,
}

/// A date with a time-of-day.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DateTime {
    pub date: Date,
    pub time: Time,
}

/// A FEEL years-and-months duration, normalised to a signed total of months.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct YmDuration {
    pub months: i64,
}

/// A FEEL days-and-time duration, stored as a signed total of nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DtDuration {
    pub nanos: i128,
}

const NANOS_PER_SEC: i128 = 1_000_000_000;
const SECS_PER_DAY: i128 = 86_400;

// --- Day-number conversions -------------------------------------------------

/// Days since 1970-01-01 for a proleptic Gregorian (y, m, d). Valid for any y.
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Inverse of [`days_from_civil`].
pub fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year as i32, m as u32, d as u32)
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 30,
    }
}

// --- Date -------------------------------------------------------------------

impl Date {
    pub fn new(year: i32, month: u32, day: u32) -> Option<Date> {
        if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
            return None;
        }
        Some(Date { year, month, day })
    }

    pub fn epoch_day(&self) -> i64 {
        days_from_civil(self.year, self.month, self.day)
    }

    pub fn from_epoch_day(d: i64) -> Date {
        let (year, month, day) = civil_from_days(d);
        Date { year, month, day }
    }

    /// Parse `YYYY-MM-DD` (optionally a leading `-` for negative years).
    pub fn parse(s: &str) -> Option<Date> {
        let (neg, rest) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s),
        };
        let mut parts = rest.split('-');
        let y: i32 = parts.next()?.parse().ok()?;
        let m: u32 = parts.next()?.parse().ok()?;
        let d: u32 = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Date::new(if neg { -y } else { y }, m, d)
    }

    /// Add a (signed) number of whole months, clamping the day-of-month.
    pub fn add_months(&self, months: i64) -> Date {
        let total = (self.year as i64) * 12 + (self.month as i64 - 1) + months;
        let year = total.div_euclid(12) as i32;
        let month = total.rem_euclid(12) as u32 + 1;
        let day = self.day.min(days_in_month(year, month));
        Date { year, month, day }
    }

    pub fn weekday(&self) -> u32 {
        // ISO: Monday = 1 ..= Sunday = 7. 1970-01-01 was a Thursday (=4).
        let d = self.epoch_day();
        (((d % 7) + 3).rem_euclid(7)) as u32 + 1
    }

    pub fn day_of_year(&self) -> u32 {
        (self.epoch_day() - days_from_civil(self.year, 1, 1) + 1) as u32
    }

    /// ISO-8601 week-of-year number.
    pub fn week_of_year(&self) -> u32 {
        let wd = self.weekday() as i64;
        let thursday = self.epoch_day() - (wd - 1) + 3;
        let (ty, _, _) = civil_from_days(thursday);
        let jan1 = days_from_civil(ty, 1, 1);
        ((thursday - jan1) / 7 + 1) as u32
    }

    pub fn format(&self) -> String {
        if self.year < 0 {
            format!("-{:04}-{:02}-{:02}", -self.year, self.month, self.day)
        } else {
            format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
        }
    }
}

// --- Time -------------------------------------------------------------------

impl Time {
    pub fn new(hour: u32, minute: u32, second: u32, nano: u32) -> Time {
        Time {
            hour,
            minute,
            second,
            nano,
            offset_seconds: None,
            zone: None,
        }
    }

    /// Nanoseconds since local midnight (ignores any offset).
    pub fn nano_of_day(&self) -> i128 {
        (((self.hour as i128 * 60 + self.minute as i128) * 60 + self.second as i128)
            * NANOS_PER_SEC)
            + self.nano as i128
    }

    fn from_nano_of_day(mut nanos: i128, offset_seconds: Option<i32>, zone: Option<String>) -> Time {
        let day = SECS_PER_DAY * NANOS_PER_SEC;
        nanos = nanos.rem_euclid(day);
        let nano = (nanos % NANOS_PER_SEC) as u32;
        let secs = nanos / NANOS_PER_SEC;
        let second = (secs % 60) as u32;
        let minute = ((secs / 60) % 60) as u32;
        let hour = ((secs / 3600) % 24) as u32;
        Time {
            hour,
            minute,
            second,
            nano,
            offset_seconds,
            zone,
        }
    }

    /// Parse `HH:MM:SS[.fff][Z|±HH:MM][@Zone]`.
    pub fn parse(s: &str) -> Option<Time> {
        let (body, zone) = match s.find('@') {
            Some(idx) => (&s[..idx], Some(s[idx + 1..].to_string())),
            None => (s, None),
        };
        let (clock, offset) = split_offset(body);
        let mut it = clock.split(':');
        let hour: u32 = it.next()?.parse().ok()?;
        let minute: u32 = it.next()?.parse().ok()?;
        let sec_part = it.next().unwrap_or("0");
        if it.next().is_some() {
            return None;
        }
        let (second, nano) = parse_seconds(sec_part)?;
        if hour > 24 || minute > 59 || second > 60 {
            return None;
        }
        Some(Time {
            hour,
            minute,
            second,
            nano,
            offset_seconds: offset,
            zone,
        })
    }

    pub fn format(&self) -> String {
        let mut out = format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second);
        if self.nano > 0 {
            let frac = format!("{:09}", self.nano);
            out.push('.');
            out.push_str(frac.trim_end_matches('0'));
        }
        if let Some(off) = self.offset_seconds {
            out.push_str(&format_offset(off));
        }
        if let Some(z) = &self.zone {
            out.push('@');
            out.push_str(z);
        }
        out
    }
}

// --- DateTime ---------------------------------------------------------------

impl DateTime {
    /// Total nanoseconds since the Unix epoch, accounting for any UTC offset.
    pub fn epoch_nanos(&self) -> i128 {
        let day_secs = self.date.epoch_day() as i128 * SECS_PER_DAY;
        let local = day_secs * NANOS_PER_SEC + self.time.nano_of_day();
        let off = self.time.offset_seconds.unwrap_or(0) as i128 * NANOS_PER_SEC;
        local - off
    }

    pub fn parse(s: &str) -> Option<DateTime> {
        let sep = s.find('T').or_else(|| s.find('t'))?;
        let date = Date::parse(&s[..sep])?;
        let time = Time::parse(&s[sep + 1..])?;
        Some(DateTime { date, time })
    }

    pub fn format(&self) -> String {
        format!("{}T{}", self.date.format(), self.time.format())
    }

    /// Add a days-time duration, rolling the date over as needed.
    pub fn add_dt(&self, dur: DtDuration) -> DateTime {
        let day = SECS_PER_DAY * NANOS_PER_SEC;
        let total = self.date.epoch_day() as i128 * day + self.time.nano_of_day() + dur.nanos;
        let epoch_day = total.div_euclid(day) as i64;
        let nanos = total.rem_euclid(day);
        DateTime {
            date: Date::from_epoch_day(epoch_day),
            time: Time::from_nano_of_day(
                nanos,
                self.time.offset_seconds,
                self.time.zone.clone(),
            ),
        }
    }
}

// --- Durations --------------------------------------------------------------

impl YmDuration {
    pub fn new(months: i64) -> YmDuration {
        YmDuration { months }
    }

    /// Parse an ISO-8601 `P[n]Y[n]M` (with optional leading sign).
    pub fn parse(s: &str) -> Option<YmDuration> {
        let (neg, body) = strip_sign(s)?;
        let body = body.strip_prefix('P')?;
        if body.contains('T') || body.is_empty() {
            return None;
        }
        let mut months: i64 = 0;
        let mut num = String::new();
        let mut saw = false;
        for c in body.chars() {
            match c {
                '0'..='9' => num.push(c),
                'Y' => {
                    months += num.parse::<i64>().ok()? * 12;
                    num.clear();
                    saw = true;
                }
                'M' => {
                    months += num.parse::<i64>().ok()?;
                    num.clear();
                    saw = true;
                }
                _ => return None,
            }
        }
        if !num.is_empty() || !saw {
            return None;
        }
        Some(YmDuration {
            months: if neg { -months } else { months },
        })
    }

    pub fn format(&self) -> String {
        let neg = self.months < 0;
        let total = self.months.unsigned_abs();
        let years = total / 12;
        let months = total % 12;
        let mut out = String::new();
        if neg {
            out.push('-');
        }
        out.push('P');
        if years > 0 {
            out.push_str(&format!("{years}Y"));
        }
        if months > 0 || years == 0 {
            out.push_str(&format!("{months}M"));
        }
        out
    }
}

impl DtDuration {
    pub fn from_nanos(nanos: i128) -> DtDuration {
        DtDuration { nanos }
    }

    pub fn from_seconds(seconds: i64) -> DtDuration {
        DtDuration {
            nanos: seconds as i128 * NANOS_PER_SEC,
        }
    }

    /// Parse an ISO-8601 `P[n]DT[n]H[n]M[n]S` (with optional leading sign).
    pub fn parse(s: &str) -> Option<DtDuration> {
        let (neg, body) = strip_sign(s)?;
        let body = body.strip_prefix('P')?;
        let (date_part, time_part) = match body.find('T') {
            Some(idx) => (&body[..idx], &body[idx + 1..]),
            None => (body, ""),
        };
        let mut nanos: i128 = 0;
        let mut saw = false;
        // Date part: only D allowed.
        let mut num = String::new();
        for c in date_part.chars() {
            match c {
                '0'..='9' => num.push(c),
                'D' => {
                    nanos += num.parse::<i128>().ok()? * SECS_PER_DAY * NANOS_PER_SEC;
                    num.clear();
                    saw = true;
                }
                _ => return None,
            }
        }
        if !num.is_empty() {
            return None;
        }
        // Time part: H, M, S (S may carry a fraction).
        num.clear();
        for c in time_part.chars() {
            match c {
                '0'..='9' | '.' => num.push(c),
                'H' => {
                    nanos += num.parse::<i128>().ok()? * 3600 * NANOS_PER_SEC;
                    num.clear();
                    saw = true;
                }
                'M' => {
                    nanos += num.parse::<i128>().ok()? * 60 * NANOS_PER_SEC;
                    num.clear();
                    saw = true;
                }
                'S' => {
                    let secs: f64 = num.parse().ok()?;
                    nanos += (secs * NANOS_PER_SEC as f64).round() as i128;
                    num.clear();
                    saw = true;
                }
                _ => return None,
            }
        }
        if !num.is_empty() || !saw {
            return None;
        }
        Some(DtDuration {
            nanos: if neg { -nanos } else { nanos },
        })
    }

    pub fn format(&self) -> String {
        let neg = self.nanos < 0;
        let mut rem = self.nanos.unsigned_abs();
        let nanos = (rem % NANOS_PER_SEC as u128) as u64;
        rem /= NANOS_PER_SEC as u128;
        let secs = rem % 60;
        rem /= 60;
        let mins = rem % 60;
        rem /= 60;
        let hours = rem % 24;
        let days = rem / 24;
        let mut out = String::new();
        if neg {
            out.push('-');
        }
        out.push('P');
        if days > 0 {
            out.push_str(&format!("{days}D"));
        }
        let has_time = hours > 0 || mins > 0 || secs > 0 || nanos > 0;
        if has_time || days == 0 {
            out.push('T');
            if hours > 0 {
                out.push_str(&format!("{hours}H"));
            }
            if mins > 0 {
                out.push_str(&format!("{mins}M"));
            }
            if secs > 0 || nanos > 0 || (hours == 0 && mins == 0) {
                if nanos > 0 {
                    let frac = format!("{nanos:09}");
                    out.push_str(&format!("{secs}.{}S", frac.trim_end_matches('0')));
                } else {
                    out.push_str(&format!("{secs}S"));
                }
            }
        }
        out
    }
}

// --- Helpers ----------------------------------------------------------------

fn strip_sign(s: &str) -> Option<(bool, &str)> {
    if let Some(rest) = s.strip_prefix('-') {
        Some((true, rest))
    } else if let Some(rest) = s.strip_prefix('+') {
        Some((false, rest))
    } else {
        Some((false, s))
    }
}

fn split_offset(clock: &str) -> (&str, Option<i32>) {
    if let Some(rest) = clock.strip_suffix('Z').or_else(|| clock.strip_suffix('z')) {
        return (rest, Some(0));
    }
    // Find a +/- that starts an offset (after the seconds field).
    let bytes = clock.as_bytes();
    for (i, &b) in bytes.iter().enumerate().skip(1) {
        if b == b'+' || b == b'-' {
            let off = parse_offset(&clock[i..]);
            if let Some(seconds) = off {
                return (&clock[..i], Some(seconds));
            }
        }
    }
    (clock, None)
}

fn parse_offset(s: &str) -> Option<i32> {
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+')?),
    };
    let mut it = rest.split(':');
    let h: i32 = it.next()?.parse().ok()?;
    let m: i32 = it.next().unwrap_or("0").parse().ok()?;
    let secs = h * 3600 + m * 60;
    Some(if neg { -secs } else { secs })
}

fn format_offset(off: i32) -> String {
    if off == 0 {
        return "Z".to_string();
    }
    let sign = if off < 0 { '-' } else { '+' };
    let a = off.unsigned_abs();
    format!("{sign}{:02}:{:02}", a / 3600, (a % 3600) / 60)
}

fn parse_seconds(s: &str) -> Option<(u32, u32)> {
    match s.split_once('.') {
        Some((sec, frac)) => {
            let second: u32 = sec.parse().ok()?;
            let mut f = frac.to_string();
            while f.len() < 9 {
                f.push('0');
            }
            f.truncate(9);
            let nano: u32 = f.parse().ok()?;
            Some((second, nano))
        }
        None => Some((s.parse().ok()?, 0)),
    }
}

// --- Ordering ---------------------------------------------------------------

impl Date {
    pub fn cmp(&self, other: &Date) -> Ordering {
        self.epoch_day().cmp(&other.epoch_day())
    }
}

impl Time {
    pub fn cmp(&self, other: &Time) -> Ordering {
        let a = self.nano_of_day() - self.offset_seconds.unwrap_or(0) as i128 * NANOS_PER_SEC;
        let b = other.nano_of_day() - other.offset_seconds.unwrap_or(0) as i128 * NANOS_PER_SEC;
        a.cmp(&b)
    }
}

impl DateTime {
    pub fn cmp(&self, other: &DateTime) -> Ordering {
        self.epoch_nanos().cmp(&other.epoch_nanos())
    }
}

impl YmDuration {
    pub fn cmp(&self, other: &YmDuration) -> Ordering {
        self.months.cmp(&other.months)
    }
}

impl DtDuration {
    pub fn cmp(&self, other: &DtDuration) -> Ordering {
        self.nanos.cmp(&other.nanos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_round_trips_and_diffs() {
        let d = Date::parse("2024-02-29").unwrap();
        assert_eq!(d.format(), "2024-02-29");
        assert!(Date::parse("2023-02-29").is_none());
        let a = Date::parse("2024-01-01").unwrap();
        let b = Date::parse("2024-12-31").unwrap();
        assert_eq!(b.epoch_day() - a.epoch_day(), 365);
    }

    #[test]
    fn add_months_clamps_day() {
        let d = Date::parse("2024-01-31").unwrap();
        assert_eq!(d.add_months(1).format(), "2024-02-29");
        assert_eq!(d.add_months(13).format(), "2025-02-28");
        assert_eq!(d.add_months(-1).format(), "2023-12-31");
    }

    #[test]
    fn weekday_and_week_of_year() {
        // 2024-01-01 is a Monday.
        assert_eq!(Date::parse("2024-01-01").unwrap().weekday(), 1);
        // 2024-01-07 is a Sunday.
        assert_eq!(Date::parse("2024-01-07").unwrap().weekday(), 7);
        assert_eq!(Date::parse("2024-01-01").unwrap().week_of_year(), 1);
    }

    #[test]
    fn time_offset_parse_and_compare() {
        let z = Time::parse("10:00:00Z").unwrap();
        assert_eq!(z.offset_seconds, Some(0));
        let plus = Time::parse("12:00:00+02:00").unwrap();
        // 12:00+02:00 == 10:00Z.
        assert_eq!(z.cmp(&plus), Ordering::Equal);
        assert_eq!(plus.format(), "12:00:00+02:00");
    }

    #[test]
    fn durations_parse_and_format() {
        assert_eq!(YmDuration::parse("P1Y2M").unwrap().months, 14);
        assert_eq!(YmDuration::new(14).format(), "P1Y2M");
        assert_eq!(YmDuration::new(-3).format(), "-P3M");
        let dt = DtDuration::parse("P1DT2H3M4S").unwrap();
        assert_eq!(dt.nanos, ((24 + 2) * 3600 + 3 * 60 + 4) as i128 * NANOS_PER_SEC);
        assert_eq!(dt.format(), "P1DT2H3M4S");
        assert_eq!(DtDuration::parse("PT0.5S").unwrap().format(), "PT0.5S");
    }

    #[test]
    fn datetime_arith_rolls_over() {
        let dt = DateTime::parse("2024-01-01T23:00:00").unwrap();
        let plus = dt.add_dt(DtDuration::from_seconds(7200));
        assert_eq!(plus.format(), "2024-01-02T01:00:00");
    }
}
