//! Search-query support: the advanced filter algebra, multi-field sorting, and
//! cursor/offset pagination shared by the `search*` endpoints.
//!
//! The generated request models express filters as either a bare value (exact
//! match) or an "advanced" object carrying the `$eq`/`$neq`/`$exists`/`$in`/
//! `$notIn`/`$like` operators. Every advanced filter has the same shape, so we
//! normalise each one into a single [`Ops`] matcher that compares against a
//! string projection of the engine value. Keys project to their decimal string,
//! enums to their wire spelling (their `Display`), and string fields to
//! themselves.

use nanobpm_gateway_rest::{models, types};

/// The advanced-filter operators, normalised to string comparisons. `None`
/// fields are simply not constrained.
#[derive(Default)]
pub struct Ops {
    eq: Option<String>,
    neq: Option<String>,
    exists: Option<bool>,
    in_: Option<Vec<String>>,
    not_in: Option<Vec<String>>,
    like: Option<String>,
}

impl Ops {
    /// Whether `value` (the engine value's string projection, or `None` when the
    /// property is absent) satisfies every operator present on this filter.
    pub fn matches(&self, value: Option<&str>) -> bool {
        if matches!(self.exists, Some(e) if e != value.is_some()) {
            return false;
        }
        let v = match value {
            Some(v) => v,
            None => {
                // An absent value can only satisfy an `$exists: false` (handled
                // above); any value-based operator fails to match.
                return self.eq.is_none()
                    && self.neq.is_none()
                    && self.in_.is_none()
                    && self.not_in.is_none()
                    && self.like.is_none();
            }
        };
        if self.eq.as_deref().is_some_and(|eq| v != eq) {
            return false;
        }
        if self.neq.as_deref().is_some_and(|neq| v == neq) {
            return false;
        }
        if self.in_.as_ref().is_some_and(|in_| !in_.iter().any(|x| x == v)) {
            return false;
        }
        if self
            .not_in
            .as_ref()
            .is_some_and(|not_in| not_in.iter().any(|x| x == v))
        {
            return false;
        }
        if self.like.as_deref().is_some_and(|like| !like_matches(like, v)) {
            return false;
        }
        true
    }
}

/// Matches a `$like` pattern against a value. `*` matches any run of characters,
/// `?` matches a single character, and `\` escapes the next metacharacter. The
/// match is anchored to the whole string.
fn like_matches(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    // Classic backtracking wildcard matcher with a remembered `*` position.
    let (mut pi, mut vi) = (0usize, 0usize);
    let (mut star_p, mut star_v): (Option<usize>, usize) = (None, 0);
    while vi < v.len() {
        let lit = if pi < p.len() && p[pi] == '\\' && pi + 1 < p.len() {
            Some(p[pi + 1])
        } else {
            None
        };
        if let Some(c) = lit {
            if c == v[vi] {
                pi += 2;
                vi += 1;
                continue;
            }
        } else if pi < p.len() && p[pi] == '?' {
            pi += 1;
            vi += 1;
            continue;
        } else if pi < p.len() && p[pi] == '*' {
            star_p = Some(pi);
            star_v = vi;
            pi += 1;
            continue;
        } else if pi < p.len() && p[pi] == v[vi] {
            pi += 1;
            vi += 1;
            continue;
        }
        // Mismatch: backtrack to the last `*` if there was one.
        if let Some(sp) = star_p {
            pi = sp + 1;
            star_v += 1;
            vi = star_v;
        } else {
            return false;
        }
    }
    // Consume any trailing `*`s in the pattern.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Builds an [`Ops`] from an advanced filter's standard fields, projecting each
/// typed value to a string via `conv`. The optional `like` token is passed
/// through directly (only string-bearing filters carry one).
macro_rules! ops {
    ($adv:expr, $conv:expr) => {{
        let a = $adv;
        let conv = $conv;
        Ops {
            eq: a.dollar_eq.as_ref().map(&conv),
            neq: a.dollar_neq.as_ref().map(&conv),
            exists: a.dollar_exists,
            in_: a
                .dollar_in
                .as_ref()
                .map(|v| v.iter().map(&conv).collect()),
            not_in: a
                .dollar_not_in
                .as_ref()
                .map(|v| v.iter().map(&conv).collect()),
            like: None,
        }
    }};
    ($adv:expr, $conv:expr, like) => {{
        let mut o = ops!($adv, $conv);
        o.like = $adv.dollar_like.clone();
        o
    }};
    // Variant for filters that carry `$like` but no `$notIn` (the enum-state
    // filters): omit `not_in`, include `like`.
    ($adv:expr, $conv:expr, like_no_notin) => {{
        let a = $adv;
        let conv = $conv;
        Ops {
            eq: a.dollar_eq.as_ref().map(&conv),
            neq: a.dollar_neq.as_ref().map(&conv),
            exists: a.dollar_exists,
            in_: a.dollar_in.as_ref().map(|v| v.iter().map(&conv).collect()),
            not_in: None,
            like: a.dollar_like.clone(),
        }
    }};
}

/// Matches a `StringFilterProperty` (bare string or advanced) against a value.
pub fn match_string(filter: &Option<models::StringFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::StringFilterProperty::String(s)) => s == value,
        Some(models::StringFilterProperty::AdvancedStringFilter(a)) => {
            ops!(a, |s: &String| s.clone(), like).matches(Some(value))
        }
    }
}

/// Matches a `BasicStringFilterProperty` (bare string or basic filter — no
/// `$like`) against a value.
pub fn match_basic_string(
    filter: &Option<models::BasicStringFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::BasicStringFilterProperty::String(s)) => s == value,
        Some(models::BasicStringFilterProperty::BasicStringFilter(a)) => {
            ops!(a, |s: &String| s.clone()).matches(Some(value))
        }
    }
}

/// Matches a `ProcessInstanceKeyFilterProperty` against a key's decimal string.
pub fn match_process_instance_key(
    filter: &Option<models::ProcessInstanceKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ProcessInstanceKeyFilterProperty::ProcessInstanceKey(k)) => k.0 == value,
        Some(models::ProcessInstanceKeyFilterProperty::AdvancedProcessInstanceKeyFilter(a)) => {
            ops!(a, |k: &models::ProcessInstanceKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches a `ProcessDefinitionKeyFilterProperty` against a key's decimal string.
pub fn match_process_definition_key(
    filter: &Option<models::ProcessDefinitionKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ProcessDefinitionKeyFilterProperty::ProcessDefinitionKey(k)) => k.0 == value,
        Some(models::ProcessDefinitionKeyFilterProperty::AdvancedProcessDefinitionKeyFilter(a)) => {
            ops!(a, |k: &models::ProcessDefinitionKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches an `ElementInstanceKeyFilterProperty` against a key's decimal string.
pub fn match_element_instance_key(
    filter: &Option<models::ElementInstanceKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ElementInstanceKeyFilterProperty::ElementInstanceKey(k)) => k.0 == value,
        Some(models::ElementInstanceKeyFilterProperty::AdvancedElementInstanceKeyFilter(a)) => {
            ops!(a, |k: &models::ElementInstanceKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches a `JobKeyFilterProperty` against a key's decimal string.
pub fn match_job_key(filter: &Option<models::JobKeyFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::JobKeyFilterProperty::JobKey(k)) => k.0 == value,
        Some(models::JobKeyFilterProperty::AdvancedJobKeyFilter(a)) => {
            ops!(a, |k: &models::JobKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches a `VariableKeyFilterProperty` against a key's decimal string.
pub fn match_variable_key(
    filter: &Option<models::VariableKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::VariableKeyFilterProperty::VariableKey(k)) => k.0 == value,
        Some(models::VariableKeyFilterProperty::AdvancedVariableKeyFilter(a)) => {
            ops!(a, |k: &models::VariableKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches a `ScopeKeyFilterProperty` against a key's decimal string.
pub fn match_scope_key(filter: &Option<models::ScopeKeyFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::ScopeKeyFilterProperty::ScopeKey(k)) => k.0 == value,
        Some(models::ScopeKeyFilterProperty::AdvancedScopeKeyFilter(a)) => {
            ops!(a, |k: &models::ScopeKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches an `IncidentStateFilterProperty` against a state's wire spelling.
pub fn match_incident_state(
    filter: &Option<models::IncidentStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::IncidentStateFilterProperty::IncidentStateEnum(e)) => e.to_string() == value,
        Some(models::IncidentStateFilterProperty::AdvancedIncidentStateFilter(a)) => {
            ops!(a, |e: &models::IncidentStateEnum| e.to_string(), like).matches(Some(value))
        }
    }
}

/// Matches an `IncidentErrorTypeFilterProperty` against an error type's wire
/// spelling.
pub fn match_incident_error_type(
    filter: &Option<models::IncidentErrorTypeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::IncidentErrorTypeFilterProperty::IncidentErrorTypeEnum(e)) => {
            e.to_string() == value
        }
        Some(models::IncidentErrorTypeFilterProperty::AdvancedIncidentErrorTypeFilter(a)) => {
            ops!(a, |e: &models::IncidentErrorTypeEnum| e.to_string(), like).matches(Some(value))
        }
    }
}

/// Matches a `ProcessInstanceStateFilterProperty` against a state's wire
/// spelling.
pub fn match_process_instance_state(
    filter: &Option<models::ProcessInstanceStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ProcessInstanceStateFilterProperty::ProcessInstanceStateEnum(e)) => {
            e.to_string() == value
        }
        Some(models::ProcessInstanceStateFilterProperty::AdvancedProcessInstanceStateFilter(a)) => {
            ops!(a, |e: &models::ProcessInstanceStateEnum| e.to_string(), like_no_notin).matches(Some(value))
        }
    }
}

/// Matches a `JobStateFilterProperty` against a job state's wire spelling.
pub fn match_job_state(filter: &Option<models::JobStateFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::JobStateFilterProperty::JobStateEnum(e)) => e.to_string() == value,
        Some(models::JobStateFilterProperty::AdvancedJobStateFilter(a)) => {
            ops!(a, |e: &models::JobStateEnum| e.to_string(), like_no_notin).matches(Some(value))
        }
    }
}

/// Matches a `UserTaskStateFilterProperty` against a user-task state's wire
/// spelling.
pub fn match_user_task_state(
    filter: &Option<models::UserTaskStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::UserTaskStateFilterProperty::UserTaskStateEnum(e)) => e.to_string() == value,
        Some(models::UserTaskStateFilterProperty::AdvancedUserTaskStateFilter(a)) => {
            ops!(a, |e: &models::UserTaskStateEnum| e.to_string(), like_no_notin)
                .matches(Some(value))
        }
    }
}

/// One normalised sort instruction: the field name and whether it is descending.
pub struct SortKey {
    pub field: String,
    pub descending: bool,
}

/// Normalises the generated sort requests (field + optional ASC/DESC) into a
/// flat list. `extract` pulls `(field, order)` from each request so this works
/// for every endpoint's sort-request type.
pub fn sort_keys<S>(
    sort: Option<&Vec<S>>,
    extract: impl Fn(&S) -> (String, Option<models::SortOrderEnum>),
) -> Vec<SortKey> {
    sort.map(|reqs| {
            reqs.iter()
                .map(|r| {
                    let (field, order) = extract(r);
                    SortKey {
                        field,
                        descending: matches!(order, Some(models::SortOrderEnum::Desc)),
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A value to sort by: numeric keys sort numerically, everything else
/// lexicographically. Numbers always sort before strings (they never mix in
/// practice).
#[derive(PartialEq, Eq)]
pub enum SortVal {
    Num(i64),
    Str(String),
}

impl PartialOrd for SortVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (SortVal::Num(a), SortVal::Num(b)) => a.cmp(b),
            (SortVal::Str(a), SortVal::Str(b)) => a.cmp(b),
            (SortVal::Num(_), SortVal::Str(_)) => Ordering::Less,
            (SortVal::Str(_), SortVal::Num(_)) => Ordering::Greater,
        }
    }
}

/// Stably sorts `items` by the given sort keys, breaking ties by entity key so
/// the order is fully deterministic (and stable for cursor paging). `project`
/// returns the [`SortVal`] for a given `(item, field)`; `key` returns the
/// entity key used as the final tiebreaker.
pub fn sort_items<T>(
    items: &mut [T],
    keys: &[SortKey],
    project: impl Fn(&T, &str) -> SortVal,
    key: impl Fn(&T) -> u64,
) {
    items.sort_by(|a, b| {
        for sk in keys {
            let ord = project(a, &sk.field).cmp(&project(b, &sk.field));
            let ord = if sk.descending { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        key(a).cmp(&key(b))
    });
}

/// A page of results plus the response envelope describing it.
pub struct Page<T> {
    pub items: Vec<T>,
    pub response: models::SearchQueryPageResponse,
}

/// Applies pagination (limit + offset/forward-cursor/backward-cursor) to an
/// already-sorted `sorted` list of `(entity key, item)` pairs. Cursors are
/// opaque encodings of the entity key (see [`encode_cursor`]); because the sort
/// always ends in an entity-key tiebreaker, resuming from a key is unambiguous.
pub fn paginate<T>(
    sorted: Vec<(u64, T)>,
    page: Option<&models::SearchQueryPageRequest>,
) -> Page<T> {
    let total = sorted.len() as i64;

    // Per the search spec every `limit` defaults to 100 and is bounded to
    // [1, 10000]; clamp so a missing, zero, or oversized limit can never
    // materialize an unbounded page. The generated `limit` fields differ in
    // width across pagination variants, so accept anything convertible to u64.
    let default_limit = 100usize;
    const MAX_LIMIT: u64 = 10_000;
    fn clamp_limit<T: Into<u64>>(limit: Option<T>, default: usize) -> usize {
        match limit {
            Some(l) => (l.into().clamp(1, MAX_LIMIT)) as usize,
            None => default,
        }
    }
    let (start, limit, backward_before) = match page {
        Some(models::SearchQueryPageRequest::LimitPagination(p)) => {
            (0usize, clamp_limit(p.limit, default_limit), None)
        }
        Some(models::SearchQueryPageRequest::OffsetPagination(p)) => (
            p.from.map(|f| f as usize).unwrap_or(0),
            clamp_limit(p.limit, default_limit),
            None,
        ),
        Some(models::SearchQueryPageRequest::CursorForwardPagination(p)) => {
            let after = p.after.as_deref().and_then(decode_cursor);
            let start = after
                .and_then(|k| sorted.iter().position(|(key, _)| *key == k).map(|i| i + 1))
                .unwrap_or(0);
            (start, clamp_limit(p.limit, default_limit), None)
        }
        Some(models::SearchQueryPageRequest::CursorBackwardPagination(p)) => {
            let before = p.before.as_deref().and_then(decode_cursor);
            (0usize, clamp_limit(p.limit, default_limit), before)
        }
        None => (0usize, default_limit, None),
    };

    let window: Vec<(u64, T)> = if let Some(before_key) = backward_before {
        // Backward paging: take the `limit` items immediately preceding the
        // cursor (keeping ascending order within the page).
        let end = sorted
            .iter()
            .position(|(key, _)| *key == before_key)
            .unwrap_or(0);
        let begin = end.saturating_sub(limit);
        sorted.into_iter().take(end).skip(begin).collect()
    } else {
        sorted.into_iter().skip(start).take(limit).collect()
    };

    let start_cursor = window
        .first()
        .map(|(k, _)| types::Nullable::Present(encode_cursor(*k)))
        .unwrap_or(types::Nullable::Null);
    let end_cursor = window
        .last()
        .map(|(k, _)| types::Nullable::Present(encode_cursor(*k)))
        .unwrap_or(types::Nullable::Null);

    Page {
        items: window.into_iter().map(|(_, item)| item).collect(),
        response: models::SearchQueryPageResponse {
            total_items: total,
            has_more_total_items: false,
            start_cursor,
            end_cursor,
        },
    }
}

/// Encodes an entity key as an opaque page cursor: nine big-endian bytes (a
/// leading zero plus the `u64`) in standard base64 without padding. Nine bytes
/// yield exactly twelve base64 characters, which satisfies the spec's cursor
/// charset (no `=` padding, length a multiple of four).
pub fn encode_cursor(key: u64) -> String {
    let mut bytes = [0u8; 9];
    bytes[1..].copy_from_slice(&key.to_be_bytes());
    base64_encode(&bytes)
}

/// Decodes a cursor produced by [`encode_cursor`] back into an entity key,
/// returning `None` if it is malformed.
pub fn decode_cursor(cursor: &str) -> Option<u64> {
    let bytes = base64_decode(cursor)?;
    if bytes.len() != 9 {
        return None;
    }
    let mut key = [0u8; 8];
    key.copy_from_slice(&bytes[1..]);
    Some(u64::from_be_bytes(key))
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 without padding. Inputs are always nine bytes, so the output
/// is a clean multiple of four characters.
fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64[(n >> 18) & 0x3f] as char);
        out.push(B64[(n >> 12) & 0x3f] as char);
        if chunk.len() > 1 {
            out.push(B64[(n >> 6) & 0x3f] as char);
        }
        if chunk.len() > 2 {
            out.push(B64[n & 0x3f] as char);
        }
    }
    out
}

/// Standard base64 decode (no padding expected), returning `None` on any invalid
/// character or length.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            return None;
        }
        let mut n = 0u32;
        for &c in chunk {
            n = (n << 6) | val(c)?;
        }
        // Left-align when the final chunk is short.
        n <<= 6 * (4 - chunk.len());
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_and_is_padding_free() {
        for key in [0u64, 1, 42, 9, u64::MAX, 1_700_000_000_000] {
            let c = encode_cursor(key);
            assert_eq!(c.len(), 12, "cursor must be 12 chars: {c}");
            assert!(!c.contains('='), "cursor must be padding-free: {c}");
            assert!(
                c.chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '+' || ch == '/'),
                "cursor charset: {c}"
            );
            assert_eq!(decode_cursor(&c), Some(key));
        }
        assert_eq!(decode_cursor("not base64!!"), None);
    }

    #[test]
    fn paginate_defaults_to_100_and_clamps_limit() {
        let rows: Vec<(u64, u64)> = (0..500).map(|k| (k, k)).collect();

        // No page request -> spec default of 100, full total reported.
        let p = paginate(rows.clone(), None);
        assert_eq!(p.items.len(), 100);
        assert_eq!(p.response.total_items, 500);

        // Explicit small limit is honored.
        let req = models::SearchQueryPageRequest::LimitPagination(models::LimitPagination {
            limit: Some(10),
        });
        let p = paginate(rows.clone(), Some(&req));
        assert_eq!(p.items.len(), 10);
        assert_eq!(p.response.total_items, 500);

        // Oversized limit clamps to MAX_LIMIT (10000); only 500 rows exist.
        let big: Vec<(u64, u64)> = (0..20_000).map(|k| (k, k)).collect();
        let req = models::SearchQueryPageRequest::LimitPagination(models::LimitPagination {
            limit: Some(u16::MAX),
        });
        let p = paginate(big, Some(&req));
        assert_eq!(p.items.len(), 10_000);
        assert_eq!(p.response.total_items, 20_000);
    }

    #[test]
    fn like_matches_wildcards() {
        assert!(like_matches("order*", "order-123"));
        assert!(like_matches("*123", "order-123"));
        assert!(like_matches("order-???", "order-123"));
        assert!(like_matches("*", "anything"));
        assert!(!like_matches("order-?", "order-12"));
        assert!(!like_matches("paid", "unpaid"));
        // Escaped metacharacters match literally.
        assert!(like_matches(r"a\*b", "a*b"));
        assert!(!like_matches(r"a\*b", "axb"));
    }

    #[test]
    fn ops_eq_in_and_exists() {
        let o = Ops {
            in_: Some(vec!["A".into(), "B".into()]),
            ..Default::default()
        };
        assert!(o.matches(Some("A")));
        assert!(!o.matches(Some("C")));

        let exists_false = Ops {
            exists: Some(false),
            ..Default::default()
        };
        assert!(exists_false.matches(None));
        assert!(!exists_false.matches(Some("x")));
    }
}
