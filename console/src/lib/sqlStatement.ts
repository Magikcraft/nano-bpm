// Client mirror of the server data gateway's read-vs-write split (issue #889).
// The SQL tab routes a statement to the row-returning `query` path when it is a
// *read* and to the mutating `exec` path otherwise, and — while the app is
// running — blocks the write path so the UI never surfaces an avoidable 409
// `app_running`. Getting the classification wrong in the permissive direction
// (calling a mutation a "read") is the dangerous failure mode: it both routes a
// write down the read path and slips it past the running-app edit gate. This is
// the single source of truth for that predicate so the routing and the gate can
// never drift from each other.

/**
 * Strip leading whitespace and any leading SQL comments (`-- …` line comments
 * and `/* … *\/` block comments) so the read/write classifier can see the real
 * first token. Only *leading* comments are removed; the remainder is left intact
 * so the body scans (mutating verb inside a CTE, assigning/call-form PRAGMA) are
 * unaffected. An unterminated comment consumes the rest of the input, which then
 * classifies as a (safe) write.
 */
function stripLeadingSqlComments(sql: string): string {
  let s = sql.trimStart();
  for (;;) {
    if (s.startsWith("--")) {
      const nl = s.indexOf("\n");
      s = nl === -1 ? "" : s.slice(nl + 1);
    } else if (s.startsWith("/*")) {
      const end = s.indexOf("*/");
      s = end === -1 ? "" : s.slice(end + 2);
    } else {
      break;
    }
    s = s.trimStart();
  }
  return s;
}

/**
 * Whether `sql` is a pure *read* (safe to send down the `query` path and to
 * allow while the app is running).
 *
 * Conservative by construction — anything not provably read-only is treated as
 * a write:
 * - `SELECT` / `EXPLAIN` are always reads.
 * - A CTE (`WITH …`) is a read only when it terminates in a `SELECT`, i.e. it
 *   carries no mutating verb; `WITH … INSERT/UPDATE/DELETE/REPLACE …` mutates
 *   and must not be classified as a read.
 * - `PRAGMA name` reads a setting, but `PRAGMA name = value` mutates. The call
 *   form `PRAGMA name(x)` is often a read (e.g. `table_info(t)`); we still treat
 *   any `=`/`(` as a write, conservatively erring toward the safe classification
 *   rather than enumerating the read-only call-form pragmas.
 * - Everything else (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, …) is a
 *   write.
 *
 * Leading SQL comments (`-- …` line and `/* … *\/` block) are stripped before
 * the leading-verb check, so a commented read (`-- note\nSELECT 1`) is still a
 * read rather than being misclassified as a write.
 */
export function isReadStatement(sql: string): boolean {
  const s = stripLeadingSqlComments(sql);
  if (/^(select|explain)\b/i.test(s)) return true;
  if (/^with\b/i.test(s)) {
    // A CTE that contains any mutating verb is a data-modifying statement.
    return !/\b(insert|update|delete|replace)\b/i.test(s);
  }
  if (/^pragma\b/i.test(s)) {
    // `=` (assignment) or `(` (call form) makes the PRAGMA a write.
    return !/[=(]/.test(s);
  }
  return false;
}
