/// Project-name rules, mirrored from the server so the New Project form can
/// validate in real time instead of failing on submit.
///
/// A project's *display name* may contain spaces ("Home Heating"); its files
/// live under a directory-safe *slug* ("home-heating"). The server computes
/// the slug in `console/projects.rs::project_slug`; `slugifyProjectName`
/// below must stay in lockstep with it.

/// Mirrors the server's `is_safe_name` (console/workspace.rs): a string that
/// can be used verbatim as a directory name.
export function isSafeName(name: string): boolean {
  return (
    name.length > 0 &&
    name.length <= 128 &&
    name !== "." &&
    !name.includes("..") &&
    [...name].every((c) => /[A-Za-z0-9_.-]/.test(c))
  );
}

/// Mirrors the server's `project_slug` (console/projects.rs): a name that is
/// already directory-safe is used verbatim (casing, dots and underscores
/// preserved); anything else is lowercased and hyphen-slugged, ASCII
/// alphanumerics only. Returns "" when nothing slug-worthy survives.
export function slugifyProjectName(raw: string): string {
  const name = raw.trim();
  if (isSafeName(name)) return name;
  let out = "";
  let prevDash = false;
  for (const c of name) {
    if (/[A-Za-z0-9]/.test(c)) {
      out += c.toLowerCase();
      prevDash = false;
    } else if (!prevDash && out) {
      out += "-";
      prevDash = true;
    }
  }
  const slug = out.replace(/-+$/, "");
  return isSafeName(slug) ? slug : "";
}

/// Validate a New Project display name. Returns a human-readable error, or
/// `null` when the name is acceptable (an empty name is "incomplete", not an
/// error to shout about). Collisions are checked against both the existing
/// projects' slugs (`name`) and their display names, case-insensitively.
export function validateProjectName(
  raw: string,
  existing: Array<{ name: string; displayName?: string }>,
): string | null {
  const name = raw.trim();
  if (!name) return null;
  if (name.length > 128) return "Too long — 128 characters max.";
  const slug = slugifyProjectName(name);
  if (!slug) return "Needs at least one letter or digit.";
  const taken = existing.some(
    (p) =>
      p.name.toLowerCase() === slug.toLowerCase() ||
      (p.displayName ?? p.name).trim().toLowerCase() === name.toLowerCase(),
  );
  if (taken) return "A project with that name already exists.";
  return null;
}

/// Validate a name that must be directory-safe as typed — used by Import by
/// reference, where the name IS the pointer file's basename and is never
/// slugged. Mirrors the server's `is_safe_name` rejection reasons. Collisions
/// are checked against both existing slugs (`name`) and display names, so an
/// import can't reproduce a title already visible in the Projects list.
export function validateSafeName(
  raw: string,
  existing: Array<{ name: string; displayName?: string }>,
): string | null {
  const name = raw.trim();
  if (!name) return null;
  if (name.length > 128) return "Too long — 128 characters max.";
  if (name === "." || name.includes(".."))
    return "Cannot be “.” or contain “..”.";
  if (/\s/.test(name)) return "No spaces — use dashes or underscores instead.";
  const bad = [...name].find((c) => !/[A-Za-z0-9_.-]/.test(c));
  if (bad)
    return `Invalid character “${bad}”. Use letters, digits, dashes, underscores or dots.`;
  const taken = existing.some(
    (p) =>
      p.name.toLowerCase() === name.toLowerCase() ||
      (p.displayName ?? p.name).trim().toLowerCase() === name.toLowerCase(),
  );
  if (taken) return "A project with that name already exists.";
  return null;
}
