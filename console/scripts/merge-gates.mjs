import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";

const root = new URL("../../", import.meta.url);
const githubActionsAppId = 15368;
const sorted = (values) => [...values].sort();

export function readMergeGates() {
  const agents = readFileSync(new URL("AGENTS.md", root), "utf8");
  const blocks = [...agents.matchAll(/```merge-protocol\s*\n([\s\S]*?)\n```/g)];
  assert.equal(
    blocks.length,
    1,
    "AGENTS.md must contain one canonical merge protocol",
  );
  return {
    protocol: JSON.parse(blocks[0][1]),
    workflow: yaml.load(
      readFileSync(new URL(".github/workflows/ci.yml", root), "utf8"),
    ),
    mergify: yaml.load(readFileSync(new URL(".mergify.yml", root), "utf8")),
  };
}

export function assertLocalGates({ protocol, workflow, mergify }) {
  const names = protocol.requiredChecks.map(({ name }) => name);
  assert.ok(names.length > 0, "The merge protocol must require CI checks");
  assert.equal(
    new Set(names).size,
    names.length,
    "Required checks must be unique",
  );
  const jobs = Object.values(workflow.jobs);
  for (const { name, acceptedConclusions } of protocol.requiredChecks) {
    assert.ok(
      typeof name === "string" && name.trim() === name && name.length > 0,
    );
    assert.ok(
      !name.startsWith("@"),
      `${name} is interpreted as @app/check by Mergify`,
    );
    assert.equal(
      jobs.filter((job) => job.name === name).length,
      1,
      `Required check ${name} must identify exactly one CI job`,
    );
    assert.deepEqual(
      sorted(acceptedConclusions),
      ["skipped", "success"],
      `${name} must retain success-or-skipped enforcement`,
    );
  }
  const gates = mergify.merge_protections.filter(
    ({ name }) => name === "required CI checks",
  );
  assert.equal(
    gates.length,
    1,
    "Mergify must have one required CI checks gate",
  );
  assert.deepEqual(gates[0].if, ["base=main"]);
  const expected = protocol.requiredChecks.map(
    ({ name, acceptedConclusions }) =>
      sorted(
        acceptedConclusions.map((conclusion) => `check-${conclusion}=${name}`),
      ).join("\n"),
  );
  const actual = gates[0].success_conditions.map((condition) => {
    assert.deepEqual(Object.keys(condition), ["or"]);
    return sorted(condition.or).join("\n");
  });
  assert.deepEqual(
    sorted(actual),
    sorted(expected),
    "Mergify required checks must match AGENTS.md",
  );
}

export function requiredStatusChecks(protocol, current) {
  assert.equal(
    typeof current.strict,
    "boolean",
    "Expected a GitHub required-status-checks response",
  );
  return {
    strict: current.strict,
    checks: protocol.requiredChecks.map(({ name }) => ({
      context: name,
      app_id: githubActionsAppId,
    })),
  };
}

export function assertBranchProtection(protocol, current) {
  const expected = requiredStatusChecks(protocol, current);
  assert.ok(
    Array.isArray(current.checks),
    "Branch protection response must contain a checks array",
  );
  const identities = (checks) =>
    sorted(checks.map(({ context, app_id }) => `${app_id}:${context}`));
  assert.deepEqual(
    identities(current.checks),
    identities(expected.checks),
    "Live branch protection must match AGENTS.md and be pinned to GitHub Actions",
  );
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const [mode, path, ...extra] = process.argv.slice(2);
  assert.ok(
    extra.length === 0 &&
      ((mode === undefined && path === undefined) ||
        (["--check-protection", "--protection-update"].includes(mode) && path)),
    "Usage: node console/scripts/merge-gates.mjs [--check-protection|--protection-update FILE]",
  );
  const config = readMergeGates();
  assertLocalGates(config);
  if (mode) {
    const current = JSON.parse(readFileSync(path, "utf8"));
    if (mode === "--check-protection") {
      assertBranchProtection(config.protocol, current);
    } else {
      process.stdout.write(
        `${JSON.stringify(requiredStatusChecks(config.protocol, current), null, 2)}\n`,
      );
    }
  }
}
