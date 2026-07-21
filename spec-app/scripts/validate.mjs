// Validate every example manifest against nano-app.schema.json using Ajv
// (draft 2020-12). Examples under examples/ MUST pass; examples under
// examples/invalid/ MUST fail (proving the schema is fail-closed, ADR 0027 §4).
//
// Run: npm run validate   (from spec-app/)
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import Ajv2020 from "ajv/dist/2020.js";

const here = dirname(fileURLToPath(import.meta.url));
const specDir = join(here, "..");
const schema = JSON.parse(readFileSync(join(specDir, "nano-app.schema.json"), "utf8"));

// strictRequired is disabled because we use the standard `oneOf: [{required:
// ["start"]}, {required:["message"]}]` idiom where the properties are declared
// on the parent schema, not the branch — Ajv's strictRequired flags that.
const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
const validate = ajv.compile(schema);

function listJson(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => join(dir, f));
}

let failures = 0;

// Valid fixtures — must pass.
for (const file of listJson(join(specDir, "examples"))) {
  const doc = JSON.parse(readFileSync(file, "utf8"));
  if (validate(doc)) {
    console.log(`  ok    ${relative(file)}`);
  } else {
    failures++;
    console.error(`  FAIL  ${relative(file)} (expected valid):`);
    console.error(ajv.errorsText(validate.errors, { separator: "\n         " }));
  }
}

// Invalid fixtures — must be rejected.
for (const file of listJson(join(specDir, "examples", "invalid"))) {
  const doc = JSON.parse(readFileSync(file, "utf8"));
  if (!validate(doc)) {
    console.log(`  ok    ${relative(file)} (correctly rejected)`);
  } else {
    failures++;
    console.error(`  FAIL  ${relative(file)} (expected INVALID but it passed)`);
  }
}

function relative(file) {
  return file.slice(specDir.length + 1);
}

if (failures > 0) {
  console.error(`\n${failures} manifest fixture(s) did not validate as expected.`);
  process.exit(1);
}
console.log("\nAll manifest fixtures validated as expected.");
