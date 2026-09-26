// Tests for the single-source corpus generator (#1258). Structural + drift
// checks that do not need a JVM (the TLA+ verdicts are covered by check.sh).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadGraphs, tlaFor, bpmnFor, scenarioFor, FAMILIES } from './generate.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const graphs = loadGraphs()

test('every graph has a well-formed structure', () => {
  assert.ok(graphs.length > 0)
  for (const g of graphs) {
    assert.equal(typeof g.id, 'string')
    assert.ok(g.nodes[g.start], `${g.id}: start ${g.start} is a node`)
    const ids = new Set()
    for (const e of g.edges) {
      assert.ok(!ids.has(e.id), `${g.id}: duplicate flow id ${e.id}`)
      ids.add(e.id)
      assert.ok(g.nodes[e.from], `${g.id}: edge ${e.id} from unknown ${e.from}`)
      assert.ok(g.nodes[e.to], `${g.id}: edge ${e.id} to unknown ${e.to}`)
    }
    for (const kind of Object.values(g.nodes)) {
      assert.ok(['start', 'end', 'task', 'and', 'or', 'xor'].includes(kind))
    }
    assert.ok(Object.keys(g.families).length > 0)
    for (const fam of Object.keys(g.families)) assert.ok(FAMILIES[fam], `${g.id}: unknown family ${fam}`)
  }
})

test('TokenFlow models are named MC*, ZeebeTokenFlow models ZMC*', () => {
  for (const g of graphs) {
    for (const [fam, meta] of Object.entries(g.families)) {
      if (fam === 'TokenFlow') assert.match(meta.module, /^MC/)
      if (fam === 'ZeebeTokenFlow') assert.match(meta.module, /^ZMC/)
    }
  }
})

test('generated .tla EXTENDS the family base and defines the model vocabulary', () => {
  for (const g of graphs) {
    for (const [fam, meta] of Object.entries(g.families)) {
      const tla = tlaFor(g, fam)
      assert.match(tla, new RegExp(`MODULE ${meta.module} `))
      assert.match(tla, new RegExp(`EXTENDS ${FAMILIES[fam].base}`))
      for (const decl of ['MCNodes', 'MCKind', 'MCStart', 'MCEdges', 'MCFlows', 'MCSrc', 'MCTgt']) {
        assert.match(tla, new RegExp(`${decl}\\s`), `${meta.module} declares ${decl}`)
      }
      // every node and every flow id appears in the model
      for (const n of Object.keys(g.nodes)) assert.ok(tla.includes(`"${n}"`))
      for (const e of g.edges) assert.ok(tla.includes(`${e.id} |->`))
    }
  }
})

test('a graph shared by both families emits one BPMN, one scenario, two .tla', () => {
  const shared = graphs.find((g) => Object.keys(g.families).length === 2)
  assert.ok(shared, 'expected at least one MC/ZMC shared graph')
  const modules = Object.values(shared.families).map((f) => f.module)
  assert.equal(new Set(modules).size, 2)
  // The single BPMN/scenario is driven off the graph id, not a family.
  assert.ok(bpmnFor(shared).includes(`id="${shared.id}"`))
  assert.ok(JSON.parse(scenarioFor(shared)).process === shared.id)
})

test('BPMN carries DI (a shape per node, an edge per flow) and is executable', () => {
  for (const g of graphs) {
    const xml = bpmnFor(g)
    assert.match(xml, /isExecutable="true"/)
    assert.match(xml, /<bpmndi:BPMNDiagram/)
    for (const n of Object.keys(g.nodes)) {
      assert.ok(xml.includes(`bpmnElement="${n}"`), `${g.id}: DI shape for ${n}`)
    }
    for (const e of g.edges) {
      assert.ok(xml.includes(`bpmnElement="${e.id}"`), `${g.id}: DI edge for ${e.id}`)
      assert.ok(xml.includes('<di:waypoint'), `${g.id}: waypoints present`)
    }
    // every serviceTask has a job definition
    for (const [n, k] of Object.entries(g.nodes)) {
      if (k === 'task') assert.ok(xml.includes(`type="${n}"`), `${g.id}: job def for ${n}`)
    }
  }
})

test('scenario lists a job per task, an ordering, and message/timer slots', () => {
  for (const g of graphs) {
    const s = JSON.parse(scenarioFor(g))
    const tasks = Object.entries(g.nodes).filter(([, k]) => k === 'task').map(([n]) => n)
    assert.deepEqual(s.jobs.map((j) => j.element).sort(), [...tasks].sort())
    assert.deepEqual([...s.jobCompletionOrder].sort(), [...tasks].sort())
    assert.ok(Array.isArray(s.messageCorrelation))
    assert.ok(Array.isArray(s.timerTicks))
  }
})

test('--check passes on the committed artifacts (no drift)', () => {
  // The generator is the source of truth: the committed artifacts must match.
  execFileSync('node', [path.join(here, 'generate.mjs'), '--check'], { stdio: 'pipe' })
})

test('diverging gateways condition every branch and declare no default', () => {
  // Regression (#1258 review): a diverging gateway must not rely on a `default`
  // flow. An inclusive (`or`) split takes EVERY matching flow, so a `default`
  // would make its first branch unreachable while the scenario still schedules
  // that branch's task — so every inclusive flow is `=true` and there is no
  // default. An exclusive (`xor`) split takes exactly ONE branch; declaring a
  // `default` plus all-`=true` conditions makes that default unreachable (the
  // gateway never falls back), so instead every branch is conditioned — exactly
  // one `=true`, the rest `=false` — and no default is declared.
  const outFlowsOf = (g, id) => g.edges.filter((e) => e.from === id)
  // Match ONLY the flow element with this id (up to its own `/>` or closing tag)
  // so a condition on a *later* flow can never be mis-read as this one's.
  const flowCondition = (xml, id) => {
    const m = xml.match(new RegExp(`<bpmn:sequenceFlow id="${id}"[\\s\\S]*?(/>|</bpmn:sequenceFlow>)`))
    assert.ok(m, `flow ${id} present`)
    const c = m[0].match(/<bpmn:conditionExpression[^>]*>([\s\S]*?)<\/bpmn:conditionExpression>/)
    return c ? c[1] : null
  }
  let sawInclusive = false; let sawExclusive = false
  for (const g of graphs) {
    const xml = bpmnFor(g)
    for (const [id, kind] of Object.entries(g.nodes)) {
      if (!['or', 'xor'].includes(kind)) continue
      const outs = outFlowsOf(g, id)
      if (outs.length <= 1) continue // not a split
      const gw = new RegExp(`<bpmn:${kind === 'or' ? 'inclusive' : 'exclusive'}Gateway id="${id}"([^>]*)>`)
      const m = xml.match(gw)
      assert.ok(m, `${g.id}: gateway ${id} present`)
      assert.ok(!/default="/.test(m[1]), `${g.id}: split ${id} must not declare a default`)
      const conds = outs.map((e) => flowCondition(xml, e.id))
      conds.forEach((c, i) => assert.ok(c !== null, `${g.id}: flow ${outs[i].id} needs a condition`))
      if (kind === 'or') {
        sawInclusive = true
        for (const c of conds) assert.equal(c, '=true', `${g.id}: inclusive split ${id} takes every branch`)
      } else {
        sawExclusive = true
        assert.equal(conds.filter((c) => c === '=true').length, 1,
          `${g.id}: exclusive split ${id} takes exactly one branch`)
        assert.ok(conds.every((c) => c === '=true' || c === '=false'),
          `${g.id}: exclusive split ${id} conditions every branch`)
      }
    }
  }
  assert.ok(sawInclusive, 'corpus exercises an inclusive split')
  assert.ok(sawExclusive, 'corpus exercises an exclusive split')
})

test('--check flags a stray generated TLA model with no graph source', () => {
  // Regression (#1258 review): the orphan scan must cover the generated
  // MC*/ZMC* models in formal/tla, not just bpmn/scenarios — a removed graph
  // otherwise leaves a stale model that --check would miss.
  const stray = path.join(here, '..', 'tla', 'MCStrayNoGraphSource.tla')
  fs.writeFileSync(stray, '---- MODULE MCStrayNoGraphSource ----\n====\n')
  try {
    assert.throws(
      () => execFileSync('node', [path.join(here, 'generate.mjs'), '--check'], { stdio: 'pipe' }),
      /MCStrayNoGraphSource\.tla has no graph source/)
  } finally {
    fs.rmSync(stray, { force: true })
  }
})
