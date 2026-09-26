// Tests for the single-source corpus generator (#1258). Structural + drift
// checks that do not need a JVM (the TLA+ verdicts are covered by check.sh).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadGraphs, tlaFor, bpmnFor, scenarioFor, FAMILIES, takenFlows, reachableNodes, validateGraphs, SAFE_ID } from './generate.mjs'

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

test('scenario schedules exactly the REACHABLE tasks and stays executable with the BPMN', () => {
  for (const g of graphs) {
    const s = JSON.parse(scenarioFor(g))
    const reach = reachableNodes(g)
    const reachableTasks = Object.entries(g.nodes)
      .filter(([n, k]) => k === 'task' && reach.has(n)).map(([n]) => n)
    // The job set is exactly the tasks a token can reach under the deterministic
    // route — no task behind a dead exclusive branch (#1258 review), or the
    // runner would wait for a job the BPMN never creates.
    assert.deepEqual(s.jobs.map((j) => j.element).sort(), [...reachableTasks].sort())
    assert.deepEqual([...s.jobCompletionOrder].sort(), [...reachableTasks].sort())
    // Executable-as-a-pair: every scheduled job element is reachable in the BPMN.
    for (const j of s.jobs) assert.ok(reach.has(j.element), `${g.id}: scheduled ${j.element} is reachable`)
    assert.ok(Array.isArray(s.messageCorrelation))
    assert.ok(Array.isArray(s.timerTicks))
  }
})

test('an XOR split into task branches keeps the scenario and BPMN executable as a pair', () => {
  // Regression (#1258 review): a `xor` split whose branches contain tasks must
  // not schedule a job for the dead branch's task — the BPMN routes a token down
  // exactly ONE branch, so a job on the other branch is never created and the
  // runner would deadlock. Both artifacts derive from the same `takenFlows`.
  const graph = {
    id: 'XorTaskBranches',
    start: 'S',
    nodes: { S: 'start', X: 'xor', A: 'task', B: 'task', E: 'end' },
    edges: [
      { id: 'f1', from: 'S', to: 'X' },
      { id: 'f2', from: 'X', to: 'A' },
      { id: 'f3', from: 'X', to: 'B' },
      { id: 'f4', from: 'A', to: 'E' },
      { id: 'f5', from: 'B', to: 'E' }
    ],
    families: { TokenFlow: { module: 'MCXorTaskBranches', comment: [] } }
  }
  const taken = takenFlows(graph)
  // Exactly one of the two task branches is live; the other stays present but dead.
  const live = ['f2', 'f3'].filter((f) => taken.has(f))
  assert.equal(live.length, 1, 'exactly one xor task branch is taken')
  const s = JSON.parse(scenarioFor(graph))
  const reach = reachableNodes(graph)
  // Only the reachable task is scheduled — not both.
  assert.equal(s.jobs.length, 1, 'only the reachable task is scheduled')
  for (const j of s.jobs) assert.ok(reach.has(j.element), `scheduled ${j.element} must be reachable`)
  const deadTask = s.jobs[0].element === 'A' ? 'B' : 'A'
  assert.ok(!reach.has(deadTask), 'the dead branch task is not reachable and not scheduled')
  // The BPMN still emits both branches (one `=true`, one `=false`) and no default.
  const xml = bpmnFor(graph)
  assert.match(xml, /<bpmn:conditionExpression[^>]*>=true<\/bpmn:conditionExpression>/)
  assert.match(xml, /<bpmn:conditionExpression[^>]*>=false<\/bpmn:conditionExpression>/)
  assert.ok(!/<bpmn:exclusiveGateway id="X"[^>]*default="/.test(xml), 'no default flow')
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

test('the committed corpus passes validation (safe ids, unique ids/modules)', () => {
  // loadGraphs() already runs validateGraphs(); make the guarantee explicit so a
  // future graph that breaks the grammar or collides is caught here too.
  assert.doesNotThrow(() => validateGraphs(loadGraphs()))
  assert.match('MC1_Ok', SAFE_ID)
  assert.doesNotMatch('1bad', SAFE_ID)
  assert.doesNotMatch('a b', SAFE_ID)
})

test('validateGraphs rejects duplicate graph ids and duplicate family modules', () => {
  // Regression (#1258 review): artifacts() keys outputs by graph id + family
  // module and --check dedups the expected path set, so a duplicate id/module
  // silently overwrites one graph while --check still reports clean. Reject it.
  const mk = (id, module) => ({
    id, start: 'S', nodes: { S: 'start', E: 'end' },
    edges: [{ id: 'f1', from: 'S', to: 'E' }],
    families: { TokenFlow: { module, comment: [] } }
  })
  assert.throws(() => validateGraphs([mk('Dup', 'MCa'), mk('Dup', 'MCb')]), /duplicate graph id/)
  assert.throws(() => validateGraphs([mk('A', 'MCsame'), mk('B', 'MCsame')]), /duplicate module/)
  assert.doesNotThrow(() => validateGraphs([mk('A', 'MCa'), mk('B', 'MCb')]))
})

test('validateGraphs rejects ids that would break generated TLA+/BPMN', () => {
  // Regression (#1258 review): node/flow ids and endpoints are interpolated into
  // TLA+ (flow ids as BARE record fields, nodes as string literals). An id with
  // `"`, `\` or a newline yields invalid TLA+. Reject out-of-grammar ids so a
  // valid graph source cannot produce an unusable generated model.
  const base = () => ({
    id: 'Ok', start: 'S', nodes: { S: 'start', E: 'end' },
    edges: [{ id: 'f1', from: 'S', to: 'E' }],
    families: { TokenFlow: { module: 'MCOk', comment: [] } }
  })
  const withNode = (bad) => { const g = base(); g.nodes = { S: 'start', [bad]: 'end' }; g.start = 'S'; g.edges = [{ id: 'f1', from: 'S', to: bad }]; return g }
  assert.throws(() => validateGraphs([withNode('E"vil')]), /not a safe identifier/)
  assert.throws(() => validateGraphs([withNode('E\\x')]), /not a safe identifier/)
  assert.throws(() => validateGraphs([withNode('E\nx')]), /not a safe identifier/)
  const badFlow = base(); badFlow.edges = [{ id: 'f 1', from: 'S', to: 'E' }]
  assert.throws(() => validateGraphs([badFlow]), /flow id .* not a safe identifier/)
  const badModule = base(); badModule.families = { TokenFlow: { module: 'MC Ok', comment: [] } }
  assert.throws(() => validateGraphs([badModule]), /module .* not a safe identifier/)
  const dupFlow = base(); dupFlow.nodes = { S: 'start', A: 'task', E: 'end' }
  dupFlow.edges = [{ id: 'f1', from: 'S', to: 'A' }, { id: 'f1', from: 'A', to: 'E' }]
  assert.throws(() => validateGraphs([dupFlow]), /duplicate flow id/)
})
