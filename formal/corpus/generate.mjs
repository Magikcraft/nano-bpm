#!/usr/bin/env node
// Single-source corpus generator (#1258, #1240 slice 3).
//
// One graph description under formal/corpus/graphs/<Id>.json is the SINGLE
// source for every downstream artifact of that process graph:
//
//   - formal/tla/<Module>.tla     one per registered spec family (TokenFlow ->
//                                 MC*, ZeebeTokenFlow -> ZMC*), EXTENDS that
//                                 family's base module. This removes the
//                                 hand-maintained MC/ZMC twin duplication.
//   - formal/corpus/bpmn/<Id>.bpmn      BPMN 2.0 XML WITH a generated BPMNDI
//                                 diagram (bounds + waypoints) so it renders.
//   - formal/corpus/scenarios/<Id>.json a scenario script: job-completion order,
//                                 message correlation, timer ticks.
//
//   node formal/corpus/generate.mjs            # (re)write every artifact
//   node formal/corpus/generate.mjs --check    # regenerate to a temp dir and
//                                              # fail on drift (the CI guard)
//
// The generated .tla stay registered through the #1226 per-spec registry
// (specs/TokenFlow.spec globs MC*.tla, specs/ZeebeTokenFlow.spec globs ZMC*.tla)
// with no edit to the descriptors or check.sh, so `formal/tla/check.sh` keeps
// model-checking them. The committed artifacts are a derived product: check.sh
// runs `--check` on a full model-check, the same drift discipline as
// formal/parity and gen-traces.sh, so a forgotten regeneration fails CI.
import fs from 'node:fs'

import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const repoTla = path.join(here, '..', 'tla')

// The spec families a graph may target: the base TLA+ module a generated model
// EXTENDS, and the check.sh glob that keeps it registered.
export const FAMILIES = {
  TokenFlow: { base: 'TokenFlow', glob: 'MC*.tla' },
  ZeebeTokenFlow: { base: 'ZeebeTokenFlow', glob: 'ZMC*.tla' }
}

// node kind -> BPMN element + diagram footprint. `task` becomes a serviceTask
// whose job type is the node id (the same key the trace tables and scenario
// jobs use).
const KIND = {
  start: { el: 'startEvent', w: 36, h: 36 },
  end: { el: 'endEvent', w: 36, h: 36 },
  task: { el: 'serviceTask', w: 110, h: 80 },
  and: { el: 'parallelGateway', w: 50, h: 50 },
  or: { el: 'inclusiveGateway', w: 50, h: 50 },
  xor: { el: 'exclusiveGateway', w: 50, h: 50 }
}
// CASE arm order for MCKind; `task` is the OTHER fallback.
const KIND_ORDER = ['start', 'end', 'and', 'or', 'xor']

export function loadGraphs () {
  const dir = path.join(here, 'graphs')
  return fs.readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .sort()
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')))
}

// ---------------------------------------------------------------------------
// TLA+ model
export function tlaFor (graph, familyName) {
  const fam = graph.families[familyName]
  const nodes = Object.keys(graph.nodes)
  const rule = '-'.repeat(30)
  const lines = []
  lines.push(`${rule} MODULE ${fam.module} ${rule}`)
  lines.push('(* GENERATED from formal/corpus/graphs/' + graph.id +
    '.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.')
  lines.push('   Edit the graph source and re-run the generator (see formal/corpus/README.md).')
  lines.push('')
  for (const c of fam.comment) lines.push('   ' + c)
  lines.push('*)')
  lines.push(`EXTENDS ${FAMILIES[familyName].base}`)
  lines.push('')
  lines.push(`MCNodes == {${nodes.map((n) => `"${n}"`).join(', ')}}`)

  // MCKind CASE, grouped by kind in a fixed order, task as OTHER.
  const byKind = {}
  for (const n of nodes) (byKind[graph.nodes[n]] ??= []).push(n)
  const arms = []
  for (const k of KIND_ORDER) {
    const ns = byKind[k]
    if (!ns || ns.length === 0) continue
    const sel = ns.length === 1 ? `n = "${ns[0]}"` : `n \\in {${ns.map((n) => `"${n}"`).join(', ')}}`
    arms.push(`${sel} -> "${k}"`)
  }
  arms.push('OTHER -> "task"')
  lines.push('MCKind   == [n \\in MCNodes |->')
  arms.forEach((a, i) => {
    lines.push(`              ${i === 0 ? 'CASE ' : '  [] '}${a}${i === arms.length - 1 ? ']' : ''}`)
  })

  lines.push(`MCStart  == "${graph.start}"`)
  const edgeLines = graph.edges.map((e) => `${e.id} |-> <<"${e.from}", "${e.to}">>`)
  lines.push('MCEdges  == [' + edgeLines.map((l, i) =>
    (i === 0 ? '' : '             ') + l + (i === edgeLines.length - 1 ? ']' : ',')
  ).join('\n'))
  lines.push('MCFlows  == DOMAIN MCEdges')
  lines.push('MCSrc    == [f \\in MCFlows |-> MCEdges[f][1]]')
  lines.push('MCTgt    == [f \\in MCFlows |-> MCEdges[f][2]]')
  lines.push('='.repeat(77))
  return lines.join('\n') + '\n'
}

// ---------------------------------------------------------------------------
// Layered layout shared by the BPMN diagram (ranks/rows, mirrors the
// processos "Sugiyama-lite" pass so a graph reads left-to-right).
function layout (graph) {
  const nodes = Object.keys(graph.nodes)
  const n = nodes.length
  const rank = Object.fromEntries(nodes.map((id) => [id, 0]))
  for (let it = 0; it < n + 2; it++) {
    for (const e of graph.edges) {
      const nr = Math.min(rank[e.from] + 1, n)
      if (rank[e.to] < nr) rank[e.to] = nr
    }
  }
  const preds = {}
  for (const e of graph.edges) (preds[e.to] ??= []).push(e.from)
  const rowOf = {}
  const maxRank = Math.max(...nodes.map((id) => rank[id]))
  const desired = (id) => {
    const ps = (preds[id] ?? []).filter((p) => rowOf[p] !== undefined)
    if (ps.length === 0) return 0
    return ps.reduce((s, p) => s + rowOf[p], 0) / ps.length
  }
  for (let r = 0; r <= maxRank; r++) {
    const here2 = nodes.filter((id) => rank[id] === r)
      .sort((a, b) => (desired(a) - desired(b)) || (a < b ? -1 : a > b ? 1 : 0))
    const used = new Set()
    for (const id of here2) {
      let row = Math.max(0, Math.round(desired(id)))
      while (used.has(row)) row++
      used.add(row)
      rowOf[id] = row
    }
  }
  const OX = 160; const OY = 100; const COL = 190; const ROW = 110
  const rects = {}
  for (const id of nodes) {
    const { w, h } = KIND[graph.nodes[id]]
    const cx = OX + rank[id] * COL
    const cy = OY + rowOf[id] * ROW
    rects[id] = { x: cx - w / 2, y: cy - h / 2, w, h, cx, cy }
  }
  return rects
}

function waypoints (s, t) {
  const sx = s.x + s.w; const sy = s.cy
  const tx = t.x; const ty = t.cy
  if (Math.abs(sy - ty) < 0.5) return [[sx, sy], [tx, ty]]
  const G = 18
  return [[sx, sy], [tx - G, sy], [tx - G, ty], [tx, ty]]
}

const xmlEscape = (s) => String(s)
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')

// ---------------------------------------------------------------------------
// BPMN 2.0 XML + BPMNDI
export function bpmnFor (graph) {
  const nodes = Object.keys(graph.nodes)
  const outFlows = {}; const inFlows = {}
  for (const e of graph.edges) {
    (outFlows[e.from] ??= []).push(e)
    ;(inFlows[e.to] ??= []).push(e)
  }
  const isSplit = (id) => ['xor', 'or'].includes(graph.nodes[id]) && (outFlows[id]?.length ?? 0) > 1

  const L = []
  L.push('<?xml version="1.0" encoding="UTF-8"?>')
  L.push('<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"')
  L.push('    xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"')
  L.push('    xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"')
  L.push('    xmlns:di="http://www.omg.org/spec/DD/20100524/DI"')
  L.push('    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')
  L.push('    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"')
  L.push(`    id="Definitions_${xmlEscape(graph.id)}" targetNamespace="http://nanobpm.io/corpus">`)
  L.push(`  <bpmn:process id="${xmlEscape(graph.id)}" isExecutable="true">`)

  for (const id of nodes) {
    const kind = graph.nodes[id]
    const el = KIND[kind].el
    const attrs = [`id="${xmlEscape(id)}"`]
    // A diverging gateway declares NO default flow: a default is only ever taken
    // when no outgoing condition matches, but every branch here is conditioned,
    // so a default would be provably unreachable and misrepresent the graph. An
    // INCLUSIVE (`or`) split conditions every branch `=true` (all are taken); an
    // EXCLUSIVE (`xor`) split conditions every branch too — exactly one `=true`
    // and the rest `=false` — so one deterministic route is taken and the other
    // branch is still faithfully present (see the sequenceFlow loop below).
    const kids = []
    for (const e of inFlows[id] ?? []) kids.push(`      <bpmn:incoming>${xmlEscape(e.id)}</bpmn:incoming>`)
    for (const e of outFlows[id] ?? []) kids.push(`      <bpmn:outgoing>${xmlEscape(e.id)}</bpmn:outgoing>`)
    if (kind === 'task') {
      kids.unshift(
        '      <bpmn:extensionElements>',
        `        <zeebe:taskDefinition type="${xmlEscape(id)}"/>`,
        '      </bpmn:extensionElements>')
    }
    if (kids.length === 0) {
      L.push(`    <bpmn:${el} ${attrs.join(' ')}/>`)
    } else {
      L.push(`    <bpmn:${el} ${attrs.join(' ')}>`)
      L.push(...kids)
      L.push(`    </bpmn:${el}>`)
    }
  }

  for (const e of graph.edges) {
    // A diverging gateway conditions ALL its outgoing flows and declares no
    // default. An inclusive (`or`) split makes every branch `=true`, so every
    // branch is taken and matches the scenario's scheduled tasks. An exclusive
    // (`xor`) split takes exactly ONE branch: the first outgoing flow is `=false`
    // and the rest `=true`, so the engine deterministically takes the first true
    // flow while the `=false` branch stays faithfully present — rather than an
    // unreachable `default` an all-`=true` gateway would never fall back to. A
    // non-split flow carries no condition.
    let cond = null
    if (isSplit(e.from)) {
      cond = (graph.nodes[e.from] === 'xor' && outFlows[e.from][0].id === e.id) ? '=false' : '=true'
    }
    if (cond) {
      L.push(`    <bpmn:sequenceFlow id="${xmlEscape(e.id)}" sourceRef="${xmlEscape(e.from)}" targetRef="${xmlEscape(e.to)}">`)
      L.push(`      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">${cond}</bpmn:conditionExpression>`)
      L.push('    </bpmn:sequenceFlow>')
    } else {
      L.push(`    <bpmn:sequenceFlow id="${xmlEscape(e.id)}" sourceRef="${xmlEscape(e.from)}" targetRef="${xmlEscape(e.to)}"/>`)
    }
  }
  L.push('  </bpmn:process>')

  // BPMNDI
  const rects = layout(graph)
  const fmt = (v) => String(Math.round(v))
  L.push('  <bpmndi:BPMNDiagram id="BPMNDiagram_1">')
  L.push(`    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="${xmlEscape(graph.id)}">`)
  for (const id of nodes) {
    const b = rects[id]
    const marker = ['xor', 'or'].includes(graph.nodes[id]) ? ' isMarkerVisible="true"' : ''
    L.push(`      <bpmndi:BPMNShape id="${xmlEscape(id)}_di" bpmnElement="${xmlEscape(id)}"${marker}>`)
    L.push(`        <dc:Bounds x="${fmt(b.x)}" y="${fmt(b.y)}" width="${fmt(b.w)}" height="${fmt(b.h)}"/>`)
    L.push('      </bpmndi:BPMNShape>')
  }
  for (const e of graph.edges) {
    L.push(`      <bpmndi:BPMNEdge id="${xmlEscape(e.id)}_di" bpmnElement="${xmlEscape(e.id)}">`)
    for (const [x, y] of waypoints(rects[e.from], rects[e.to])) {
      L.push(`        <di:waypoint x="${fmt(x)}" y="${fmt(y)}"/>`)
    }
    L.push('      </bpmndi:BPMNEdge>')
  }
  L.push('    </bpmndi:BPMNPlane>')
  L.push('  </bpmndi:BPMNDiagram>')
  L.push('</bpmn:definitions>')
  return L.join('\n') + '\n'
}

// ---------------------------------------------------------------------------
// Scenario script: job-completion order, message correlation, timer ticks.
export function scenarioFor (graph) {
  const nodes = Object.keys(graph.nodes)
  const rects = layout(graph) // reuse the rank via layout coordinates
  const OX = 160; const COL = 190
  const rankOf = (id) => Math.round((rects[id].cx - OX) / COL)
  const jobs = nodes.filter((id) => graph.nodes[id] === 'task')
    .map((id) => ({ element: id, jobType: id }))
  // A deterministic, causally-plausible completion order: by rank, then id.
  const completionOrder = jobs
    .map((j) => j.element)
    .sort((a, b) => (rankOf(a) - rankOf(b)) || (a < b ? -1 : a > b ? 1 : 0))
  return JSON.stringify({
    // GENERATED — see formal/corpus/generate.mjs. Edit the graph source instead.
    generated: 'formal/corpus/generate.mjs',
    process: graph.id,
    // Every serviceTask becomes a job; the driver completes them in this order.
    jobs,
    jobCompletionOrder: completionOrder,
    // This corpus has no message catch/throw or timer elements yet, so these
    // are empty. They are part of the scenario contract (#1240 slice 3) for the
    // scenario driver (slice 5) and populate once such elements enter a graph.
    messageCorrelation: [],
    timerTicks: []
  }, null, 2) + '\n'
}

// ---------------------------------------------------------------------------
// Emit / drift-check
function artifacts (graph) {
  const out = []
  for (const familyName of Object.keys(graph.families)) {
    out.push({
      path: path.join(repoTla, `${graph.families[familyName].module}.tla`),
      content: tlaFor(graph, familyName)
    })
  }
  out.push({ path: path.join(here, 'bpmn', `${graph.id}.bpmn`), content: bpmnFor(graph) })
  out.push({ path: path.join(here, 'scenarios', `${graph.id}.json`), content: scenarioFor(graph) })
  return out
}

function main () {
  const check = process.argv.includes('--check')
  const graphs = loadGraphs()
  if (check) {
    let drift = false
    for (const g of graphs) {
      for (const a of artifacts(g)) {
        const rel = path.relative(path.join(here, '..', '..'), a.path)
        const current = fs.existsSync(a.path) ? fs.readFileSync(a.path, 'utf8') : null
        if (current !== a.content) {
          console.error(`FAIL corpus drift: ${rel} (run node formal/corpus/generate.mjs and commit)`)
          drift = true
        }
      }
    }
    // A stray generated file whose graph source was removed must also fail.
    const expected = new Set(graphs.flatMap((g) => artifacts(g).map((a) => a.path)))
    for (const sub of ['bpmn', 'scenarios']) {
      const d = path.join(here, sub)
      if (!fs.existsSync(d)) continue
      for (const f of fs.readdirSync(d)) {
        const p = path.join(d, f)
        if (!expected.has(p)) {
          console.error(`FAIL corpus drift: ${path.relative(path.join(here, '..', '..'), p)} has no graph source (delete it)`)
          drift = true
        }
      }
    }
    // The generated MC*/ZMC* models are written straight into formal/tla beside
    // the hand-written base modules (TokenFlow.tla, ZeebeTokenFlow.tla), so scan
    // them too — otherwise removing/renaming a graph leaves a stale model in the
    // descriptor glob while --check reports clean. Restrict the scan to the
    // families' own generated-model globs (MC*.tla / ZMC*.tla) so a hand-written
    // root module — a new spec's base, say — is never mistaken for a stray
    // generated file. Base modules do not match those globs, so they are skipped
    // for free.
    const globToRe = (glob) =>
      new RegExp('^' + glob.replace(/[.]/g, '\\$&').replace(/\*/g, '.*') + '$')
    const generatedModelRes = Object.values(FAMILIES).map((f) => globToRe(f.glob))
    const isGeneratedModel = (name) => generatedModelRes.some((re) => re.test(name))
    if (fs.existsSync(repoTla)) {
      for (const ent of fs.readdirSync(repoTla, { withFileTypes: true })) {
        if (!ent.isFile() || !isGeneratedModel(ent.name)) continue
        const p = path.join(repoTla, ent.name)
        if (!expected.has(p)) {
          console.error(`FAIL corpus drift: ${path.relative(path.join(here, '..', '..'), p)} has no graph source (delete it)`)
          drift = true
        }
      }
    }
    if (drift) process.exit(1)
    console.log(`ok    corpus artifacts current (${graphs.length} graphs)`)
    return
  }
  fs.mkdirSync(path.join(here, 'bpmn'), { recursive: true })
  fs.mkdirSync(path.join(here, 'scenarios'), { recursive: true })
  for (const g of graphs) {
    for (const a of artifacts(g)) {
      fs.mkdirSync(path.dirname(a.path), { recursive: true })
      fs.writeFileSync(a.path, a.content)
    }
  }
  console.log(`wrote artifacts for ${graphs.length} graphs`)
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main()
}
