//! **Field solver (v2, experimental)** — unified charged-particle simulation
//! where nodes and edges are both particles in the same 2D field.
//!
//! ## Model
//!
//! * **Node particle** — one per BPMN element. Movable, rectangular collision
//!   shape. Carries a `Charges` vector derived from the semantic annotations
//!   (flow-kind one-hot, cluster memberships, graph-distance from start).
//! * **Edge chain** — each BPMN sequence flow becomes `N` intermediate
//!   particles connected by springs, with endpoints anchored to the perimeter
//!   of the source/target node rectangle. Intermediates inherit their edge's
//!   inherited flow-kind charge (from source ∪ target) so same-kind edges
//!   bundle into shared corridors.
//! * **Label particle** *(future)* — a tiny particle sprung to its edge
//!   midpoint, repelled by everything else. Not implemented in this pass.
//!
//! ## Forces
//!
//! For every pair of particles `(a, b)`:
//! * **Flow-kind alignment** — attraction proportional to `dot(a.flow, b.flow)`;
//!   repulsion proportional to `1 − dot(...)`. Same-kind pulls together;
//!   cross-kind pushes apart.
//! * **Cluster attraction** — for each cluster both belong to, an extra pull
//!   scaled by their shared cluster affinity.
//! * **Hard overlap repulsion** — for node-node pairs only, a strong inverse-
//!   square push when their bounding rects overlap.
//! * **Left-to-right drift** — a constant `+x` force proportional to a
//!   particle's `graph_dist` charge (longest-path from a source event). This
//!   is what encodes the BPMN reading convention as a physical law rather
//!   than a hard constraint.
//!
//! ## Integrator + convergence
//!
//! Semi-implicit Verlet with velocity damping. Kinetic energy is tracked per
//! step; convergence when KE < `EPS` for `SETTLED_STEPS` in a row. If a
//! particle's position variance over a rolling window exceeds a threshold
//! while global KE stays high (it's oscillating not settling), we pin it in
//! place and continue.
//!
//! Determinism: init positions come from the row-bias solver's output plus a
//! seeded RNG (currently unseeded — the init is deterministic-by-construction
//! since we only jitter zero-degree ties).

use std::collections::HashMap;

use nanobpmn_engine_core::{Element, ProcessDefinition};

use super::schema::{FlowKind, SemanticAnnotations};

// --- Force constants -------------------------------------------------------
//
// All tuned by hand against `fixtures/layout/tiny.bpmn`. These are the *only*
// magic numbers in the solver — every other threshold is derived from these
// or from node dimensions. If the layout misbehaves, this is the first place
// to look.
const K_ATTRACT: f64 = 40.0;
const K_REPEL: f64 = 4000.0;
const K_CLUSTER: f64 = 60.0;
const K_HARD_OVERLAP: f64 = 8000.0;
const K_LTR_DRIFT: f64 = 30.0;
const K_EDGE_SPRING: f64 = 6.0;
const EDGE_REST_LEN: f64 = 30.0;

const DT: f64 = 0.05;
const DAMPING: f64 = 0.85;
const EPS_KINETIC: f64 = 0.5;
const SETTLED_STEPS: usize = 20;
const MAX_STEPS: usize = 2000;
const OSCILLATION_WINDOW: usize = 40;
const OSCILLATION_VAR: f64 = 900.0; // px² — if position variance exceeds this over the window, pin

const NODE_W: f64 = 110.0;
const NODE_H: f64 = 80.0;
const EDGE_SEGMENTS: usize = 4;

/// Which flavour of particle. Different kinds obey different force rules
/// (nodes have hard-overlap repulsion; edge segments have spring forces to
/// their neighbours; endpoint segments are pinned to a node's perimeter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticleKind {
    Node,
    EdgeSegment {
        /// Index in the parent edge's chain (0..N inclusive; 0 and N are endpoints).
        index: usize,
        /// Total chain length (so we can detect endpoints as `index == 0 || index == last`).
        chain_len: usize,
    },
}

/// One particle in the field. `charges` participates in pairwise forces;
/// `fixed=true` freezes the particle in place (used for pinned oscillators
/// and for node-anchored edge endpoints).
#[derive(Debug, Clone)]
pub struct Particle {
    pub id: String,
    pub kind: ParticleKind,
    pub pos: (f64, f64),
    pub vel: (f64, f64),
    pub mass: f64,
    pub fixed: bool,
    pub charges: Charges,
    /// For nodes only — the bounding rect (width, height). Zero for edge segments.
    pub size: (f64, f64),
    /// For edge segments only — the parent edge's id, and which node ids the
    /// endpoints must stay anchored to. `None` for nodes.
    pub edge_anchors: Option<EdgeAnchors>,
    /// Rolling window of recent positions for oscillation detection.
    history: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct EdgeAnchors {
    pub edge_id: String,
    pub source_node: String,
    pub target_node: String,
}

/// Multi-dimensional charge vector — this is where semantic annotations enter
/// the physics. Fields sum independently in the force calculation.
#[derive(Debug, Clone, Default)]
pub struct Charges {
    /// One-hot-ish over [primary, exception, escalation, compensation]. Values
    /// can be fractional so a node can be, e.g., 70% primary + 30% exception.
    pub flow_kind: [f64; 4],
    /// Membership share per cluster id (0..1).
    pub cluster: HashMap<String, f64>,
    /// Normalized longest-path distance from a source event, 0..1. Higher =
    /// further right (a constant +x drift is applied proportional to this).
    pub graph_dist: f64,
}

impl Charges {
    pub fn flow_kind_from(kind: Option<FlowKind>) -> [f64; 4] {
        let mut v = [0.0; 4];
        match kind {
            Some(FlowKind::Primary) => v[0] = 1.0,
            Some(FlowKind::Exception) => v[1] = 1.0,
            Some(FlowKind::Escalation) => v[2] = 1.0,
            Some(FlowKind::Compensation) => v[3] = 1.0,
            None => {}
        }
        v
    }
}

/// A fully-simulated field: node positions and edge waypoints ready for the
/// DI emitter or the debug SVG.
#[derive(Debug, Clone)]
pub struct FieldOutput {
    pub nodes: HashMap<String, (f64, f64)>,
    /// `edge_id -> polyline of waypoints`. First and last waypoints sit on
    /// the source/target node perimeter respectively.
    pub edges: HashMap<String, Vec<(f64, f64)>>,
    /// Diagnostic — steps until convergence, kinetic energy trace, pinned particle ids.
    pub diagnostics: SimDiagnostics,
}

#[derive(Debug, Clone, Default)]
pub struct SimDiagnostics {
    pub steps: usize,
    pub final_kinetic_energy: f64,
    pub pinned_by_oscillation: Vec<String>,
    pub converged: bool,
}

/// Run the field simulation for `def` under `ann`. Deterministic given the
/// same inputs (init positions are derived from graph distance, no RNG).
pub fn simulate(def: &ProcessDefinition, ann: &SemanticAnnotations) -> FieldOutput {
    let mut particles = init_particles(def, ann);
    let mut ke_streak: usize = 0;
    let mut diag = SimDiagnostics::default();

    for step in 0..MAX_STEPS {
        let forces = compute_all_forces(&particles);
        let ke = integrate(&mut particles, &forces);
        diag.final_kinetic_energy = ke;
        diag.steps = step + 1;

        // Oscillation detection — nudge stuck particles by pinning them so the
        // rest of the field can settle around them.
        if step > 0 && step % OSCILLATION_WINDOW == 0 {
            pin_oscillators(&mut particles, &mut diag.pinned_by_oscillation);
        }

        if ke < EPS_KINETIC {
            ke_streak += 1;
            if ke_streak >= SETTLED_STEPS {
                diag.converged = true;
                break;
            }
        } else {
            ke_streak = 0;
        }
    }

    extract_output(&particles, diag)
}

// --- Initialization --------------------------------------------------------

fn init_particles(def: &ProcessDefinition, ann: &SemanticAnnotations) -> Vec<Particle> {
    let node_kind = flatten_flow_kinds(ann);
    let cluster_membership = flatten_cluster_membership(ann);
    let graph_dist = compute_graph_distance(def);

    let mut particles: Vec<Particle> = Vec::new();

    // --- Node particles --------------------------------------------------
    // Init position: x from graph distance × 160px, y from flow-kind band
    // (same y-offsets as the row-bias solver — see schema.rs). This gives the
    // sim a sensible starting point so it doesn't have to discover LTR from
    // scratch, and lets us fall back to it if the sim never converges.
    let max_dist = graph_dist
        .values()
        .copied()
        .fold(0.0_f64, f64::max)
        .max(1.0);
    for id in def.elements.keys() {
        let kind = node_kind.get(id).copied();
        let d = graph_dist.get(id).copied().unwrap_or(0.0);
        let init_x = 100.0 + (d / max_dist) * 800.0;
        let init_y = 200.0 + kind.map(|k| k.target_row()).unwrap_or(0.0) * 110.0;
        let mut charges = Charges {
            flow_kind: Charges::flow_kind_from(kind),
            cluster: cluster_membership.get(id).cloned().unwrap_or_default(),
            graph_dist: d / max_dist,
        };
        // Boost cluster charge magnitudes into the same range as flow-kind so
        // the two forces compete on equal footing.
        for v in charges.cluster.values_mut() {
            *v = v.clamp(0.0, 1.0);
        }
        particles.push(Particle {
            id: id.clone(),
            kind: ParticleKind::Node,
            pos: (init_x, init_y),
            vel: (0.0, 0.0),
            mass: 1.0,
            fixed: false,
            charges,
            size: node_dims(&def.elements[id]),
            edge_anchors: None,
            history: Vec::new(),
        });
    }

    // --- Edge chains -----------------------------------------------------
    // Every sequence flow becomes `EDGE_SEGMENTS + 1` particles including the
    // two endpoints. Endpoints get pinned each step to the nearest point on
    // their host node's perimeter (see `project_endpoints`).
    for (src_id, el) in &def.elements {
        for f in &el.outgoing {
            let src_pos = particles
                .iter()
                .find(|p| p.id == *src_id)
                .map(|p| p.pos)
                .unwrap_or((0.0, 0.0));
            let tgt_pos = particles
                .iter()
                .find(|p| p.id == f.to)
                .map(|p| p.pos)
                .unwrap_or((0.0, 0.0));
            let edge_id = format!("{}__{}", src_id, f.to);
            let edge_kind = edge_flow_kind(&edge_id, src_id, &f.to, ann);
            let charges = Charges {
                flow_kind: Charges::flow_kind_from(edge_kind),
                cluster: HashMap::new(),
                graph_dist: 0.0,
            };
            let chain_len = EDGE_SEGMENTS + 1;
            for i in 0..chain_len {
                let t = i as f64 / (chain_len - 1) as f64;
                let px = src_pos.0 + (tgt_pos.0 - src_pos.0) * t;
                let py = src_pos.1 + (tgt_pos.1 - src_pos.1) * t;
                let is_endpoint = i == 0 || i == chain_len - 1;
                particles.push(Particle {
                    id: format!("{}#seg{}", edge_id, i),
                    kind: ParticleKind::EdgeSegment {
                        index: i,
                        chain_len,
                    },
                    pos: (px, py),
                    vel: (0.0, 0.0),
                    mass: 0.2,          // edge segments are lighter so they yield to nodes
                    fixed: is_endpoint, // endpoints pinned every step to node perimeter
                    charges: charges.clone(),
                    size: (0.0, 0.0),
                    edge_anchors: Some(EdgeAnchors {
                        edge_id: edge_id.clone(),
                        source_node: src_id.clone(),
                        target_node: f.to.clone(),
                    }),
                    history: Vec::new(),
                });
            }
        }
    }

    particles
}

// --- Forces ----------------------------------------------------------------

fn compute_all_forces(particles: &[Particle]) -> Vec<(f64, f64)> {
    let n = particles.len();
    let mut forces = vec![(0.0_f64, 0.0_f64); n];
    for i in 0..n {
        // LTR drift (per-particle, not pairwise).
        forces[i].0 += particles[i].charges.graph_dist * K_LTR_DRIFT;

        for j in 0..n {
            if i == j {
                continue;
            }
            let (fx, fy) = pairwise_force(&particles[i], &particles[j]);
            forces[i].0 += fx;
            forces[i].1 += fy;
        }

        // Intra-edge spring forces (only for edge segments).
        if let ParticleKind::EdgeSegment { index, chain_len } = particles[i].kind {
            if let Some(anchor) = &particles[i].edge_anchors {
                for (di, other_idx) in [(-1i32, index as i32 - 1), (1, index as i32 + 1)] {
                    let _ = di;
                    if other_idx < 0 || other_idx as usize >= chain_len {
                        continue;
                    }
                    // Find the neighbour in the same edge chain.
                    let neighbour = particles.iter().find(|p| {
                        matches!(
                            p.kind,
                            ParticleKind::EdgeSegment { index: ii, .. } if ii == other_idx as usize
                        ) && p
                            .edge_anchors
                            .as_ref()
                            .is_some_and(|a| a.edge_id == anchor.edge_id)
                    });
                    if let Some(nb) = neighbour {
                        let dx = nb.pos.0 - particles[i].pos.0;
                        let dy = nb.pos.1 - particles[i].pos.1;
                        let r = (dx * dx + dy * dy).sqrt().max(1e-6);
                        let stretch = r - EDGE_REST_LEN;
                        forces[i].0 += K_EDGE_SPRING * stretch * dx / r;
                        forces[i].1 += K_EDGE_SPRING * stretch * dy / r;
                    }
                }
            }
        }
    }
    forces
}

fn pairwise_force(a: &Particle, b: &Particle) -> (f64, f64) {
    let dx = b.pos.0 - a.pos.0;
    let dy = b.pos.1 - a.pos.1;
    let r2 = (dx * dx + dy * dy).max(1.0);
    let r = r2.sqrt();

    // Flow-kind alignment — same-kind attracts, cross-kind repels.
    let dot: f64 = a
        .charges
        .flow_kind
        .iter()
        .zip(b.charges.flow_kind.iter())
        .map(|(x, y)| x * y)
        .sum();
    let alignment = dot; // 0 if orthogonal, 1 if aligned
    let flow_attract = alignment * K_ATTRACT;
    let flow_repel = (1.0 - alignment) * K_REPEL / r;
    let mut net = (flow_attract - flow_repel) / r;

    // Cluster attraction — shared cluster membership pulls together.
    let mut cluster_share = 0.0;
    for (cid, share_a) in &a.charges.cluster {
        let share_b = b.charges.cluster.get(cid).copied().unwrap_or(0.0);
        cluster_share += share_a * share_b;
    }
    net += cluster_share * K_CLUSTER / r;

    // Node-node hard overlap — inverse-square push if bounding rects intersect.
    if a.kind == ParticleKind::Node && b.kind == ParticleKind::Node {
        let overlap_x = ((a.size.0 + b.size.0) * 0.5 - dx.abs() - 8.0).max(0.0);
        let overlap_y = ((a.size.1 + b.size.1) * 0.5 - dy.abs() - 8.0).max(0.0);
        if overlap_x > 0.0 && overlap_y > 0.0 {
            let mag = K_HARD_OVERLAP * (overlap_x + overlap_y) / r2;
            net -= mag / r;
        }
    }

    (dx / r * net, dy / r * net)
}

// --- Integrator ------------------------------------------------------------

fn integrate(particles: &mut [Particle], forces: &[(f64, f64)]) -> f64 {
    // Endpoints get re-anchored to their host node's perimeter *before* the
    // velocity update — this is the physical equivalent of a rigid pin
    // sliding along the rectangle's edge.
    project_endpoints(particles);

    let mut ke = 0.0;
    for (p, &(fx, fy)) in particles.iter_mut().zip(forces.iter()) {
        if p.fixed {
            p.vel = (0.0, 0.0);
            continue;
        }
        let ax = fx / p.mass;
        let ay = fy / p.mass;
        p.vel.0 = (p.vel.0 + ax * DT) * DAMPING;
        p.vel.1 = (p.vel.1 + ay * DT) * DAMPING;
        p.pos.0 += p.vel.0 * DT;
        p.pos.1 += p.vel.1 * DT;
        ke += 0.5 * p.mass * (p.vel.0 * p.vel.0 + p.vel.1 * p.vel.1);

        p.history.push(p.pos);
        if p.history.len() > OSCILLATION_WINDOW {
            p.history.remove(0);
        }
    }
    ke
}

fn project_endpoints(particles: &mut [Particle]) {
    // Snapshot node rects (Node particles) so we can perimeter-project edge
    // endpoints against them without an aliasing borrow.
    let node_rects: HashMap<String, (f64, f64, f64, f64)> = particles
        .iter()
        .filter(|p| p.kind == ParticleKind::Node)
        .map(|p| (p.id.clone(), (p.pos.0, p.pos.1, p.size.0, p.size.1)))
        .collect();

    for p in particles.iter_mut() {
        if let ParticleKind::EdgeSegment { index, chain_len } = p.kind {
            let is_endpoint = index == 0 || index == chain_len - 1;
            if !is_endpoint {
                continue;
            }
            let Some(anchor) = &p.edge_anchors else {
                continue;
            };
            let host_id = if index == 0 {
                &anchor.source_node
            } else {
                &anchor.target_node
            };
            let Some(&(cx, cy, w, h)) = node_rects.get(host_id) else {
                continue;
            };
            // Project p.pos onto the nearest edge of the rect centered at (cx, cy).
            let dx = p.pos.0 - cx;
            let dy = p.pos.1 - cy;
            let hw = w * 0.5;
            let hh = h * 0.5;
            let projected = if dx.abs() * hh > dy.abs() * hw {
                // Cross the vertical edge (left or right face).
                let sx = if dx > 0.0 { cx + hw } else { cx - hw };
                (sx, (cy + dy * hw / dx.abs()).clamp(cy - hh, cy + hh))
            } else if dy.abs() > 0.0 {
                // Cross the horizontal edge (top or bottom face).
                let sy = if dy > 0.0 { cy + hh } else { cy - hh };
                ((cx + dx * hh / dy.abs()).clamp(cx - hw, cx + hw), sy)
            } else {
                (cx + hw, cy)
            };
            p.pos = projected;
        }
    }
}

fn pin_oscillators(particles: &mut [Particle], pinned: &mut Vec<String>) {
    for p in particles.iter_mut() {
        if p.fixed || p.history.len() < OSCILLATION_WINDOW {
            continue;
        }
        let (mx, my) = mean(&p.history);
        let var: f64 = p
            .history
            .iter()
            .map(|(x, y)| (x - mx).powi(2) + (y - my).powi(2))
            .sum::<f64>()
            / p.history.len() as f64;
        if var > OSCILLATION_VAR {
            // Actively oscillating rather than settling — pin to the mean and
            // let the rest of the field relax around it.
            p.pos = (mx, my);
            p.vel = (0.0, 0.0);
            p.fixed = true;
            pinned.push(p.id.clone());
        }
    }
}

fn mean(pts: &[(f64, f64)]) -> (f64, f64) {
    let n = pts.len() as f64;
    let (sx, sy) = pts
        .iter()
        .fold((0.0, 0.0), |(ax, ay), (x, y)| (ax + x, ay + y));
    (sx / n, sy / n)
}

// --- Extraction ------------------------------------------------------------

fn extract_output(particles: &[Particle], diagnostics: SimDiagnostics) -> FieldOutput {
    let mut nodes = HashMap::new();
    for p in particles {
        if p.kind == ParticleKind::Node {
            nodes.insert(p.id.clone(), p.pos);
        }
    }

    // Collect edge segments keyed by parent edge id, then sort by segment
    // index so waypoint order matches the chain direction (source → target).
    let mut per_edge: HashMap<String, Vec<(usize, (f64, f64))>> = HashMap::new();
    for p in particles {
        if let ParticleKind::EdgeSegment { index, .. } = p.kind {
            if let Some(a) = &p.edge_anchors {
                per_edge
                    .entry(a.edge_id.clone())
                    .or_default()
                    .push((index, p.pos));
            }
        }
    }
    let mut edges: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    for (edge_id, mut list) in per_edge {
        list.sort_by_key(|&(i, _)| i);
        edges.insert(edge_id, list.into_iter().map(|(_, p)| p).collect());
    }

    FieldOutput {
        nodes,
        edges,
        diagnostics,
    }
}

// --- Helpers --------------------------------------------------------------

fn node_dims(el: &Element) -> (f64, f64) {
    use nanobpmn_engine_core::ElementKind::*;
    match el.kind {
        StartEvent
        | EndEvent
        | IntermediateThrowEvent
        | TimerIntermediateCatchEvent { .. }
        | MessageIntermediateCatchEvent { .. }
        | SignalIntermediateCatchEvent { .. }
        | ConditionalIntermediateCatchEvent { .. }
        | MessageStartEvent { .. }
        | TimerStartEvent { .. }
        | ErrorBoundaryEvent { .. }
        | TimerBoundaryEvent { .. }
        | MessageBoundaryEvent { .. }
        | SignalBoundaryEvent { .. }
        | ConditionalBoundaryEvent { .. } => (36.0, 36.0),
        ExclusiveGateway | ParallelGateway => (50.0, 50.0),
        _ => (NODE_W, NODE_H),
    }
}

fn flatten_flow_kinds(ann: &SemanticAnnotations) -> HashMap<String, FlowKind> {
    let mut out: HashMap<String, FlowKind> = HashMap::new();
    for f in &ann.flows {
        for n in &f.nodes {
            let take = out.get(n).is_none_or(|k| f.kind.priority() > k.priority());
            if take {
                out.insert(n.clone(), f.kind);
            }
        }
    }
    out
}

fn flatten_cluster_membership(ann: &SemanticAnnotations) -> HashMap<String, HashMap<String, f64>> {
    let mut out: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for c in &ann.clusters {
        for n in &c.nodes {
            out.entry(n.clone())
                .or_default()
                .insert(c.id.clone(), c.affinity.clamp(0.0, 1.0));
        }
    }
    out
}

fn edge_flow_kind(
    _edge_id: &str,
    src: &str,
    tgt: &str,
    ann: &SemanticAnnotations,
) -> Option<FlowKind> {
    // An edge takes the higher-priority flow kind of its endpoints. When both
    // endpoints are primary, the edge is primary; when either is exception,
    // the whole edge is exception (matches how you'd read the diagram — an
    // edge INTO the error-handling section is itself part of that section).
    let nk = flatten_flow_kinds(ann);
    let a = nk.get(src).copied();
    let b = nk.get(tgt).copied();
    match (a, b) {
        (None, None) => None,
        (Some(k), None) | (None, Some(k)) => Some(k),
        (Some(a), Some(b)) => Some(if a.priority() >= b.priority() { a } else { b }),
    }
}

/// Longest-path distance from any source event to each node — used as the
/// LTR drift charge. Cyclic graphs cap at `n` (same as `append_diagram`'s
/// rank fixpoint).
fn compute_graph_distance(def: &ProcessDefinition) -> HashMap<String, f64> {
    let ids: Vec<&String> = def.elements.keys().collect();
    let n = ids.len();
    let mut dist: HashMap<String, f64> = ids.iter().map(|s| ((*s).clone(), 0.0)).collect();
    for _ in 0..(n + 2) {
        for (id, el) in &def.elements {
            let cur = dist.get(id).copied().unwrap_or(0.0);
            for f in &el.outgoing {
                let nd = cur + 1.0;
                if dist.get(&f.to).copied().unwrap_or(0.0) < nd && nd < n as f64 {
                    dist.insert(f.to.clone(), nd);
                }
            }
        }
    }
    dist
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::bpmn::parse_bpmn;

    use super::*;

    #[test]
    fn simulate_runs_end_to_end_on_tiny_fixture() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let ann_json = include_str!("../../fixtures/layout/tiny.annotations.json");
        let ann: SemanticAnnotations = serde_json::from_str(ann_json).unwrap();
        let defs = parse_bpmn(bpmn).unwrap();
        let def = defs.into_iter().next().unwrap();
        let out = simulate(&def, &ann);

        assert!(out.nodes.contains_key("TaskA"));
        assert!(out.nodes.contains_key("HandleError"));
        // Every sequence flow gets a waypoint chain.
        assert!(!out.edges.is_empty());
        for (_, waypoints) in &out.edges {
            assert!(waypoints.len() >= 2, "each edge has at least two endpoints");
        }
    }

    #[test]
    fn exception_lands_below_primary_after_settling() {
        // The physics analogue of the row-bias test: after the field settles,
        // the exception-band node should sit below the primary-band node it
        // branched from. This is the whole point of the semantic charges.
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let ann_json = include_str!("../../fixtures/layout/tiny.annotations.json");
        let ann: SemanticAnnotations = serde_json::from_str(ann_json).unwrap();
        let defs = parse_bpmn(bpmn).unwrap();
        let def = defs.into_iter().next().unwrap();
        let out = simulate(&def, &ann);
        let taskb_y = out.nodes["TaskB"].1;
        let handle_y = out.nodes["HandleError"].1;
        assert!(
            handle_y > taskb_y + 40.0,
            "HandleError (y={handle_y}) should settle below TaskB (y={taskb_y})"
        );
    }
}
