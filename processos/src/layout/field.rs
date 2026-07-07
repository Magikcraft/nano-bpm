//! **Fromme engine (v2, experimental)** — unified charged-particle simulation
//! where nodes and edges are both particles in the same 2D field. The Fromme
//! engine is a physics-based BPMN DI layout solver; nodes carry semantic
//! charges (flow-kind, cluster, graph-distance) and settle into a diagram
//! under pairwise attraction/repulsion and a longitudinal (LTR) spring.
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
const K_ATTRACT: f64 = 20.0;
const K_REPEL: f64 = 3000.0;
const K_CLUSTER: f64 = 40.0;
const K_HARD_OVERLAP: f64 = 6000.0;
/// Spring toward each node's target x (derived from longest-path graph
/// distance). A restoring force, so the system can converge — the earlier
/// constant LTR drift never let terminal velocity reach zero.
const K_LTR_SPRING: f64 = 8.0;
/// Spring toward each node's flow-kind band y. Weaker than the LTR spring
/// (nodes are freer along y to accommodate cluster/repel pushes) but strong
/// enough to keep exception nodes below and escalation nodes above.
const K_BAND_SPRING: f64 = 4.0;
/// Column spacing for `target_x` (px). 160 matches the row-bias solver.
const COL_SPACING: f64 = 160.0;
/// First-column x, so target_x = FIRST_COL_X + graph_dist * COL_SPACING.
const FIRST_COL_X: f64 = 120.0;
/// Soft-core repulsion between edge segments — prevents unrelated edges from
/// occupying the same coordinates while still letting same-kind ones bundle.
const K_EDGE_REPEL: f64 = 200.0;
const EDGE_MIN_R: f64 = 28.0;
const K_EDGE_SPRING: f64 = 6.0;
const EDGE_REST_LEN: f64 = 30.0;

const DT: f64 = 0.05;
const DAMPING: f64 = 0.90;
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
    /// For nodes only — the visual shape (rect/diamond/circle). Determines
    /// how edge endpoints project onto the perimeter. Meaningless for edge
    /// segments (defaults to Rect).
    pub shape: NodeShape,
    /// Target x from the LTR spring (derived from graph distance). Zero for
    /// edge segments (they're free to slide along x, held by chain springs).
    pub target_x: f64,
    /// Target y from the flow-kind band spring (nodes only). Same y-offsets
    /// as the row-bias solver — keeps nodes in their semantic band without
    /// forcing a hard row constraint.
    pub target_y: f64,
    /// Net force applied in the *last* integrator step. Used by the debug
    /// SVG's force-vector overlay; otherwise informational.
    pub last_force: (f64, f64),
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
    #[allow(dead_code)] // retained for future force-tuning experiments
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
    /// Per-node net force from the *last* integrator step. Used by the debug
    /// SVG force-vector overlay.
    pub node_forces: HashMap<String, (f64, f64)>,
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
        let target_x = FIRST_COL_X + d * COL_SPACING;
        let init_x = target_x;
        let init_y = 200.0 + kind.map(|k| k.target_row()).unwrap_or(0.0) * 110.0;
        let target_y = init_y;
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
            shape: node_shape(&def.elements[id]),
            target_x,
            target_y,
            last_force: (0.0, 0.0),
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
                    shape: NodeShape::Rect,
                    target_x: 0.0,
                    target_y: 0.0,
                    last_force: (0.0, 0.0),
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
        // LTR spring — nodes are pulled toward their target x (from graph
        // distance). This is a restoring force; without it terminal velocity
        // is nonzero and the system never converges.
        if particles[i].kind == ParticleKind::Node {
            let dx = particles[i].target_x - particles[i].pos.0;
            let dy = particles[i].target_y - particles[i].pos.1;
            forces[i].0 += K_LTR_SPRING * dx;
            forces[i].1 += K_BAND_SPRING * dy;
        }

        for j in 0..n {
            if i == j {
                continue;
            }
            // Skip node↔edge-segment pairwise interactions. Edge segments
            // are already coupled to their host nodes via the chain springs
            // and endpoint perimeter projection; letting the pairwise
            // flow-kind attraction/repulsion also act between them just
            // yanks nodes around when edges drift.
            let cross = matches!(
                (particles[i].kind, particles[j].kind),
                (ParticleKind::Node, ParticleKind::EdgeSegment { .. })
                    | (ParticleKind::EdgeSegment { .. }, ParticleKind::Node)
            );
            if cross {
                continue;
            }
            let (fx, fy) = pairwise_force(&particles[i], &particles[j]);
            forces[i].0 += fx;
            forces[i].1 += fy;
        }

        // Intra-edge spring forces (only for edge segments).
        if let ParticleKind::EdgeSegment { index, chain_len } = particles[i].kind {
            if let Some(anchor) = &particles[i].edge_anchors {
                for other_idx in [index as i32 - 1, index as i32 + 1] {
                    if other_idx < 0 || other_idx as usize >= chain_len {
                        continue;
                    }
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

    // Cross-edge soft repulsion — prevents unrelated edge chains from
    // occupying the same coordinates. Only kicks in below EDGE_MIN_R and
    // only between segments belonging to *different* parent edges (same-edge
    // chain neighbours are handled by the spring force elsewhere).
    if let (ParticleKind::EdgeSegment { .. }, ParticleKind::EdgeSegment { .. }) = (a.kind, b.kind) {
        let same_edge = match (&a.edge_anchors, &b.edge_anchors) {
            (Some(ea), Some(eb)) => ea.edge_id == eb.edge_id,
            _ => false,
        };
        if !same_edge && r < EDGE_MIN_R {
            let mag = K_EDGE_REPEL * (EDGE_MIN_R - r) / EDGE_MIN_R;
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
        p.last_force = (fx, fy);
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
    // endpoints against them without an aliasing borrow. The shape is
    // captured too so gateways and events use diamond/circle projection.
    let node_rects: HashMap<String, (f64, f64, f64, f64, NodeShape)> = particles
        .iter()
        .filter(|p| p.kind == ParticleKind::Node)
        .map(|p| {
            (
                p.id.clone(),
                (p.pos.0, p.pos.1, p.size.0, p.size.1, p.shape),
            )
        })
        .collect();

    // Snapshot the position of each edge's interior neighbour segment so
    // endpoints can be projected in the direction the edge is actually
    // heading, not their own stale position. Endpoints are `fixed=true` and
    // otherwise never move — without this the source endpoint sits at the
    // node centre forever and always projects onto the right face (the
    // fallback branch below), which is exactly the "both flows leave the
    // gateway from the same point" bug.
    // Key: (edge_id, endpoint_index). Value: interior neighbour position.
    let mut neighbour_pos: HashMap<(String, usize), (f64, f64)> = HashMap::new();
    for p in particles.iter() {
        if let ParticleKind::EdgeSegment { index, chain_len } = p.kind {
            let is_endpoint = index == 0 || index == chain_len - 1;
            if is_endpoint {
                continue;
            }
            if let Some(a) = &p.edge_anchors {
                // Segment at index i is the neighbour of endpoint 0 iff i==1,
                // and neighbour of the last endpoint iff i == chain_len-2.
                if index == 1 {
                    neighbour_pos.insert((a.edge_id.clone(), 0), p.pos);
                }
                if index == chain_len - 2 {
                    neighbour_pos.insert((a.edge_id.clone(), chain_len - 1), p.pos);
                }
            }
        }
    }

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
            let Some(&(cx, cy, w, h, shape)) = node_rects.get(host_id) else {
                continue;
            };
            // Aim toward the interior-neighbour segment if we've seen it;
            // otherwise fall back to the *other end's* host node so at init
            // the projection is still directional rather than at the centre.
            let aim = neighbour_pos
                .get(&(anchor.edge_id.clone(), index))
                .copied()
                .or_else(|| {
                    let other_host = if index == 0 {
                        &anchor.target_node
                    } else {
                        &anchor.source_node
                    };
                    node_rects.get(other_host).map(|&(x, y, _, _, _)| (x, y))
                })
                .unwrap_or((cx + w, cy));
            let dx = aim.0 - cx;
            let dy = aim.1 - cy;
            let hw = w * 0.5;
            let hh = h * 0.5;
            let projected = project_to_shape(shape, cx, cy, hw, hh, dx, dy);
            p.pos = projected;
        }
    }
}

/// Project a ray from `(cx, cy)` in direction `(dx, dy)` onto the perimeter
/// of a shape (rect / diamond / circle) inscribed in the bounding box
/// (`2·hw`, `2·hh`). This is what puts edge endpoints on the shape actually
/// rendered by Modeler rather than on the invisible bounding rect.
fn project_to_shape(
    shape: NodeShape,
    cx: f64,
    cy: f64,
    hw: f64,
    hh: f64,
    dx: f64,
    dy: f64,
) -> (f64, f64) {
    match shape {
        NodeShape::Rect => {
            if dx.abs() * hh > dy.abs() * hw {
                let sx = if dx > 0.0 { cx + hw } else { cx - hw };
                let denom = dx.abs().max(1e-6);
                (sx, (cy + dy * hw / denom).clamp(cy - hh, cy + hh))
            } else if dy.abs() > 1e-6 {
                let sy = if dy > 0.0 { cy + hh } else { cy - hh };
                let denom = dy.abs().max(1e-6);
                ((cx + dx * hh / denom).clamp(cx - hw, cx + hw), sy)
            } else {
                (cx + hw, cy)
            }
        }
        NodeShape::Diamond => {
            // Diamond perimeter satisfies |x'/hw| + |y'/hh| = 1 (in local
            // coordinates). Ray param t solves t·(|dx|/hw + |dy|/hh) = 1.
            let denom = dx.abs() / hw + dy.abs() / hh;
            if denom < 1e-6 {
                return (cx + hw, cy);
            }
            let t = 1.0 / denom;
            (cx + dx * t, cy + dy * t)
        }
        NodeShape::Circle => {
            // Ellipse perimeter (equals circle when hw == hh). Ray param t
            // solves (t·dx/hw)² + (t·dy/hh)² = 1.
            let denom = ((dx / hw).powi(2) + (dy / hh).powi(2)).sqrt();
            if denom < 1e-6 {
                return (cx + hw, cy);
            }
            let t = 1.0 / denom;
            (cx + dx * t, cy + dy * t)
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
    let mut node_forces = HashMap::new();
    // Snapshot node rects so we can clip edge polylines out of source/target
    // rectangles below.
    let mut node_rects: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
    for p in particles {
        if p.kind == ParticleKind::Node {
            nodes.insert(p.id.clone(), p.pos);
            node_forces.insert(p.id.clone(), p.last_force);
            node_rects.insert(p.id.clone(), (p.pos.0, p.pos.1, p.size.0, p.size.1));
        }
    }

    // Collect edge segments keyed by parent edge id, then sort by segment
    // index so waypoint order matches the chain direction (source → target).
    type EdgeBucket = (Vec<(usize, (f64, f64))>, Option<EdgeAnchors>);
    let mut per_edge: HashMap<String, EdgeBucket> = HashMap::new();
    for p in particles {
        if let ParticleKind::EdgeSegment { index, .. } = p.kind {
            if let Some(a) = &p.edge_anchors {
                let entry = per_edge
                    .entry(a.edge_id.clone())
                    .or_insert((Vec::new(), Some(a.clone())));
                entry.0.push((index, p.pos));
            }
        }
    }
    let mut edges: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    for (edge_id, (mut list, anchors)) in per_edge {
        list.sort_by_key(|&(i, _)| i);
        let raw: Vec<(f64, f64)> = list.into_iter().map(|(_, p)| p).collect();
        let clipped = clip_edge_polyline(&raw, anchors.as_ref(), &node_rects);
        edges.insert(edge_id, clipped);
    }

    FieldOutput {
        nodes,
        edges,
        node_forces,
        diagnostics,
    }
}

/// Trim the raw waypoint chain into a valid perimeter-to-perimeter polyline:
/// drop any interior waypoint that sits inside the source rect *or* the
/// target rect, then drop trailing waypoints on the source side that lie
/// "behind" the source face (i.e., on the interior-side of its outward
/// normal), and equivalently on the target side.
///
/// Without this pass, interior segments initialised at `t = 1/4` between two
/// close nodes could sit inside a rectangle, producing polylines that dived
/// into a node before exiting — which Modeler renders as an arrow pointing
/// backward with its head inside the target.
fn clip_edge_polyline(
    raw: &[(f64, f64)],
    anchors: Option<&EdgeAnchors>,
    node_rects: &HashMap<String, (f64, f64, f64, f64)>,
) -> Vec<(f64, f64)> {
    if raw.len() < 2 {
        return raw.to_vec();
    }
    let src_rect = anchors.and_then(|a| node_rects.get(&a.source_node).copied());
    let tgt_rect = anchors.and_then(|a| node_rects.get(&a.target_node).copied());

    let inside = |pt: (f64, f64), rect: (f64, f64, f64, f64)| -> bool {
        let (cx, cy, w, h) = rect;
        let hw = w * 0.5;
        let hh = h * 0.5;
        pt.0 > cx - hw && pt.0 < cx + hw && pt.1 > cy - hh && pt.1 < cy + hh
    };

    // Always keep the two endpoints (they were projected onto perimeters);
    // filter interior waypoints that sit inside either rect.
    let mut kept: Vec<(f64, f64)> = Vec::with_capacity(raw.len());
    kept.push(raw[0]);
    let interior_end = raw.len().saturating_sub(1);
    for p in raw.iter().take(interior_end).skip(1) {
        let p = *p;
        let in_src = src_rect.map(|r| inside(p, r)).unwrap_or(false);
        let in_tgt = tgt_rect.map(|r| inside(p, r)).unwrap_or(false);
        if !in_src && !in_tgt {
            kept.push(p);
        }
    }
    kept.push(raw[raw.len() - 1]);
    kept
}

// --- Helpers --------------------------------------------------------------

/// The visual shape of a node in Modeler-style BPMN rendering. Endpoint
/// projection has to match, otherwise edges appear disconnected from
/// diamond-shaped gateways or circular events even though the coordinates
/// lie on the bounding rectangle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeShape {
    Rect,
    Diamond,
    Circle,
}

fn node_shape(el: &Element) -> NodeShape {
    use nanobpmn_engine_core::ElementKind::*;
    match el.kind {
        ExclusiveGateway | ParallelGateway => NodeShape::Diamond,
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
        | ConditionalBoundaryEvent { .. } => NodeShape::Circle,
        _ => NodeShape::Rect,
    }
}

fn node_dims(el: &Element) -> (f64, f64) {
    match node_shape(el) {
        NodeShape::Circle => (36.0, 36.0),
        NodeShape::Diamond => (50.0, 50.0),
        NodeShape::Rect => (NODE_W, NODE_H),
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
        for waypoints in out.edges.values() {
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
