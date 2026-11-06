// Workflow DAG: graph building with spawned-group collapsing, and a layered
// (Sugiyama-style) layout. Pure: no DOM.
//
// Steps come from GET /workflows/{id}: static steps have no parent_step; spawned
// steps are named "<parent>/<child>" and carry parent_step (nesting is possible:
// "X.evaluate/fc3/d0001"). Every spawned descendant of a step P is drawn as one
// group node "P/…" with counts by state, unless P is in `expanded`, in which case
// P's direct children are drawn individually (their own children grouped again).

import { STATE_ORDER, stateTone } from './format.js';

export function groupId(parent) { return `group:${parent}`; }

/** Aggregate state of a set of state counts, for colouring a group node. */
export function aggregateState(counts) {
  const has = (s) => (counts[s] || 0) > 0;
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  if (!total) return 'WAITING';
  if (['FAILED', 'EXPIRED', 'VALIDATION_FAILED'].some(has)) return 'FAILED';
  if (['RUNNING', 'COMMITTING', 'ASSIGNED', 'LEASED'].some(has)) return 'RUNNING';
  if (has('RETRY_PENDING')) return 'RETRY_PENDING';
  const done = (counts.COMPLETED || 0) + (counts.SUCCEEDED || 0) + (counts.SKIPPED || 0) + (counts.CANCELLED || 0);
  if (done === total) return (counts.COMPLETED || counts.SUCCEEDED) ? 'COMPLETED' : (has('CANCELLED') ? 'CANCELLED' : 'SKIPPED');
  if (has('QUEUED') || has('CREATED')) return 'QUEUED';
  return 'WAITING';
}

/**
 * Build the visible graph.
 * Returns {nodes:[{id, kind:'step'|'group', name, label, state, step?, parent?, counts?, total?, childCount}],
 *          edges:[{from, to, spawn:boolean}]}
 */
export function buildGraph(steps, expanded = new Set()) {
  steps = steps || [];
  const byName = new Map(steps.map((s) => [s.name, s]));
  const children = new Map();   // parent name → [child names]
  for (const s of steps) {
    if (s.parent_step) {
      if (!children.has(s.parent_step)) children.set(s.parent_step, []);
      children.get(s.parent_step).push(s.name);
    }
  }
  const parentOf = (name) => byName.get(name)?.parent_step || '';
  const ancestors = (name) => {   // [root, …, parent]
    const out = [];
    const seen = new Set([name]);
    let p = parentOf(name);
    while (p && !seen.has(p)) { out.unshift(p); seen.add(p); p = parentOf(p); }
    return out;
  };
  const repCache = new Map();
  const rep = (name) => {
    if (repCache.has(name)) return repCache.get(name);
    let r = name;
    for (const a of ancestors(name)) {
      if (!expanded.has(a)) { r = groupId(a); break; }
    }
    repCache.set(name, r);
    return r;
  };
  const descendants = (name) => {
    const out = [];
    const stack = [...(children.get(name) || [])];
    const seen = new Set();
    while (stack.length) {
      const c = stack.pop();
      if (seen.has(c)) continue;
      seen.add(c);
      out.push(c);
      stack.push(...(children.get(c) || []));
    }
    return out;
  };

  const nodes = [];
  const nodeIds = new Set();
  for (const s of steps) {
    const r = rep(s.name);
    if (r === s.name) {
      nodes.push({ id: s.name, kind: 'step', name: s.name, label: shortLabel(s), state: s.state || 'WAITING',
        step: s, parent: s.parent_step || '', childCount: (children.get(s.name) || []).length,
        expanded: expanded.has(s.name) });
      nodeIds.add(s.name);
    } else if (!nodeIds.has(r)) {
      const parent = r.slice('group:'.length);
      const desc = descendants(parent);
      const counts = {};
      for (const d of desc) {
        const st = byName.get(d)?.state || 'WAITING';
        counts[st] = (counts[st] || 0) + 1;
      }
      nodes.push({ id: r, kind: 'group', name: `${parent}/…`, label: `${lastSegment(parent)}/… ×${desc.length}`,
        state: aggregateState(counts), parent, counts, total: desc.length,
        childCount: (children.get(parent) || []).length });
      nodeIds.add(r);
    }
  }

  const resolve = (from, ref) => {
    if (!ref || ref === '.') return parentOf(from) || '';
    if (byName.has(ref)) return ref;
    const p = parentOf(from);       // spawned steps may name siblings by short name
    if (p && byName.has(`${p}/${ref}`)) return `${p}/${ref}`;
    return '';
  };

  const edgeKeys = new Map();
  const addEdge = (a, b, spawn) => {
    const ra = rep(a); const rb = rep(b);
    if (!ra || !rb || ra === rb || !nodeIds.has(ra) || !nodeIds.has(rb)) return;
    const k = `${ra}\u0000${rb}`;
    if (edgeKeys.has(k)) { if (spawn) edgeKeys.get(k).spawn = true; return; }
    const e = { from: ra, to: rb, spawn };
    edgeKeys.set(k, e);
  };
  for (const s of steps) {
    const spec = s.spec || {};
    for (const d of spec.depends_on || []) {
      const n = resolve(s.name, d);
      if (n) addEdge(n, s.name, false);
    }
    for (const inp of spec.inputs || []) {
      const n = resolve(s.name, inp.step);
      if (!n) continue;
      addEdge(n, s.name, false);
      if (inp.spawned) for (const d of descendants(n)) addEdge(d, s.name, false);
    }
    if (s.parent_step && byName.has(s.parent_step)) addEdge(s.parent_step, s.name, true);
  }
  return { nodes, edges: [...edgeKeys.values()] };
}

function lastSegment(name) {
  const i = name.lastIndexOf('/');
  return i < 0 ? name : name.slice(i + 1);
}
function shortLabel(s) { return s.parent_step ? lastSegment(s.name) : s.name; }

/**
 * Layered layout, left to right. Layer = longest path from a source; order within a
 * layer by barycenter sweeps. Returns {nodes (with layer, order, x, y, w, h), edges,
 * width, height, layers}.
 */
export function layout(graph, { nodeW = 168, nodeH = 30, gapX = 56, gapY = 10, sweeps = 6 } = {}) {
  const nodes = graph.nodes.map((n, i) => ({ ...n, index: i }));
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const edges = graph.edges.filter((e) => byId.has(e.from) && byId.has(e.to));
  const preds = new Map(nodes.map((n) => [n.id, []]));
  const succs = new Map(nodes.map((n) => [n.id, []]));
  for (const e of edges) { succs.get(e.from).push(e.to); preds.get(e.to).push(e.from); }

  // Longest-path layering via Kahn's algorithm; nodes left over (cycles) go last.
  const indeg = new Map(nodes.map((n) => [n.id, preds.get(n.id).length]));
  const layer = new Map();
  const queue = nodes.filter((n) => indeg.get(n.id) === 0).map((n) => n.id);
  for (const id of queue) layer.set(id, 0);
  let qi = 0;
  while (qi < queue.length) {
    const id = queue[qi++];
    for (const s of succs.get(id)) {
      layer.set(s, Math.max(layer.get(s) ?? 0, layer.get(id) + 1));
      indeg.set(s, indeg.get(s) - 1);
      if (indeg.get(s) === 0) queue.push(s);
    }
  }
  let maxLayer = 0;
  for (const v of layer.values()) maxLayer = Math.max(maxLayer, v);
  for (const n of nodes) if (!layer.has(n.id)) layer.set(n.id, maxLayer + 1);
  for (const n of nodes) n.layer = layer.get(n.id);
  const nLayers = nodes.reduce((m, n) => Math.max(m, n.layer + 1), 0);

  const layers = Array.from({ length: nLayers }, () => []);
  for (const n of nodes) layers[n.layer].push(n);
  layers.forEach((L) => L.forEach((n, i) => { n.order = i; }));

  const pos = (id) => { const n = byId.get(id); return n.order / Math.max(1, layers[n.layer].length - 1); };
  const reorder = (L, neighbours) => {
    for (const n of L) {
      const ns = neighbours.get(n.id);
      n.bary = ns.length ? ns.reduce((a, id) => a + pos(id), 0) / ns.length : n.order / Math.max(1, L.length - 1);
    }
    L.sort((a, b) => a.bary - b.bary || a.order - b.order);
    L.forEach((n, i) => { n.order = i; });
  };
  for (let it = 0; it < sweeps; it++) {
    if (it % 2 === 0) for (let l = 1; l < nLayers; l++) reorder(layers[l], preds);
    else for (let l = nLayers - 2; l >= 0; l--) reorder(layers[l], succs);
  }

  const tallest = layers.reduce((m, L) => Math.max(m, L.length), 0);
  const height = Math.max(0, tallest * nodeH + (tallest - 1) * gapY);
  for (const L of layers) {
    const h = L.length * nodeH + (L.length - 1) * gapY;
    const top = (height - h) / 2;
    for (const n of L) {
      n.x = n.layer * (nodeW + gapX);
      n.y = top + n.order * (nodeH + gapY);
      n.w = nodeW;
      n.h = nodeH;
      delete n.bary;
    }
  }
  const width = nLayers ? nLayers * nodeW + (nLayers - 1) * gapX : 0;
  return { nodes, edges, width, height, layers: nLayers, byId };
}

/** Counts by state over all steps (for the step table filter chips). */
export function countStates(steps) {
  const c = {};
  for (const s of steps || []) c[s.state || 'WAITING'] = (c[s.state || 'WAITING'] || 0) + 1;
  return Object.fromEntries(STATE_ORDER.filter((s) => c[s]).map((s) => [s, c[s]])
    .concat(Object.entries(c).filter(([s]) => !STATE_ORDER.includes(s))));
}

export { stateTone };
