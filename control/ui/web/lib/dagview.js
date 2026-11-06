// SVG rendering of a workflow DAG (layout from dag.js).

import { h, s } from './dom.js';
import { buildGraph, layout } from './dag.js';
import { stateTone, stateGlyph, stateLabel, orderedCounts } from './format.js';

const NODE_W = 176;
const NODE_H = 30;
const PAD = 12;

function fit(text, width, charW = 6.7) {
  const max = Math.max(3, Math.floor(width / charW));
  return text.length <= max ? text : `${text.slice(0, max - 1)}…`;
}

/**
 * dagView({steps, expanded:Set, onToggle(parentName), onSelect(step)}) → element.
 */
export function dagView({ steps, expanded, onToggle, onSelect, selected }) {
  const graph = buildGraph(steps, expanded);
  const L = layout(graph, { nodeW: NODE_W, nodeH: NODE_H, gapX: 44, gapY: 8 });
  const W = L.width + PAD * 2;
  const H = L.height + PAD * 2;
  const svg = s('svg', { class: 'dag', width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'group',
    'aria-label': `Workflow graph: ${graph.nodes.length} nodes in ${L.layers} layers` });

  const edgeLayer = s('g', { class: 'edges' });
  const nodeLayer = s('g', { class: 'nodes' });
  svg.append(edgeLayer, nodeLayer);
  const edgeEls = new Map(); // node id → [path]
  for (const e of L.edges) {
    const a = L.byId.get(e.from); const b = L.byId.get(e.to);
    const x1 = PAD + a.x + a.w; const y1 = PAD + a.y + a.h / 2;
    const x2 = PAD + b.x; const y2 = PAD + b.y + b.h / 2;
    const dx = Math.max(16, (x2 - x1) / 2);
    const p = s('path', { class: e.spawn ? 'edge spawn' : 'edge', d: `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}` });
    edgeLayer.append(p);
    for (const id of [e.from, e.to]) { if (!edgeEls.has(id)) edgeEls.set(id, []); edgeEls.get(id).push(p); }
  }

  for (const n of L.nodes) {
    const tone = stateTone(n.state);
    const isGroup = n.kind === 'group';
    const toggleable = isGroup || (n.kind === 'step' && n.childCount > 0 && n.expanded);
    const g = s('g', { class: ['node', `tone-${tone}`, isGroup ? 'group' : '', selected === n.id ? 'selected' : ''].join(' '),
      transform: `translate(${PAD + n.x},${PAD + n.y})`, tabindex: '0', role: 'button',
      'aria-label': isGroup ? `${n.name}: ${n.total} spawned steps, expand` : `${n.name}: ${stateLabel(n.state)}` });
    const tip = isGroup
      ? `${n.parent} spawned ${n.total} steps\n${orderedCounts(n.counts).map(([st, c]) => `${c} ${stateLabel(st)}`).join(', ')}\nclick to expand`
      : `${n.name}\n${stateLabel(n.state)}${n.step?.error ? `\n${n.step.error}` : ''}${toggleable ? '\nclick to collapse spawned steps' : ''}`;
    g.append(s('title', null, tip));
    g.append(s('rect', { class: 'node-box', width: n.w, height: n.h, rx: isGroup ? 8 : 4 }));
    g.append(s('rect', { class: 'node-stripe', width: 4, height: n.h, rx: 2 }));
    g.append(s('text', { class: 'node-glyph', x: 13, y: n.h / 2 + 4 }, stateGlyph(n.state)));
    const label = isGroup ? `${n.label}` : n.label;
    g.append(s('text', { class: 'node-label', x: 26, y: isGroup ? 13 : n.h / 2 + 4 }, fit(label, n.w - 26 - (toggleable ? 18 : 6))));
    if (isGroup) {
      // Mini state bar along the bottom of a group.
      let x = 26; const bw = n.w - 26 - 22;
      for (const [st, c] of orderedCounts(n.counts)) {
        const w = (bw * c) / n.total;
        g.append(s('rect', { class: `mini tone-${stateTone(st)}`, x: x.toFixed(2), y: n.h - 9, width: Math.max(0.5, w - 1).toFixed(2), height: 4, rx: 1 }));
        x += w;
      }
    }
    if (toggleable) g.append(s('text', { class: 'node-toggle', x: n.w - 13, y: n.h / 2 + 5 }, isGroup ? '+' : '−'));
    const act = () => {
      if (isGroup) onToggle(n.parent);
      else if (toggleable) onToggle(n.name);
      else onSelect?.(n.step);
    };
    g.addEventListener('click', act);
    g.addEventListener('keydown', (ev) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); act(); } });
    const hl = (on) => { for (const p of edgeEls.get(n.id) || []) p.classList.toggle('hl', on); };
    g.addEventListener('mouseenter', () => hl(true));
    g.addEventListener('mouseleave', () => hl(false));
    g.addEventListener('focus', () => hl(true));
    g.addEventListener('blur', () => hl(false));
    nodeLayer.append(g);
  }

  const legend = h('div', { class: 'dag-legend' },
    [['COMPLETED', 'completed'], ['RUNNING', 'running'], ['LEASED', 'leased'], ['QUEUED', 'queued / waiting'],
      ['RETRY_PENDING', 'retrying'], ['FAILED', 'failed'], ['CANCELLED', 'cancelled / skipped']].map(([st, t]) =>
      h('span', { class: 'segbar-item' }, h('span', { class: `dot tone-${stateTone(st)}`, 'aria-hidden': 'true' }), t)),
    h('span', { class: 'segbar-item muted' }, h('span', { class: 'legend-dash', 'aria-hidden': 'true' }), 'spawned'),
    h('span', { class: 'segbar-item muted' }, 'Groups (+) hold spawned steps; click to expand.'));
  return h('div', { class: 'dag-wrap' }, h('div', { class: 'dag-scroll', tabindex: '-1' }, svg), legend);
}
