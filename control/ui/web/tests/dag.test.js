import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildGraph, layout, aggregateState, groupId, countStates } from '../lib/dag.js';
import { fixture } from './helpers.js';

const wf = fixture('workflow_toy_ZrNiSn.json');

function checkLayout(L) {
  // every edge goes strictly to a later layer
  for (const e of L.edges) {
    assert.ok(L.byId.get(e.from).layer < L.byId.get(e.to).layer, `${e.from} -> ${e.to}`);
  }
  // no two nodes overlap
  const ns = L.nodes;
  for (let i = 0; i < ns.length; i++) {
    for (let j = i + 1; j < ns.length; j++) {
      const a = ns[i]; const b = ns[j];
      const overlap = a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
      assert.ok(!overlap, `${a.id} overlaps ${b.id}`);
    }
  }
  for (const n of ns) {
    assert.ok(n.x >= 0 && n.y >= 0 && n.x + n.w <= L.width + 1e-9 && n.y + n.h <= L.height + 1e-9, `${n.id} inside bounds`);
  }
}

test('collapsed: spawned steps become one group per spawning step', () => {
  const g = buildGraph(wf.steps);
  const groups = g.nodes.filter((n) => n.kind === 'group');
  assert.deepEqual(groups.map((n) => n.id).sort(), [groupId('ZrNiSn.evaluate'), groupId('ZrNiSn.phonon')].sort());
  const ev = groups.find((n) => n.parent === 'ZrNiSn.evaluate');
  // evaluate spawned fc3 + evaluate.bte; fc3 spawned 8 (trimmed) displacement steps + kappa3
  const spawnedUnderEval = wf.steps.filter((s) => s.name.startsWith('ZrNiSn.evaluate/')).length;
  assert.equal(ev.total, spawnedUnderEval);
  assert.equal(ev.counts.RUNNING, 1);
  assert.equal(ev.counts.FAILED, 1);
  assert.equal(ev.state, 'FAILED');
  assert.equal(g.nodes.filter((n) => n.kind === 'step' && n.parent).length, 0, 'no spawned step is visible');
  // the group hangs off its spawning step
  assert.ok(g.edges.some((e) => e.from === 'ZrNiSn.evaluate' && e.to === ev.id && e.spawn));
  // spawned:true input (validate <- evaluate) also depends on the group
  assert.ok(g.edges.some((e) => e.from === ev.id && e.to === 'validate'));
  // phonon group feeds the evaluate step (spawned fc2/phonon inputs)
  assert.ok(g.edges.some((e) => e.from === groupId('ZrNiSn.phonon') && e.to === 'ZrNiSn.evaluate'));
  checkLayout(layout(g));
});

test('expanded: direct children visible, nested spawns grouped again', () => {
  const g = buildGraph(wf.steps, new Set(['ZrNiSn.evaluate']));
  const ids = new Set(g.nodes.map((n) => n.id));
  assert.ok(ids.has('ZrNiSn.evaluate/fc3'));
  assert.ok(ids.has('ZrNiSn.evaluate/evaluate.bte'));
  assert.ok(ids.has(groupId('ZrNiSn.evaluate/fc3')));
  assert.ok(!ids.has(groupId('ZrNiSn.evaluate')));
  assert.ok(!ids.has('ZrNiSn.evaluate/fc3/d0001'));
  assert.ok(g.edges.some((e) => e.from === 'ZrNiSn.evaluate/fc3' && e.to === 'ZrNiSn.evaluate/evaluate.bte'));
  checkLayout(layout(g));
});

test('fully expanded: every step is a node, layers respect all dependencies', () => {
  const parents = new Set(wf.steps.map((s) => s.parent_step).filter(Boolean));
  const g = buildGraph(wf.steps, parents);
  assert.equal(g.nodes.length, wf.steps.length);
  assert.ok(g.nodes.every((n) => n.kind === 'step'));
  const L = layout(g);
  checkLayout(L);
  assert.ok(L.byId.get('ZrNiSn.relax').layer > L.byId.get('ZrNiSn.build').layer);
  assert.ok(L.byId.get('validate').layer > L.byId.get('ZrNiSn.evaluate/evaluate.bte').layer);
});

test('edges are deduplicated and never self-loops', () => {
  const g = buildGraph(wf.steps);
  const keys = g.edges.map((e) => `${e.from}>${e.to}`);
  assert.equal(new Set(keys).size, keys.length);
  assert.ok(g.edges.every((e) => e.from !== e.to));
});

test('layout handles cycles and empty graphs', () => {
  const steps = [
    { name: 'a', state: 'COMPLETED', spec: { depends_on: ['c'] } },
    { name: 'b', state: 'RUNNING', spec: { depends_on: ['a'] } },
    { name: 'c', state: 'WAITING', spec: { depends_on: ['b'] } },
    { name: 'd', state: 'WAITING', spec: {} },
  ];
  const L = layout(buildGraph(steps));
  assert.equal(L.nodes.length, 4);
  const L0 = layout(buildGraph([]));
  assert.equal(L0.width, 0);
  assert.equal(L0.nodes.length, 0);
});

test('short-name dependencies of spawned siblings resolve', () => {
  const steps = [
    { name: 'p', state: 'COMPLETED', spec: {} },
    { name: 'p/x', parent_step: 'p', state: 'COMPLETED', spec: {} },
    { name: 'p/y', parent_step: 'p', state: 'QUEUED', spec: { depends_on: ['x'] } },
  ];
  const g = buildGraph(steps, new Set(['p']));
  assert.ok(g.edges.some((e) => e.from === 'p/x' && e.to === 'p/y'));
});

test('aggregateState', () => {
  assert.equal(aggregateState({ COMPLETED: 3 }), 'COMPLETED');
  assert.equal(aggregateState({ COMPLETED: 3, RUNNING: 1, QUEUED: 2 }), 'RUNNING');
  assert.equal(aggregateState({ QUEUED: 2, WAITING: 1 }), 'QUEUED');
  assert.equal(aggregateState({ WAITING: 1 }), 'WAITING');
  assert.equal(aggregateState({ COMPLETED: 1, FAILED: 1 }), 'FAILED');
  assert.equal(aggregateState({}), 'WAITING');
});

test('countStates follows display order', () => {
  assert.deepEqual(Object.keys(countStates([{ state: 'WAITING' }, { state: 'COMPLETED' }, { state: 'RUNNING' }])),
    ['COMPLETED', 'RUNNING', 'WAITING']);
});

test('real QE workflow sample lays out without overlaps', () => {
  const g = buildGraph(fixture('workflow_toy_ZrNiSn.json').steps, new Set(['ZrNiSn.phonon']));
  checkLayout(layout(g, { nodeW: 120, nodeH: 20, gapX: 20, gapY: 4 }));
});
