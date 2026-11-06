import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as p from '../lib/proto.js';
import { fixture } from './helpers.js';

test('toNum handles int64 strings and omitted defaults', () => {
  assert.equal(p.toNum('32212254720'), 32212254720);
  assert.equal(p.toNum(undefined), 0);
  assert.equal(p.toNum(''), 0);
  assert.equal(p.toNum(5), 5);
  assert.ok(Number.isNaN(p.toNum('x')));
});

test('parseJson: strings, empty, invalid', () => {
  assert.deepEqual(p.parseJson('{"a": 1}'), { a: 1 });
  assert.deepEqual(p.parseJson(''), {});
  assert.deepEqual(p.parseJson(undefined), {});
  assert.deepEqual(p.parseJson('{bad'), {});
  assert.deepEqual(p.parseJson('null', []), []);
});

test('calcView parses *_json and fills the PREDICTED default', () => {
  const calcs = fixture('calcs_ZrNiSn.json').calculations;
  const relax = p.calcView(calcs.find((c) => c.kind === 'qe.relax'));
  assert.equal(relax.evidence, 'PREDICTED');
  assert.equal(typeof relax.props.a_angstrom, 'number');
  assert.equal(relax.conds.fix_cell, false);
  assert.ok(Array.isArray(relax.settings.kmesh));
  assert.equal(relax.conv.converged, true);
  assert.equal(typeof relax.created, 'number');
  const lit = p.calcView(calcs.find((c) => c.kind === 'literature.lattice'));
  assert.equal(lit.evidence, 'LITERATURE');
  assert.deepEqual(lit.settings, {});   // method {} has no settings_json
  assert.equal(p.calcView(relax), relax);  // idempotent
});

test('latestByKind keeps the newest per kind', () => {
  const calcs = fixture('calcs_ZrNiSn.json').calculations;
  const m = p.latestByKind(calcs);
  const relaxes = calcs.filter((c) => c.kind === 'qe.relax').map(p.calcView);
  const newest = Math.max(...relaxes.map((c) => c.created));
  assert.equal(m.get('qe.relax').created, newest);
  assert.ok(m.has('literature.zt_peak'));
});

test('resources fill omitted fields with zero', () => {
  const n = fixture('nodes.json').nodes[0];
  assert.deepEqual(p.resources(n.last_heartbeat.capacity), { cpu_cores: 20, memory_bytes: 32212254720, disk_bytes: 0, gpu_count: 0 });
});

test('errorOf: router shape, nested shape, plain text', () => {
  assert.deepEqual(p.errorOf({ code: 'forbidden', message: 'admin only' }, 403), { code: 'forbidden', message: 'admin only' });
  assert.deepEqual(p.errorOf({ error: { code: 'csrf', message: 'cross-site' } }, 403), { code: 'csrf', message: 'cross-site' });
  assert.deepEqual(p.errorOf('gateway', 502), { code: 'http_502', message: 'gateway' });
  assert.deepEqual(p.errorOf(null, 500), { code: 'http_500', message: '' });
});

test('stepCounts and sum', () => {
  const c = p.stepCounts({ step_counts: { COMPLETED: 34, WAITING: '16' } });
  assert.deepEqual(c, { COMPLETED: 34, WAITING: 16 });
  assert.equal(p.sum(c), 50);
});
