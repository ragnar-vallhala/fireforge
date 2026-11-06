import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseHash, buildHash, hrefForId } from '../lib/router.js';

test('parse: empty and root go to overview', () => {
  for (const h of ['', '#', '#/', '#//']) assert.equal(parseHash(h).page, 'overview');
});

test('parse: page, id, rest and query', () => {
  const r = parseHash('#/materials/mat-2ca105572a0e89c6?formula=Zr%20Ni&element=Sn');
  assert.equal(r.page, 'materials');
  assert.equal(r.id, 'mat-2ca105572a0e89c6');
  assert.deepEqual(r.query, { formula: 'Zr Ni', element: 'Sn' });
  const r2 = parseHash('#/workflows/wf-1/extra');
  assert.deepEqual(r2.rest, ['extra']);
  assert.equal(parseHash('#/jobs?state=').query.state, '');
});

test('parse: unknown page is notfound', () => {
  const r = parseHash('#/bogus/1');
  assert.equal(r.page, 'notfound');
  assert.equal(r.id, '');
});

test('parse: malformed escapes do not throw', () => {
  assert.equal(parseHash('#/jobs/%E0%A4%A').id, '%E0%A4%A');
  assert.deepEqual(parseHash('#/jobs?a=%E0%A4%A').query, {});
});

test('build round-trips', () => {
  const h = buildHash('calculations', '', { kind: 'te.evaluate', evidence: '', material_id: 'mat-1' });
  assert.equal(h, '#/calculations?kind=te.evaluate&material_id=mat-1');
  assert.deepEqual(parseHash(h).query, { kind: 'te.evaluate', material_id: 'mat-1' });
  assert.equal(buildHash('overview'), '#/');
  assert.equal(buildHash('workflows', 'wf-a/b'), '#/workflows/wf-a%2Fb');
  assert.equal(parseHash(buildHash('workflows', 'wf-a/b')).id, 'wf-a/b');
});

test('hrefForId by prefix', () => {
  assert.equal(hrefForId('job-1'), '#/jobs/job-1');
  assert.equal(hrefForId('ff-node-46'), '#/nodes/ff-node-46');
  assert.equal(hrefForId('camp-0e'), '#/campaigns/camp-0e');
  assert.equal(hrefForId('st-1'), '');
  assert.equal(hrefForId(undefined), '');
});
