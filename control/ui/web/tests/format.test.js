import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as f from '../lib/format.js';

test('bytes: int64 strings, units, missing', () => {
  assert.equal(f.bytes('32212254720'), '30.0 GiB');
  assert.equal(f.bytes(512), '512 B');
  assert.equal(f.bytes('2147483648'), '2.0 GiB');
  assert.equal(f.bytes('1005867986944'), '937 GiB');
  assert.equal(f.bytes(undefined), '—');
  assert.equal(f.bytes(''), '—');
  assert.equal(f.bytes('nope'), '—');
});

test('duration', () => {
  assert.equal(f.duration(850), '850 ms');
  assert.equal(f.duration(12_000), '12s');
  assert.equal(f.duration(192_000), '3m 12s');
  assert.equal(f.duration(2 * 3600_000 + 5 * 60_000), '2h 05m');
  assert.equal(f.duration(3 * 86400_000 + 3600_000), '3d 01h');
  assert.equal(f.duration(undefined), '—');
  assert.equal(f.duration(-5), '—');
});

test('ago is relative to the given now', () => {
  const now = 1_793_971_023_670;
  assert.equal(f.ago(String(now - 65_000), now), '1m 05s ago');
  assert.equal(f.ago(String(now + 120_000), now), 'in 2m 00s');
  assert.equal(f.ago(String(now - 1000), now), 'just now');
  assert.equal(f.ago(undefined, now), '—');
});

test('timestamp in UTC', () => {
  assert.equal(f.timestamp('1793964391441', { utc: true }), '2026-11-06 11:26:31');
  assert.equal(f.timestamp(undefined), '—');
});

test('num: significant digits, scientific outside range, booleans', () => {
  assert.equal(f.num(6.150253516385458), '6.15');
  assert.equal(f.num(-0.785340034308041), '-0.785');
  assert.equal(f.num(1e20), '1.00e20');
  assert.equal(f.num(1e-14), '1.00e-14');
  assert.equal(f.num(0), '0');
  assert.equal(f.num(364), '364');
  assert.equal(f.num(true), 'yes');
  assert.equal(f.num(null), '—');
  assert.equal(f.num('indirect'), 'indirect');
});

test('percent, signed, fixed', () => {
  assert.equal(f.percent(0.92522821576763481), '93%');
  assert.equal(f.signed(-0.0036, 4), '−0.0036');
  assert.equal(f.signed(0.032, 3), '+0.032');
  assert.equal(f.fixed(undefined), '—');
  assert.equal(f.fixed(2.68870284, 2), '2.69');
});

test('ids', () => {
  assert.equal(f.shortId('job-3d63a3ddbfafc942'), 'job-3d63a3dd');
  assert.equal(f.shortId('ff-node-46dea36f2b2afb4e'), 'ff-node-46dea36f');
  assert.equal(f.shortId('my-custom-job'), 'my-custom-job');
  assert.equal(f.shortArtifact('artifact://sha256/' + 'a'.repeat(64)), 'sha256:aaaaaaaaaaaa…');
});

test('states: tone, glyph, label, order', () => {
  assert.equal(f.stateTone('RUNNING'), 'active');
  assert.equal(f.stateTone('completed'), 'good');
  assert.equal(f.stateTone('VALIDATION_FAILED'), 'bad');
  assert.equal(f.stateTone('STALE'), 'warn');
  assert.equal(f.stateTone('whatever'), 'idle');
  assert.equal(f.stateGlyph('FAILED'), '✕');
  assert.equal(f.stateLabel('RETRY_PENDING'), 'retry pending');
  assert.deepEqual(f.orderedCounts({ WAITING: 16, COMPLETED: 34, RUNNING: 5, QUEUED: 29, ODD: 1 }),
    [['COMPLETED', 34], ['RUNNING', 5], ['QUEUED', 29], ['WAITING', 16], ['ODD', 1]]);
  assert.deepEqual(f.actor('admin:alice'), { role: 'admin', name: 'alice' });
});
