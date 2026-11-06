import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sortRows } from '../lib/sort.js';

test('numbers, missing last both ways, stable', () => {
  const rows = [{ v: 2 }, { v: null }, { v: 10 }, { v: 1 }, { v: undefined }, { v: NaN }];
  assert.deepEqual(sortRows(rows, (r) => r.v).map((r) => r.v).slice(0, 3), [1, 2, 10]);
  assert.deepEqual(sortRows(rows, (r) => r.v, 'desc').map((r) => r.v).slice(0, 3), [10, 2, 1]);
  const tail = sortRows(rows, (r) => r.v, 'desc').slice(3);
  assert.equal(tail[0], rows[1]);
  assert.equal(tail[1], rows[4]);
});

test('strings sort naturally; booleans as numbers', () => {
  assert.deepEqual(sortRows(['v10', 'v2', 'V1'].map((s) => ({ s })), (r) => r.s).map((r) => r.s), ['V1', 'v2', 'v10']);
  assert.deepEqual(sortRows([{ b: true }, { b: false }], (r) => r.b).map((r) => r.b), [false, true]);
});
