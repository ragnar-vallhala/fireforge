import { test } from 'node:test';
import assert from 'node:assert/strict';
import { curvesOf, kappaPoints, rankingRow, validationChecks, keyProps, unitOf, latticeParams, cleanFrac, splitProps } from '../lib/science.js';
import { fixture } from './helpers.js';

const calcs = fixture('calcs_ZrNiSn.json').calculations;
const withCurves = fixture('te_evaluate_curves.json');

test('curves: aligned arrays with nulls; absent → null', () => {
  const c = curvesOf(withCurves);
  assert.equal(c.t_k.length, 25);
  for (const k of ['zt_p', 'zt_n', 'pf_p', 'pf_n', 'kappa_l']) assert.equal(c[k].length, 25, k);
  assert.equal(c.zt_n[24], null);
  const old = calcs.find((x) => x.kind === 'te.evaluate');
  assert.equal(curvesOf(old), null);
  const noKappa = { ...withCurves, properties_json: JSON.stringify({ curves: { t_k: [300, 400], zt_p: [0.1], kappa_l_w_mk: null } }) };
  const c2 = curvesOf(noKappa);
  assert.deepEqual(c2.zt_p, [0.1, null]);
  assert.equal(c2.kappa_l, null);
  assert.equal(c2.zt_n, null);
});

test('kappaPoints from phonon.slack properties', () => {
  const s = calcs.find((x) => x.kind === 'phonon.slack');
  const pts = kappaPoints(s);
  assert.deepEqual(pts.map((p) => p[0]), [300, 500, 800, 1100]);
  assert.ok(Math.abs(pts[0][1] - 0.030036615957833395) < 1e-15);
});

test('rankingRow', () => {
  const r = rankingRow(calcs.find((x) => x.kind === 'te.evaluate'));
  assert.equal(r.formula, 'ZrNiSn');
  assert.equal(r.kappa_source, 'bte');
  assert.equal(r.engine, 'toy');
  assert.ok(Math.abs(r.zt_p - 2.6887) < 1e-3);
  assert.equal(r.t_p, 1100);
  assert.equal(r.screen_pass, false);
  assert.equal(r.e_hull, 0);
  assert.equal(r.stable, false);
  assert.equal(r.zt_max, r.zt_p);
});

test('validationChecks from te.validation', () => {
  const v = validationChecks(calcs.find((x) => x.kind === 'te.validation'));
  assert.deepEqual(v.map((c) => c.name), ['lattice', 'gap', 'gap_character', 'stable', 'kappa_slack', 'kappa_bte']);
  assert.equal(v[0].status, 'pass');
  assert.equal(v[0].mode, 'rel');
  assert.equal(v[2].status, 'fail');
  assert.equal(v[2].value, null);
  assert.equal(v[5].mode, 'ratio');
});

test('keyProps prefers the kind\'s headline properties', () => {
  const relax = calcs.find((x) => x.kind === 'qe.relax');
  assert.deepEqual(keyProps(relax, 2).map((k) => k[0]), ['a_angstrom', 'energy_ev_per_atom']);
  const val = keyProps(calcs.find((x) => x.kind === 'te.validation'), 3);
  assert.deepEqual(val, [['lattice_pass', true], ['gap_pass', true], ['gap_character_pass', false]]);
});

test('units by key suffix', () => {
  assert.equal(unitOf('a_angstrom'), 'Å');
  assert.equal(unitOf('energy_ev_per_atom'), 'eV/atom');
  assert.equal(unitOf('kappa_l_w_mk_300k'), 'W/m·K');
  assert.equal(unitOf('kappa_l_300k'), 'W/m·K');
  assert.equal(unitOf('pf_peak_p_uw_cmk2'), 'µW/cm·K²');
  assert.equal(unitOf('t_peak_p_k'), 'K');
  assert.equal(unitOf('doping_peak_n_cm3'), 'cm⁻³');
  assert.equal(unitOf('screen_pass'), '');
});

test('lattice parameters of an fcc primitive cell', () => {
  const p = latticeParams([0, 3.055, 3.055, 3.055, 0, 3.055, 3.055, 3.055, 0]);
  assert.ok(Math.abs(p.a - 3.055 * Math.SQRT2) < 1e-9);
  assert.ok(Math.abs(p.alpha - 60) < 1e-9);
  assert.ok(Math.abs(p.volume - (6.11 ** 3) / 4) < 1e-6);
  assert.equal(latticeParams([1, 2]), null);
  assert.equal(cleanFrac(1.1303232447858504e-16), 0);
  assert.equal(cleanFrac(0.24999999999999992), 0.25);
});

test('splitProps separates nested values', () => {
  const { scalars, nested } = splitProps({ b: 1, a: 2, c: [1], d: { x: 1 } });
  assert.deepEqual(scalars.map((x) => x[0]), ['a', 'b']);
  assert.deepEqual(nested.map((x) => x[0]), ['c', 'd']);
});

import { alignSeries } from '../lib/science.js';
test('alignSeries unions x and fills null', () => {
  const r = alignSeries([{ x: [300, 500], y: [1, 2] }, { x: [400, 500], y: [3, null] }, { x: [873], y: [1.1] }]);
  assert.deepEqual(r.x, [300, 400, 500, 873]);
  assert.deepEqual(r.ys, [[1, null, 2, null], [null, 3, null, null], [null, null, null, 1.1]]);
});
