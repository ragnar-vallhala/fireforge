import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compareWithLiterature, predictedSources, trackPosition, citation, DEFAULT_TOLERANCES } from '../lib/literature.js';
import { fixture } from './helpers.js';

const calcs = fixture('calcs_ZrNiSn.json').calculations;
const QE = 'wf-e3988d44d56f0017';
const TOY = 'wf-1db237cbfb7b2002';
const row = (r, key) => r.rows.find((x) => x.key === key);

test('sources: newest predicting workflow first, literature excluded', () => {
  const s = predictedSources(calcs);
  assert.deepEqual(s.map((x) => x.workflow_id), [QE, TOY]);
  assert.equal(s[0].count, 1);
});

test('default source = newest workflow; no validation there → computed status', () => {
  const r = compareWithLiterature(calcs);
  assert.equal(r.workflowId, QE);
  assert.equal(r.validation, null);
  const lat = row(r, 'lattice');
  assert.equal(lat.literature, 6.11);
  assert.ok(Math.abs(lat.predicted - 6.1503) < 1e-3);
  assert.ok(Math.abs(lat.deviation - (6.150253516385458 - 6.11) / 6.11) < 1e-12);
  assert.equal(lat.tolerance, DEFAULT_TOLERANCES.lattice_rel);
  assert.equal(lat.status, 'pass');
  assert.equal(lat.statusSource, 'computed');
  const gap = row(r, 'gap');
  assert.equal(gap.predicted, null, 'QE run has no bands yet');
  assert.equal(gap.literature, 0.52);
  assert.equal(gap.status, 'no_data');
});

test('toy workflow: statuses and tolerances come from its te.validation record', () => {
  const r = compareWithLiterature(calcs, { workflowId: TOY });
  assert.ok(r.validation);
  assert.equal(r.tolerances.kappa_bte_factor, 1.5);
  assert.equal(row(r, 'lattice').statusSource, 'te.validation');
  assert.equal(row(r, 'lattice').status, 'pass');
  assert.equal(row(r, 'gap').status, 'pass');
  assert.ok(Math.abs(row(r, 'gap').deviation - 0.032) < 1e-9);
  // te.validation says gap_character fails: computed direct vs literature indirect
  const ch = row(r, 'gap_character');
  assert.equal(ch.predicted, 'direct');
  assert.equal(ch.literature, 'indirect');
  assert.equal(ch.status, 'fail');
  const ks = row(r, 'kappa_slack');
  assert.equal(ks.status, 'fail');
  assert.ok(Math.abs(ks.deviation - 0.030036615957833395 / 10.1) < 1e-12);
  assert.equal(ks.predictedCalc.kind, 'phonon.slack');
  const kb = row(r, 'kappa_bte');
  assert.equal(kb.status, 'fail');
  assert.ok(Math.abs(kb.deviation - 0.07048991842988311) < 1e-9, 'same ratio as te.validation');
  const zt = row(r, 'zt_peak');
  assert.equal(zt.status, 'info');
  assert.equal(zt.carrier, 'n');
  assert.equal(zt.literature, 1.1);
  assert.equal(zt.literatureT, 873);
  assert.equal(zt.predictedCalc.conds.kappa_source, 'slack', 'prefers the Slack screening evaluation');
});

test('literature only (no predictions) still lists the references', () => {
  const lit = calcs.filter((c) => c.evidence === 'LITERATURE');
  const r = compareWithLiterature(lit);
  assert.deepEqual(r.rows.map((x) => x.key), ['lattice', 'gap', 'gap_character', 'kappa_slack', 'zt_peak']);
  assert.ok(r.rows.every((x) => x.predicted === null));
  assert.ok(r.rows.filter((x) => x.key !== 'zt_peak').every((x) => x.status === 'no_data'));
});

test('computed ratio check uses [1/f, f]', () => {
  const lit = calcs.filter((c) => c.kind === 'literature.kappa_l');
  const mk = (k) => ({ calc_id: 'calc-x', kind: 'phonon.slack', workflow_id: 'wf-x', created_unix_ms: '2',
    properties_json: JSON.stringify({ kappa_l_w_mk_300k: k }) });
  assert.equal(row(compareWithLiterature([...lit, mk(10.1 * 2.9)]), 'kappa_slack').status, 'pass');
  assert.equal(row(compareWithLiterature([...lit, mk(10.1 / 3.1)]), 'kappa_slack').status, 'fail');
});

test('trackPosition: band in the middle half, clamped outside', () => {
  assert.deepEqual(trackPosition({ mode: 'rel', deviation: 0, tolerance: 0.02 }), { t: 0.5, lo: 0.25, hi: 0.75, mid: 0.5, clamped: false });
  assert.ok(Math.abs(trackPosition({ mode: 'abs', deviation: 0.2, tolerance: 0.2 }).t - 0.75) < 1e-12);
  const far = trackPosition({ mode: 'ratio', deviation: 0.003, tolerance: 3 });
  assert.equal(far.t, 0);
  assert.ok(far.clamped);
  assert.ok(Math.abs(trackPosition({ mode: 'ratio', deviation: 3, tolerance: 3 }).t - 0.75) < 1e-12);
  assert.equal(trackPosition({ mode: 'info', deviation: null, tolerance: null }), null);
});

test('citation splits text and DOI', () => {
  const c = citation({ note: 'Muta et al., Mater. Trans. 47, 1453 (2006) doi:10.2320/matertrans.47.1453 undoped, room temperature' });
  assert.equal(c.doi, '10.2320/matertrans.47.1453');
  assert.equal(c.text, 'Muta et al., Mater. Trans. 47, 1453 (2006)');
  assert.equal(c.rest, 'undoped, room temperature');
  assert.equal(citation({}).doi, '');
});
