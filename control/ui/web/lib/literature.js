// Predicted-vs-literature matcher (pure). Mirrors the checks of
// science/fireforge_science/validate.py: when a te.validation record from the same
// workflow exists, its pass/fail and tolerances are used as-is; otherwise the same
// rule is evaluated here with the default tolerances and marked "computed".

import { calcView } from './proto.js';

export const DEFAULT_TOLERANCES = { lattice_rel: 0.02, gap_ev: 0.2, kappa_slack_factor: 3.0, kappa_bte_factor: 1.5 };

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

function newest(list) {
  let best = null;
  for (const c of list) if (!best || c.created > best.created || (c.created === best.created && c.calc_id > best.calc_id)) best = c;
  return best;
}

/** Workflows that produced PREDICTED calculations, newest first: [{workflow_id, created, count}]. */
export function predictedSources(calcs) {
  const m = new Map();
  for (const raw of calcs || []) {
    const c = calcView(raw);
    if (c.evidence !== 'PREDICTED' || c.kind.startsWith('literature.')) continue;
    const id = c.workflow_id || '';
    const cur = m.get(id) || { workflow_id: id, created: 0, count: 0 };
    cur.created = Math.max(cur.created, c.created);
    cur.count++;
    m.set(id, cur);
  }
  return [...m.values()].sort((a, b) => b.created - a.created);
}

/**
 * Compare one material's calculations with its literature records.
 * opts.workflowId: take predicted values from this workflow only (default: the
 * workflow with the newest predicted calculation; "" means calculations without one).
 */
export function compareWithLiterature(calcs, opts = {}) {
  const all = (calcs || []).map(calcView);
  const lit = all.filter((c) => c.evidence === 'LITERATURE' || c.evidence === 'MEASURED');
  const sources = predictedSources(all);
  const workflowId = opts.workflowId !== undefined ? opts.workflowId : (sources[0]?.workflow_id ?? '');
  const pred = all.filter((c) => c.evidence === 'PREDICTED' && (c.workflow_id || '') === workflowId);
  const ofKind = (list, kind, pred2 = () => true) => newest(list.filter((c) => c.kind === kind && pred2(c)));

  const validation = ofKind(pred, 'te.validation');
  const vprops = validation ? validation.props : {};
  const tolerances = { ...DEFAULT_TOLERANCES, ...((validation && validation.settings.tolerances) || {}) };
  const status = (checkName, computed) => {
    const k = `${checkName}_pass`;
    if (validation && typeof vprops[k] === 'boolean') return { status: vprops[k] ? 'pass' : 'fail', statusSource: 'te.validation' };
    if (computed === null || computed === undefined) return { status: 'no_data', statusSource: '' };
    return { status: computed ? 'pass' : 'fail', statusSource: 'computed' };
  };

  const rows = [];
  const litOf = (kind) => ofKind(lit, kind);

  // Lattice constant (relative deviation).
  {
    const l = litOf('literature.lattice');
    const p = ofKind(pred, 'qe.relax', (c) => isNum(c.props.a_angstrom));
    const lv = l?.props.a_angstrom; const pv = p?.props.a_angstrom;
    const dev = isNum(lv) && isNum(pv) && lv ? (pv - lv) / lv : null;
    if (l || p) rows.push({ key: 'lattice', label: 'Lattice constant a', unit: 'Å', mode: 'rel',
      predicted: pv ?? null, predictedCalc: p, literature: lv ?? null, literatureCalc: l,
      deviation: dev, tolerance: tolerances.lattice_rel,
      ...status('lattice', dev === null ? null : Math.abs(dev) <= tolerances.lattice_rel) });
  }

  // Band gap (absolute deviation) and its character.
  {
    const l = litOf('literature.band_gap');
    const p = ofKind(pred, 'qe.bands', (c) => isNum(c.props.band_gap_ev))
      || ofKind(pred, 'te.evaluate', (c) => isNum(c.props.band_gap_ev));
    const lv = l?.props.band_gap_ev; const pv = p?.props.band_gap_ev;
    const dev = isNum(lv) && isNum(pv) ? pv - lv : null;
    if (l || p) rows.push({ key: 'gap', label: `Band gap${l?.props.functional ? ` (${l.props.functional})` : ''}`, unit: 'eV',
      mode: 'abs', predicted: pv ?? null, predictedCalc: p, literature: lv ?? null, literatureCalc: l,
      deviation: dev, tolerance: tolerances.gap_ev,
      ...status('gap', dev === null ? null : Math.abs(dev) <= tolerances.gap_ev) });
    const ch = l?.props.character;
    const bands = ofKind(pred, 'qe.bands', (c) => typeof c.props.gap_direct === 'boolean');
    const pc = bands ? (bands.props.gap_direct ? 'direct' : 'indirect') : null;
    if (ch || pc) {
      const ok = (ch === 'direct' || ch === 'indirect') && pc ? ch === pc : null;
      rows.push({ key: 'gap_character', label: 'Gap character', unit: '', mode: 'match',
        predicted: pc, predictedCalc: bands, literature: ch ?? null, literatureCalc: l,
        deviation: null, tolerance: null, ...status('gap_character', ok) });
    }
  }

  // Lattice thermal conductivity at 300 K (ratio; Slack and BTE).
  {
    const l = litOf('literature.kappa_l');
    const lv = l?.props.kappa_l_w_mk_300k;
    const quantity = l?.props.quantity || 'lattice';
    for (const [src, kind, factorKey, label] of [
      ['slack', 'phonon.slack', 'kappa_slack_factor', 'κ_l 300 K (Slack)'],
      ['bte', 'phonon.bte', 'kappa_bte_factor', 'κ_l 300 K (BTE)']]) {
      const p = ofKind(pred, kind, (c) => isNum(c.props.kappa_l_w_mk_300k))
        || ofKind(pred, 'te.evaluate', (c) => c.conds.kappa_source === src && isNum(c.props.kappa_l_300k));
      const pv = p ? (p.kind === 'te.evaluate' ? p.props.kappa_l_300k : p.props.kappa_l_w_mk_300k) : undefined;
      if (src === 'bte' && !p && !(validation && `kappa_bte_pass` in vprops)) continue;
      if (!l && !p) continue;
      const f = tolerances[factorKey];
      const ratio = isNum(pv) && isNum(lv) && lv ? pv / lv : null;
      rows.push({ key: `kappa_${src}`, label, unit: 'W/m·K', mode: 'ratio',
        predicted: pv ?? null, predictedCalc: p, literature: lv ?? null, literatureCalc: l,
        deviation: ratio, tolerance: f, literatureQuantity: quantity,
        ...status(`kappa_${src}`, ratio === null ? null : ratio >= 1 / f && ratio <= f) });
    }
  }

  // Peak ZT (informational: no check in te.validate).
  {
    const l = litOf('literature.zt_peak');
    const carrier = l?.props.carrier_type === 'p' ? 'p' : 'n';
    const evs = pred.filter((c) => c.kind === 'te.evaluate');
    const p = newest(evs.filter((c) => c.conds.kappa_source === 'slack')) || newest(evs);
    const pv = p?.props[`zt_peak_${carrier}`];
    if (l || p) rows.push({ key: 'zt_peak', label: `Peak ZT (${carrier}-type)`, unit: '', mode: 'info',
      predicted: pv ?? null, predictedCalc: p, literature: l?.props.zt_peak ?? null, literatureCalc: l,
      predictedT: p?.props[`t_peak_${carrier}_k`] ?? null, literatureT: l?.conds.t_k ?? null,
      composition: l?.conds.composition || '', carrier,
      deviation: null, tolerance: null, status: 'info', statusSource: '' });
  }

  return { rows, validation, tolerances, workflowId, sources };
}

/**
 * Position of a deviation on a check track, 0..1, with the tolerance band
 * [lo, hi] also in 0..1. The track spans ±2 tolerances (rel/abs) or
 * [1/f², f²] on a log scale (ratio). Values beyond the track are clamped and
 * flagged `clamped`.
 */
export function trackPosition(row) {
  if (row.deviation === null || row.deviation === undefined || !row.tolerance) return null;
  if (row.mode === 'ratio') {
    const f = row.tolerance;
    const span = 2 * Math.log(f);
    const x = Math.log(row.deviation);
    const t = (x + span) / (2 * span);
    return { t: Math.min(1, Math.max(0, t)), lo: 0.25, hi: 0.75, mid: 0.5, clamped: t < 0 || t > 1 };
  }
  const span = 2 * row.tolerance;
  const t = (row.deviation + span) / (2 * span);
  return { t: Math.min(1, Math.max(0, t)), lo: 0.25, hi: 0.75, mid: 0.5, clamped: t < 0 || t > 1 };
}

/** Citation from a literature record's note: {text, doi}. */
export function citation(calc) {
  const note = calc?.note || '';
  const m = /doi:(10\.\d{4,9}\/[^\s;,)]+)/i.exec(note);
  const doi = m ? m[1] : '';
  const text = m ? note.slice(0, m.index).trim() : note;
  return { text, doi, rest: m ? note.slice(m.index + m[0].length).trim() : '' };
}
