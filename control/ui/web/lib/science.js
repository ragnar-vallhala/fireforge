// Science record helpers (pure): te.evaluate curves, κ_l(T) points, ranking rows and
// a short summary of the key properties of each record kind (phase8.md §4).

import { calcView } from './proto.js';

const fin = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/**
 * te.evaluate `curves` → aligned arrays (null where missing), or null when the record
 * predates curves or they are malformed.
 */
export function curvesOf(raw) {
  const c = calcView(raw);
  const cv = c?.props?.curves;
  if (!cv || !Array.isArray(cv.t_k) || cv.t_k.length < 2) return null;
  const n = cv.t_k.length;
  const align = (a) => (Array.isArray(a) ? Array.from({ length: n }, (_, i) => fin(a[i])) : null);
  const t = cv.t_k.map(fin);
  if (t.some((x) => x === null)) return null;
  return {
    t_k: t,
    zt_p: align(cv.zt_p), zt_n: align(cv.zt_n),
    pf_p: align(cv.pf_p_uw_cmk2), pf_n: align(cv.pf_n_uw_cmk2),
    kappa_l: align(cv.kappa_l_w_mk),
  };
}

/** phonon.slack / phonon.bte record → [[T, κ_l], …] from kappa_l_w_mk_<T>k keys, sorted by T. */
export function kappaPoints(raw) {
  const c = calcView(raw);
  const pts = [];
  for (const [k, v] of Object.entries(c?.props || {})) {
    const m = /^kappa_l_w_mk_(\d+(?:\.\d+)?)k$/.exec(k);
    if (m && fin(v) !== null) pts.push([Number(m[1]), v]);
  }
  return pts.sort((a, b) => a[0] - b[0]);
}

/** One row of the thermoelectric ranking. */
export function rankingRow(raw) {
  const c = calcView(raw);
  const p = c.props;
  return {
    calc: c,
    calc_id: c.calc_id,
    formula: c.reduced_formula || '',
    material_id: c.material_id || '',
    kappa_source: c.conds.kappa_source || c.settings.kappa_source || '',
    engine: c.engine,
    preset: c.method.preset || '',
    zt_p: fin(p.zt_peak_p), t_p: fin(p.t_peak_p_k),
    zt_n: fin(p.zt_peak_n), t_n: fin(p.t_peak_n_k),
    zt_max: Math.max(fin(p.zt_peak_p) ?? -Infinity, fin(p.zt_peak_n) ?? -Infinity),
    kappa_l_300k: fin(p.kappa_l_300k),
    gap: fin(p.band_gap_ev),
    e_hull: fin(p.e_above_hull_ev_per_atom),
    pf: fin(p.pf_peak_uw_cmk2),
    screen_pass: typeof p.screen_pass === 'boolean' ? p.screen_pass : null,
    stable: typeof p.dynamically_stable === 'boolean' && typeof p.born_stable === 'boolean'
      ? p.dynamically_stable && p.born_stable : null,
    workflow_id: c.workflow_id || '',
    created: c.created,
    has_curves: Boolean(p.curves),
  };
}

/** Check names of a te.validation record, in display order, from its *_pass keys. */
export const CHECK_ORDER = ['lattice', 'gap', 'gap_character', 'stable', 'kappa_slack', 'kappa_bte'];
export const CHECK_LABELS = {
  lattice: 'Lattice', gap: 'Gap', gap_character: 'Gap character', stable: 'Stable',
  kappa_slack: 'κ_l Slack', kappa_bte: 'κ_l BTE',
};

export function validationChecks(raw) {
  const c = calcView(raw);
  const p = c.props;
  const names = new Set();
  for (const k of Object.keys(p)) {
    const m = /^(.*)_(pass|deviation_rel|deviation_ev|ratio)$/.exec(k);
    if (m) names.add(m[1]);
  }
  const ordered = [...CHECK_ORDER.filter((n) => names.has(n)), ...[...names].filter((n) => !CHECK_ORDER.includes(n)).sort()];
  return ordered.map((name) => {
    const pass = p[`${name}_pass`];
    let value = null; let mode = '';
    if (fin(p[`${name}_deviation_rel`]) !== null) { value = p[`${name}_deviation_rel`]; mode = 'rel'; }
    else if (fin(p[`${name}_deviation_ev`]) !== null) { value = p[`${name}_deviation_ev`]; mode = 'abs'; }
    else if (fin(p[`${name}_ratio`]) !== null) { value = p[`${name}_ratio`]; mode = 'ratio'; }
    return { name, label: CHECK_LABELS[name] || name, status: pass === true ? 'pass' : pass === false ? 'fail' : 'no_data', value, mode };
  });
}

// Key properties shown for each record kind in summaries (first ones that exist).
const KEY_PROPS = {
  'qe.relax': ['a_angstrom', 'energy_ev_per_atom', 'pressure_kbar', 'magnetization_mub'],
  'qe.scf': ['energy_ev_per_atom', 'fermi_ev'],
  'qe.bands': ['band_gap_ev', 'gap_direct', 'vbm_ev', 'cbm_ev'],
  'stability.elastic': ['bulk_gpa', 'shear_gpa', 'born_stable', 'theta_d_elastic_k'],
  'stability.formation': ['formation_ev_per_atom', 'e_above_hull_ev_per_atom', 'on_hull'],
  'phonon.harmonic': ['dynamically_stable', 'min_freq_thz', 'theta_d_k', 'gruneisen'],
  'phonon.slack': ['kappa_l_w_mk_300k', 'kappa_l_w_mk_800k'],
  'phonon.bte': ['kappa_l_w_mk_300k', 'kappa_l_w_mk_800k'],
  'transport.crta': ['pf_max_p_uw_cmk2', 'pf_max_n_uw_cmk2', 'seebeck_p_uv_k_300k', 'tau_s'],
  'te.evaluate': ['zt_peak_p', 'zt_peak_n', 'kappa_l_300k', 'screen_pass'],
  'te.validation': [],
  'literature.lattice': ['a_angstrom'],
  'literature.band_gap': ['band_gap_ev', 'character'],
  'literature.kappa_l': ['kappa_l_w_mk_300k', 'quantity'],
  'literature.zt_peak': ['zt_peak', 'carrier_type'],
};

/** [[key, value], …] of up to `max` key properties of a calculation. */
export function keyProps(raw, max = 4) {
  const c = calcView(raw);
  const p = c.props || {};
  if (c.kind === 'te.validation') {
    return validationChecks(c).slice(0, max).map((ch) => [`${ch.name}_pass`, ch.status === 'pass' ? true : ch.status === 'fail' ? false : null]);
  }
  const keys = (KEY_PROPS[c.kind] || []).filter((k) => k in p);
  const rest = Object.keys(p).filter((k) => !keys.includes(k) && typeof p[k] !== 'object');
  return [...keys, ...rest].slice(0, max).map((k) => [k, p[k]]);
}

/** Properties split into scalars and structured values (lists, objects) for display. */
export function splitProps(props) {
  const scalars = []; const nested = [];
  for (const [k, v] of Object.entries(props || {})) {
    (v !== null && typeof v === 'object' ? nested : scalars).push([k, v]);
  }
  scalars.sort((a, b) => a[0].localeCompare(b[0]));
  return { scalars, nested };
}

/** Unit from a property key's suffix, for table display ("a_angstrom" → "Å"). */
const UNITS = [
  [/_angstrom$/, 'Å'], [/_ev_per_atom$/, 'eV/atom'], [/_ev$/, 'eV'], [/_kbar$/, 'kbar'], [/_gpa$/, 'GPa'],
  [/_thz$/, 'THz'], [/_w_mk(_\d+k)?$/, 'W/m·K'], [/kappa_l_300k$/, 'W/m·K'], [/_uw_cmk2$/, 'µW/cm·K²'],
  [/_uv_k(_\d+k)?$/, 'µV/K'], [/_cm3$/, 'cm⁻³'], [/_k$/, 'K'], [/_m_s$/, 'm/s'], [/_mub$/, 'µB'],
  [/_a3_per_atom$/, 'Å³/atom'], [/_usd_kg$/, 'USD/kg'], [/_ppm(_min)?$/, 'ppm'], [/^tau_s$/, 's'],
  [/_w_mk2s$/, 'W/m·K²·s'], [/_rel$/, 'rel.'],
];
export function unitOf(key) {
  for (const [re, u] of UNITS) if (re.test(key)) return u;
  return '';
}

/** Lattice vectors (9 floats) → lengths, angles, volume. */
export function latticeParams(lat) {
  if (!Array.isArray(lat) || lat.length !== 9) return null;
  const v = [lat.slice(0, 3), lat.slice(3, 6), lat.slice(6, 9)];
  const dot = (a, b) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
  const len = (a) => Math.sqrt(dot(a, a));
  const ang = (a, b) => (Math.acos(Math.max(-1, Math.min(1, dot(a, b) / (len(a) * len(b))))) * 180) / Math.PI;
  const [a, b, c] = v;
  const cross = [b[1] * c[2] - b[2] * c[1], b[2] * c[0] - b[0] * c[2], b[0] * c[1] - b[1] * c[0]];
  return { a: len(a), b: len(b), c: len(c), alpha: ang(b, c), beta: ang(a, c), gamma: ang(a, b), volume: Math.abs(dot(a, cross)) };
}

/** Round tiny float noise in fractional coordinates (1.1e-16 → 0, 0.9999999 → 1). */
export function cleanFrac(x) {
  if (typeof x !== 'number') return x;
  const r = Math.round(x * 1e6) / 1e6;
  return Object.is(r, -0) ? 0 : r;
}

/**
 * Align several (x, y) series on the union of their x values for uPlot:
 * input [{x:[…], y:[…]}, …] → {x:[sorted union], ys:[[y or null per x], …]}.
 */
export function alignSeries(list) {
  const xs = [...new Set(list.flatMap((s) => s.x.filter((v) => typeof v === 'number' && Number.isFinite(v))))].sort((a, b) => a - b);
  const index = new Map(xs.map((x, i) => [x, i]));
  const ys = list.map((s) => {
    const out = new Array(xs.length).fill(null);
    s.x.forEach((x, i) => { if (index.has(x)) out[index.get(x)] = fin(s.y[i]); });
    return out;
  });
  return { x: xs, ys };
}
