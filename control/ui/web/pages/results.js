// #/results: thermoelectric ranking (te.evaluate) and the latest te.validation checks.

import { h, replace, time, section, errorBanner, loading, pageHead, toolbar, select, cellBar, empty, stateBadge } from '../lib/ui.js';
import { api, getAll } from '../lib/api.js';
import { num, signed, fixed } from '../lib/format.js';
import { calcView } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';
import { rankingRow, validationChecks, CHECK_ORDER, CHECK_LABELS } from '../lib/science.js';

function yesNo(v, yes = 'yes', no = 'no') {
  if (v === null || v === undefined) return h('span', { class: 'muted' }, '—');
  return stateBadge(v ? 'PASS' : 'FAIL', { label: v ? yes : no });
}

function latestPer(list, keyFn) {
  const m = new Map();
  for (const x of list) { const k = keyFn(x); const cur = m.get(k); if (!cur || x.created > cur.created) m.set(k, x); }
  return [...m.values()];
}

function checkValue(ch) {
  if (ch.value === null) return '';
  if (ch.mode === 'rel') return `${signed(ch.value * 100, 1)}%`;
  if (ch.mode === 'abs') return `${signed(ch.value, 2)} eV`;
  if (ch.mode === 'ratio') return `×${num(ch.value, 2)}`;
  return '';
}

export async function render(el, route, ctx) {
  const q = { engine: route.query.engine ?? '', kappa: route.query.kappa || '', all: route.query.all || '' };
  const tools = h('div');
  const body = h('div', null, loading());
  const valBody = h('div', null, loading());
  el.append(pageHead('Thermoelectric results', { sub: 'Peak ZT under the constant relaxation-time approximation with an assumed τ; ranks candidates, not absolute performance.' }),
    section('Ranking', tools, body),
    section('Validation against literature', valBody));

  let evals = []; let vals = []; let wfNames = new Map(); let engines = [];
  let max = 1;
  const peak = (zt, t) => (zt === null || zt === undefined ? null
    : h('span', { class: 'peak' }, cellBar(zt, max), t === null || t === undefined ? null : h('span', { class: 'muted small peak-t' }, `${Math.round(t)} K`)));
  const table = sortableTable([
    { key: 'formula', label: 'Material', render: (r) => h('a', { href: buildHash('materials', r.material_id), class: 'formula' }, r.formula) },
    { key: 'kappa_source', label: 'κ_l from', render: (r) => r.kappa_source || '—' },
    { key: 'zt_p', label: 'Peak ZT p', num: true, render: (r) => peak(r.zt_p, r.t_p), title: 'Best-doping peak ZT, p-type, and the temperature of the peak' },
    { key: 'zt_n', label: 'Peak ZT n', num: true, render: (r) => peak(r.zt_n, r.t_n), title: 'Best-doping peak ZT, n-type, and the temperature of the peak' },
    { key: 'kappa_l_300k', label: 'κ_l 300 K', num: true, render: (r) => num(r.kappa_l_300k), title: 'W/m·K' },
    { key: 'gap', label: 'Gap (eV)', num: true, render: (r) => fixed(r.gap, 3) },
    { key: 'e_hull', label: 'E_hull (eV/at)', num: true, render: (r) => fixed(r.e_hull, 3) },
    { key: 'pf', label: 'PF peak', num: true, render: (r) => num(r.pf), title: 'µW/cm·K²' },
    { key: 'stable', label: 'Stable', value: (r) => r.stable, render: (r) => yesNo(r.stable) },
    { key: 'screen_pass', label: 'Screen', value: (r) => r.screen_pass, render: (r) => yesNo(r.screen_pass, 'pass', 'fail') },
    { key: 'engine', label: 'Engine' },
    { key: 'workflow', cls: 'nowrap', label: 'Workflow', value: (r) => wfNames.get(r.workflow_id) || r.workflow_id,
      render: (r) => (r.workflow_id ? h('a', { href: buildHash('workflows', r.workflow_id) }, wfNames.get(r.workflow_id) || r.workflow_id) : null) },
    { key: 'created', label: 'Recorded', render: (r) => h('a', { href: buildHash('calculations', r.calc_id) }, time(r.calc.created_unix_ms)) },
  ], { sort: { key: 'zt_p', dir: 'desc' }, empty: 'No te.evaluate results match.' });

  const drawTools = () => {
    replace(tools, toolbar(
      select('Engine', [['', 'Any engine'], ...engines.map((e) => [e, e === 'toy' ? 'toy (not physical)' : e])], q.engine, (v) => { q.engine = v; ctx.setQuery(q); draw(); }),
      select('κ_l source', [['', 'Any'], ['slack', 'Slack model'], ['bte', 'phono3py BTE']], q.kappa, (v) => { q.kappa = v; ctx.setQuery(q); draw(); }),
      select('Records', [['', 'Latest per material and κ source'], ['1', 'Every evaluation']], q.all, (v) => { q.all = v; ctx.setQuery(q); draw(); })));
  };

  const draw = () => {
    let rows = evals.map(rankingRow).filter((r) => (!q.engine || r.engine === q.engine) && (!q.kappa || r.kappa_source === q.kappa));
    if (!q.all) rows = latestPer(rows, (r) => `${r.material_id}|${r.kappa_source}`);
    max = Math.max(0.0001, ...rows.flatMap((r) => [r.zt_p ?? 0, r.zt_n ?? 0]));
    table.update(rows);

    const vs = latestPer(vals.filter((v) => !q.engine || v.engine === q.engine), (v) => v.material_id);
    if (!vs.length) { replace(valBody, empty('No te.validation records yet.', 'The validate step runs after every evaluate step of the workflow.')); return; }
    const names = new Set();
    const checks = new Map(vs.map((v) => { const cs = validationChecks(v); cs.forEach((c) => names.add(c.name)); return [v.calc_id, new Map(cs.map((c) => [c.name, c]))]; }));
    const ordered = [...CHECK_ORDER.filter((n) => names.has(n)), ...[...names].filter((n) => !CHECK_ORDER.includes(n))];
    const cols = [
      { key: 'formula', label: 'Material', value: (v) => v.reduced_formula, render: (v) => h('a', { href: buildHash('materials', v.material_id, { source: v.workflow_id || '' }), class: 'formula' }, v.reduced_formula) },
      ...ordered.map((n) => ({ key: n, label: CHECK_LABELS[n] || n, value: (v) => checks.get(v.calc_id).get(n)?.status || '',
        render: (v) => { const ch = checks.get(v.calc_id).get(n); if (!ch) return null;
          return h('span', { class: 'check-cell' }, stateBadge(ch.status === 'pass' ? 'PASS' : ch.status === 'fail' ? 'FAIL' : 'NO_DATA', { label: ch.status === 'no_data' ? 'no data' : ch.status }),
            ch.value !== null ? h('span', { class: 'muted small' }, ` ${checkValue(ch)}`) : null); } })),
      { key: 'passed', label: 'Passed', num: true, value: (v) => [...checks.get(v.calc_id).values()].filter((c) => c.status === 'pass').length,
        render: (v) => { const cs = [...checks.get(v.calc_id).values()]; return `${cs.filter((c) => c.status === 'pass').length} / ${cs.filter((c) => c.status !== 'no_data').length}`; } },
      { key: 'engine', label: 'Engine', value: (v) => v.engine },
      { key: 'created', label: 'Recorded', value: (v) => v.created, render: (v) => h('a', { href: buildHash('calculations', v.calc_id) }, time(v.created_unix_ms)) },
    ];
    const t = sortableTable(cols, { sort: { key: 'formula', dir: 'asc' }, dense: true });
    t.update(vs);
    const tol = vs[0].settings.tolerances || {};
    replace(valBody, t.el, h('p', { class: 'muted small' },
      `Tolerances: lattice ±${num((tol.lattice_rel ?? 0.02) * 100)} %, gap ±${num(tol.gap_ev ?? 0.2)} eV, κ_l within ×${num(tol.kappa_slack_factor ?? 3)} (Slack) and ×${num(tol.kappa_bte_factor ?? 1.5)} (BTE) of experiment. `,
      'Per-material details are on each material page.'));
  };

  let shown = false;
  ctx.poll(async () => {
    try {
      const [e, v, w] = await Promise.all([
        getAll('/calculations', { kind: 'te.evaluate', limit: 1000 }, 'calculations'),
        getAll('/calculations', { kind: 'te.validation', limit: 1000 }, 'calculations'),
        api.get('/workflows', { limit: 1000 }).catch(() => ({ workflows: [] })),
      ]);
      evals = e.map(calcView); vals = v.map(calcView);
      wfNames = new Map((w.workflows || []).map((x) => [x.workflow_id, x.name]));
      const eng = [...new Set([...evals, ...vals].map((c) => c.engine).filter(Boolean))].sort();
      if (eng.join() !== engines.join()) {
        engines = eng;
        if (route.query.engine === undefined && !q.engine && eng.some((x) => x !== 'toy') && eng.includes('toy')) {
          q.engine = eng.find((x) => x !== 'toy') || '';   // default to physical results when there are any
        }
        drawTools();
      }
      if (!shown) { replace(body, table.el); shown = true; }
      draw();
    } catch (err) { shown = false; replace(body, errorBanner(err, ctx.role)); }
  }, { intervalMs: 15000 });
}
