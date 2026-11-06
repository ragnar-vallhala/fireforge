// #/materials and #/materials/ID: structures, latest calculation per kind,
// predicted vs literature, ZT(T) / power factor / κ_l(T) charts.

import { h, replace, clear, idLink, time, kv, section, sectionWith, errorBanner, empty, loading, pageHead, toolbar, select,
  input, valueText, stateBadge, noticeBanner } from '../lib/ui.js';
import { api, getAll } from '../lib/api.js';
import { num, timestamp, signed, fixed } from '../lib/format.js';
import { toNum, calcView, latestByKind } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';
import { compareWithLiterature, trackPosition, citation } from '../lib/literature.js';
import { curvesOf, kappaPoints, keyProps, latticeParams, cleanFrac, unitOf, alignSeries } from '../lib/science.js';
import { chartBlock } from '../lib/chart.js';

export function elementChips(elements) {
  return h('span', { class: 'elements' }, (elements || []).map((e) => h('span', { class: 'el-tile' }, e)));
}

export async function render(el, route, ctx) {
  if (route.id) return renderDetail(el, route, ctx);
  const q = { formula: route.query.formula || '', element: route.query.element || '', campaign_id: route.query.campaign_id || '' };
  const tools = h('div');
  const body = h('div', null, loading());
  el.append(pageHead('Materials', { sub: 'Compounds and elements in the materials database, with their structures and calculations.' }), section(null, tools, body));
  let all = [];
  const table = sortableTable([
    { key: 'reduced_formula', label: 'Formula', render: (m) => h('a', { href: buildHash('materials', m.material_id), class: 'formula' }, m.reduced_formula) },
    { key: 'elements', label: 'Elements', value: (m) => (m.elements || []).length, render: (m) => elementChips(m.elements) },
    { key: 'structures', label: 'Structures', num: true, value: (m) => toNum(m.structure_count) },
    { key: 'calcs', label: 'Calculations', num: true, value: (m) => toNum(m.calculation_count) },
    { key: 'created', label: 'Added', value: (m) => toNum(m.created_unix_ms), render: (m) => time(m.created_unix_ms) },
    { key: 'id', label: 'Id', value: (m) => m.material_id, render: (m) => h('span', { class: 'id' }, m.material_id) },
  ], { sort: { key: 'calcs', dir: 'desc' }, empty: 'No materials match.' });
  const apply = () => {
    const f = q.formula.toLowerCase();
    table.update(all.filter((m) => (!f || (m.reduced_formula || '').toLowerCase().includes(f)) && (!q.element || (m.elements || []).includes(q.element))));
  };
  const drawTools = () => {
    const els = [...new Set(all.flatMap((m) => m.elements || []))].sort();
    replace(tools, toolbar(
      input('Formula contains', q.formula, (v) => { q.formula = v; ctx.setQuery(q); apply(); }, { placeholder: 'e.g. NiSn' }),
      select('Element', [['', 'Any element'], ...els.map((e) => [e, e])], q.element, (v) => { q.element = v; ctx.setQuery(q); apply(); }),
      q.campaign_id ? h('span', { class: 'chip' }, 'campaign ', idLink(q.campaign_id), ' ',
        h('a', { href: buildHash('materials', '', { formula: q.formula, element: q.element }) }, 'clear')) : null));
  };
  let shown = false; let toolsDrawn = '';
  ctx.poll(async () => {
    try {
      const r = await api.get('/materials', { campaign_id: q.campaign_id, limit: 1000 });
      all = r.materials || [];
      const sig = [...new Set(all.flatMap((m) => m.elements || []))].sort().join();
      if (sig !== toolsDrawn) { drawTools(); toolsDrawn = sig; }
      if (!shown) { replace(body, table.el); shown = true; }
      apply();
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  }, { intervalMs: 15000 });
}

// ---- detail ------------------------------------------------------------------------

function structureSection(structures) {
  if (!structures.length) return empty('No structures recorded.');
  return h('div', { class: 'structures' }, structures.map((si, i) => {
    const st = si.structure || {};
    const lp = latticeParams(st.lattice);
    const sites = st.sites || [];
    return h('details', { class: 'structure', open: i === structures.length - 1 ? true : undefined },
      h('summary', null,
        h('span', { class: 'formula' }, st.formula || st.reduced_formula || '?'),
        h('span', { class: 'muted' }, ` ${st.spacegroup_symbol || '?'} (${st.spacegroup_number || '?'})`),
        st.prototype ? h('span', { class: 'chip' }, st.prototype) : null,
        h('span', { class: 'chip' }, st.source || 'unknown source'),
        lp ? h('span', { class: 'muted small' }, ` a = ${fixed(lp.a, 4)} Å, V = ${fixed(lp.volume, 2)} Å³`) : null,
        h('span', { class: 'id small' }, ` ${si.structure_id}`)),
      h('div', { class: 'structure-body' },
        kv([
          ['Lattice (cell vectors)', lp ? `a ${fixed(lp.a, 4)}, b ${fixed(lp.b, 4)}, c ${fixed(lp.c, 4)} Å; α ${fixed(lp.alpha, 2)}°, β ${fixed(lp.beta, 2)}°, γ ${fixed(lp.gamma, 2)}°` : null],
          ['Cell volume', lp ? `${fixed(lp.volume, 3)} Å³ (${fixed(lp.volume / Math.max(1, sites.length), 3)} Å³/atom)` : null],
          ['Fingerprint', st.fingerprint ? h('span', { class: 'id wrap' }, st.fingerprint) : null],
          ['Derived from', st.parent_structure_id ? h('span', { class: 'id' }, st.parent_structure_id) : undefined],
          ['Source job', si.source_job_id ? idLink(si.source_job_id) : undefined],
          ['Added', timestamp(si.created_unix_ms)],
        ]),
        h('div', { class: 'table-wrap' }, h('table', { class: 'data dense sites' },
          h('thead', null, h('tr', null, h('th', null, 'Site'), h('th', null, 'Species'), h('th', { class: 'num' }, 'x'), h('th', { class: 'num' }, 'y'), h('th', { class: 'num' }, 'z'),
            st.magmoms?.length ? h('th', { class: 'num' }, 'µB') : null)),
          h('tbody', null, sites.map((s, k) => h('tr', null, h('td', { class: 'num' }, String(k + 1)), h('td', null, s.species),
            (s.frac || []).map((x) => h('td', { class: 'num' }, fixed(cleanFrac(x), 4))),
            st.magmoms?.length ? h('td', { class: 'num' }, fixed(st.magmoms[k], 2)) : null)))))));
  }));
}

function devText(r) {
  if (r.deviation === null || r.deviation === undefined) return '—';
  if (r.mode === 'rel') return `${signed(r.deviation * 100, 2)}%`;
  if (r.mode === 'abs') return `${signed(r.deviation, 3)} ${r.unit}`;
  if (r.mode === 'ratio') return `×${num(r.deviation, 3)}`;
  return '—';
}
function tolText(r) {
  if (!r.tolerance) return '';
  if (r.mode === 'rel') return `±${num(r.tolerance * 100, 3)}%`;
  if (r.mode === 'abs') return `±${num(r.tolerance, 3)} ${r.unit}`;
  if (r.mode === 'ratio') return `within ×${num(r.tolerance, 3)}`;
  return '';
}

/** The check track: tolerance band in the middle half, a dot at the deviation. */
function checkTrack(r) {
  const p = trackPosition(r);
  if (!p) return h('span', { class: 'track track-none', 'aria-hidden': 'true' });
  const dot = h('span', { class: `track-dot ${r.status === 'pass' ? 'is-pass' : r.status === 'fail' ? 'is-fail' : ''} ${p.clamped ? 'clamped' : ''}` });
  dot.style.left = `${(p.t * 100).toFixed(2)}%`;
  const band = h('span', { class: 'track-band' });
  band.style.left = `${p.lo * 100}%`;
  band.style.width = `${(p.hi - p.lo) * 100}%`;
  return h('span', { class: 'track', role: 'img', 'aria-label': `deviation ${devText(r)}, tolerance ${tolText(r)}${p.clamped ? ', off the scale' : ''}` },
    band, h('span', { class: 'track-mid' }), dot);
}

function statusChip(r) {
  if (r.status === 'info') return h('span', { class: 'badge tone-idle' }, 'reference');
  const map = { pass: 'PASS', fail: 'FAIL', no_data: 'NO_DATA' };
  const label = { pass: 'pass', fail: 'fail', no_data: 'no data' }[r.status];
  return h('span', { class: 'status-cell' }, stateBadge(map[r.status], { label }),
    r.statusSource === 'computed' ? h('span', { class: 'muted small', title: 'No te.validation record for this source: checked here with the default tolerances.' }, ' computed') : null);
}

function litSource(r) {
  const c = r.literatureCalc;
  if (!c) return h('span', { class: 'muted' }, 'no literature value');
  const ci = citation(c);
  return h('span', { class: 'cite' },
    ci.doi ? h('a', { href: `https://doi.org/${ci.doi}`, target: '_blank', rel: 'noreferrer noopener', title: ci.rest || ci.doi }, ci.text || ci.doi) : (ci.text || '—'),
    r.literatureQuantity === 'total' ? h('span', { class: 'chip warn-chip', title: 'Literature reports total thermal conductivity, not the lattice part' }, 'total κ') : null);
}

function predictedCell(r) {
  if (r.predicted === null || r.predicted === undefined) return h('span', { class: 'muted' }, 'not yet');
  const v = typeof r.predicted === 'number' ? num(r.predicted, 4) : String(r.predicted);
  const extra = r.key === 'zt_peak' && r.predictedT ? ` at ${Math.round(r.predictedT)} K` : '';
  return h('span', null, r.predictedCalc ? h('a', { href: buildHash('calculations', r.predictedCalc.calc_id), title: `${r.predictedCalc.kind} ${r.predictedCalc.calc_id}` }, v) : v,
    extra ? h('span', { class: 'muted' }, extra) : null);
}
function literatureCell(r) {
  if (r.literature === null || r.literature === undefined) return h('span', { class: 'muted' }, '—');
  const v = typeof r.literature === 'number' ? num(r.literature, 4) : String(r.literature);
  const extra = r.key === 'zt_peak' ? `${r.literatureT ? ` at ${r.literatureT} K` : ''}${r.composition ? `, ${r.composition}` : ''}` : '';
  return h('span', null, r.literatureCalc ? h('a', { href: buildHash('calculations', r.literatureCalc.calc_id) }, v) : v, extra ? h('span', { class: 'muted' }, extra) : null);
}

function comparisonTable(cmp) {
  if (!cmp.rows.length) return empty('No literature values for this material.', 'Import reference values with tdrctl cp calc-import.');
  return h('div', { class: 'table-wrap' }, h('table', { class: 'data compare' },
    h('thead', null, h('tr', null, h('th', null, 'Property'), h('th', { class: 'num' }, 'Predicted'), h('th', { class: 'num' }, 'Literature'),
      h('th', { class: 'num' }, 'Deviation'), h('th', { class: 'track-col' }, 'Against tolerance'), h('th', null, 'Check'), h('th', null, 'Reference'))),
    h('tbody', null, cmp.rows.map((r) => h('tr', { class: `check-${r.status}` },
      h('th', { scope: 'row' }, r.label, r.unit ? h('span', { class: 'unit' }, ` ${r.unit}`) : null),
      h('td', { class: 'num' }, predictedCell(r)),
      h('td', { class: 'num' }, literatureCell(r)),
      h('td', { class: 'num' }, devText(r), r.tolerance ? h('div', { class: 'muted small' }, tolText(r)) : null),
      h('td', { class: 'track-col' }, checkTrack(r)),
      h('td', null, statusChip(r)),
      h('td', null, litSource(r)))))));
}

function chartsFor(calcs, cmp, ctx, chosenSource, onSource) {
  const wf = cmp.workflowId;
  const pred = calcs.filter((c) => c.evidence === 'PREDICTED' && (c.workflow_id || '') === wf);
  const evs = pred.filter((c) => c.kind === 'te.evaluate').sort((a, b) => b.created - a.created);
  const sources = [...new Set(evs.map((c) => c.conds.kappa_source || c.settings.kappa_source || ''))];
  const src = sources.includes(chosenSource) ? chosenSource : (sources.includes('bte') ? 'bte' : sources[0]);
  const ev = evs.find((c) => (c.conds.kappa_source || c.settings.kappa_source || '') === src) || evs[0];
  const litZt = cmp.rows.find((r) => r.key === 'zt_peak');
  const litK = calcs.find((c) => c.kind === 'literature.kappa_l');
  const blocks = [];
  const tools = sources.length > 1 ? toolbar(select('κ_l used for ZT', sources.map((s) => [s, s === 'bte' ? 'phono3py BTE' : s === 'slack' ? 'Slack model' : s]), src, onSource)) : null;
  const curves = ev ? curvesOf(ev) : null;
  if (ev && curves) {
    if (curves.zt_p || curves.zt_n) {
      const series = [];
      const xs = [];
      if (curves.zt_p) { series.push({ label: 'p-type', color: '--series-1' }); xs.push({ x: curves.t_k, y: curves.zt_p }); }
      if (curves.zt_n) { series.push({ label: 'n-type', color: '--series-2' }); xs.push({ x: curves.t_k, y: curves.zt_n }); }
      if (litZt?.literature !== null && litZt?.literature !== undefined && litZt.literatureT) {
        series.push({ label: `literature (${litZt.carrier})`, color: '--ink-1', points: 'only' });
        xs.push({ x: [Number(litZt.literatureT)], y: [litZt.literature] });
      }
      const al = alignSeries(xs);
      blocks.push(chartBlock('ZT(T), best doping', { x: al.x, xLabel: 'T (K)', yLabel: 'ZT', series: series.map((s, i) => ({ ...s, y: al.ys[i] })) },
        { ctx, note: `${ev.note || 'CRTA'}${src ? `; κ_l from ${src}` : ''}` }));
    }
    if (curves.pf_p || curves.pf_n) {
      const series = [];
      if (curves.pf_p) series.push({ label: 'p-type', color: '--series-1', y: curves.pf_p });
      if (curves.pf_n) series.push({ label: 'n-type', color: '--series-2', y: curves.pf_n });
      blocks.push(chartBlock('Power factor S²σ', { x: curves.t_k, xLabel: 'T (K)', yLabel: 'µW/cm·K²', series }, { ctx, note: 'Best doping per temperature, τ assumed' }));
    }
  }
  // κ_l(T): phonon records (points), the evaluation's curve, and the literature value at 300 K.
  const kSeries = [];
  const slack = pred.filter((c) => c.kind === 'phonon.slack').sort((a, b) => b.created - a.created)[0];
  const bte = pred.filter((c) => c.kind === 'phonon.bte').sort((a, b) => b.created - a.created)[0];
  if (slack) { const p = kappaPoints(slack); if (p.length) kSeries.push({ label: 'Slack model', color: '--series-3', points: 'always', x: p.map((q) => q[0]), y: p.map((q) => q[1]) }); }
  if (bte) { const p = kappaPoints(bte); if (p.length) kSeries.push({ label: 'phono3py BTE', color: '--series-7', points: 'always', x: p.map((q) => q[0]), y: p.map((q) => q[1]) }); }
  if (!kSeries.length && curves?.kappa_l) kSeries.push({ label: `κ_l (${src || 'evaluation'})`, color: '--series-3', x: curves.t_k, y: curves.kappa_l });
  const litKv = litK ? calcView(litK).props.kappa_l_w_mk_300k : null;
  if (kSeries.length && typeof litKv === 'number') kSeries.push({ label: calcView(litK).props.quantity === 'total' ? 'literature (total κ)' : 'literature', color: '--ink-1', points: 'only', x: [300], y: [litKv] });
  if (kSeries.length) {
    const al = alignSeries(kSeries);
    const positive = al.ys.every((ys) => ys.every((v) => v === null || v > 0));
    blocks.push(chartBlock('Lattice thermal conductivity κ_l(T)', { x: al.x, xLabel: 'T (K)', yLabel: 'W/m·K', yLog: positive,
      series: kSeries.map((s, i) => ({ label: s.label, color: s.color, points: s.points, y: al.ys[i] })) }, { ctx, note: positive ? 'log scale' : '' }));
  }
  const note = ev && !curves ? noticeBanner('This evaluation predates per-temperature curves: the charts show phonon records only; peak values are in the table above.') : null;
  if (!blocks.length) return h('div', null, tools, note, empty('No thermoelectric evaluation from this source yet.', 'Charts appear when te.evaluate has run for this material.'));
  return h('div', null, tools, note, h('div', { class: 'charts' }, blocks));
}

async function renderDetail(el, route, ctx) {
  const id = route.id;
  const head = h('div');
  const cmpTools = h('span', { class: 'panel-tools-inline' });
  const cmpBody = h('div', null, loading());
  const chartBody = h('div');
  const latestBody = h('div', null, loading());
  const structBody = h('div', null, loading());
  el.append(head,
    sectionWith('Predicted vs literature', cmpTools, cmpBody),
    section('Thermoelectric curves', chartBody),
    sectionWith('Latest calculation per kind', h('a', { href: buildHash('calculations', '', { material_id: id }) }, 'All calculations'), latestBody),
    section('Structures', structBody));

  let source = route.query.source;   // workflow id the predictions come from ('' = none)
  let kappaSource = route.query.kappa || '';
  let sig = '';
  let calcs = [];
  const wfNames = new Map();
  let chartScope = null;   // destroy charts on re-render
  const scope = () => {
    chartScope?.forEach((fn) => fn());
    const fns = [];
    chartScope = fns;
    return { onDestroy: (fn) => fns.push(fn) };
  };
  ctx.onDestroy(() => chartScope?.forEach((fn) => fn()));

  const latestTable = sortableTable([
    { key: 'kind', label: 'Kind', render: (c) => h('a', { href: buildHash('calculations', c.calc_id) }, c.kind) },
    { key: 'evidence', label: 'Evidence', render: (c) => h('span', { class: `evidence ev-${c.evidence.toLowerCase()}` }, c.evidence.toLowerCase()) },
    { key: 'props', label: 'Key properties', sortable: false, render: (c) => h('span', { class: 'props-inline' },
      keyProps(c, 4).map(([k, v]) => h('span', { class: 'prop' }, h('span', { class: 'prop-k' }, k), ' ', valueText(v), unitOf(k) ? h('span', { class: 'unit' }, ` ${unitOf(k)}`) : null))) },
    { key: 'engine', label: 'Engine', value: (c) => c.engine || c.method.solver || '' },
    { key: 'source', label: 'Source', value: (c) => c.workflow_id || '', render: (c) => (c.workflow_id ? h('a', { href: buildHash('workflows', c.workflow_id) }, wfNames.get(c.workflow_id) || c.workflow_id) : (c.note ? h('span', { class: 'muted clip-inline', title: c.note }, citation(c).text || 'import') : null)) },
    { key: 'created', label: 'Recorded', value: (c) => c.created, render: (c) => time(c.created_unix_ms) },
  ], { sort: { key: 'kind', dir: 'asc' }, empty: 'No calculations yet.', dense: true });

  const draw = () => {
    const cmp = compareWithLiterature(calcs, source !== undefined ? { workflowId: source } : {});
    const opts = cmp.sources.map((s) => [s.workflow_id, `${s.workflow_id ? (wfNames.get(s.workflow_id) || s.workflow_id) : 'no workflow'} (${s.count})`]);
    replace(cmpTools, opts.length > 1 ? select('Predictions from', opts, cmp.workflowId, (v) => { source = v; ctx.setQuery({ source: v, kappa: kappaSource }); draw(); }) : null);
    replace(cmpBody, comparisonTable(cmp),
      cmp.validation ? h('p', { class: 'muted small' }, 'Checks and tolerances from ', h('a', { href: buildHash('calculations', cmp.validation.calc_id) }, 'te.validation'), ` (${timestamp(cmp.validation.created_unix_ms)}). Peak ZT is listed for reference; te.validate doesn't check it per compound.`)
        : h('p', { class: 'muted small' }, 'No te.validation record for this source yet: checks use the default tolerances of te.validate (lattice ±2 %, gap ±0.2 eV, κ_l within ×3 Slack / ×1.5 BTE).'));
    replace(chartBody, chartsFor(calcs, cmp, scope(), kappaSource, (v) => { kappaSource = v; ctx.setQuery({ source: cmp.workflowId, kappa: v }); draw(); }));
  };

  ctx.poll(async () => {
    let m;
    try {
      [m, calcs] = await Promise.all([
        api.get(`/materials/${encodeURIComponent(id)}`),
        getAll('/calculations', { material_id: id, limit: 1000 }, 'calculations'),
      ]);
    } catch (e) {
      replace(head, pageHead(id, { crumbs: [[buildHash('materials'), 'Materials']] })); replace(cmpBody, errorBanner(e, ctx.role)); clear(latestBody); clear(structBody);
      return;
    }
    calcs = calcs.map(calcView);
    const newSig = `${calcs.length}:${calcs[0]?.calc_id || ''}:${(m.structures || []).length}`;
    if (newSig === sig) return;
    sig = newSig;
    const wfIds = [...new Set(calcs.map((c) => c.workflow_id).filter(Boolean))].filter((w) => !wfNames.has(w));
    if (wfIds.length) {
      try { const r = await api.get('/workflows', { limit: 1000 }); for (const w of r.workflows || []) wfNames.set(w.workflow_id, w.name); } catch { /* names optional */ }
    }
    replace(head, pageHead(h('span', { class: 'formula' }, m.reduced_formula), {
      crumbs: [[buildHash('materials'), 'Materials'], ['', m.reduced_formula]],
      badges: [elementChips(m.elements)],
      sub: h('span', null, h('span', { class: 'id' }, m.material_id), `, ${toNum(m.structure_count)} structures, ${toNum(m.calculation_count)} calculations, added ${timestamp(m.created_unix_ms)}`),
    }));
    draw();
    replace(latestBody, latestTable.el);
    const latest = (m.latest && m.latest.length) ? m.latest.map(calcView) : [...latestByKind(calcs).values()];
    latestTable.update(latest);
    replace(structBody, structureSection(m.structures || []));
  }, { intervalMs: 15000 });
}
