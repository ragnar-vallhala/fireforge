// #/calculations and #/calculations/ID.

import { h, replace, idLink, time, kv, section, errorBanner, loading, pageHead, toolbar, select, valueText,
  jsonBlock, empty, input } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { timestamp, shortArtifact } from '../lib/format.js';
import { calcView } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';
import { keyProps, splitProps, unitOf, validationChecks } from '../lib/science.js';
import { citation } from '../lib/literature.js';

export const KINDS = ['qe.relax', 'qe.scf', 'qe.bands', 'stability.elastic', 'stability.formation', 'phonon.harmonic',
  'phonon.slack', 'phonon.bte', 'transport.crta', 'te.evaluate', 'te.validation',
  'literature.lattice', 'literature.band_gap', 'literature.kappa_l', 'literature.zt_peak'];

export function evidenceTag(ev) { return h('span', { class: `evidence ev-${(ev || 'PREDICTED').toLowerCase()}` }, (ev || 'PREDICTED').toLowerCase()); }

export function propsInline(c, n = 3) {
  return h('span', { class: 'props-inline' }, keyProps(c, n).map(([k, v]) =>
    h('span', { class: 'prop' }, h('span', { class: 'prop-k' }, k), ' ', valueText(v), unitOf(k) ? h('span', { class: 'unit' }, ` ${unitOf(k)}`) : null)));
}

export async function render(el, route, ctx) {
  if (route.id) return renderDetail(el, route, ctx);
  const q = {
    kind: route.query.kind || '', evidence: route.query.evidence || '', material_id: route.query.material_id || '',
    campaign_id: route.query.campaign_id || '', workflow_id: route.query.workflow_id || '',
  };
  const tools = h('div');
  const body = h('div', null, loading());
  const more = h('div', { class: 'more' });
  el.append(pageHead('Calculations', { sub: 'Every record in the materials database, newest first.' }), section(null, tools, body, more));

  const opts = { materials: [], campaigns: [], workflows: [] };
  let rows = []; let next = ''; let pages = 1;
  let poller = null;
  const change = (k) => (v) => { q[k] = v; ctx.setQuery(q); pages = 1; poller.refresh(); };
  const drawTools = () => replace(tools, toolbar(
    select('Kind', [['', 'Any kind'], ...KINDS.map((k) => [k, k]), ...(q.kind && !KINDS.includes(q.kind) ? [[q.kind, q.kind]] : [])], q.kind, change('kind')),
    select('Evidence', [['', 'Any'], ['PREDICTED', 'predicted'], ['LITERATURE', 'literature'], ['MEASURED', 'measured']], q.evidence, change('evidence')),
    select('Material', [['', 'Any material'], ...opts.materials], q.material_id, change('material_id')),
    select('Campaign', [['', 'Any campaign'], ...opts.campaigns], q.campaign_id, change('campaign_id')),
    select('Workflow', [['', 'Any workflow'], ...opts.workflows], q.workflow_id, change('workflow_id'))));
  drawTools();
  Promise.allSettled([api.get('/materials', { limit: 1000 }), api.get('/campaigns'), api.get('/workflows', { limit: 1000 })]).then(([m, c, w]) => {
    if (m.status === 'fulfilled') opts.materials = (m.value.materials || []).map((x) => [x.material_id, x.reduced_formula]).sort((a, b) => a[1].localeCompare(b[1]));
    if (c.status === 'fulfilled') opts.campaigns = (c.value.campaigns || []).map((x) => [x.campaign_id, x.name || x.campaign_id]);
    if (w.status === 'fulfilled') opts.workflows = (w.value.workflows || []).map((x) => [x.workflow_id, x.name || x.workflow_id]);
    drawTools();
  });

  const table = sortableTable([
    { key: 'calc_id', label: 'Calculation', render: (c) => idLink(c.calc_id) },
    { key: 'kind', label: 'Kind', render: (c) => h('a', { href: buildHash('calculations', c.calc_id) }, c.kind) },
    { key: 'formula', label: 'Material', value: (c) => c.reduced_formula || '', render: (c) => (c.material_id ? h('a', { href: buildHash('materials', c.material_id), class: 'formula' }, c.reduced_formula || c.material_id) : null) },
    { key: 'evidence', label: 'Evidence', render: (c) => evidenceTag(c.evidence) },
    { key: 'props', label: 'Key properties', sortable: false, render: (c) => propsInline(c) },
    { key: 'engine', label: 'Engine', value: (c) => c.engine },
    { key: 'step', label: 'Step', value: (c) => c.step || '', cls: 'clip' },
    { key: 'created', label: 'Recorded', value: (c) => c.created, render: (c) => time(c.created_unix_ms) },
  ], { empty: 'No calculations match these filters.', dense: true });

  const load = async (after) => {
    const r = await api.get('/calculations', { ...q, limit: 200, after });
    return [(r.calculations || []).map(calcView), r.next || ''];
  };
  const drawMore = () => replace(more, next ? h('button', { class: 'btn', type: 'button', onclick: async (e) => {
    e.target.disabled = true;
    try { const [r, n] = await load(next); rows = rows.concat(r); next = n; pages++; table.update(rows); drawMore(); } catch (err) { replace(more, errorBanner(err, ctx.role)); }
  } }, 'Load more') : h('span', { class: 'muted small' }, `${rows.length} calculations`));

  let shown = false;
  poller = ctx.poll(async () => {
    if (pages > 1) return;   // keep what the user paged through
    try {
      [rows, next] = await load('');
      if (!shown) { replace(body, table.el); shown = true; }
      table.update(rows);
      drawMore();
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  }, { intervalMs: 15000 });
}

function propsTable(props) {
  const { scalars, nested } = splitProps(props);
  if (!scalars.length && !nested.length) return empty('No properties.');
  return h('div', null,
    scalars.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'data dense props' },
      h('tbody', null, scalars.map(([k, v]) => h('tr', null, h('th', { scope: 'row' }, h('code', null, k)),
        h('td', { class: 'num' }, valueText(v)), h('td', { class: 'unit' }, unitOf(k))))))) : null,
    nested.map(([k, v]) => h('details', { class: 'nested-prop' }, h('summary', null, h('code', null, k),
      h('span', { class: 'muted small' }, Array.isArray(v) ? ` list of ${v.length}` : ` ${Object.keys(v).length} fields`)), jsonBlock(v))));
}

async function renderDetail(el, route, ctx) {
  const id = route.id;
  const body = h('div', null, loading());
  el.append(body);
  let raw;
  try { raw = await api.get(`/calculations/${encodeURIComponent(id)}`); } catch (e) {
    replace(body, pageHead(id, { crumbs: [[buildHash('calculations'), 'Calculations']] }), errorBanner(e, ctx.role)); return;
  }
  const c = calcView(raw);
  const names = await ctx.nodeNames();
  const m = c.method;
  const ci = c.evidence !== 'PREDICTED' ? citation(c) : null;
  replace(body,
    pageHead(c.kind, {
      crumbs: [[buildHash('calculations'), 'Calculations'], c.material_id ? [buildHash('materials', c.material_id), c.reduced_formula || c.material_id] : null, ['', c.calc_id]].filter(Boolean),
      badges: [evidenceTag(c.evidence), c.engine ? h('span', { class: 'chip' }, `engine ${c.engine}`) : null].filter(Boolean),
      sub: h('span', null, h('span', { class: 'id' }, c.calc_id), `, recorded ${timestamp(c.created_unix_ms)}`),
    }),
    c.note ? h('div', { class: 'banner banner-info' }, ci?.doi ? h('span', null, ci.text, ' ', h('a', { href: `https://doi.org/${ci.doi}`, target: '_blank', rel: 'noreferrer noopener' }, `doi:${ci.doi}`), ci.rest ? ` ${ci.rest}` : '') : c.note) : null,
    h('div', { class: 'grid-2' },
      section('Properties', c.kind === 'te.validation' ? validationList(c) : null, propsTable(c.props)),
      h('div', { class: 'stack' },
        section('Provenance', kv([
          ['Material', c.material_id ? h('a', { href: buildHash('materials', c.material_id), class: 'formula' }, c.reduced_formula || c.material_id) : null],
          ['Structure', c.structure_id ? h('span', { class: 'id' }, c.structure_id) : null],
          ['Job', c.job_id ? idLink(c.job_id, { full: true }) : undefined],
          ['Attempt', c.attempt_id ? h('span', { class: 'id' }, c.attempt_id) : undefined],
          ['Workflow', c.workflow_id ? h('span', null, idLink(c.workflow_id, { full: true }), c.step ? h('span', { class: 'muted' }, ` step ${c.step}`) : null) : undefined],
          ['Campaign', c.campaign_id ? idLink(c.campaign_id, { full: true }) : undefined],
          ['Node', c.node_id ? h('a', { href: buildHash('nodes', c.node_id) }, names.get(c.node_id) || c.node_id) : undefined],
        ])),
        section('Method', kv([
          ['Solver', m.solver ? `${m.solver}${m.solver_version ? ` ${m.solver_version}` : ''}` : null],
          ['Engine', m.engine || null],
          ['Functional', m.functional || undefined],
          ['Preset', m.preset || undefined],
          ['Pseudopotentials', m.pseudopotentials?.length ? h('ul', { class: 'plain-list' }, m.pseudopotentials.map((p) => h('li', { class: 'mono small' }, p))) : undefined],
        ]), Object.keys(c.settings).length ? h('div', { class: 'sub-block' }, h('h3', null, 'Settings'), jsonBlock(c.settings, { collapsedOver: 30 })) : null))),
    h('div', { class: 'grid-3' },
      section('Conditions', Object.keys(c.conds).length ? jsonBlock(c.conds) : empty('None recorded.')),
      section('Convergence', Object.keys(c.conv).length ? jsonBlock(c.conv) : empty('None recorded.')),
      section('Uncertainty', Object.keys(c.unc).length ? jsonBlock(c.unc) : empty('None recorded.'))),
    section('Artifacts', c.outputs.length || c.input_artifacts.length ? h('ul', { class: 'artifact-list' },
      c.outputs.map((o) => h('li', null, h('span', { class: 'artifact-name' }, o.name), h('span', { class: 'id', title: o.artifact_id }, shortArtifact(o.artifact_id)), h('span', { class: 'muted small' }, 'output'))),
      c.input_artifacts.map((a) => h('li', null, h('span', { class: 'artifact-name muted' }, 'input'), h('span', { class: 'id', title: a }, shortArtifact(a)))))
      : empty('No artifacts linked.')));
}

function validationList(c) {
  const checks = validationChecks(c);
  return h('ul', { class: 'check-list' }, checks.map((ch) => h('li', { class: `check-${ch.status}` },
    h('span', { class: `badge tone-${ch.status === 'pass' ? 'good' : ch.status === 'fail' ? 'bad' : 'off'}` },
      h('span', { class: 'glyph', 'aria-hidden': 'true' }, ch.status === 'pass' ? '✓' : ch.status === 'fail' ? '✕' : '–'), ch.status === 'no_data' ? 'no data' : ch.status),
    h('span', null, ch.label))));
}

export { input };
