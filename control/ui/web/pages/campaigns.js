// #/campaigns and #/campaigns/ID.

import { h, replace, stateBadge, stateBar, time, kv, section, sectionWith, errorBanner, empty, loading, pageHead,
  actionButton, jsonBlock, link } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { timestamp } from '../lib/format.js';
import { toNum, parseJson, stepCounts, sum } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';

export async function render(el, route, ctx) {
  if (route.id) return renderDetail(el, route, ctx);
  const body = h('div', null, loading());
  el.append(pageHead('Campaigns', { sub: 'Research programmes that group jobs, workflows and results.' }), section(null, body));
  const table = sortableTable([
    { key: 'name', label: 'Name', render: (c) => h('a', { href: buildHash('campaigns', c.campaign_id) }, c.name || c.campaign_id) },
    { key: 'state', label: 'State', value: (c) => c.state || 'CAMPAIGN_STATE_UNSPECIFIED', render: (c) => stateBadge(c.state || 'DRAFT') },
    { key: 'domain', label: 'Domain' },
    { key: 'description', label: 'Description', cls: 'clip' },
    { key: 'created_by', label: 'Created by' },
    { key: 'created', label: 'Created', value: (c) => toNum(c.created_unix_ms), render: (c) => time(c.created_unix_ms) },
    { key: 'updated', label: 'Updated', value: (c) => toNum(c.updated_unix_ms), render: (c) => time(c.updated_unix_ms) },
  ], { sort: { key: 'created', dir: 'desc' }, empty: 'No campaigns yet. Create one with tdrctl cp campaign create.' });
  let shown = false;
  ctx.poll(async () => {
    try {
      const r = await api.get('/campaigns');
      if (!shown) { replace(body, table.el); shown = true; }
      table.update(r.campaigns || []);
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  });
}

// Allowed admin transitions shown as buttons (the server validates).
const TRANSITIONS = {
  DRAFT: [['ACTIVE', 'Activate']],
  ACTIVE: [['PAUSED', 'Pause'], ['COMPLETED', 'Complete']],
  PAUSED: [['ACTIVE', 'Resume'], ['COMPLETED', 'Complete']],
  COMPLETED: [['ARCHIVED', 'Archive']],
};
const CONFIRM = {
  PAUSED: 'Pause this campaign? Its queued jobs stay queued until you resume.',
  COMPLETED: 'Mark this campaign completed?',
  ARCHIVED: 'Archive this campaign?',
};

async function renderDetail(el, route, ctx) {
  const id = route.id;
  const head = h('div');
  const info = h('div', null, loading());
  const wfBody = h('div', null, loading());
  const jobsBody = h('div', null, loading());
  const matBody = h('div', null, loading());
  el.append(head, info,
    h('div', { class: 'grid-2' },
      sectionWith('Jobs', link(buildHash('jobs', '', { campaign: id }), 'Jobs in this campaign'), jobsBody),
      sectionWith('Materials', link(buildHash('materials', '', { campaign_id: id }), 'Materials list'), matBody)),
    sectionWith('Workflows', link(buildHash('workflows', '', { campaign: id }), 'Workflows list'), wfBody));

  const wfTable = sortableTable([
    { key: 'name', label: 'Name', render: (w) => h('a', { href: buildHash('workflows', w.workflow_id) }, w.name || w.workflow_id) },
    { key: 'state', label: 'State', render: (w) => stateBadge(w.state) },
    { key: 'progress', label: 'Steps', sortable: false, cls: 'wide', render: (w) => stateBar(stepCounts(w)) },
    { key: 'total', label: 'Total', num: true, value: (w) => sum(stepCounts(w)) },
    { key: 'created', label: 'Started', value: (w) => toNum(w.created_unix_ms), render: (w) => time(w.created_unix_ms) },
  ], { sort: { key: 'created', dir: 'desc' }, empty: 'No workflows in this campaign.', dense: true });
  let wfShown = false;

  const poller = ctx.poll(async () => {
    let c;
    try { c = await api.get(`/campaigns/${encodeURIComponent(id)}`); } catch (e) {
      replace(head, pageHead(id, { crumbs: [[buildHash('campaigns'), 'Campaigns']] })); replace(info, errorBanner(e, ctx.role)); return;
    }
    const st = c.state || 'DRAFT';
    const actions = ctx.isAdmin ? (TRANSITIONS[st] || []).map(([to, label]) => actionButton({
      label, danger: to !== 'ACTIVE', confirm: CONFIRM[to], confirmLabel: label, role: ctx.role,
      run: () => api.patch(`/campaigns/${encodeURIComponent(id)}`, { state: to }),
      onDone: () => poller.refresh(),
    })) : [];
    replace(head, pageHead(c.name || id, {
      crumbs: [[buildHash('campaigns'), 'Campaigns'], ['', c.name || id]],
      badges: [stateBadge(st)], actions,
      sub: c.description || null,
    }));
    const params = parseJson(c.parameters_json);
    replace(info, h('div', { class: 'grid-2' },
      section('Details', kv([
        ['Campaign id', h('span', { class: 'id' }, c.campaign_id)],
        ['Domain', c.domain || null],
        ['Created', h('span', null, timestamp(c.created_unix_ms), c.created_by ? ` by ${c.created_by}` : '')],
        ['Updated', timestamp(c.updated_unix_ms)],
      ])),
      section('Parameters', Object.keys(params).length ? jsonBlock(params, { collapsedOver: 40 }) : empty('No parameters set.'))));

    const [wfs, jobs, mats] = await Promise.allSettled([
      api.get('/workflows', { campaign: id, limit: 1000 }),
      api.get('/jobs', { campaign: id, limit: 1000 }),
      api.get('/materials', { campaign_id: id, limit: 1000 }),
    ]);
    if (wfs.status === 'fulfilled') {
      if (!wfShown) { replace(wfBody, wfTable.el); wfShown = true; }
      wfTable.update(wfs.value.workflows || []);
    } else { wfShown = false; replace(wfBody, errorBanner(wfs.reason, ctx.role)); }
    if (jobs.status === 'fulfilled') {
      const counts = {};
      for (const j of jobs.value.jobs || []) counts[j.state] = (counts[j.state] || 0) + 1;
      const n = (jobs.value.jobs || []).length;
      replace(jobsBody, n ? h('div', null, stateBar(counts), n >= 1000 ? h('p', { class: 'muted small' }, 'Counts cover the newest 1000 jobs.') : null)
        : empty('No jobs in this campaign yet.'));
    } else replace(jobsBody, errorBanner(jobs.reason, ctx.role));
    if (mats.status === 'fulfilled') {
      const ms = mats.value.materials || [];
      replace(matBody, ms.length ? h('ul', { class: 'formula-list' }, ms.map((m) => h('li', null,
        h('a', { href: buildHash('materials', m.material_id), class: 'formula' }, m.reduced_formula),
        h('span', { class: 'muted small' }, ` ${toNum(m.calculation_count)} calcs`))))
        : empty('No materials recorded for this campaign yet.'));
    } else replace(matBody, errorBanner(mats.reason, ctx.role));
  });
}
