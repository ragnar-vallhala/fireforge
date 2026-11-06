// #/workflows and #/workflows/ID (DAG + step table).

import { h, replace, stateBadge, stateBar, idLink, time, kv, section, sectionWith, errorBanner, loading, pageHead,
  actionButton, toolbar, select, input } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { timestamp, duration, stateLabel } from '../lib/format.js';
import { toNum, stepCounts, sum } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';
import { dagView } from '../lib/dagview.js';
import { countStates } from '../lib/dag.js';

const WF_STATES = ['RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'];

export async function render(el, route, ctx) {
  if (route.id) return renderDetail(el, route, ctx);
  const q = { state: route.query.state || '', campaign: route.query.campaign || '' };
  const body = h('div', null, loading());
  const campaigns = new Map();
  const tools = h('div');
  el.append(pageHead('Workflows', { sub: 'Step DAGs run by the control plane, newest first.' }), section(null, tools, body));
  let poller = null;
  const drawTools = () => replace(tools, toolbar(
    select('State', [['', 'Any state'], ...WF_STATES.map((s) => [s, stateLabel(s)])], q.state, (v) => { q.state = v; ctx.setQuery(q); poller.refresh(); }),
    select('Campaign', [['', 'All campaigns'], ...[...campaigns].map(([id, n]) => [id, n])], q.campaign, (v) => { q.campaign = v; ctx.setQuery(q); poller.refresh(); })));
  drawTools();
  api.get('/campaigns').then((r) => { for (const c of r.campaigns || []) campaigns.set(c.campaign_id, c.name || c.campaign_id); drawTools(); table.update(last); }).catch(() => {});

  let last = [];
  const table = sortableTable([
    { key: 'name', label: 'Name', render: (w) => h('a', { href: buildHash('workflows', w.workflow_id) }, w.name || w.workflow_id) },
    { key: 'state', label: 'State', render: (w) => stateBadge(w.state) },
    { key: 'progress', label: 'Steps', sortable: false, cls: 'wide', render: (w) => stateBar(stepCounts(w)) },
    { key: 'total', label: 'Total', num: true, value: (w) => sum(stepCounts(w)) },
    { key: 'campaign', label: 'Campaign', value: (w) => campaigns.get(w.campaign_id) || w.campaign_id || '',
      render: (w) => (w.campaign_id ? h('a', { href: buildHash('campaigns', w.campaign_id) }, campaigns.get(w.campaign_id) || w.campaign_id) : null) },
    { key: 'created', label: 'Started', value: (w) => toNum(w.created_unix_ms), render: (w) => time(w.created_unix_ms) },
    { key: 'duration', label: 'Duration', num: true, value: (w) => (toNum(w.finished_unix_ms) || Date.now()) - toNum(w.created_unix_ms),
      render: (w) => duration((toNum(w.finished_unix_ms) || Date.now()) - toNum(w.created_unix_ms)) },
  ], { sort: { key: 'created', dir: 'desc' }, empty: 'No workflows match these filters.' });
  let shown = false;
  poller = ctx.poll(async () => {
    try {
      const r = await api.get('/workflows', { state: q.state, campaign: q.campaign, limit: 1000 });
      last = r.workflows || [];
      if (!shown) { replace(body, table.el); shown = true; }
      table.update(last);
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  });
}

async function renderDetail(el, route, ctx) {
  const id = route.id;
  const head = h('div');
  const summary = h('div');
  const dagBody = h('div', null, loading());
  const tableBody = h('div');
  const tools = h('div');
  const expanded = new Set((route.query.expand || '').split(',').filter(Boolean));
  const filt = { state: route.query.step_state || '', text: route.query.q || '' };
  let selected = '';
  let steps = [];
  const names = await ctx.nodeNames();

  const dagTools = h('span', { class: 'panel-tools-inline' });
  el.append(head, summary, sectionWith('Step graph', dagTools, dagBody), section('Steps', tools, tableBody));

  const syncQuery = () => ctx.setQuery({ expand: [...expanded].join(','), step_state: filt.state, q: filt.text });
  const drawDag = () => {
    const parents = new Set(steps.map((s) => s.parent_step).filter(Boolean));
    replace(dagTools,
      h('button', { class: 'btn btn-quiet', type: 'button', disabled: expanded.size >= parents.size || !parents.size,
        onclick: () => { for (const p of parents) expanded.add(p); syncQuery(); drawDag(); } }, 'Expand all'),
      h('button', { class: 'btn btn-quiet', type: 'button', disabled: !expanded.size,
        onclick: () => { expanded.clear(); syncQuery(); drawDag(); } }, 'Collapse all'));
    const scrollEl = dagBody.querySelector('.dag-scroll');
    const keep = scrollEl ? [scrollEl.scrollLeft, scrollEl.scrollTop] : null;
    replace(dagBody, dagView({
      steps, expanded, selected,
      onToggle: (name) => { if (expanded.has(name)) expanded.delete(name); else expanded.add(name); syncQuery(); drawDag(); },
      onSelect: (step) => { selected = step.name; filt.text = step.name; syncQuery(); drawDag(); drawTools(); drawTable(); },
    }));
    if (keep) { const sc = dagBody.querySelector('.dag-scroll'); sc.scrollLeft = keep[0]; sc.scrollTop = keep[1]; }
  };

  const table = sortableTable([
    { key: 'name', label: 'Step', render: (s) => h('span', { class: s.parent_step ? 'step-name spawned' : 'step-name' }, s.name) },
    { key: 'state', label: 'State', render: (s) => stateBadge(s.state) },
    { key: 'op', label: 'Operation', value: (s) => s.spec?.job?.operation || '' },
    { key: 'job', label: 'Job', value: (s) => s.job_id || '', render: (s) => (s.job_id ? idLink(s.job_id) : null) },
    { key: 'node', label: 'Node', value: (s) => names.get(s.node_id) || s.node_id || '', render: (s) => (s.node_id ? h('a', { href: buildHash('nodes', s.node_id) }, names.get(s.node_id) || s.node_id) : null) },
    { key: 'updated', label: 'Updated', value: (s) => toNum(s.updated_unix_ms), render: (s) => (s.updated_unix_ms ? time(s.updated_unix_ms) : null) },
    { key: 'error', label: 'Error', cls: 'clip', render: (s) => s.error || '' },
  ], { empty: 'No steps match.', dense: true });

  const drawTools = () => {
    const counts = countStates(steps);
    replace(tools, toolbar(
      select('State', [['', `Any state (${steps.length})`], ...Object.entries(counts).map(([s, n]) => [s, `${stateLabel(s)} (${n})`])], filt.state,
        (v) => { filt.state = v; syncQuery(); drawTable(); }),
      input('Name contains', filt.text, (v) => { filt.text = v; selected = ''; syncQuery(); drawTable(); }, { placeholder: 'e.g. ZrNiSn.phonon' })));
  };
  const drawTable = () => {
    const t = filt.text.toLowerCase();
    table.update(steps.filter((s) => (!filt.state || s.state === filt.state) && (!t || s.name.toLowerCase().includes(t))));
  };
  replace(tableBody, table.el);

  let toolsDrawn = false;
  let lastSig = '';
  const poller = ctx.poll(async () => {
    let w;
    try { w = await api.get(`/workflows/${encodeURIComponent(id)}`); } catch (e) {
      replace(head, pageHead(id, { crumbs: [[buildHash('workflows'), 'Workflows']] })); replace(dagBody, errorBanner(e, ctx.role)); return;
    }
    steps = w.steps || [];
    const running = w.state === 'RUNNING';
    const actions = [];
    if (ctx.isAdmin && running) {
      actions.push(actionButton({ label: 'Cancel workflow', danger: true, confirm: 'Cancel this workflow? Its queued and running steps are cancelled.',
        confirmLabel: 'Cancel workflow', role: ctx.role,
        run: () => api.post(`/workflows/${encodeURIComponent(id)}/cancel`, { reason: `cancelled from the dashboard by ${ctx.whoami?.name || 'admin'}` }),
        onDone: () => poller.refresh() }));
    }
    replace(head, pageHead(w.name || id, {
      crumbs: [[buildHash('workflows'), 'Workflows'], ['', w.name || id]],
      badges: [stateBadge(w.state)], actions,
      sub: h('span', null, h('span', { class: 'id' }, w.workflow_id), `, submitted by ${w.submitted_by || '?'} `, time(w.created_unix_ms)),
    }));
    const counts = stepCounts(w);
    const end = toNum(w.finished_unix_ms) || Date.now();
    replace(summary, h('div', { class: 'panel summary-strip' },
      stateBar(counts),
      kv([
        ['Campaign', w.campaign_id ? idLink(w.campaign_id) : null],
        ['Steps', `${sum(counts)} (${steps.filter((s) => s.parent_step).length} spawned)`],
        [w.finished_unix_ms ? 'Took' : 'Running for', duration(end - toNum(w.created_unix_ms))],
        ['Finished', w.finished_unix_ms ? timestamp(w.finished_unix_ms) : undefined],
        ['Error', w.error ? h('span', { class: 'pre' }, w.error) : undefined],
      ], 'kv-inline')));
    const sig = steps.map((s) => `${s.name}:${s.state}`).join('|');
    if (sig !== lastSig) { lastSig = sig; drawDag(); drawTable(); }
    if (!toolsDrawn) { drawTools(); toolsDrawn = true; }
    return running;   // false stops polling once finished
  });
}
