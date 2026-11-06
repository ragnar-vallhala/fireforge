// #/audit: the control plane's audit log (admin actions).

import { h, replace, idLink, section, errorBanner, loading, pageHead, toolbar, select, input } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { timestamp, actor } from '../lib/format.js';
import { toNum, parseJson } from '../lib/proto.js';
import { sortableTable } from '../lib/table.js';

function details(json) {
  const d = parseJson(json, null);
  if (!d || typeof d !== 'object' || !Object.keys(d).length) return h('span', { class: 'muted' }, '—');
  return h('span', { class: 'props-inline' }, Object.entries(d).map(([k, v]) =>
    h('span', { class: 'prop' }, h('span', { class: 'prop-k' }, k), ' ', typeof v === 'object' ? JSON.stringify(v) : String(v))));
}

export async function render(el, route, ctx) {
  const q = { action: route.query.action || '', text: route.query.q || '', limit: route.query.limit || '200' };
  const tools = h('div');
  const body = h('div', null, loading());
  el.append(pageHead('Audit log', { sub: 'Every admin action recorded by the control plane, newest first.' }), section(null, tools, body));
  let all = [];
  const table = sortableTable([
    { key: 'at', label: 'When', value: (a) => toNum(a.at_unix_ms), render: (a) => h('span', { class: 'mono small' }, timestamp(a.at_unix_ms)) },
    { key: 'actor', label: 'Actor', render: (a) => { const x = actor(a.actor); return h('span', null, x.name, x.role ? h('span', { class: 'muted small' }, ` ${x.role}`) : null); } },
    { key: 'action', label: 'Action', render: (a) => h('code', null, a.action) },
    { key: 'target', label: 'Target', render: (a) => idLink(a.target, { full: true }) },
    { key: 'details', label: 'Details', sortable: false, render: (a) => details(a.details_json) },
  ], { sort: { key: 'at', dir: 'desc' }, empty: 'No audit entries match.', dense: true });
  const apply = () => {
    const t = q.text.toLowerCase();
    table.update(all.filter((a) => (!q.action || a.action === q.action)
      && (!t || [a.actor, a.action, a.target, a.details_json].some((x) => (x || '').toLowerCase().includes(t)))));
  };
  let poller = null;
  let actionsSig = '';
  const drawTools = () => {
    const actions = [...new Set(all.map((a) => a.action))].sort();
    replace(tools, toolbar(
      select('Action', [['', 'Any action'], ...actions.map((a) => [a, a])], q.action, (v) => { q.action = v; ctx.setQuery(q); apply(); }),
      input('Contains', q.text, (v) => { q.text = v; ctx.setQuery(q); apply(); }, { placeholder: 'actor, target, details' }),
      select('Show', [['200', 'Newest 200'], ['1000', 'Newest 1000']], q.limit, (v) => { q.limit = v; ctx.setQuery(q); poller.refresh(); })));
  };
  let shown = false;
  poller = ctx.poll(async () => {
    try {
      const r = await api.get('/audit', { limit: q.limit });
      all = r.entries || [];
      const sig = [...new Set(all.map((a) => a.action))].sort().join();
      if (sig !== actionsSig || !shown) { actionsSig = sig; drawTools(); }
      if (!shown) { replace(body, table.el); shown = true; }
      apply();
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  }, { intervalMs: 10000 });
}
