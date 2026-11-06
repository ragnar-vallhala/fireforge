// #/nodes and #/nodes/ID.

import { h, replace, stateBadge, idLink, time, kv, section, sectionWith, errorBanner, empty, loading, pageHead,
  actionButton, noticeBanner } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { bytes, percent, timestamp, num } from '../lib/format.js';
import { toNum, resources } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';
import { nodeLoad, nodeMeters } from './overview.js';

export async function render(el, route, ctx) {
  if (route.id) return renderDetail(el, route, ctx);
  const body = h('div', null, loading());
  el.append(pageHead('Nodes', { sub: 'Machines running tdrd that registered with this control plane.' }), section(null, body));
  const table = sortableTable([
    { key: 'name', label: 'Name', render: (n) => h('a', { href: buildHash('nodes', n.node_id) }, n.name || n.node_id) },
    { key: 'health', label: 'Health', render: (n) => stateBadge(n.health || 'UNKNOWN') },
    { key: 'cpu', label: 'CPU booked', num: true, value: (n) => nodeLoad(n).alloc.cpu_cores,
      render: (n) => { const l = nodeLoad(n); return `${l.alloc.cpu_cores} / ${l.cap.cpu_cores}`; } },
    { key: 'util', label: 'CPU busy', num: true, value: (n) => nodeLoad(n).util, render: (n) => percent(nodeLoad(n).util) },
    { key: 'mem', label: 'Memory booked', num: true, value: (n) => nodeLoad(n).alloc.memory_bytes,
      render: (n) => { const l = nodeLoad(n); return `${bytes(l.alloc.memory_bytes)} / ${bytes(l.cap.memory_bytes)}`; } },
    { key: 'running', label: 'Running', num: true, value: (n) => nodeLoad(n).running },
    { key: 'gpus', label: 'GPUs', num: true, value: (n) => resources(n.physical).gpu_count },
    { key: 'caps', label: 'Capabilities', sortable: false, render: (n) => h('span', { class: 'chips' }, (n.capabilities || []).map((c) => h('span', { class: 'chip' }, c))) },
    { key: 'seen', label: 'Last seen', value: (n) => toNum(n.last_seen_unix_ms), render: (n) => time(n.last_seen_unix_ms) },
    { key: 'version', label: 'tdrd', value: (n) => n.daemon_version },
  ], { sort: { key: 'name', dir: 'asc' }, empty: 'No nodes have registered yet.' });
  let shown = false;
  ctx.poll(async () => {
    try {
      const r = await api.get('/nodes');
      if (!shown) { replace(body, table.el); shown = true; }
      table.update(r.nodes || []);
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  });
}

function resourcesTable(n) {
  const hb = n.last_heartbeat || {};
  const rows = [['Physical', resources(n.physical)], ['Configured (tdr.conf)', resources(n.configured)],
    ['Effective capacity', resources(hb.capacity)], ['Booked by jobs', resources(hb.allocated)]];
  return h('div', { class: 'table-wrap' }, h('table', { class: 'data dense' },
    h('thead', null, h('tr', null, h('th', null, ''), ['CPU cores', 'Memory', 'Disk', 'GPUs'].map((t) => h('th', { class: 'num' }, t)))),
    h('tbody', null, rows.map(([label, r]) => h('tr', null, h('th', { scope: 'row' }, label),
      h('td', { class: 'num' }, String(r.cpu_cores || '—')), h('td', { class: 'num' }, r.memory_bytes ? bytes(r.memory_bytes) : '—'),
      h('td', { class: 'num' }, r.disk_bytes ? bytes(r.disk_bytes) : '—'), h('td', { class: 'num' }, String(r.gpu_count || '—')))))));
}

function policyKv(p) {
  p = p || {};
  const cap = (v, fmt = String) => (toNum(v) ? fmt(v) : 'no cap');
  return kv([
    ['Version', String(toNum(p.version) || '—')],
    ['Profile', p.profile || 'none'],
    ['Max CPU cores', cap(p.max_cpu_cores)],
    ['Max memory', cap(p.max_memory_bytes, bytes)],
    ['Max jobs', cap(p.max_jobs)],
    ['Max disk', cap(p.max_disk_bytes, bytes)],
    ['Max GPUs', cap(p.max_gpus)],
    ['Updated', p.updated_unix_ms ? h('span', null, timestamp(p.updated_unix_ms), p.updated_by ? ` by ${p.updated_by}` : '') : null],
  ]);
}

const GIB = 1024 ** 3;

function limitsForm(n, ctx, onSaved) {
  const p = n.policy || {};
  const out = h('div', { class: 'form-result' });
  const field = (label, name, value, props = {}) => h('label', { class: 'field' }, h('span', null, label),
    h('input', { name, value: value ?? '', inputmode: 'decimal', autocomplete: 'off', ...props }));
  const profile = h('select', { name: 'profile' }, ['', 'none', 'small', 'medium', 'compute', 'gpu'].map((v) => {
    const o = h('option', { value: v }, v === '' ? '(unchanged)' : v);
    return o;
  }));
  const gib = (v) => (toNum(v) ? String(Math.round((toNum(v) / GIB) * 100) / 100) : '');
  const form = h('form', { class: 'limits-form', novalidate: true },
    h('label', { class: 'field' }, h('span', null, 'Profile'), profile),
    field('Max CPU cores', 'max_cpu_cores', toNum(p.max_cpu_cores) || '', { placeholder: 'no cap' }),
    field('Max memory (GiB)', 'max_memory_gib', gib(p.max_memory_bytes), { placeholder: 'no cap' }),
    field('Max jobs', 'max_jobs', toNum(p.max_jobs) || '', { placeholder: 'no cap' }),
    field('Max disk (GiB)', 'max_disk_gib', gib(p.max_disk_bytes), { placeholder: 'no cap' }),
    field('Max GPUs', 'max_gpus', toNum(p.max_gpus) || '', { placeholder: 'no cap' }),
    h('div', { class: 'form-actions' }, h('button', { class: 'btn btn-primary', type: 'submit' }, 'Save limits'),
      h('span', { class: 'muted small' }, 'Empty means no cap. A profile sets its preset limits first.')),
    out);
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(form);
    const body = {};
    const intField = (name, key) => {
      const v = String(fd.get(name) || '').trim();
      if (v === '') { if (toNum(p[key])) body[key] = 0; return true; }
      if (!/^\d+$/.test(v)) return false;
      if (Number(v) !== toNum(p[key])) body[key] = Number(v);
      return true;
    };
    const gibField = (name, key) => {
      const v = String(fd.get(name) || '').trim();
      if (v === '') { if (toNum(p[key])) body[key] = '0'; return true; }
      const g = Number(v);
      if (!Number.isFinite(g) || g < 0) return false;
      const b = Math.round(g * GIB);
      if (b !== toNum(p[key])) body[key] = String(b);
      return true;
    };
    const ok = intField('max_cpu_cores', 'max_cpu_cores') && gibField('max_memory_gib', 'max_memory_bytes')
      && intField('max_jobs', 'max_jobs') && gibField('max_disk_gib', 'max_disk_bytes') && intField('max_gpus', 'max_gpus');
    if (!ok) { replace(out, noticeBanner('Limits must be whole numbers (GiB may have decimals).', 'error')); return; }
    const prof = String(fd.get('profile') || '');
    if (prof) body.profile = prof === 'none' ? '' : prof;
    if (!Object.keys(body).length) { replace(out, noticeBanner('Nothing changed.')); return; }
    replace(out, noticeBanner('Saving…'));
    try {
      const pol = await api.patch(`/nodes/${encodeURIComponent(n.node_id)}/limits`, body);
      replace(out, noticeBanner(`Saved: policy version ${toNum(pol.version)}. The node applies it on its next heartbeat.`, 'ok'));
      onSaved();
    } catch (err) { replace(out, errorBanner(err, ctx.role)); }
  });
  return form;
}

async function renderDetail(el, route, ctx) {
  const id = route.id;
  const head = h('div');
  const load = h('div', null, loading());
  const hw = h('div');
  const pol = h('div');
  const jobsBody = h('div', null, loading());
  const eventsBody = h('div', null, loading());
  const peersBody = h('div');
  const adminBody = h('div');
  el.append(head,
    h('div', { class: 'grid-2' }, section('Load', load), section('Policy', pol)),
    section('Resources', hw),
    sectionWith('Running jobs', h('a', { href: buildHash('jobs', '', { state: 'RUNNING' }) }, 'All running jobs'), jobsBody),
    h('div', { class: 'grid-2' }, section('Recent events', eventsBody), section('Peer links', peersBody)),
    adminBody);

  const jobsTable = sortableTable([
    { key: 'job_id', label: 'Job', render: (j) => idLink(j.job_id) },
    { key: 'step', label: 'Step / operation', value: (j) => j.step || j.spec?.operation || '', render: (j) => j.step || j.spec?.operation || '—' },
    { key: 'cpu', label: 'Cores', num: true, value: (j) => toNum(j.spec?.resources?.cpu_cores) || 1 },
    { key: 'mem', label: 'Memory', num: true, value: (j) => toNum(j.spec?.resources?.memory_bytes), render: (j) => bytes(j.spec?.resources?.memory_bytes) },
    { key: 'progress', label: 'Progress', num: true, value: (j) => j.progress_fraction ?? null, render: (j) => (j.progress_fraction !== undefined ? percent(j.progress_fraction) : '—') },
    { key: 'updated', label: 'Updated', value: (j) => toNum(j.updated_unix_ms), render: (j) => time(j.updated_unix_ms) },
  ], { sort: { key: 'updated', dir: 'desc' }, empty: 'No job is running on this node.', dense: true });
  const eventsTable = sortableTable([
    { key: 'at', label: 'When', value: (e) => toNum(e.at_unix_ms), render: (e) => h('span', { title: timestamp(e.at_unix_ms) }, timestamp(e.at_unix_ms)) },
    { key: 'change', label: 'Change', sortable: false, render: (e) => h('span', { class: 'change' }, e.from ? stateBadge(e.from) : h('span', { class: 'muted' }, 'new'), ' → ', stateBadge(e.to || 'UNKNOWN')) },
    { key: 'reason', label: 'Reason' },
  ], { sort: { key: 'at', dir: 'desc' }, empty: 'No events recorded.', dense: true });
  let jobsShown = false; let eventsShown = false; let adminShown = false;

  const poller = ctx.poll(async () => {
    let n;
    try {
      n = await api.get(`/nodes/${encodeURIComponent(id)}`);
    } catch (e) { replace(head, pageHead(id, { crumbs: [[buildHash('nodes'), 'Nodes']] }), errorBanner(e, ctx.role)); return; }
    const hb = n.last_heartbeat || {};
    const actions = [];
    if (ctx.isAdmin && n.health !== 'OFFLINE') {
      actions.push(actionButton({ label: 'Set offline', danger: true, confirm: `Take ${n.name} offline? Its leases are revoked and its jobs requeued.`,
        confirmLabel: 'Set offline', role: ctx.role,
        run: () => api.post(`/nodes/${encodeURIComponent(id)}/offline`, { reason: `set offline from the dashboard by ${ctx.whoami?.name || 'admin'}` }),
        onDone: () => poller.refresh() }));
    }
    replace(head, pageHead(n.name || id, {
      crumbs: [[buildHash('nodes'), 'Nodes'], ['', n.name || id]],
      badges: [stateBadge(n.health || 'UNKNOWN')],
      actions,
      sub: h('span', null, h('span', { class: 'id' }, n.node_id), n.offline_reason ? ` — offline: ${n.offline_reason}` : '',
        `, tdrd ${n.daemon_version || '?'}, registered ${timestamp(n.registered_unix_ms)}, last seen `, time(n.last_seen_unix_ms)),
    }));
    replace(load, nodeMeters(n),
      kv([['Jobs running', String(toNum(hb.running_jobs))], ['Queued locally', String(toNum(hb.queued_jobs))],
        ['Failed (total)', String(toNum(hb.failed_jobs))], ['Disk free', bytes(hb.disk_available_bytes)],
        ['Heartbeat sent', hb.sent_unix_ms ? time(hb.sent_unix_ms) : null],
        ['Policy applied', `v${toNum(hb.policy_version)} of v${toNum(n.policy?.version)}`]], 'kv-tight'));
    replace(pol, policyKv(n.policy));
    replace(hw, resourcesTable(n),
      kv([['Capabilities', h('span', { class: 'chips' }, (n.capabilities || []).map((c) => h('span', { class: 'chip' }, c)))],
        ['Addresses', h('span', { class: 'chips' }, (n.addresses || []).map((a) => h('span', { class: 'chip mono' }, `${a.type || '?'} ${a.address}:${a.port || ''}`)))],
        ['Certificate', h('span', { class: 'id wrap' }, n.cert_fingerprint || '—')]]));
    const peers = hb.peers || [];
    replace(peersBody, peers.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'data dense' },
      h('thead', null, h('tr', null, ['Peer', 'State', 'Path', 'RTT', 'Since'].map((t) => h('th', null, t)))),
      h('tbody', null, peers.map((p) => h('tr', null, h('td', null, p.node_id ? h('a', { href: buildHash('nodes', p.node_id) }, p.name || p.node_id) : p.name),
        h('td', null, stateBadge(p.state === 'CONNECTED' ? 'ONLINE' : p.state === 'UNAVAILABLE' ? 'OFFLINE' : 'QUEUED', { label: (p.state || '').toLowerCase() })),
        h('td', null, p.path_type ? `${p.path_type} ${p.address || ''}` : '—'),
        h('td', { class: 'num' }, p.rtt_ms ? `${num(p.rtt_ms)} ms` : '—'),
        h('td', null, p.connected_since_unix_ms ? time(p.connected_since_unix_ms) : (p.last_error || '—')))))))
      : empty('No peer links reported.', 'Peers appear when more than one node is online.'));

    try {
      const r = await api.get('/jobs', { state: 'RUNNING', limit: 1000 });
      const mine = (r.jobs || []).filter((j) => j.node_id === id);
      if (!jobsShown) { replace(jobsBody, jobsTable.el); jobsShown = true; }
      jobsTable.update(mine);
    } catch (e) { jobsShown = false; replace(jobsBody, errorBanner(e, ctx.role)); }
    try {
      const r = await api.get(`/nodes/${encodeURIComponent(id)}/events`, { limit: 20 });
      if (!eventsShown) { replace(eventsBody, eventsTable.el); eventsShown = true; }
      eventsTable.update(r.events || []);
    } catch (e) { eventsShown = false; replace(eventsBody, errorBanner(e, ctx.role)); }

    if (ctx.isAdmin && !adminShown) {
      adminShown = true;   // rendered once so typing isn't interrupted by polling
      replace(adminBody, section('Change limits', limitsForm(n, ctx, () => poller.refresh())));
    }
  });
}
