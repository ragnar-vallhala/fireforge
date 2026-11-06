// #/jobs and #/jobs/ID.

import { h, replace, stateBadge, idLink, time, kv, section, errorBanner, empty, loading, pageHead, actionButton,
  toolbar, select, jsonBlock, noticeBanner } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { bytes, percent, timestamp, duration, JOB_STATES, TERMINAL_JOB_STATES, shortArtifact, stateLabel } from '../lib/format.js';
import { toNum, parseJson } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';

export async function render(el, route, ctx) {
  if (route.id) return renderDetail(el, route, ctx);
  const q = { state: route.query.state || '', campaign: route.query.campaign || '' };
  const body = h('div', null, loading());
  const campaignOpts = [['', 'All campaigns']];
  const tools = h('div');
  const note = h('p', { class: 'muted small' });
  el.append(pageHead('Jobs', { sub: 'Central queue, newest first.' }), section(null, tools, body, note));

  let poller = null;
  const drawTools = () => replace(tools, toolbar(
    select('State', [['', 'Any state'], ...JOB_STATES.map((s) => [s, stateLabel(s)])], q.state, (v) => { q.state = v; ctx.setQuery(q); poller.refresh(); }),
    select('Campaign', campaignOpts, q.campaign, (v) => { q.campaign = v; ctx.setQuery(q); poller.refresh(); })));
  drawTools();
  api.get('/campaigns').then((r) => {
    for (const c of r.campaigns || []) campaignOpts.push([c.campaign_id, c.name || c.campaign_id]);
    drawTools();
  }).catch(() => {});

  const names = await ctx.nodeNames();
  const table = sortableTable([
    { key: 'job_id', label: 'Job', render: (j) => idLink(j.job_id) },
    { key: 'what', label: 'Step / operation', value: (j) => j.step || j.spec?.operation || '',
      render: (j) => h('span', null, j.step || j.spec?.operation || h('span', { class: 'muted' }, (j.spec?.command?.argv || []).join(' ') || '—')) },
    { key: 'state', label: 'State', render: (j) => h('span', null, stateBadge(j.state), j.cancel_requested && !TERMINAL_JOB_STATES.has(j.state) ? h('span', { class: 'muted small' }, ' cancelling') : null) },
    { key: 'node', label: 'Node', value: (j) => names.get(j.node_id) || j.node_id || '', render: (j) => (j.node_id ? h('a', { href: buildHash('nodes', j.node_id) }, names.get(j.node_id) || j.node_id) : null) },
    { key: 'cpu', label: 'Cores', num: true, value: (j) => toNum(j.spec?.resources?.cpu_cores) || 1 },
    { key: 'mem', label: 'Memory', num: true, value: (j) => toNum(j.spec?.resources?.memory_bytes), render: (j) => bytes(j.spec?.resources?.memory_bytes) },
    { key: 'attempts', label: 'Attempts', num: true, value: (j) => toNum(j.attempts_used),
      render: (j) => `${toNum(j.attempts_used)} / ${toNum(j.spec?.policy?.max_attempts) || 1}` },
    { key: 'progress', label: 'Progress', num: true, value: (j) => j.progress_fraction ?? null, render: (j) => (j.progress_fraction !== undefined ? percent(j.progress_fraction) : null) },
    { key: 'created', label: 'Created', value: (j) => toNum(j.created_unix_ms), render: (j) => time(j.created_unix_ms) },
    { key: 'updated', label: 'Updated', value: (j) => toNum(j.updated_unix_ms), render: (j) => time(j.updated_unix_ms) },
    { key: 'why', label: 'Note', sortable: false, cls: 'clip', render: (j) => j.last_error || j.pending_reason || j.progress_message || '' },
  ], { sort: { key: 'created', dir: 'desc' }, empty: 'No jobs match these filters.' });
  let shown = false;
  poller = ctx.poll(async () => {
    try {
      const r = await api.get('/jobs', { state: q.state, campaign: q.campaign, limit: 1000 });
      if (!shown) { replace(body, table.el); shown = true; }
      table.update(r.jobs || []);
      note.textContent = (r.jobs || []).length >= 1000 ? 'Showing the newest 1000 jobs; filter by state or campaign to see others.' : `${(r.jobs || []).length} jobs`;
    } catch (e) { shown = false; replace(body, errorBanner(e, ctx.role)); }
  });
}

function specBlock(spec, names) {
  spec = spec || {};
  const res = spec.resources || {};
  const pol = spec.policy || {};
  const params = parseJson(spec.parameters_json, null);
  const inputs = (spec.input_artifacts || []).map((a, i) => [spec.input_names?.[i] || '', a]);
  return h('div', null,
    kv([
      ['Operation', spec.operation || null],
      ['Command', spec.command?.argv?.length ? h('code', null, spec.command.argv.join(' ')) : null],
      ['Resources', `${toNum(res.cpu_cores) || 1} core${(toNum(res.cpu_cores) || 1) > 1 ? 's' : ''}, ${res.memory_bytes ? bytes(res.memory_bytes) : 'default memory'}${res.disk_bytes ? `, ${bytes(res.disk_bytes)} disk` : ''}${res.gpu_count ? `, ${res.gpu_count} GPU` : ''}`],
      ['MPI', spec.mpi?.ranks ? `${spec.mpi.ranks} ranks${spec.mpi.max_ranks_per_node ? `, ≤ ${spec.mpi.max_ranks_per_node} per node` : ''}` : undefined],
      ['Policy', `${toNum(pol.max_attempts) || 1} attempt(s), timeout ${toNum(pol.timeout_seconds) ? duration(toNum(pol.timeout_seconds) * 1000) : 'none'}${pol.checkpoint ? ', checkpoints' : ''}`],
      ['Capabilities', spec.required_capabilities?.length ? h('span', { class: 'chips' }, spec.required_capabilities.map((c) => h('span', { class: 'chip' }, c))) : null],
      ['Campaign', spec.campaign_id ? idLink(spec.campaign_id) : null],
      ['Material', spec.material_id ? idLink(spec.material_id) : undefined],
      ['Depends on', spec.depends_on?.length ? h('span', { class: 'chips' }, spec.depends_on.map((d) => idLink(d))) : undefined],
      ['Resume from', spec.resume_checkpoint ? h('span', { class: 'id' }, shortArtifact(spec.resume_checkpoint)) : undefined],
    ]),
    params && Object.keys(params).length ? h('div', { class: 'sub-block' }, h('h3', null, 'Parameters'), jsonBlock(params, { collapsedOver: 30 })) : null,
    inputs.length ? h('div', { class: 'sub-block' }, h('h3', null, `Inputs (${inputs.length})`), artifactList(inputs)) : null);
}

function artifactList(pairs) {
  return h('ul', { class: 'artifact-list' }, pairs.map(([name, id, extra]) => h('li', null,
    h('span', { class: 'artifact-name' }, name || h('span', { class: 'muted' }, 'unnamed')),
    h('span', { class: 'id', title: id }, shortArtifact(id)), extra ? h('span', { class: 'muted small' }, extra) : null)));
}

async function renderDetail(el, route, ctx) {
  const id = route.id;
  const head = h('div');
  const main = h('div', null, loading());
  el.append(head, main);
  const names = await ctx.nodeNames();
  const poller = ctx.poll(async () => {
    let j;
    try { j = await api.get(`/jobs/${encodeURIComponent(id)}`); } catch (e) {
      replace(head, pageHead(id, { crumbs: [[buildHash('jobs'), 'Jobs']] })); replace(main, errorBanner(e, ctx.role)); return;
    }
    const terminal = TERMINAL_JOB_STATES.has(j.state);
    const actions = [];
    if (ctx.isAdmin && !terminal && !j.cancel_requested) {
      actions.push(actionButton({ label: 'Cancel job', danger: true, confirm: 'Cancel this job? A running attempt is stopped.', confirmLabel: 'Cancel job', role: ctx.role,
        run: () => api.post(`/jobs/${encodeURIComponent(id)}/cancel`, { reason: `cancelled from the dashboard by ${ctx.whoami?.name || 'admin'}` }),
        onDone: () => poller.refresh() }));
    }
    replace(head, pageHead(j.step || j.spec?.operation || id, {
      crumbs: [[buildHash('jobs'), 'Jobs'], ['', id]],
      badges: [stateBadge(j.state), j.cancel_requested && !terminal ? h('span', { class: 'badge tone-warn' }, 'cancel requested') : null].filter(Boolean),
      actions,
      sub: h('span', null, h('span', { class: 'id' }, j.job_id), `, submitted by ${j.submitted_by || '?'} `, time(j.created_unix_ms)),
    }));
    const attempts = j.attempts || [];
    replace(main,
      j.last_error ? h('div', { class: 'banner banner-error' }, h('strong', null, 'Last error'), h('span', { class: 'pre' }, j.last_error)) : null,
      j.pending_reason ? noticeBanner(`Waiting: ${j.pending_reason}`) : null,
      h('div', { class: 'grid-2' },
        section('Status', kv([
          ['State', stateBadge(j.state)],
          ['Node', j.node_id ? h('a', { href: buildHash('nodes', j.node_id) }, names.get(j.node_id) || j.node_id) : null],
          ['Workflow', j.workflow_id ? h('span', null, idLink(j.workflow_id), j.step ? ` step ${j.step}` : '') : undefined],
          ['Progress', j.progress_fraction !== undefined ? `${percent(j.progress_fraction)}${j.progress_message ? ` — ${j.progress_message}` : ''}` : (j.progress_message || undefined)],
          ['Attempts', `${toNum(j.attempts_used)} of ${toNum(j.spec?.policy?.max_attempts) || 1}`],
          ['Reserved', j.reserved_node ? h('span', null, names.get(j.reserved_node) || j.reserved_node, ' until ', timestamp(j.reserved_until_unix_ms)) : undefined],
          ['Checkpoint', j.checkpoint?.artifact_id ? h('span', { class: 'id' }, `${j.checkpoint.name || ''} ${shortArtifact(j.checkpoint.artifact_id)}`) : undefined],
          ['Created', timestamp(j.created_unix_ms)],
          ['Updated', h('span', null, timestamp(j.updated_unix_ms), ' (', time(j.updated_unix_ms), ')')],
        ])),
        section('Spec', specBlock(j.spec, names))),
      section(`Attempts (${attempts.length})`, attempts.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'data dense' },
        h('thead', null, h('tr', null, ['#', 'State', 'Node', 'Leased', 'Ran for', 'Error'].map((t) => h('th', null, t)))),
        h('tbody', null, attempts.map((a) => h('tr', null,
          h('td', { class: 'num' }, String(a.number || '')),
          h('td', null, stateBadge(a.state)),
          h('td', null, a.node_id ? h('a', { href: buildHash('nodes', a.node_id) }, names.get(a.node_id) || a.node_id) : '—',
            a.members?.length ? h('span', { class: 'muted small' }, ` + ${a.members.length - 1} gang members`) : null),
          h('td', null, timestamp(a.leased_unix_ms)),
          h('td', { class: 'num' }, a.ended_unix_ms ? duration(toNum(a.ended_unix_ms) - toNum(a.leased_unix_ms)) : h('span', { class: 'muted' }, `running ${duration(Date.now() - toNum(a.leased_unix_ms))}`)),
          h('td', { class: 'pre' }, a.error || '')))))) : empty('Not leased yet.', j.pending_reason || 'The job waits in the queue for a node with free capacity.')),
      j.gang?.length ? section('Gang', h('div', { class: 'table-wrap' }, h('table', { class: 'data dense' },
        h('thead', null, h('tr', null, ['Member', 'Node', 'Role', 'Ranks', 'State'].map((t) => h('th', null, t)))),
        h('tbody', null, j.gang.map((m) => h('tr', null, h('td', { class: 'num' }, String(m.member_index || 0)),
          h('td', null, m.name || m.node_id), h('td', null, (m.role || '').replace('GANG_', '').toLowerCase()),
          h('td', { class: 'num' }, `${m.rank_offset || 0}–${(m.rank_offset || 0) + (m.ranks || 0) - 1}`), h('td', null, stateBadge(m.state)))))))) : null,
      section(`Outputs (${(j.outputs || []).length})`, (j.outputs || []).length
        ? artifactList(j.outputs.map((o) => [o.name, o.artifact_id, `${bytes(o.size)}${o.media_type ? `, ${o.media_type}` : ''}`]))
        : empty(terminal ? 'This job published no outputs.' : 'Outputs appear when the job completes.')));
    return !terminal;   // false stops polling once terminal (refresh restarts it)
  });
}
