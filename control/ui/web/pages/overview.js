// #/ overview: nodes, job counts, running workflows, latest audit entries.

import { h, replace, stateBadge, stateBar, meter, idLink, time, errorBanner, empty, loading, link, sectionWith } from '../lib/ui.js';
import { api } from '../lib/api.js';
import { bytes, percent, stateLabel, orderedCounts, stateTone, actor } from '../lib/format.js';
import { toNum, resources, stepCounts, sum } from '../lib/proto.js';
import { buildHash } from '../lib/router.js';
import { sortableTable } from '../lib/table.js';

export function nodeLoad(n) {
  const hb = n.last_heartbeat || {};
  const cap = resources(hb.capacity || n.configured);
  const alloc = resources(hb.allocated);
  const phys = resources(n.physical);
  const memAvail = toNum(hb.memory_available_bytes);
  return { hb, cap, alloc, phys, memAvail, util: Number(hb.cpu_utilization || 0), running: toNum(hb.running_jobs), queued: toNum(hb.queued_jobs) };
}

export function nodeMeters(n, { compact = false } = {}) {
  const l = nodeLoad(n);
  const offline = (n.health || '') !== 'ONLINE';
  const tone = offline ? 'off' : undefined;
  return h('div', { class: compact ? 'meters compact' : 'meters' },
    meter(l.alloc.cpu_cores, l.cap.cpu_cores, { label: 'CPU booked', detail: `${l.alloc.cpu_cores} / ${l.cap.cpu_cores} cores`, tone: tone || 'cool' }),
    meter(l.util, 1, { label: 'CPU busy', detail: percent(l.util), tone }),
    meter(l.alloc.memory_bytes, l.cap.memory_bytes, { label: 'Mem booked', detail: `${bytes(l.alloc.memory_bytes)} / ${bytes(l.cap.memory_bytes)}`, tone: tone || 'cool' }),
    l.phys.memory_bytes ? meter(l.phys.memory_bytes - l.memAvail, l.phys.memory_bytes,
      { label: 'Mem used', detail: `${bytes(l.memAvail)} free of ${bytes(l.phys.memory_bytes)}`, tone }) : null);
}

export async function render(el, route, ctx) {
  const nodesBody = h('div', null, loading());
  const jobsBody = h('div', null, loading());
  const wfBody = h('div', null, loading());
  const auditBody = h('div', null, loading());

  el.append(
    h('header', { class: 'page-head' }, h('div', { class: 'page-title-row' }, h('h1', null, 'Overview'))),
    h('div', { class: 'grid-2' },
      sectionWith('Nodes', link(buildHash('nodes'), 'All nodes'), nodesBody),
      sectionWith('Jobs', link(buildHash('jobs'), 'All jobs'), jobsBody)),
    sectionWith('Running workflows', link(buildHash('workflows'), 'All workflows'), wfBody),
    sectionWith('Latest audit entries', link(buildHash('audit'), 'Audit log'), auditBody));

  const auditTable = sortableTable([
    { key: 'at', label: 'When', value: (r) => toNum(r.at_unix_ms), render: (r) => time(r.at_unix_ms), num: false },
    { key: 'actor', label: 'Actor', render: (r) => actor(r.actor).name, value: (r) => r.actor },
    { key: 'action', label: 'Action', render: (r) => h('code', null, r.action) },
    { key: 'target', label: 'Target', render: (r) => idLink(r.target) },
  ], { sort: { key: 'at', dir: 'desc' }, empty: 'No audit entries yet.', dense: true });

  async function loadNodes() {
    try {
      const r = await api.get('/nodes');
      const nodes = r.nodes || [];
      if (!nodes.length) { replace(nodesBody, empty('No nodes have registered.', 'Start tdrd on a machine with a node certificate.')); return; }
      replace(nodesBody, h('ul', { class: 'node-cards' }, nodes.map((n) => {
        const l = nodeLoad(n);
        return h('li', { class: `node-card tone-${stateTone(n.health)}` },
          h('div', { class: 'node-card-head' },
            h('a', { href: buildHash('nodes', n.node_id), class: 'node-name' }, n.name || n.node_id),
            stateBadge(n.health || 'NODE_HEALTH_UNSPECIFIED'),
            h('span', { class: 'muted small' }, `${l.running} running`, l.queued ? `, ${l.queued} queued locally` : '', ', seen ', time(n.last_seen_unix_ms))),
          nodeMeters(n, { compact: true }));
      })));
    } catch (e) { replace(nodesBody, errorBanner(e, ctx.role)); }
  }

  async function loadJobs() {
    try {
      const r = await api.get('/jobs', { limit: 1000 });
      const jobs = r.jobs || [];
      const counts = {};
      for (const j of jobs) counts[j.state || 'JOB_STATE_UNSPECIFIED'] = (counts[j.state || 'JOB_STATE_UNSPECIFIED'] || 0) + 1;
      if (!jobs.length) { replace(jobsBody, empty('No jobs yet.', 'Submit work with tdrctl cp submit or a workflow.')); return; }
      replace(jobsBody,
        stateBar(counts, { legend: false }),
        h('ul', { class: 'count-list' }, orderedCounts(counts).map(([st, n]) =>
          h('li', null, h('a', { href: buildHash('jobs', '', { state: st }) }, stateBadge(st)), h('span', { class: 'count' }, String(n))))),
        jobs.length >= 1000 ? h('p', { class: 'muted small' }, 'Counts cover the newest 1000 jobs.') : null);
    } catch (e) { replace(jobsBody, errorBanner(e, ctx.role)); }
  }

  async function loadWorkflows() {
    try {
      const r = await api.get('/workflows', { state: 'RUNNING', limit: 50 });
      const wfs = r.workflows || [];
      if (!wfs.length) { replace(wfBody, empty('No workflow is running.', 'Finished workflows are under Workflows.')); return; }
      replace(wfBody, h('ul', { class: 'wf-list' }, wfs.map((w) => {
        const c = stepCounts(w);
        const total = sum(c);
        const done = (c.COMPLETED || 0) + (c.SKIPPED || 0);
        return h('li', null,
          h('div', { class: 'wf-line' },
            h('a', { href: buildHash('workflows', w.workflow_id), class: 'wf-name' }, w.name || w.workflow_id),
            h('span', { class: 'muted small' }, `${done} of ${total} steps done, started `, time(w.created_unix_ms))),
          stateBar(c));
      })));
    } catch (e) { replace(wfBody, errorBanner(e, ctx.role)); }
  }

  let auditShown = false;
  async function loadAudit() {
    try {
      const r = await api.get('/audit', { limit: 8 });
      if (!auditShown) { replace(auditBody, auditTable.el); auditShown = true; }
      auditTable.update(r.entries || []);
    } catch (e) { auditShown = false; replace(auditBody, errorBanner(e, ctx.role)); }
  }

  ctx.poll(() => Promise.all([loadNodes(), loadJobs(), loadWorkflows(), loadAudit()]));
}

export { stateLabel };
