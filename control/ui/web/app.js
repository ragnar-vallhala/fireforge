// FireForge dashboard: shell, identity, hash router, page lifecycle.

import { h, clear, replace } from './lib/dom.js';
import { parseHash, buildHash } from './lib/router.js';
import { createPoller } from './lib/poller.js';
import { api, ApiError } from './lib/api.js';
import { errorBanner, empty } from './lib/ui.js';
import { ago } from './lib/format.js';

const NAV = [
  ['Cluster', [['overview', 'Overview'], ['nodes', 'Nodes'], ['jobs', 'Jobs'], ['workflows', 'Workflows'], ['campaigns', 'Campaigns']]],
  ['Science', [['materials', 'Materials'], ['results', 'Results'], ['calculations', 'Calculations']]],
  ['Records', [['audit', 'Audit log']]],
];

const PAGE_MODULES = {
  overview: './pages/overview.js',
  nodes: './pages/nodes.js',
  jobs: './pages/jobs.js',
  workflows: './pages/workflows.js',
  campaigns: './pages/campaigns.js',
  materials: './pages/materials.js',
  calculations: './pages/calculations.js',
  results: './pages/results.js',
  audit: './pages/audit.js',
};

const state = {
  whoami: null,
  whoamiError: null,
  lastUpdate: 0,
  route: null,
  teardown: [],
  nodeCache: { at: 0, map: new Map(), promise: null },
};

// ---- shell ---------------------------------------------------------------------

const navLinks = new Map();
const sidebar = h('nav', { class: 'sidebar', id: 'sidebar', 'aria-label': 'Main' },
  h('a', { class: 'brand', href: '#/' },
    h('span', { class: 'brand-tile', 'aria-hidden': 'true' }, h('span', { class: 'brand-z' }, '26'), h('span', { class: 'brand-sym' }, 'Ff')),
    h('span', { class: 'brand-text' }, h('span', { class: 'brand-name' }, 'FireForge'), h('span', { class: 'brand-sub' }, 'control plane'))),
  NAV.map(([group, items]) => h('div', { class: 'nav-group' },
    h('div', { class: 'nav-title' }, group),
    h('ul', null, items.map(([page, label]) => {
      const a = h('a', { href: buildHash(page), class: 'nav-link' }, label);
      navLinks.set(page, a);
      return h('li', null, a);
    })))));

const menuBtn = h('button', { class: 'menu-btn', type: 'button', 'aria-controls': 'sidebar', 'aria-expanded': 'false',
  onclick: () => setNav(!document.body.classList.contains('nav-open')) }, h('span', { class: 'menu-icon', 'aria-hidden': 'true' }), h('span', { class: 'sr-only' }, 'Menu'));
const freshness = h('span', { class: 'freshness', 'aria-live': 'off' });
const identity = h('span', { class: 'identity' });
const topbar = h('header', { class: 'topbar' }, menuBtn, h('span', { class: 'topbar-spacer' }), freshness, identity);
const view = h('main', { class: 'view', id: 'view', tabindex: '-1' });
const scrim = h('div', { class: 'scrim', onclick: () => setNav(false) });

function setNav(open) {
  document.body.classList.toggle('nav-open', open);
  menuBtn.setAttribute('aria-expanded', String(open));
}

document.body.append(h('div', { class: 'shell' }, sidebar, scrim, h('div', { class: 'main' }, topbar, view)));

function renderIdentity() {
  const w = state.whoami;
  if (!w) {
    replace(identity, h('span', { class: 'who who-unknown', title: state.whoamiError ? state.whoamiError.message : '' }, 'identity unknown'));
    return;
  }
  replace(identity,
    h('span', { class: `role role-${w.role || 'unknown'}`, title: w.fingerprint ? `certificate sha256 ${w.fingerprint}` : '' }, w.role || 'unknown'),
    h('span', { class: 'who' }, w.name || '—'));
}

setInterval(() => {
  if (document.hidden) return;
  freshness.textContent = state.lastUpdate ? `updated ${ago(state.lastUpdate)}` : '';
}, 1000);

// ---- page context -----------------------------------------------------------------

function makeCtx(route) {
  const destroyers = [];
  const pollers = [];
  const ctx = {
    route,
    get whoami() { return state.whoami; },
    get role() { return state.whoami?.role || ''; },
    get isAdmin() { return state.whoami?.role === 'admin'; },
    alive: true,
    onDestroy(fn) { destroyers.push(fn); },
    /** Poll fn every ms while visible; fn's errors are passed to onError (default: logged). */
    poll(fn, { intervalMs = 5000, onError } = {}) {
      const p = createPoller(async () => {
        if (!ctx.alive) return false;
        const r = await fn();
        state.lastUpdate = Date.now();
        return r;
      }, { intervalMs, onError: onError || ((e) => console.warn('poll failed', e)) });
      pollers.push(p);
      p.start();
      return p;
    },
    refresh() { for (const p of pollers) p.refresh(); },
    /** Update the hash query without re-rendering the page. */
    setQuery(query) {
      const hash = buildHash(route.page, route.id, query);
      history.replaceState(null, '', hash);
      route.query = { ...query };
    },
    /** node_id → name (cached 30 s). */
    async nodeNames() {
      const c = state.nodeCache;
      if (Date.now() - c.at < 30000) return c.map;
      if (!c.promise) {
        c.promise = api.get('/nodes').then((r) => {
          c.map = new Map((r.nodes || []).map((n) => [n.node_id, n.name]));
          c.at = Date.now();
          return c.map;
        }).catch(() => c.map).finally(() => { c.promise = null; });
      }
      return c.promise;
    },
    destroy() {
      ctx.alive = false;
      for (const p of pollers) p.stop();
      for (const fn of destroyers) { try { fn(); } catch (e) { console.warn(e); } }
    },
  };
  return ctx;
}

// ---- router ----------------------------------------------------------------------

let generation = 0;
async function route() {
  const r = parseHash(location.hash);
  const gen = ++generation;
  if (state.route?.ctx) state.route.ctx.destroy();
  setNav(false);
  for (const [page, a] of navLinks) {
    if (page === r.page) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
  }
  clear(view);
  if (r.page === 'notfound') {
    document.title = 'Not found · FireForge';
    view.append(empty('There is no page at this address.', 'Use the navigation to pick a page.'));
    state.route = null;
    return;
  }
  const ctx = makeCtx(r);
  state.route = { ...r, ctx };
  try {
    const mod = await import(PAGE_MODULES[r.page]);
    if (gen !== generation) return;
    const label = NAV.flatMap(([, items]) => items).find(([p]) => p === r.page)?.[1] || 'FireForge';
    document.title = `${r.id ? `${r.id} · ` : ''}${label} · FireForge`;
    await mod.render(view, r, ctx);
  } catch (e) {
    if (gen !== generation) return;
    console.error(e);
    view.append(errorBanner(e instanceof ApiError ? e : new Error(`This page failed to load: ${e.message}`), ctx.role));
  }
}

window.addEventListener('hashchange', () => { route(); view.focus({ preventScroll: true }); window.scrollTo(0, 0); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') setNav(false); });

async function boot() {
  try {
    state.whoami = await api.get('/whoami');
  } catch (e) {
    state.whoamiError = e;
  }
  renderIdentity();
  route();
}
boot();
