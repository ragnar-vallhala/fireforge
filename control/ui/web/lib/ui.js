// Shared UI components (DOM). All data goes in as text.

import { h, s, clear, replace } from './dom.js';
import { stateTone, stateGlyph, stateLabel, orderedCounts, num, shortId, timestamp, ago } from './format.js';
import { hrefForId } from './router.js';
import { explain } from './api.js';

export function stateBadge(state, { label } = {}) {
  const st = state || 'UNKNOWN';
  return h('span', { class: `badge tone-${stateTone(st)}`, title: st },
    h('span', { class: 'glyph', 'aria-hidden': 'true' }, stateGlyph(st)), label || stateLabel(st));
}

export function link(href, text, props = {}) { return h('a', { href, ...props }, text); }

/** An id rendered as a link to its page (by prefix), monospace, shortened. */
export function idLink(id, { label, full = false } = {}) {
  if (!id) return h('span', { class: 'muted' }, '—');
  const href = hrefForId(id);
  const text = label || (full ? id : shortId(id));
  return href ? h('a', { href, class: 'id', title: id }, text) : h('span', { class: 'id', title: id }, text);
}

export function time(unixMs) {
  if (!unixMs) return h('span', { class: 'muted' }, '—');
  return h('time', { title: timestamp(unixMs), datetime: new Date(Number(unixMs)).toISOString() }, ago(unixMs));
}

/** Definition list from [[label, value], …]; values may be nodes. Skips undefined. */
export function kv(rows, cls = '') {
  const dl = h('dl', { class: `kv ${cls}` });
  for (const [k, v] of rows) {
    if (v === undefined) continue;
    dl.append(h('dt', null, k), h('dd', null, v === null || v === '' ? h('span', { class: 'muted' }, '—') : v));
  }
  return dl;
}

export function section(title, ...children) {
  return h('section', { class: 'panel' }, title ? h('h2', null, title) : null, ...children);
}

/** Section with a title row that has controls on the right. */
export function sectionWith(title, controls, ...children) {
  return h('section', { class: 'panel' },
    h('div', { class: 'panel-head' }, h('h2', null, title), h('div', { class: 'panel-tools' }, controls)),
    ...children);
}

/** Horizontal meter; `tone` overrides the automatic one. */
export function meter(value, max, { label = '', detail = '', tone } = {}) {
  const frac = max > 0 ? Math.max(0, Math.min(1, value / max)) : 0;
  const t = tone || (frac >= 0.95 ? 'hot' : frac >= 0.75 ? 'warm' : 'cool');
  const fill = h('span', { class: `meter-fill meter-${t}` });
  fill.style.width = `${(frac * 100).toFixed(1)}%`;
  return h('div', { class: 'meter-row' },
    label ? h('span', { class: 'meter-label' }, label) : null,
    h('span', { class: 'meter', role: 'meter', 'aria-valuemin': '0', 'aria-valuemax': String(max), 'aria-valuenow': String(value) }, fill),
    detail ? h('span', { class: 'meter-detail' }, detail) : null);
}

/** Segmented bar of state counts, with a legend of the counts. */
export function stateBar(counts, { legend = true, total } = {}) {
  const entries = orderedCounts(counts);
  const sum = total || entries.reduce((a, [, n]) => a + n, 0);
  const bar = h('span', { class: 'segbar', role: 'img',
    'aria-label': entries.map(([st, n]) => `${n} ${stateLabel(st)}`).join(', ') });
  for (const [st, n] of entries) {
    const seg = h('span', { class: `seg tone-${stateTone(st)}`, title: `${n} ${stateLabel(st)}` });
    seg.style.flexGrow = String(n);
    bar.append(seg);
  }
  if (!sum) bar.append(h('span', { class: 'seg seg-empty' }));
  if (!legend) return bar;
  return h('div', { class: 'segbar-wrap' }, bar,
    h('span', { class: 'segbar-legend' }, entries.map(([st, n]) =>
      h('span', { class: 'segbar-item' }, h('span', { class: `dot tone-${stateTone(st)}`, 'aria-hidden': 'true' }), `${n} ${stateLabel(st)}`))));
}

export function empty(text, hint) {
  return h('div', { class: 'empty' }, h('p', null, text), hint ? h('p', { class: 'muted' }, hint) : null);
}

export function loading(text = 'Loading…') { return h('div', { class: 'loading' }, text); }

/** Error banner for an API error. */
export function errorBanner(err, role) {
  const e = explain(err, role);
  return h('div', { class: 'banner banner-error', role: 'alert' },
    h('strong', null, e.title), e.detail ? h('span', null, e.detail) : null);
}

export function noticeBanner(text, kind = 'info') {
  return h('div', { class: `banner banner-${kind}`, role: 'status' }, text);
}

/** Pretty JSON in a <pre>, as text. */
export function jsonBlock(value, { collapsedOver = 0 } = {}) {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  const pre = h('pre', { class: 'json' }, text);
  if (collapsedOver && text.split('\n').length > collapsedOver) {
    return h('details', { class: 'json-details' }, h('summary', null, `${text.split('\n').length} lines`), pre);
  }
  return pre;
}

/** Value cell for a scientific property. */
export function valueText(v) {
  if (v === null || v === undefined) return h('span', { class: 'muted' }, '—');
  if (typeof v === 'boolean') return h('span', { class: v ? 'yes' : 'no' }, v ? 'yes' : 'no');
  if (typeof v === 'object') return h('code', { class: 'inline-json' }, JSON.stringify(v));
  return num(v, 4);
}

/**
 * A button that asks for confirmation in place, runs `run()`, shows its error inline,
 * and calls `onDone(result)` on success.
 */
export function actionButton({ label, confirm, confirmLabel, danger = false, run, onDone, role }) {
  const wrap = h('span', { class: 'action' });
  const show = () => {
    const b = h('button', { class: danger ? 'btn btn-danger' : 'btn', type: 'button', onclick: ask }, label);
    replace(wrap, b);
  };
  const ask = () => {
    if (!confirm) { go(); return; }
    const yes = h('button', { class: danger ? 'btn btn-danger' : 'btn btn-primary', type: 'button', onclick: go }, confirmLabel || label);
    const no = h('button', { class: 'btn btn-quiet', type: 'button', onclick: show }, 'Keep');
    replace(wrap, h('span', { class: 'confirm', role: 'group', 'aria-label': confirm }, h('span', null, confirm), yes, no));
    yes.focus();
  };
  const go = async () => {
    replace(wrap, h('button', { class: 'btn', type: 'button', disabled: true }, `${label}…`));
    try {
      const r = await run();
      show();
      onDone?.(r);
    } catch (e) {
      show();
      wrap.append(errorBanner(e, role));
    }
  };
  show();
  return wrap;
}

/** Toolbar row of filter controls. */
export function toolbar(...children) { return h('div', { class: 'toolbar' }, ...children); }

export function select(label, options, value, onChange) {
  const sel = h('select', { onchange: (e) => onChange(e.target.value) });
  for (const o of options) {
    const [v, t] = Array.isArray(o) ? o : [o, o];
    const opt = h('option', { value: v }, t);
    if (v === value) opt.selected = true;
    sel.append(opt);
  }
  return h('label', { class: 'field' }, h('span', null, label), sel);
}

export function input(label, value, onInput, props = {}) {
  let t = null;
  const el = h('input', { type: 'search', value: value || '', ...props,
    oninput: (e) => { clearTimeout(t); t = setTimeout(() => onInput(e.target.value), 200); } });
  return h('label', { class: 'field' }, h('span', null, label), el);
}

/** Header block of a detail page. */
export function pageHead(title, { sub, badges = [], actions = [], crumbs = [] } = {}) {
  return h('header', { class: 'page-head' },
    crumbs.length ? h('nav', { class: 'crumbs', 'aria-label': 'Breadcrumb' }, crumbs.map(([href, t], i) =>
      [i ? h('span', { class: 'sep', 'aria-hidden': 'true' }, '/') : null, href ? link(href, t) : h('span', null, t)])) : null,
    h('div', { class: 'page-title-row' },
      h('h1', null, title), badges.length ? h('span', { class: 'badges' }, badges) : null,
      actions.length ? h('span', { class: 'actions' }, actions) : null),
    sub ? h('p', { class: 'page-sub' }, sub) : null);
}

/** Small inline bar for a value in a table cell, scaled against max. */
export function cellBar(value, max) {
  if (value === null || value === undefined || !Number.isFinite(value)) return h('span', { class: 'muted' }, '—');
  const frac = max > 0 ? Math.max(0, Math.min(1, value / max)) : 0;
  const fill = h('span', { class: 'cellbar-fill' });
  fill.style.width = `${(frac * 100).toFixed(1)}%`;
  return h('span', { class: 'cellbar' }, h('span', { class: 'cellbar-track', 'aria-hidden': 'true' }, fill), h('span', { class: 'cellbar-num' }, value.toFixed(2)));
}

export { h, s, clear, replace };
