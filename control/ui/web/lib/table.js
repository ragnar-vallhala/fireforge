// Sortable table. Columns: {key, label, value(row) → sort value, render(row) → node|text,
// num (right-aligned, tabular), title, sortable (default true), cls}.
// update(rows) re-renders the body and keeps the chosen sort.

import { h, clear } from './dom.js';
import { sortRows } from './sort.js';

export function sortableTable(columns, { sort = null, empty = 'Nothing here yet.', rowClass, onRowClick, caption, dense = false, onSortChange } = {}) {
  let state = sort ? { ...sort } : null;
  let rows = [];
  const thead = h('thead');
  const tbody = h('tbody');
  const table = h('table', { class: ['data', dense ? 'dense' : ''] }, caption ? h('caption', null, caption) : null, thead, tbody);
  const wrap = h('div', { class: 'table-wrap' }, table);

  function header() {
    clear(thead);
    const tr = h('tr');
    for (const c of columns) {
      const sortable = c.sortable !== false && (c.value || c.key);
      const active = state && state.key === c.key;
      const th = h('th', { scope: 'col', class: [c.num ? 'num' : '', c.cls || ''], title: c.title,
        'aria-sort': active ? (state.dir === 'asc' ? 'ascending' : 'descending') : undefined });
      if (sortable) {
        const btn = h('button', { type: 'button', class: 'sort', onclick: () => {
          state = active ? { key: c.key, dir: state.dir === 'asc' ? 'desc' : 'asc' } : { key: c.key, dir: c.num ? 'desc' : 'asc' };
          onSortChange?.(state);
          header(); body();
        } }, c.label, h('span', { class: 'sort-mark', 'aria-hidden': 'true' }, active ? (state.dir === 'asc' ? '▲' : '▼') : ''));
        th.append(btn);
      } else {
        th.append(c.label);
      }
      tr.append(th);
    }
    thead.append(tr);
  }

  function valueOf(c, r) { return c.value ? c.value(r) : r[c.key]; }

  function body() {
    clear(tbody);
    let list = rows;
    if (state) {
      const c = columns.find((x) => x.key === state.key);
      if (c) list = sortRows(rows, (r) => valueOf(c, r), state.dir);
    }
    if (!list.length) {
      tbody.append(h('tr', { class: 'empty-row' }, h('td', { colspan: String(columns.length) }, empty)));
      return;
    }
    for (const r of list) {
      const tr = h('tr', { class: rowClass ? rowClass(r) : undefined });
      if (onRowClick) {
        tr.classList.add('clickable');
        tr.addEventListener('click', (e) => { if (!e.target.closest('a,button,input,select')) onRowClick(r); });
      }
      for (const c of columns) {
        const v = c.render ? c.render(r) : valueOf(c, r);
        tr.append(h('td', { class: [c.num ? 'num' : '', c.cls || ''] }, v === undefined || v === null || v === '' ? h('span', { class: 'muted' }, '—') : v));
      }
      tbody.append(tr);
    }
  }

  header();
  body();
  return {
    el: wrap,
    update(next) { rows = next || []; body(); },
    get sort() { return state; },
  };
}
