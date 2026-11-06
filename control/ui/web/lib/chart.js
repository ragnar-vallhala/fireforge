// Line charts over the vendored uPlot, following the page theme (CSS custom
// properties) and the container width. Series colours are named slots
// (--series-1 …) so light and dark use their own validated steps.

import uPlot from '../vendor/uplot/uPlot.esm.js';
import { h, clear } from './dom.js';
import { num } from './format.js';

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const darkQuery = typeof matchMedia === 'function' ? matchMedia('(prefers-color-scheme: dark)') : null;

/**
 * lineChart(container, {
 *   x: [...], xLabel, yLabel, yLog,
 *   series: [{label, y:[...], color:'--series-1', points:'always'|'auto'|'only', dash}],
 *   height, yFormat(v), xFormat(v)
 * }) → {destroy()}
 */
// Log axes: one tick and grid line per decade, so labels never pile up.
function decades(self, axisIdx, min, max) {
  const out = [];
  for (let e = Math.floor(Math.log10(min)); e <= Math.ceil(Math.log10(max)); e++) {
    const v = 10 ** e;
    if (v >= min * 0.999 && v <= max * 1.001) out.push(v);
  }
  return out.length ? out : [min, max];
}

export function lineChart(container, spec) {
  let u = null;
  const holder = h('div', { class: 'chart-canvas' });
  container.append(holder);

  function opts(width) {
    const ink = css('--ink-2');
    const grid = css('--chart-grid');
    const axis = css('--chart-axis');
    const surface = css('--surface');
    const font = `12px ${css('--font-ui') || 'system-ui, sans-serif'}`;
    const yFmt = spec.yFormat || ((v) => num(v, 3));
    const axisCommon = { stroke: ink, font, labelFont: font, grid: { stroke: grid, width: 1 }, ticks: { stroke: axis, width: 1, size: 4 } };
    return {
      width,
      height: spec.height || 220,
      cursor: { points: { size: 8 }, drag: { x: false, y: false } },
      legend: { live: true },
      scales: {
        x: { time: false },
        y: spec.yLog ? { distr: 3, log: 10 } : { range: (self, min, max) => [Math.min(0, min), max > 0 ? max * 1.08 : 1] },
      },
      axes: [
        { ...axisCommon, label: spec.xLabel, values: (self, ticks) => ticks.map(spec.xFormat || ((v) => String(Math.round(v)))) },
        spec.yLog
          ? { ...axisCommon, label: spec.yLabel, size: 54, splits: decades, values: (self, ticks) => ticks.map(yFmt) }
          : { ...axisCommon, label: spec.yLabel, size: 54, values: (self, ticks) => ticks.map(yFmt) },
      ],
      series: [
        { label: spec.xLabel || 'x', value: (self, v) => (v === null || v === undefined ? '—' : (spec.xFormat || ((x) => `${Math.round(x)}`))(v)) },
        ...spec.series.map((sr) => {
          const color = css(sr.color || '--series-1');
          const only = sr.points === 'only';
          return {
            label: sr.label,
            stroke: color,
            width: only ? 0 : 2,
            dash: sr.dash,
            spanGaps: true,   // gaps come from aligning series on a shared x; bridge them
            paths: only ? () => null : undefined,
            points: {
              show: only || sr.points === 'always',
              size: only ? 10 : 8,
              width: 2,
              stroke: only ? surface : color,
              fill: color,
            },
            value: (self, v) => (v === null || v === undefined ? '—' : yFmt(v)),
          };
        }),
      ],
    };
  }

  function build() {
    if (u) { u.destroy(); u = null; }
    clear(holder);
    const width = Math.max(240, Math.floor(container.clientWidth || 480));
    u = new uPlot(opts(width), [spec.x, ...spec.series.map((s) => s.y)], holder);
  }

  build();
  let lastW = container.clientWidth;
  const ro = typeof ResizeObserver === 'function' ? new ResizeObserver(() => {
    const w = container.clientWidth;
    if (u && w && Math.abs(w - lastW) > 2) { lastW = w; u.setSize({ width: Math.max(240, Math.floor(w)), height: spec.height || 220 }); }
  }) : null;
  ro?.observe(container);
  const onTheme = () => build();
  darkQuery?.addEventListener?.('change', onTheme);

  return {
    destroy() {
      ro?.disconnect();
      darkQuery?.removeEventListener?.('change', onTheme);
      if (u) u.destroy();
      u = null;
    },
  };
}

/** A chart block: title, note, the chart, and a collapsible data table (the accessible view). */
export function chartBlock(title, spec, { note, ctx } = {}) {
  const body = h('div', { class: 'chart-body' });
  const tableRows = spec.x.map((x, i) => h('tr', null, h('td', { class: 'num' }, String(Math.round(x))),
    spec.series.map((s) => h('td', { class: 'num' }, s.y[i] === null || s.y[i] === undefined ? '—' : num(s.y[i], 3)))));
  const table = h('details', { class: 'chart-table' }, h('summary', null, 'Data table'),
    h('div', { class: 'table-wrap' }, h('table', { class: 'data dense' },
      h('thead', null, h('tr', null, h('th', { class: 'num' }, spec.xLabel || 'x'), spec.series.map((s) => h('th', { class: 'num' }, s.label)))),
      h('tbody', null, tableRows))));
  const block = h('figure', { class: 'chart' }, h('figcaption', null, h('span', { class: 'chart-title' }, title), note ? h('span', { class: 'chart-note' }, note) : null), body, table);
  // Build after insertion so the width is known.
  queueMicrotask(() => {
    requestAnimationFrame(() => {
      if (!block.isConnected) return;
      const c = lineChart(body, spec);
      ctx?.onDestroy(() => c.destroy());
    });
  });
  return block;
}
