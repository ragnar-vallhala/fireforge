// Formatting helpers (pure: no DOM). Values from the API may be proto3 JSON int64
// strings, numbers, or missing (defaults are omitted).

import { toNum } from './proto.js';

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];

/** 32212254720 → "30.0 GiB". Missing → "—". */
export function bytes(v, digits = 1) {
  if (v === undefined || v === null || v === '') return '—';
  let n = toNum(v);
  if (!Number.isFinite(n)) return '—';
  if (n < 1024) return `${n} B`;
  let i = 0;
  while (n >= 1024 && i < BYTE_UNITS.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(n >= 100 ? 0 : digits)} ${BYTE_UNITS[i]}`;
}

/** Milliseconds → "2h 05m", "3m 12s", "850 ms". */
export function duration(ms) {
  if (ms === undefined || ms === null) return '—';
  const n = toNum(ms);
  if (!Number.isFinite(n)) return '—';
  if (n < 0) return '—';
  if (n < 1000) return `${Math.round(n)} ms`;
  const s = Math.floor(n / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, '0')}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${String(m % 60).padStart(2, '0')}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${String(h % 24).padStart(2, '0')}h`;
}

/** Unix ms → "3m ago" / "in 2m" relative to now. */
export function ago(unixMs, now = Date.now()) {
  const t = toNum(unixMs);
  if (!t) return '—';
  const d = now - t;
  if (Math.abs(d) < 5000) return 'just now';
  return d >= 0 ? `${duration(d)} ago` : `in ${duration(-d)}`;
}

function pad(n) { return String(n).padStart(2, '0'); }

/** Unix ms → "2026-11-06 14:03:22" in local time (or UTC with utc=true). */
export function timestamp(unixMs, { utc = false } = {}) {
  const t = toNum(unixMs);
  if (!t) return '—';
  const d = new Date(t);
  const g = utc
    ? [d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()]
    : [d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds()];
  return `${g[0]}-${pad(g[1])}-${pad(g[2])} ${pad(g[3])}:${pad(g[4])}:${pad(g[5])}`;
}

/**
 * A scientific number for tables: 3 significant digits, scientific notation outside
 * [1e-3, 1e5), "—" for missing. Integers stay integers when small.
 */
export function num(v, sig = 3) {
  if (v === undefined || v === null || v === '') return '—';
  if (typeof v === 'boolean') return v ? 'yes' : 'no';
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) return typeof v === 'string' ? v : '—';
  if (n === 0) return '0';
  const a = Math.abs(n);
  if (Number.isInteger(n) && a < 1e5) return String(n);
  if (a >= 1e5 || a < 1e-3) {
    const [m, e] = n.toExponential(sig - 1).split('e');
    return `${m}e${Number(e)}`;
  }
  return String(Number(n.toPrecision(sig)));
}

/** Fixed decimals, "—" when missing. */
export function fixed(v, digits = 2) {
  if (v === undefined || v === null || v === '') return '—';
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(digits) : '—';
}

/** 0.925 → "93 %". */
export function percent(fraction, digits = 0) {
  if (fraction === undefined || fraction === null) return '—';
  const n = Number(fraction);
  return Number.isFinite(n) ? `${(n * 100).toFixed(digits)}%` : '—';
}

/** Signed with explicit +. */
export function signed(v, digits = 3) {
  if (v === undefined || v === null) return '—';
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return (n > 0 ? '+' : n < 0 ? '−' : '') + Math.abs(n).toFixed(digits);
}

/** "job-3d63a3ddbfafc942" → "job-3d63a3dd". Keeps the prefix, shortens the hex. */
export function shortId(id, keep = 8) {
  if (!id) return '';
  const m = /^([a-z-]+?-)([0-9a-f]{12,})$/.exec(id);
  if (m) return m[1] + m[2].slice(0, keep);
  return id;
}

/** "artifact://sha256/<64 hex>" → "sha256:abcd1234…". */
export function shortArtifact(id) {
  if (!id) return '';
  const m = /^artifact:\/\/sha256\/([0-9a-f]{64})$/.exec(id);
  return m ? `sha256:${m[1].slice(0, 12)}…` : id;
}

// ---- states --------------------------------------------------------------------

// Job / step / attempt / workflow / campaign / node states → a tone used for colour
// (CSS class `tone-<tone>`) and a glyph so state never relies on colour alone.
const TONES = {
  // jobs and steps
  CREATED: 'idle', QUEUED: 'idle', WAITING: 'idle', HELD: 'idle', READY: 'idle',
  LEASED: 'pending', ASSIGNED: 'pending', COMMITTING: 'active', RUNNING: 'active',
  RETRY_PENDING: 'warn', COMPLETED: 'good', SUCCEEDED: 'good',
  FAILED: 'bad', EXPIRED: 'bad', VALIDATION_FAILED: 'bad', ABANDONED: 'bad',
  CANCELLED: 'off', SKIPPED: 'off',
  // nodes
  ONLINE: 'good', STALE: 'warn', OFFLINE: 'off',
  // campaigns
  DRAFT: 'idle', ACTIVE: 'active', PAUSED: 'warn', ARCHIVED: 'off',
  // checks
  PASS: 'good', FAIL: 'bad', NO_DATA: 'off', INFO: 'idle',
};

const GLYPHS = { idle: '○', pending: '◔', active: '●', warn: '▲', good: '✓', bad: '✕', off: '–' };

export function stateTone(state) {
  return TONES[String(state || '').toUpperCase()] || 'idle';
}
export function stateGlyph(state) { return GLYPHS[stateTone(state)]; }

/** "RETRY_PENDING" → "retry pending". */
export function stateLabel(state) {
  if (!state) return 'unknown';
  return String(state).toLowerCase().replace(/_/g, ' ');
}

export const TERMINAL_JOB_STATES = new Set(['COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED', 'VALIDATION_FAILED']);
export const JOB_STATES = ['CREATED', 'QUEUED', 'LEASED', 'ASSIGNED', 'RUNNING', 'COMMITTING', 'COMPLETED',
  'FAILED', 'RETRY_PENDING', 'CANCELLED', 'EXPIRED', 'VALIDATION_FAILED'];

// Display order for step/job state counts (progress bars): done first, then active, then waiting.
export const STATE_ORDER = ['COMPLETED', 'SUCCEEDED', 'COMMITTING', 'RUNNING', 'ASSIGNED', 'LEASED', 'RETRY_PENDING',
  'QUEUED', 'CREATED', 'WAITING', 'FAILED', 'EXPIRED', 'VALIDATION_FAILED', 'CANCELLED', 'SKIPPED'];

export function orderedCounts(counts) {
  const out = [];
  const seen = new Set();
  for (const s of STATE_ORDER) {
    const n = toNum(counts?.[s]);
    if (n > 0) out.push([s, n]);
    seen.add(s);
  }
  for (const [s, v] of Object.entries(counts || {})) {
    if (!seen.has(s) && toNum(v) > 0) out.push([s, toNum(v)]);
  }
  return out;
}

/** "admin:alice" → {role:'admin', name:'alice'}. */
export function actor(s) {
  const i = (s || '').indexOf(':');
  return i < 0 ? { role: '', name: s || '' } : { role: s.slice(0, i), name: s.slice(i + 1) };
}
