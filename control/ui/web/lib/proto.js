// Proto3 JSON helpers (pure: no DOM). The API sends proto field names, int64 as
// strings, enums as names, and omits every field that has its default value.

/** int64 string / number / missing → number (0 when missing, NaN when garbage). */
export function toNum(v) {
  if (v === undefined || v === null || v === '') return 0;
  if (typeof v === 'number') return v;
  if (typeof v === 'boolean') return v ? 1 : 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}

/** Parse a *_json field; "" / missing / invalid → fallback ({} by default). */
export function parseJson(s, fallback = {}) {
  if (s === undefined || s === null || s === '') return fallback;
  if (typeof s === 'object') return s;
  try {
    const v = JSON.parse(s);
    return v === null ? fallback : v;
  } catch {
    return fallback;
  }
}

/** Enum default: proto3 JSON omits the zero value. */
export function enumOr(v, zeroName) { return v === undefined || v === null || v === '' ? zeroName : v; }

/**
 * A Calculation with its JSON strings parsed and defaults filled in:
 * `props`, `conds`, `settings`, `conv`, `unc`, `evidence` ("PREDICTED" when omitted),
 * `created` (number), `engine`.
 */
export function calcView(c) {
  if (!c || c.__view) return c;
  const method = c.method || {};
  return {
    ...c,
    __view: true,
    method,
    evidence: enumOr(c.evidence, 'PREDICTED'),
    props: parseJson(c.properties_json),
    conds: parseJson(c.conditions_json),
    settings: parseJson(method.settings_json),
    conv: parseJson(c.convergence_json),
    unc: parseJson(c.uncertainty_json),
    created: toNum(c.created_unix_ms),
    engine: method.engine || '',
    outputs: c.outputs || [],
    input_artifacts: c.input_artifacts || [],
  };
}

/** Newest calculation per kind (input in any order). Returns Map kind → calc. */
export function latestByKind(calcs) {
  const m = new Map();
  for (const raw of calcs || []) {
    const c = calcView(raw);
    const cur = m.get(c.kind);
    if (!cur || c.created > cur.created || (c.created === cur.created && c.calc_id > cur.calc_id)) m.set(c.kind, c);
  }
  return m;
}

/** Node resources with defaults → numbers. */
export function resources(r) {
  r = r || {};
  return {
    cpu_cores: toNum(r.cpu_cores),
    memory_bytes: toNum(r.memory_bytes),
    disk_bytes: toNum(r.disk_bytes),
    gpu_count: toNum(r.gpu_count),
  };
}

/** API error body → {code, message}; accepts {code,message} and {error:{code,message}}. */
export function errorOf(body, status) {
  const e = body && typeof body === 'object' ? (body.error && typeof body.error === 'object' ? body.error : body) : null;
  const code = (e && typeof e.code === 'string' && e.code) || (status ? `http_${status}` : 'error');
  const message = (e && typeof e.message === 'string' && e.message) || (typeof body === 'string' ? body : '');
  return { code, message };
}

/** Workflow step counts map with numeric values. */
export function stepCounts(w) {
  const out = {};
  for (const [k, v] of Object.entries(w?.step_counts || {})) out[k] = toNum(v);
  return out;
}

export function sum(obj) { return Object.values(obj || {}).reduce((a, b) => a + toNum(b), 0); }
