// API client: same-origin fetch of /api/v1/*, proto3 JSON. Mutations send
// Content-Type: application/json, a JSON body and X-FireForge-Client: ui (the
// server's browser CSRF guard, phase9.md §3).

import { errorOf } from './proto.js';

// The UI lives at /ui/; the API is at the origin root.
const base = () => `${location.origin}/api/v1`;

export class ApiError extends Error {
  constructor(status, code, message, method = 'GET') {
    super(message || code);
    this.status = status;
    this.code = code;
    this.method = method;
  }
}

function url(path, query) {
  const u = new URL(base() + path);
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && v !== '') u.searchParams.set(k, String(v));
  }
  return u.toString();
}

async function request(method, path, { query, body } = {}) {
  const headers = { Accept: 'application/json', 'X-FireForge-Client': 'ui' };
  const init = { method, headers, credentials: 'same-origin', cache: 'no-store' };
  if (method !== 'GET' && method !== 'HEAD') {
    headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body ?? {});
  }
  let res;
  try {
    res = await fetch(url(path, query), init);
  } catch (e) {
    throw new ApiError(0, 'network', `Can't reach the control plane (${e.message || 'network error'}).`, method);
  }
  const text = await res.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch { data = text; }
  }
  if (!res.ok) {
    const e = errorOf(data, res.status);
    throw new ApiError(res.status, e.code, e.message || res.statusText, method);
  }
  return data ?? {};
}

export const api = {
  get: (path, query) => request('GET', path, { query }),
  post: (path, body) => request('POST', path, { body }),
  patch: (path, body) => request('PATCH', path, { body }),
};

/** Follow `next` / ?after= pages of a list endpoint (calculations) up to `max` items. */
export async function getAll(path, query, listKey, max = 5000) {
  const out = [];
  let after = '';
  for (let i = 0; i < 50; i++) {
    const page = await api.get(path, { ...query, after: after || undefined });
    out.push(...(page[listKey] || []));
    after = page.next || '';
    if (!after || out.length >= max) break;
  }
  return out;
}

/** Human explanation of an API error for banners. */
export function explain(err, role) {
  if (!(err instanceof ApiError)) return { title: 'Something went wrong', detail: String(err?.message || err) };
  if (err.status === 403) {
    return {
      title: err.code === 'csrf' ? 'Request blocked as cross-site' : `Your certificate's role${role ? ` (${role})` : ''} can't ${err.method === 'GET' ? 'view this' : 'do this'}`,
      detail: err.message,
    };
  }
  if (err.status === 401) return { title: 'No client certificate', detail: 'Import your FireForge .p12 certificate into the browser and reload.' };
  if (err.status === 404) return { title: 'Not found', detail: err.message };
  if (err.status === 0) return { title: 'Control plane unreachable', detail: err.message };
  return { title: `Request failed (${err.status} ${err.code})`, detail: err.message };
}
