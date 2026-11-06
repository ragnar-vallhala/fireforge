import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

globalThis.location = { origin: 'https://cp.example:8443' };
const calls = [];
let responder = () => ({ status: 200, body: '{}' });
globalThis.fetch = async (url, init) => {
  calls.push({ url, init });
  const r = responder(url, init);
  return { ok: r.status >= 200 && r.status < 300, status: r.status, statusText: 'x', text: async () => r.body };
};
const { api, getAll, explain, ApiError } = await import('../lib/api.js');

beforeEach(() => { calls.length = 0; responder = () => ({ status: 200, body: '{}' }); });

test('GET builds an absolute /api/v1 URL, drops empty query values, sends no body', async () => {
  responder = () => ({ status: 200, body: '{"jobs":[{"job_id":"job-1","attempts_used":"2"}]}' });
  const r = await api.get('/jobs', { state: 'RUNNING', campaign: '', limit: 10, x: undefined });
  assert.equal(calls[0].url, 'https://cp.example:8443/api/v1/jobs?state=RUNNING&limit=10');
  assert.equal(calls[0].init.method, 'GET');
  assert.equal(calls[0].init.body, undefined);
  assert.equal(calls[0].init.credentials, 'same-origin');
  assert.equal(calls[0].init.headers['X-FireForge-Client'], 'ui');
  assert.equal(calls[0].init.headers['Content-Type'], undefined);
  assert.equal(r.jobs[0].attempts_used, '2');
});

test('mutations send JSON content type, the client header and {} when empty', async () => {
  await api.post('/jobs/job-1/cancel');
  await api.patch('/campaigns/c1', { state: 'PAUSED' });
  assert.equal(calls[0].init.method, 'POST');
  assert.equal(calls[0].init.body, '{}');
  assert.equal(calls[0].init.headers['Content-Type'], 'application/json');
  assert.equal(calls[0].init.headers['X-FireForge-Client'], 'ui');
  assert.equal(calls[1].init.method, 'PATCH');
  assert.deepEqual(JSON.parse(calls[1].init.body), { state: 'PAUSED' });
});

test('an empty 200 body becomes {}', async () => {
  responder = () => ({ status: 200, body: '' });
  assert.deepEqual(await api.post('/nodes/n/offline', { reason: 'x' }), {});
});

test('errors carry status, code, message and method', async () => {
  responder = () => ({ status: 403, body: '{"code":"forbidden","message":"viewer may not"}' });
  await assert.rejects(api.get('/audit'), (e) => e instanceof ApiError && e.status === 403 && e.code === 'forbidden'
    && e.message === 'viewer may not' && e.method === 'GET');
  responder = () => ({ status: 500, body: 'plain text' });
  await assert.rejects(api.post('/x'), (e) => e.status === 500 && e.method === 'POST');
});

test('network failures become status 0', async () => {
  const saved = globalThis.fetch;
  globalThis.fetch = async () => { throw new TypeError('Failed to fetch'); };
  try { await assert.rejects(api.get('/nodes'), (e) => e.status === 0 && e.code === 'network'); } finally { globalThis.fetch = saved; }
});

test('getAll follows next tokens', async () => {
  responder = (url) => {
    const after = new URL(url).searchParams.get('after');
    if (!after) return { status: 200, body: '{"calculations":[{"calc_id":"a"},{"calc_id":"b"}],"next":"b"}' };
    return { status: 200, body: '{"calculations":[{"calc_id":"c"}]}' };
  };
  const all = await getAll('/calculations', { kind: 'te.evaluate' }, 'calculations');
  assert.deepEqual(all.map((c) => c.calc_id), ['a', 'b', 'c']);
  assert.match(calls[1].url, /after=b/);
  assert.match(calls[1].url, /kind=te\.evaluate/);
});

test('explain wording: view vs act, csrf, 401, 404', () => {
  assert.match(explain(new ApiError(403, 'forbidden', 'm', 'GET'), 'viewer').title, /role \(viewer\) can't view this/);
  assert.match(explain(new ApiError(403, 'forbidden', 'm', 'POST'), 'viewer').title, /can't do this/);
  assert.match(explain(new ApiError(403, 'csrf', 'm', 'POST')).title, /cross-site/);
  assert.match(explain(new ApiError(401, 'unauthenticated', 'm')).title, /certificate/);
  assert.equal(explain(new ApiError(404, 'not_found', 'gone')).detail, 'gone');
  assert.equal(explain(new Error('boom')).detail, 'boom');
});
