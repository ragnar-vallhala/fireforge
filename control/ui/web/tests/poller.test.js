import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createPoller } from '../lib/poller.js';

// Fake document + timers: time only moves when the test says so.
function env() {
  const listeners = new Set();
  const doc = {
    hidden: false,
    addEventListener: (t, fn) => { if (t === 'visibilitychange') listeners.add(fn); },
    removeEventListener: (t, fn) => { if (t === 'visibilitychange') listeners.delete(fn); },
    setHidden(h) { this.hidden = h; for (const fn of listeners) fn(); },
    listeners,
  };
  let next = 1;
  const timers = new Map();
  return {
    doc,
    timers,
    setTimer: (cb, ms) => { const id = next++; timers.set(id, { cb, ms }); return id; },
    clearTimer: (id) => timers.delete(id),
    async fire() {   // run every pending timer once
      const due = [...timers.entries()];
      timers.clear();
      for (const [, t] of due) await t.cb();
      await flush();
    },
  };
}
const flush = () => new Promise((r) => setImmediate(r));

test('polls immediately, then on each interval', async () => {
  const e = env();
  let calls = 0;
  const p = createPoller(() => { calls++; }, { intervalMs: 5000, doc: e.doc, setTimer: e.setTimer, clearTimer: e.clearTimer }).start();
  await flush();
  assert.equal(calls, 1);
  assert.equal(e.timers.size, 1);
  assert.equal([...e.timers.values()][0].ms, 5000);
  await e.fire();
  assert.equal(calls, 2);
  await e.fire();
  assert.equal(calls, 3);
  p.stop();
});

test('stops while hidden and resumes on visible', async () => {
  const e = env();
  let calls = 0;
  const p = createPoller(() => { calls++; }, { doc: e.doc, setTimer: e.setTimer, clearTimer: e.clearTimer }).start();
  await flush();
  e.doc.setHidden(true);
  assert.equal(e.timers.size, 0, 'pending timer cleared when hidden');
  await e.fire();
  assert.equal(calls, 1, 'nothing runs while hidden');
  e.doc.setHidden(false);
  await flush();
  assert.equal(calls, 2, 'polls right away when visible again');
  assert.equal(e.timers.size, 1);
  p.stop();
});

test('does not start while hidden', async () => {
  const e = env();
  e.doc.hidden = true;
  let calls = 0;
  const p = createPoller(() => { calls++; }, { doc: e.doc, setTimer: e.setTimer, clearTimer: e.clearTimer }).start();
  await flush();
  assert.equal(calls, 0);
  assert.equal(e.timers.size, 0);
  p.stop();
});

test('stop clears timers and listeners', async () => {
  const e = env();
  let calls = 0;
  const p = createPoller(() => { calls++; }, { doc: e.doc, setTimer: e.setTimer, clearTimer: e.clearTimer }).start();
  await flush();
  p.stop();
  assert.equal(e.timers.size, 0);
  assert.equal(e.doc.listeners.size, 0);
  e.doc.setHidden(false);
  await flush();
  assert.equal(calls, 1);
});

test('returning false finishes polling; refresh restarts it', async () => {
  const e = env();
  let calls = 0;
  const p = createPoller(() => { calls++; return calls >= 2 ? false : undefined; },
    { doc: e.doc, setTimer: e.setTimer, clearTimer: e.clearTimer }).start();
  await flush();
  await e.fire();
  assert.equal(calls, 2);
  assert.equal(e.timers.size, 0);
  p.refresh();
  await flush();
  assert.equal(calls, 3);
  p.stop();
});

test('errors are reported and polling continues; one call at a time', async () => {
  const e = env();
  const errors = [];
  let release;
  let calls = 0;
  const p = createPoller(async () => {
    calls++;
    if (calls === 1) throw new Error('boom');
    await new Promise((r) => { release = r; });
  }, { doc: e.doc, setTimer: e.setTimer, clearTimer: e.clearTimer, onError: (x) => errors.push(x.message) }).start();
  await flush();
  assert.deepEqual(errors, ['boom']);
  assert.equal(e.timers.size, 1);
  const pending = e.fire();   // second call hangs until released
  await flush();
  assert.equal(calls, 2);
  e.doc.setHidden(true); e.doc.setHidden(false);   // visibility flip must not start a parallel call
  await flush();
  assert.equal(calls, 2);
  release();
  await pending;
  await flush();
  assert.equal(e.timers.size, 1);
  p.stop();
});
