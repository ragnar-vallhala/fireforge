// Poller (pure: the document and timers are injected, so it runs under node --test).
// Calls `fn` now and then every `intervalMs` while the document is visible; pauses
// while hidden, polls again right away when the tab becomes visible. One call at a
// time: the next tick is scheduled when the previous call has settled. `fn` may
// return (or resolve to) false to stop polling, e.g. once a workflow is finished.

export function createPoller(fn, {
  intervalMs = 5000,
  doc = globalThis.document,
  setTimer = (cb, ms) => globalThis.setTimeout(cb, ms),
  clearTimer = (t) => globalThis.clearTimeout(t),
  onError = () => {},
} = {}) {
  let timer = null;
  let running = false;   // a call is in flight
  let stopped = false;
  let finished = false;  // fn asked to stop

  const hidden = () => Boolean(doc && doc.hidden);

  function schedule() {
    if (stopped || finished || timer !== null || hidden()) return;
    timer = setTimer(tick, intervalMs);
  }

  async function tick() {
    timer = null;
    if (stopped || finished || running) return;
    if (hidden()) return;
    running = true;
    try {
      const r = await fn();
      if (r === false) finished = true;
    } catch (e) {
      onError(e);
    } finally {
      running = false;
    }
    schedule();
  }

  function onVisibility() {
    if (stopped || finished) return;
    if (hidden()) {
      if (timer !== null) { clearTimer(timer); timer = null; }
    } else if (timer === null && !running) {
      tick();
    }
  }

  if (doc && doc.addEventListener) doc.addEventListener('visibilitychange', onVisibility);

  const api = {
    start() { tick(); return api; },
    /** Run now (e.g. after an action) without waiting for the next tick. */
    refresh() {
      if (stopped) return;
      if (timer !== null) { clearTimer(timer); timer = null; }
      finished = false;
      tick();
    },
    stop() {
      stopped = true;
      if (timer !== null) { clearTimer(timer); timer = null; }
      if (doc && doc.removeEventListener) doc.removeEventListener('visibilitychange', onVisibility);
    },
    get active() { return timer !== null || running; },
  };
  return api;
}
