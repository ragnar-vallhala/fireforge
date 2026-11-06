// Hash routing (pure: no DOM). "#/materials/mat-1?formula=Zr" →
// {page:'materials', id:'mat-1', rest:[], query:{formula:'Zr'}}.

export const PAGES = ['overview', 'nodes', 'jobs', 'workflows', 'campaigns', 'materials', 'calculations', 'results', 'audit'];

export function parseHash(hash) {
  let h = String(hash || '');
  if (h.startsWith('#')) h = h.slice(1);
  let qs = '';
  const qi = h.indexOf('?');
  if (qi >= 0) { qs = h.slice(qi + 1); h = h.slice(0, qi); }
  const segs = h.split('/').filter(Boolean).map((s) => {
    try { return decodeURIComponent(s); } catch { return s; }
  });
  const query = {};
  for (const part of qs.split('&')) {
    if (!part) continue;
    const eq = part.indexOf('=');
    const k = eq < 0 ? part : part.slice(0, eq);
    const v = eq < 0 ? '' : part.slice(eq + 1);
    try { query[decodeURIComponent(k.replace(/\+/g, ' '))] = decodeURIComponent(v.replace(/\+/g, ' ')); } catch { /* skip */ }
  }
  const page = segs.length ? segs[0] : 'overview';
  const known = PAGES.includes(page);
  return {
    page: known ? page : 'notfound',
    id: known ? (segs[1] || '') : '',
    rest: known ? segs.slice(2) : segs,
    query,
  };
}

/** Build "#/page/id?k=v" (empty query values are dropped). */
export function buildHash(page, id = '', query = {}) {
  let h = `#/${page === 'overview' ? '' : encodeURIComponent(page)}`;
  if (id) h += `/${encodeURIComponent(id)}`;
  const q = Object.entries(query || {})
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  return q ? `${h}?${q}` : h;
}

/** Link to the page that shows an id, by its prefix ("" when not linkable). */
export function hrefForId(id) {
  if (!id || typeof id !== 'string') return '';
  const map = [['job-', 'jobs'], ['wf-', 'workflows'], ['camp-', 'campaigns'], ['mat-', 'materials'],
    ['calc-', 'calculations'], ['ff-node-', 'nodes']];
  for (const [p, page] of map) if (id.startsWith(p)) return buildHash(page, id);
  return '';
}
