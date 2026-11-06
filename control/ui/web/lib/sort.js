// Sorting (pure). Missing values (undefined/null/NaN/"") always sort last, whatever
// the direction, so a sorted column never starts with a wall of dashes.

function missing(v) {
  return v === undefined || v === null || v === '' || (typeof v === 'number' && Number.isNaN(v));
}

export function compareValues(a, b) {
  if (typeof a === 'boolean') a = a ? 1 : 0;
  if (typeof b === 'boolean') b = b ? 1 : 0;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' });
}

/** Stable sort of rows by key(row); dir 'asc' | 'desc'. Returns a new array. */
export function sortRows(rows, key, dir = 'asc') {
  const sign = dir === 'desc' ? -1 : 1;
  return rows
    .map((r, i) => [r, key(r), i])
    .sort((x, y) => {
      const mx = missing(x[1]); const my = missing(y[1]);
      if (mx || my) return mx === my ? x[2] - y[2] : mx ? 1 : -1;
      return sign * compareValues(x[1], y[1]) || x[2] - y[2];
    })
    .map((x) => x[0]);
}
