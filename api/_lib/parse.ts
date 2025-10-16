export function getDate(q: URLSearchParams, key: string, fallbackDays?: number): Date | undefined {
  const raw = q.get(key);
  if (raw) {
    const d = new Date(raw);
    if (!isNaN(d.getTime())) return d;
  }
  if (fallbackDays !== undefined) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - fallbackDays);
    return d;
  }
}

export function getList(q: URLSearchParams, key: string) {
  const v = q.get(key);
  return v ? v.split(",").map(s => s.trim()).filter(Boolean) : [];
}

export function getInt(q: URLSearchParams, key: string, def: number, max?: number) {
  const n = parseInt(q.get(key) ?? "", 10);
  if (isNaN(n)) return def;
  return Math.min(max ?? n, n);
}
