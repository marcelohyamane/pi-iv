export function okJSON(data: unknown, ttlSeconds = 60) {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": `public, s-maxage=${ttlSeconds}, stale-while-revalidate=${ttlSeconds * 10}`
    }
  });
}

export function okCSV(rows: Array<Record<string, any>>, filename = "export.csv", ttlSeconds = 60) {
  const headers = {
    "content-type": "text/csv; charset=utf-8",
    "cache-control": `public, s-maxage=${ttlSeconds}, stale-while-revalidate=${ttlSeconds * 10}`,
    "content-disposition": `inline; filename="${filename}"`
  };

  if (!rows?.length) return new Response("", { status: 200, headers });

  const cols = Object.keys(rows[0]);
  const esc = (v: any) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n;]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };

  const body = [cols.join(","), ...rows.map(r => cols.map(c => esc(r[c])).join(","))].join("\n");
  return new Response(body, { status: 200, headers });
}

export function badRequest(msg: string) {
  return new Response(JSON.stringify({ error: msg }), {
    status: 400,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}

export function serverError(e: unknown) {
  const msg = e instanceof Error ? e.message : "internal error";
  return new Response(JSON.stringify({ error: msg }), {
    status: 500,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}

