// api/firms/daily.csv.ts
import type { VercelRequest, VercelResponse } from '@vercel/node';
import { poolRead } from '../_lib/db_read'; // usa o pool central

// helper pra ler o host usado pelo Pool (apenas da env)
function maskConn(u?: string) {
  if (!u) return "n/a";
  try {
    const x = new URL(u);
    return `${x.host}${x.port ? ":" + x.port : ""}`;
  } catch { return "parse_error"; }
}
const DB_USED = maskConn(process.env.DATABASE_URL_READONLY);


// ===== Utils locais =====
function clampWindow(from: Date, to: Date): { from: Date; to: Date } {
  const MAX_DAYS = 90;
  const msDay = 24 * 60 * 60 * 1000;
  const span = Math.ceil((to.getTime() - from.getTime()) / msDay);
  if (span > MAX_DAYS) return { from: new Date(to.getTime() - MAX_DAYS * msDay), to };
  return { from, to };
}
function parseDate(s?: string | string[] | null): Date | null {
  if (!s) return null;
  const v = Array.isArray(s) ? s[0] : s;
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(+d) ? null : d;
}
function parseCods(s?: string | string[] | null): string[] {
  if (!s) return [];
  const v = Array.isArray(s) ? s[0] : s;
  return v.split(',').map(x => x.trim()).filter(Boolean);
}
function unauthorized(res: VercelResponse) {
  res.status(401).send('unauthorized');
}
function badRequest(res: VercelResponse, msg: string) {
  res.status(400).send(msg);
}

// ===== Handler Node (finaliza com res.send) =====
export default async function handler(req: VercelRequest, res: VercelResponse) {
  const t0 = Date.now();
  try {
    // 🔐 Auth simples por header
    const apiKey = req.headers['x-api-key'];
    const expected = process.env.API_KEY;
    const got = Array.isArray(apiKey) ? apiKey[0] : apiKey;
    if (!expected || got !== expected) return unauthorized(res);

    // 📅 Datas (default: últimos 90 dias até hoje UTC)
    const fromQ = parseDate(req.query.from);
    const toQ = parseDate(req.query.to) ?? new Date(new Date().toISOString().slice(0, 10));
    const fromRaw = fromQ ?? new Date(toQ.getTime() - 90 * 24 * 60 * 60 * 1000);
    const { from, to } = clampWindow(fromRaw, toQ);

    // 📍 Filtro opcional por municípios (?cod_ibge=3550308,3509502)
    const cods = parseCods(req.query.cod_ibge);

    // 🎯 Consulta SEM ORDER BY; sempre na MV
    const params: any[] = [from, to];
    let where = `dt >= $1::date AND dt < $2::date`;
    if (cods.length) {
      params.push(cods);
      where += ` AND cod_ibge = ANY($${params.length})`;
    }

    const sql = `
      SELECT
        dt,
        SUM(focos)        AS focos,
        AVG(brilho_medio) AS brilho_medio,
        AVG(brilho_p95)   AS brilho_p95,
        AVG(frp_medio)    AS frp_medio
      FROM public.mv_firms_diario
      WHERE ${where}
      GROUP BY dt
    `;

    const tSql0 = Date.now();
    const result = await poolRead.query<{
      dt: string;
      focos: number | null;
      brilho_medio: number | null;
      brilho_p95: number | null;
      frp_medio: number | null;
    }>(sql, params);
    const sqlMs = Date.now() - tSql0;

    // 📄 Monta CSV em memória
    const tCsv0 = Date.now();
    const header = 'dt,focos,brilho_medio,brilho_p95,frp_medio';
    const lines = result.rows.map(r =>
      [
        r.dt,
        r.focos ?? 0,
        r.brilho_medio ?? '',
        r.brilho_p95 ?? '',
        r.frp_medio ?? ''
      ].join(',')
    );
    const csv = [header, ...lines].join('\n');
    const csvMs = Date.now() - tCsv0;

    // 🧠 Headers: cache + telemetria
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Cache-Control', 's-maxage=900, stale-while-revalidate=3600'); // 15min + SWR 1h
    res.setHeader('x-rows', String(result.rowCount ?? result.rows.length));
    res.setHeader('x-sql-ms', String(sqlMs));
    res.setHeader('x-csv-ms', String(csvMs));

   res.setHeader("x-db-host", DB_USED);
    // ✅ Finaliza a resposta
    res.status(200).send(csv);
  } catch (e: any) {
    console.error('[firms/daily.csv] error', e);
    res.status(500).send('internal error');
  } finally {
    const totalMs = Date.now() - t0;
    console.log(`[firms/daily.csv] total=${totalMs}ms`);
  }
}
