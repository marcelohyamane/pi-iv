// api/firms/daily.csv.ts
import { poolRead } from "../_lib/db_read";
import { badRequest, serverError } from "../_lib/respond";
import { getDate, getList } from "../_lib/parse";
import { requireAuth } from "../_lib/auth";

// Garantir Node.js (evita edge frio)
export const config = { runtime: "nodejs" };

// Mantém a janela máxima para proteger o backend
function clampWindow(from: Date, to: Date): { from: Date; to: Date } {
  const MAX_DAYS = 90;
  const msDay = 24 * 60 * 60 * 1000;
  const span = Math.ceil((to.getTime() - from.getTime()) / msDay);
  if (span > MAX_DAYS) {
    const nf = new Date(to.getTime() - MAX_DAYS * msDay);
    return { from: nf, to };
  }
  return { from, to };
}

export default async function handler(req: Request): Promise<Response> {
  const t0 = Date.now();
  try {
    // 🔐 auth
    requireAuth(req);

    // 📅 datas
    const q = new URL(req.url).searchParams;
    const fromQ = getDate(q, "from", 90);
    if (!fromQ) return badRequest("Parâmetro 'from' inválido");
    const toQ = getDate(q, "to") ?? new Date(new Date().toISOString().slice(0, 10));
    const { from, to } = clampWindow(fromQ, toQ);

    // 📍 filtro opcional por município(s)
    const cods = getList(q, "cod_ibge");

    // ⚠️ Removemos o caminho "include_p95" que lia a TABELA bruta com PERCENTILE_CONT.
    // Isso era pesado e desnecessário porque a MV já traz brilho_p95 agregado.
    // A rota passa a ler *sempre* da MV.
    //
    // 🎯 Importante: SEM ORDER BY (o BI ordena depois se precisar)
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
    const { rows } = await poolRead.query(sql, params);
    const sqlMs = Date.now() - tSql0;

    // 📄 CSV rápido (sem libs)
    const tCsv0 = Date.now();
    const header = "dt,focos,brilho_medio,brilho_p95,frp_medio";
    const lines = rows.map(r =>
      [
        r.dt,
        r.focos ?? 0,
        r.brilho_medio ?? "",
        r.brilho_p95 ?? "",
        r.frp_medio ?? ""
      ].join(",")
    );
    const csv = [header, ...lines].join("\n");
    const csvMs = Date.now() - tCsv0;

    // 🧠 Telemetria + Cache CDN
    const hdrs = new Headers();
    hdrs.set("Content-Type", "text/csv; charset=utf-8");
    // Cache 15 min na edge + SWR 1h
    hdrs.set("Cache-Control", "s-maxage=900, stale-while-revalidate=3600");
    hdrs.set("x-rows", String(rows.length));
    hdrs.set("x-sql-ms", String(sqlMs));
    hdrs.set("x-csv-ms", String(csvMs));

    return new Response(csv, { status: 200, headers: hdrs });
  } catch (e: any) {
    if (e instanceof Response) return e;
    return serverError(e);
  } finally {
    const totalMs = Date.now() - t0;
    console.log(`[daily.csv] total=${totalMs}ms`);
  }
}



