// api/firms/daily.csv.ts
import { poolRead } from "../_lib/db_read";
import { badRequest, serverError } from "../_lib/respond";
import { getDate, getList } from "../_lib/parse";
import { requireAuth } from "../_lib/auth";

// Garante execução em Node (evita edge frio)
export const config = { runtime: "nodejs" };

// Limita a janela para proteger o backend (ajuste se quiser)
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

type RowMV = {
  dt: string;              // date (YYYY-MM-DD)
  focos: number | null;
  brilho_medio: number | null;
  brilho_p95: number | null;
  frp_medio: number | null;
};

export default async function handler(req: Request): Promise<Response> {
  const t0 = Date.now();
  try {
    // 🔐 Autenticação (usa x-api-key conforme seu _lib/auth)
    requireAuth(req);

    // 📅 Datas (default: 90 dias atrás até hoje UTC)
    const q = new URL(req.url).searchParams;
    const fromQ = getDate(q, "from", 90);
    if (!fromQ) return badRequest("Parâmetro 'from' inválido");
    const toQ = getDate(q, "to") ?? new Date(new Date().toISOString().slice(0, 10));
    const { from, to } = clampWindow(fromQ, toQ);

    // 📍 Filtro opcional por município(s)
    const cods = getList(q, "cod_ibge"); // ex.: ?cod_ibge=3550308,3509502

    // 🎯 Consulta *sempre* na MV — sem ORDER BY (o BI ordena depois)
    // Agregação diária (soma/medias sobre os municípios selecionados, se houver filtro)
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
    const { rows } = await poolRead.query<RowMV>(sql, params);
    const sqlMs = Date.now() - tSql0;

    // 📄 CSV rápido em memória (sem libs/streams)
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

    // 🧠 Telemetria + Cache CDN (2ª chamada deve vir com x-vercel-cache: HIT)
    const hdrs = new Headers();
    hdrs.set("Content-Type", "text/csv; charset=utf-8");
    hdrs.set("Cache-Control", "s-maxage=900, stale-while-revalidate=3600"); // 15min + SWR 1h
    hdrs.set("x-rows", String(rows.length));
    hdrs.set("x-sql-ms", String(sqlMs));
    hdrs.set("x-csv-ms", String(csvMs));

    return new Response(csv, { status: 200, headers: hdrs });
  } catch (e: any) {
    if (e instanceof Response) return e; // badRequest/requireAuth podem lançar Response
    return serverError(e);
  } finally {
    const totalMs = Date.now() - t0;
    console.log(`[firms/daily.csv] total=${totalMs}ms`);
  }
}
