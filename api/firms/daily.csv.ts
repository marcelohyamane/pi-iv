// api/firms/daily.csv.ts
import { poolRead } from "../_lib/db_read";
import { okCSV, badRequest, serverError } from "../_lib/respond";
import { getDate, getList } from "../_lib/parse";
import { requireAuth } from "../_lib/auth";

export const config = { runtime: "nodejs" };

export default async function handler(req: Request) {
  try {
    requireAuth(req);

    const q = new URL(req.url).searchParams;

    // período padrão: últimos 365 dias
    const from = getDate(q, "from", 365);
    if (!from) return badRequest("Parâmetro 'from' inválido");
    const to = getDate(q, "to") ?? new Date(new Date().toISOString().slice(0, 10)); // hoje (UTC) sem hora
    const cods = getList(q, "cod_ibge"); // opcional: lista de IBGEs

    // ---- 1) Tenta ler a VIEW agregada (rápido) ----
    const paramsView: any[] = [from, to];
    let whereView = `dt >= $1 AND dt < $2`;
    if (cods.length) {
      paramsView.push(cods);
      whereView += ` AND cod_ibge = ANY($${paramsView.length})`;
    }

    const sqlFromView = `
      SELECT
        dt,
        SUM(focos)        AS focos,
        AVG(brilho_medio) AS brilho_medio,
        AVG(brilho_p95)   AS brilho_p95,
        AVG(frp_medio)    AS frp_medio
      FROM public.vw_firms_diario
      WHERE ${whereView}
      GROUP BY dt
      ORDER BY dt;
    `;

    try {
      const { rows } = await poolRead.query(sqlFromView, paramsView);
      return okCSV(rows, "firms_daily.csv", 60);
    } catch (e: any) {
      // Se a VIEW não existir (relation does not exist), cai pro fallback
      const msg = (e?.message || "").toLowerCase();
      const missingView =
        msg.includes("relation") && msg.includes("vw_firms_diario") && msg.includes("does not exist");

      if (!missingView) {
        // outro erro qualquer: propaga
        throw e;
      }
      // continua no fallback abaixo
    }

    // ---- 2) Fallback: agrupa direto na tabela (um pouco mais pesado) ----
    const paramsTbl: any[] = [from, to];
    let whereTbl = `acq_datetime_utc >= $1 AND acq_datetime_utc < $2`;
    if (cods.length) {
      paramsTbl.push(cods);
      whereTbl += ` AND cod_ibge = ANY($${paramsTbl.length})`;
    }

    const sqlFromTable = `
      SELECT
        date(acq_datetime_utc)                   AS dt,
        COUNT(*)                                 AS focos,
        AVG(brightness)::float                   AS brilho_medio,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY brightness) AS brilho_p95,
        AVG(frp)::float                          AS frp_medio
      FROM public.firms_focos
      WHERE ${whereTbl}
      GROUP BY 1
      ORDER BY 1;
    `;

    const { rows } = await poolRead.query(sqlFromTable, paramsTbl);
    return okCSV(rows, "firms_daily.csv", 60);
  } catch (e) {
    if (e instanceof Response) return e;
    return serverError(e);
  }
}


