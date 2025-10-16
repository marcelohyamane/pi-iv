import { poolRead } from "../_lib/db_read";
import { okCSV, badRequest, serverError } from "../_lib/respond";
import { getDate, getList } from "../_lib/parse";
import { requireAuth } from "../_lib/auth";

export const config = { runtime: "nodejs" };

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

export default async function handler(req: Request) {
  try {
    requireAuth(req);

    const q = new URL(req.url).searchParams;
    const fromQ = getDate(q, "from", 90); // padrão 90d atrás
    if (!fromQ) return badRequest("Parâmetro 'from' inválido");
    const toQ = getDate(q, "to") ?? new Date(new Date().toISOString().slice(0, 10)); // hoje UTC
    const { from, to } = clampWindow(fromQ, toQ);

    const cods = getList(q, "cod_ibge");
    const includeP95 = (q.get("include_p95") ?? "false").toLowerCase() === "true";

    const params: any[] = [from, to];
    let where = `acq_datetime_utc >= $1 AND acq_datetime_utc < $2`;
    if (cods.length) { params.push(cods); where += ` AND cod_ibge = ANY($${params.length})`; }

    // Versão padrão (sem p95) – MUITO rápida
    let sql = `
SELECT
  dt,
  SUM(focos)        AS focos,
  AVG(brilho_medio) AS brilho_medio,
  AVG(brilho_p95)   AS brilho_p95,
  AVG(frp_medio)    AS frp_medio
FROM public.mv_firms_diario
WHERE dt >= $1 AND dt < $2
GROUP BY dt;
    `;

    // Se quiser p95 explicitamente
    if (includeP95) {
      sql = `
        SELECT
          date(acq_datetime_utc) AS dt,
          COUNT(*)               AS focos,
          AVG(brightness)::float AS brilho_medio,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY brightness) AS brilho_p95,
          AVG(frp)::float        AS frp_medio
        FROM public.firms_focos
        WHERE ${where}
        GROUP BY 1
        ORDER BY 1;
      `;
    }

    const { rows } = await poolRead.query(sql, params);
    return okCSV(rows, "firms_daily.csv", 60);
  } catch (e) {
    if (e instanceof Response) return e;
    return serverError(e);
  }
}


