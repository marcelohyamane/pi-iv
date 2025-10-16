import { poolRead } from "../_lib/db_read";
import { okCSV, badRequest, serverError } from "../_lib/respond";
import { getDate, getList } from "../_lib/parse";

export const config = { runtime: "nodejs" }; // usa 'pg'

export default async function handler(req: Request) {
  try {
    const q = new URL(req.url).searchParams;
    const from = getDate(q, "from", 365);
    if (!from) return badRequest("Parâmetro 'from' inválido");
    const to   = getDate(q, "to");
    const cods = getList(q, "cod_ibge");
    const conf = getList(q, "confidence");

    const params: any[] = [from];
    let where = `acq_datetime_utc >= $1`;
    if (to)               { params.push(to);   where += ` AND acq_datetime_utc < $${params.length}`; }
    if (cods.length)      { params.push(cods); where += ` AND cod_ibge = ANY($${params.length})`; }
    if (conf.length)      { params.push(conf); where += ` AND confidence = ANY($${params.length})`; }

    const sql = `
      SELECT
        date(acq_datetime_utc) AS dt,
        COUNT(*)               AS focos,
        AVG(brightness)::float AS brilho_medio,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY brightness) AS brilho_p95,
        AVG(frp)::float       AS frp_medio
      FROM public.firms_focos
      WHERE ${where}
      GROUP BY 1
      ORDER BY 1;
    `;

    const { rows } = await poolRead.query(sql, params);
    return okCSV(rows, "firms_daily.csv", 60);
  } catch (e) {
    return serverError(e);
  }
}
