import { poolRead } from "../_lib/db_read";
import { okJSON, badRequest, serverError } from "../_lib/respond";
import { getDate, getList, getInt } from "../_lib/parse";
import { requireAuth } from "../_lib/auth";

export const config = { runtime: "nodejs" };

export default async function handler(req: Request) {
  try {
    requireAuth(req);

    const q = new URL(req.url).searchParams;
    const from = getDate(q, "from", 7);
    if (!from) return badRequest("Parâmetro 'from' inválido");
    const to     = getDate(q, "to");
    const cods   = getList(q, "cod_ibge");
    const conf   = getList(q, "confidence");
    const limit  = getInt(q, "limit", 5000, 10000);
    const offset = getInt(q, "offset", 0);

    const params: any[] = [from];
    let where = `acq_datetime_utc >= $1`;
    if (to)          { params.push(to);   where += ` AND acq_datetime_utc < $${params.length}`; }
    if (cods.length) { params.push(cods); where += ` AND cod_ibge = ANY($${params.length})`; }
    if (conf.length) { params.push(conf); where += ` AND confidence = ANY($${params.length})`; }

    const sql = `
      SELECT
        id_firms,
        acq_datetime_utc,
        brightness, frp, confidence,
        cod_ibge, municipio,
        latitude, longitude
      FROM public.firms_focos
      WHERE ${where}
      ORDER BY acq_datetime_utc DESC
      LIMIT ${limit} OFFSET ${offset};
    `;

    const { rows } = await poolRead.query(sql, params);
    return okJSON({ rows, paging: { limit, offset, count: rows.length } }, 30);
  } catch (e) {
    if (e instanceof Response) return e;
    return serverError(e);
  }
}
