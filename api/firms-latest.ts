import { Client } from "pg";

export default async function handler(req, res) {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await client.connect();

    const { rows } = await client.query(`
      SELECT
        id_firms,
        satellite,
        acq_datetime_utc,
        brightness,
        confidence,
        frp,
        cod_ibge,
        municipio,
        ST_X(geom) AS lon,
        ST_Y(geom) AS lat
      FROM public.firms_focos
      WHERE acq_datetime_utc >= NOW() - INTERVAL '3 days'
      ORDER BY acq_datetime_utc DESC
      LIMIT 5000
    `);

    res.status(200).json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    await client.end();
  }
}