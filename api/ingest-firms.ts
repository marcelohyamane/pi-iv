import { Client } from "pg";
import { parse } from "csv-parse/sync";
import crypto from "node:crypto";

interface FireRow {
  latitude: string;
  longitude: string;
  acq_date: string;
  acq_time: string;
  satellite?: string;
  brightness?: string;
  confidence?: string;
  frp?: string;
}

export default async function handler(req: any, res: any) {
  try {
    const { source, area, days } = req.query;
    const API_BASE = process.env.API_BASE!;
    const MAP_KEY = process.env.MAP_KEY!;
    const DB_URL = process.env.DATABASE_URL!;
    const AREA = (area as string) || process.env.DEFAULT_AREA!;
    const DAYS = (days as string) || process.env.DEFAULT_DAY_RANGE || "1";

    // Se vier `source` usa só ele, senão pega lista da env
    const sources = source
      ? [source as string]
      : (process.env.DEFAULT_SOURCES || "VIIRS_SNPP_URT_NRT").split(",").map(s => s.trim());

    const results: any[] = [];

    for (const SRC of sources) {
      const url = `${API_BASE}/csv/${encodeURIComponent(MAP_KEY)}/${encodeURIComponent(SRC)}/${encodeURIComponent(AREA)}/${encodeURIComponent(DAYS)}`;
      const r = await fetch(url);
      if (!r.ok) {
        results.push({ source: SRC, error: `Falha ao baixar CSV (${r.status})` });
        continue;
      }

      const csvText = await r.text();
      const rows: FireRow[] = parse(csvText, { columns: true, skip_empty_lines: true, trim: true });

      if (!rows.length) {
        results.push({ source: SRC, inserted: 0, message: "CSV vazio" });
        continue;
      }

      const client = new Client({ connectionString: DB_URL });
      await client.connect();

      const values: any[] = [];
      const params: any[] = [];
      let p = 1;

      for (const row of rows) {
        const lat = Number(row.latitude);
        const lon = Number(row.longitude);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

        const date = row.acq_date;
        const time = row.acq_time.padStart(4, "0");
        const hh = time.slice(0, 2);
        const mm = time.slice(2, 4);
        const acqIso = `${date}T${hh}:${mm}:00Z`;

        const id = crypto.createHash("sha256").update(`${row.satellite || ""}|${acqIso}|${lat}|${lon}`).digest("hex");

        values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
        params.push(
          id,
          row.satellite || null,
          acqIso,
          row.brightness ? Number(row.brightness) : null,
          row.confidence || null,
          row.frp ? Number(row.frp) : null,
          lon,
          lat
        );
      }

      const sql = `
        INSERT INTO public.firms_focos
          (id_firms, satellite, acq_datetime_utc, brightness, confidence, frp, geom)
        VALUES ${values.join(",")}
        ON CONFLICT (id_firms, acq_datetime_utc) DO NOTHING
      `;

      const rr = await client.query(sql, params);
      await client.end();

      results.push({ source: SRC, parsed: rows.length, inserted: rr.rowCount });
    }

    return res.status(200).json({ ok: true, results });
  } catch (e: any) {
    return res.status(500).json({ error: "Falha na ingestão", details: e.message });
  }
}
