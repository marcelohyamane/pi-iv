// api/ingest-firms.ts
import { Client } from "pg";
import { parse } from "csv-parse/sync";
import crypto from "node:crypto";

type AnyRow = Record<string, string>;

type NormalizedRow = {
  lat: number;
  lon: number;
  acqIso: string;
  satellite: string | null;
  brightness: number | null;
  confidence: string | null;
  frp: number | null;
};

export default async function handler(req: any, res: any) {
  try {
    const q = req.query;

    const API_BASE = (q.api_base as string) || process.env.API_BASE!;
    const MAP_KEY = (q.map_key as string) || process.env.MAP_KEY!;
    const DB_URL = process.env.DATABASE_URL!;
    const AREA = (q.area as string) || process.env.DEFAULT_AREA!;
    const DAYS = Number(
      (q.days as string) || process.env.DEFAULT_DAY_RANGE || "1"
    );
    const endDateQ = (q.end_date as string) || ""; // YYYY-MM-DD opcional

    if (!API_BASE || !MAP_KEY || !DB_URL || !AREA) {
      return res.status(500).json({
        error: "Faltam variáveis",
        details: {
          API_BASE: !!API_BASE,
          MAP_KEY: !!MAP_KEY,
          DB_URL: !!DB_URL,
          AREA: !!AREA,
        },
      });
    }

    const sources = q.source
      ? [decodeURIComponent(q.source as string)]
      : (process.env.DEFAULT_SOURCES || "VIIRS S-NPP (URT+NRT)")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);

    const client = new Client({
      connectionString: DB_URL,
      application_name: "firms_ingest",
    });
    await client.connect();

    const results: any[] = [];
    for (const SRC of sources) {
      const r = await ingestByBatches({
        apiBase: API_BASE,
        mapKey: MAP_KEY,
        source: SRC,
        area: AREA,
        totalDays: Math.max(1, DAYS),
        endDateStr: endDateQ,
        client,
      });
      results.push({ source: SRC, ...r });
    }

    await client.end();
    return res.status(200).json({ ok: true, results });
  } catch (e: any) {
    console.error(e);
    return res.status(500).json({
      error: "Falha na ingestão",
      details: e?.message || String(e),
    });
  }
}

async function ingestByBatches(opts: {
  apiBase: string;
  mapKey: string;
  source: string;
  area: string;
  totalDays: number;
  endDateStr?: string;
  client: Client;
}) {
  const { apiBase, mapKey, source, area, client } = opts;
  let remaining = opts.totalDays;
  let endDate = opts.endDateStr ? new Date(opts.endDateStr) : new Date();
  const maxBlock = 10;

  let totalParsed = 0,
    totalValid = 0,
    totalInserted = 0;

  while (remaining > 0) {
    const range = Math.min(maxBlock, remaining);
    const yyyy = endDate.toISOString().slice(0, 10);
    const url = `${apiBase}/csv/${encodeURIComponent(
      mapKey
    )}/${encodeURIComponent(source)}/${encodeURIComponent(
      area
    )}/${range}/${yyyy}`;

    const resp = await fetch(url);
    const text = await resp.text();

    if (!resp.ok || /^</.test(text) || /^"?Invalid/i.test(text)) {
      throw new Error(
        `FIRMS erro ${resp.status} em ${source} (${range}d até ${yyyy}): ${text.slice(
          0,
          240
        )}`
      );
    }

    const rows = parse(text, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as AnyRow[];
    totalParsed += rows.length;

    const items: NormalizedRow[] = rows
      .map(normalizeRow)
      .filter((r): r is NormalizedRow => r !== null);
    totalValid += items.length;

    const batch = 800;
    for (let i = 0; i < items.length; i += batch) {
      const chunk = items.slice(i, i + batch);
      const params: any[] = [];
      const values: string[] = [];
      let p = 1;

      for (const it of chunk) {
        const id = crypto
          .createHash("sha256")
          .update(`${it.satellite || ""}|${it.acqIso}|${it.lat}|${it.lon}`)
          .digest("hex");

        values.push(
          `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`
        );

        params.push(
          id,
          it.satellite,
          it.acqIso,
          it.brightness,
          it.confidence,
          it.frp,
          it.lon,
          it.lat
        );
      }

      if (values.length) {
        const sql = `
          INSERT INTO public.firms_focos
            (id_firms, satellite, acq_datetime_utc, brightness, confidence, frp, geom)
          VALUES ${values.join(",")}
          ON CONFLICT (id_firms, acq_datetime_utc) DO NOTHING
        `;
        const rr = await client.query(sql, params);
        totalInserted += rr.rowCount || 0;
      }
    }

    endDate.setUTCDate(endDate.getUTCDate() - range);
    remaining -= range;
  }

  return { parsed: totalParsed, valid: totalValid, inserted: totalInserted };
}

function normalizeRow(row: AnyRow): NormalizedRow | null {
  const lat = Number(row.latitude);
  const lon = Number(row.longitude);
  const date = (row.acq_date || "").trim();
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || !date) return null;

  let t = (row.acq_time || "").trim();
  let hh = "00",
    mm = "00";
  if (/^\d{3,4}$/.test(t)) {
    const s = t.padStart(4, "0");
    hh = s.slice(0, 2);
    mm = s.slice(2, 4);
  } else if (/^\d{2}:\d{2}$/.test(t)) {
    [hh, mm] = t.split(":");
  }
  const acqIso = `${date}T${hh}:${mm}:00Z`;

  const satellite = (row.satellite || "").trim() || null;

  const brightness =
    row.bright_ti4 != null && row.bright_ti4 !== ""
      ? Number(row.bright_ti4)
      : row.brightness != null && row.brightness !== ""
      ? Number(row.brightness)
      : row.bright_t31 != null && row.bright_t31 !== ""
      ? Number(row.bright_t31)
      : null;

  const frp = row.frp != null && row.frp !== "" ? Number(row.frp) : null;

  let confidence = (row.confidence ?? "").toString().trim();
  if (/^[lnh]$/i.test(confidence)) confidence = confidence.toUpperCase();

  return {
    lat,
    lon,
    acqIso,
    satellite,
    brightness,
    confidence: confidence || null,
    frp,
  };
}
