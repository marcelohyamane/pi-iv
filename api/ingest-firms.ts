// api/ingest-firms.ts
import { Client } from "pg";
import { parse } from "csv-parse/sync";
import crypto from "node:crypto";

// =====================================================
// Util: fetch com timeout + retry exponencial (3 tentativas)
// =====================================================
async function fetchWithRetry(
  url: string,
  init: RequestInit = {},
  tries = 3,
  timeoutMs = 15000
): Promise<any> {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const resp = await fetch(url, { ...init, signal: ctrl.signal } as any);
      clearTimeout(timer);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      return resp;
    } catch (e) {
      clearTimeout(timer);
      lastErr = e;
      // backoff: 0.5s, 1s, 2s
      const backoff = 500 * Math.pow(2, i);
      await new Promise((r) => setTimeout(r, backoff));
    }
  }
  throw lastErr;
}

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

// =====================================================
// Map de fontes (se vierem com nome "amigável")
// =====================================================
function normalizeSource(src: string): string {
  if (!src) return src;
  const raw = src.trim();

  // Normaliza variações comuns (case, espaçamentos e símbolos)
  const key = raw
    .replace(/\s+/g, ' ')
    .replace(/[-()]/g, '')       // remove traços e parênteses
    .replace(/\s*\+\s*/g, '+')   // "URT + NRT" -> "URT+NRT"
    .toUpperCase()
    .trim();

  // Códigos válidos aceitos pela API (sem "URT_NRT")
  const VALID_CODES = new Set([
    'VIIRS_SNPP_NRT',
    'VIIRS_NOAA20_NRT',
    'VIIRS_NOAA21_NRT',
    'MODIS_NRT',
    'MODIS_SP',
    'VIIRS_SNPP_SP',
    'VIIRS_NOAA20_SP',
    'VIIRS_NOAA21_SP',
  ]);

  // Se já veio como código e for válido, devolve como está
  if (VALID_CODES.has(raw.toUpperCase())) return raw.toUpperCase();

  // Mapeia nomes “amigáveis” (inclui suas variantes)
  const ALIASES: Record<string, string> = {
    'VIIRS S NPP URT+NRT': 'VIIRS_SNPP_NRT',
    'VIIRS SNPP URT+NRT' : 'VIIRS_SNPP_NRT',
    'VIIRS S NPP'        : 'VIIRS_SNPP_NRT',
    'VIIRS S NPP NRT'    : 'VIIRS_SNPP_NRT',

    'VIIRS NOAA 20 URT+NRT': 'VIIRS_NOAA20_NRT',
    'VIIRS NOAA20 URT+NRT' : 'VIIRS_NOAA20_NRT',
    'VIIRS NOAA 20'        : 'VIIRS_NOAA20_NRT',
    'VIIRS NOAA 20 NRT'    : 'VIIRS_NOAA20_NRT',

    'VIIRS NOAA 21 URT+NRT': 'VIIRS_NOAA21_NRT',
    'VIIRS NOAA21 URT+NRT' : 'VIIRS_NOAA21_NRT',
    'VIIRS NOAA 21'        : 'VIIRS_NOAA21_NRT',
    'VIIRS NOAA 21 NRT'    : 'VIIRS_NOAA21_NRT',

    'MODIS URT+NRT': 'MODIS_NRT',
    'MODIS NRT'    : 'MODIS_NRT',
    'MODIS'        : 'MODIS_NRT',

    'MODIS SP'           : 'MODIS_SP',
    'VIIRS S NPP SP'     : 'VIIRS_SNPP_SP',
    'VIIRS NOAA 20 SP'   : 'VIIRS_NOAA20_SP',
    'VIIRS NOAA 21 SP'   : 'VIIRS_NOAA21_SP',
  };

  // Corrige entradas do tipo "VIIRS_SNPP_URT_NRT" para "VIIRS_SNPP_NRT"
  if (/^VIIRS_(SNPP|NOAA20|NOAA21)_URT_NRT$/.test(raw.toUpperCase()))
    return raw.toUpperCase().replace('_URT_NRT', '_NRT');

  // Tenta mapear nome amigável
  if (ALIASES[key]) return ALIASES[key];

  // Último recurso: se vier algo tipo "VIIRS SNPP" sem sufixo, assume NRT
  if (/^VIIRS\s+SNPP$/i.test(raw)) return 'VIIRS_SNPP_NRT';
  if (/^VIIRS\s+NOAA[-\s]*20$/i.test(raw)) return 'VIIRS_NOAA20_NRT';
  if (/^VIIRS\s+NOAA[-\s]*21$/i.test(raw)) return 'VIIRS_NOAA21_NRT';

  // Mantém como veio (pode falhar e te mostrar no log)
  return raw;
}


// =====================================================
// Handler principal (mantemos req/res como any p/ simplicidade)
// =====================================================
export default async function handler(req: any, res: any) {
  const t0 = Date.now();
  const ua = String(req.headers["user-agent"] || "");
  const authHdr = String(req.headers["authorization"] || "");
  const urlHit = String(req.url || "");

  // (Opcional) exigir CRON_SECRET se definido nas ENVs (Production)
  if (process.env.CRON_SECRET) {
    if (authHdr !== `Bearer ${process.env.CRON_SECRET}`) {
      console.log("[ingest-firms] unauthorized", {
        ua,
        urlHit,
        env: process.env.VERCEL_ENV,
      });
      res.status(401).end("Unauthorized");
      return;
    }
  }

  // ⚠️ Não logar segredo do DB. Mascaramos usuário/senha:
  const dbUrlMasked = process.env.DATABASE_URL
    ? process.env.DATABASE_URL.replace(/:\/\/[^@]+@/, "://****:****@")
    : "missing";

  console.log("[ingest-firms] start", {
    ua,
    urlHit,
    env: process.env.VERCEL_ENV,
    db: dbUrlMasked,
    now: new Date().toISOString(),
  });

  try {
    const q = req.query || {};

    const API_BASE = (q.api_base as string) || process.env.API_BASE!;
    const MAP_KEY = (q.map_key as string) || process.env.MAP_KEY!;
    const DB_URL = process.env.DATABASE_URL!;
    const AREA = (q.area as string) || process.env.DEFAULT_AREA!;
    const DAYS = Number(
      (q.days as string) || process.env.DEFAULT_DAY_RANGE || "1"
    );
    const endDateQ = (q.end_date as string) || ""; // YYYY-MM-DD opcional

    if (!API_BASE || !MAP_KEY || !DB_URL || !AREA) {
      console.error("[ingest-firms] missing envs", {
        API_BASE: !!API_BASE,
        MAP_KEY: !!MAP_KEY,
        DB_URL: !!DB_URL,
        AREA: !!AREA,
      });
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
      ssl: { rejectUnauthorized: false },
    });

    // conectar com log de tempo
    const tConn0 = Date.now();
    await client.connect();
    console.log("[ingest-firms] db_connected_ms", { ms: Date.now() - tConn0 });

    const results: any[] = [];
    let sumParsed = 0,
      sumValid = 0,
      sumInserted = 0;

    for (const SRC of sources) {
      const r = await ingestByBatches({
        apiBase: API_BASE,
        mapKey: MAP_KEY,
        source: SRC,
        area: AREA,
        totalDays: Math.max(1, DAYS),
        endDateStr: endDateQ,
        client,
        fetchFn: fetchWithRetry, // usa timeout + retry
      });
      results.push({ source: SRC, ...r });
      sumParsed += r.parsed;
      sumValid += r.valid;
      sumInserted += r.inserted;
    }

    await client.end();

    const payload = {
      ok: true,
      results,
      totals: { parsed: sumParsed, valid: sumValid, inserted: sumInserted },
    };

    console.log("[ingest-firms] done", {
      ...payload.totals,
      durationMs: Date.now() - t0,
    });

    res.status(200).json(payload);
  } catch (e: any) {
    console.error("[ingest-firms] error", { message: e?.message, stack: e?.stack });
    res
      .status(500)
      .json({ error: "Falha na ingestão", details: e?.message || String(e) });
  }
}

// =====================================================
// Ingestão em blocos com logs por batch
// =====================================================
async function ingestByBatches(opts: {
  apiBase: string;
  mapKey: string;
  source: string;
  area: string;
  totalDays: number;
  endDateStr?: string;
  client: Client;
  fetchFn?: (url: string, init?: RequestInit) => Promise<any>;
}) {
  const { apiBase, mapKey, source, area, client } = opts;
  const fetchFn = opts.fetchFn || (fetch as any);

  let remaining = opts.totalDays;
  let endDate = opts.endDateStr ? new Date(opts.endDateStr) : new Date();
  const maxBlock = 10;

  let totalParsed = 0,
    totalValid = 0,
    totalInserted = 0;

  while (remaining > 0) {
    const range = Math.min(maxBlock, remaining);
    const yyyy = endDate.toISOString().slice(0, 10);

    const code = normalizeSource(source);
    const url = `${apiBase}/csv/${encodeURIComponent(
      mapKey
    )}/${encodeURIComponent(code)}/${area}/${range}/${yyyy}`;

    const tDl0 = Date.now();
    const resp = await fetchFn(url);
    const text = await resp.text();
    const dlMs = Date.now() - tDl0;

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

    console.log("[ingest-firms] batch_download", {
      source,
      yyyy,
      rangeDays: range,
      rowsParsed: rows.length,
      rowsValid: items.length,
      downloadMs: dlMs,
    });

    const batch = 800; // ajuste se precisar
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
        const tIns0 = Date.now();
        const sql = `
          INSERT INTO public.firms_focos
            (id_firms, satellite, acq_datetime_utc, brightness, confidence, frp, geom)
          VALUES ${values.join(",")}
          ON CONFLICT (id_firms, acq_datetime_utc) DO NOTHING
        `;
        const rr = await client.query(sql, params);
        const insMs = Date.now() - tIns0;
        const inserted = rr.rowCount || 0;
        totalInserted += inserted;

        console.log("[ingest-firms] batch_insert", {
          rowsChunk: chunk.length,
          inserted,
          insertMs: insMs,
        });
      }
    }

    endDate.setUTCDate(endDate.getUTCDate() - range);
    remaining -= range;
  }

  return { parsed: totalParsed, valid: totalValid, inserted: totalInserted };
}

// =====================================================
// Normalização de uma linha do CSV (aceita formatos VIIRS/MODIS)
// =====================================================
function normalizeRow(row: AnyRow): NormalizedRow | null {
  const lat = Number(row.latitude);
  const lon = Number(row.longitude);
  const date = (row.acq_date || "").trim();
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || !date) return null;

  // acq_time pode vir "417" (HHmm) ou "04:17"
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
