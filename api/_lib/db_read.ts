// api/_lib/db_read.ts
import { Pool, QueryConfig } from "pg";

// ⚠️ Sem fallback: exige DATABASE_URL_READONLY em produção
const raw = process.env.DATABASE_URL_READONLY;
if (!raw) {
  throw new Error(
    "DATABASE_URL_READONLY ausente. Defina em Vercel → Project → Settings → Environment Variables (Production)."
  );
}

// Força sslmode=no-verify na string de conexão (e loga host/porta/db)
let connStr = raw;
let logLine = "[db_read] using ";
try {
  const u = new URL(raw);
  // garante sslmode=no-verify para evitar SELF_SIGNED_CERT_IN_CHAIN
  u.searchParams.set("sslmode", "no-verify");
  connStr = u.toString();

  const user = u.username || "?";
  const host = u.host; // inclui porta se houver
  const dbname = u.pathname.replace("/", "") || "(none)";
  const sslmode = u.searchParams.get("sslmode") || "(default)";
  logLine += `${user}@${host}/${dbname} sslmode=${sslmode}`;
} catch {
  logLine += "parse_error";
}
console.log(logLine);

// Pool único e leve p/ serverless
export const poolRead = new Pool({
  connectionString: connStr,
  // reforço: ignora cadeia de certificados (compatível com Pooler do Supabase)
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  application_name: "firms_daily_csv",
});

// Timeouts por sessão no servidor
poolRead.on("connect", (client) => {
  client
    .query(`
      SET statement_timeout = '20s';
      SET idle_in_transaction_session_timeout = '10s';
    `)
    .catch(() => {});
});

// Helper com telemetria (tempo e linhas)
export async function query<T = any>(
  q: string | QueryConfig<any[]>,
  params?: any[]
) {
  const t0 = Date.now();
  const res =
    typeof q === "string"
      ? await poolRead.query<T>(q, params)
      : await poolRead.query<T>(q);
  const ms = Date.now() - t0;

  const text =
    typeof q === "string"
      ? q
      : (q as QueryConfig).text || "[no-sql-text]";
  const firstLine = text.split("\n").map((s) => s.trim()).find(Boolean) || "";
  const preview = firstLine.slice(0, 140);

  // Log enxuto: tempo, linhas e 1ª linha do SQL
  // (aparece nos logs da Vercel em cada invoke)
  console.log(`[db] ${ms}ms rows=${(res as any).rowCount ?? res?.['rowCount']} :: ${preview}`);
  return res;
}

