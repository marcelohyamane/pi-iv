// api/_lib/db_read.ts
import { Pool } from "pg";

const connStr = process.env.DATABASE_URL_READONLY;
if (!connStr) {
  throw new Error("DATABASE_URL_READONLY ausente no ambiente! Defina em Vercel → Settings → Environment Variables (Production).");
}

// Log seguro do host/porta/banco (sem senha)
let masked = "n/a";
try {
  const u = new URL(connStr);
  masked = `${u.username || "?"}@${u.host}${u.port ? ":" + u.port : ""}/${u.pathname.replace("/", "")}`;
} catch {
  masked = "parse_error";
}
console.log(`[db_read] using ${masked}`);

export const poolRead = new Pool({
  connectionString: connStr,
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  application_name: "firms_daily_csv"
});

poolRead.on("connect", (client) => {
  client.query(`
    SET statement_timeout = '20s';
    SET idle_in_transaction_session_timeout = '10s';
  `).catch(() => {});
});
