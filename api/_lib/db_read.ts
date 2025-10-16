// api/_lib/db_read.ts
import { Pool, QueryConfig } from "pg";

const connStr = process.env.DATABASE_URL_READONLY ?? process.env.DATABASE_URL;
if (!connStr) throw new Error("DATABASE_URL_READONLY (ou DATABASE_URL) não configurada");

export const poolRead = new Pool({
  connectionString: connStr,
  ssl: { rejectUnauthorized: false },
  max: 5,                              // bom para serverless + readonly
  idleTimeoutMillis: 30_000,           // fecha conexões ociosas
  connectionTimeoutMillis: 5_000,      // evita travar no connect
  application_name: "firms_daily_csv", // aparece no pg_stat_activity
  keepAlive: true
});

// timeouts por sessão (servidor)
poolRead.on("connect", client => {
  client.query(`
    SET statement_timeout = '20s';
    SET idle_in_transaction_session_timeout = '10s';
  `).catch(()=>{});
});

// helper com telemetria
export async function query<T = any>(q: string | QueryConfig<any[]>, params?: any[]) {
  const t0 = Date.now();
  const res = typeof q === "string" ? await poolRead.query<T>(q, params) : await poolRead.query<T>(q);
  const ms = Date.now() - t0;
  const preview = typeof q === "string" ? q.split("\n").map(s=>s.trim()).filter(Boolean)[0]?.slice(0,120) : (q as any).text?.slice(0,120);
  console.log(`[db] ${ms}ms rows=${(res as any).rowCount} :: ${preview}`);
  return res;
}


