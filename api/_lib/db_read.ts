import { Pool } from "pg";

const connStr = process.env.DATABASE_URL_READONLY ?? process.env.DATABASE_URL;
if (!connStr) throw new Error("DATABASE_URL_READONLY (ou DATABASE_URL) não configurada");

export const poolRead = new Pool({
  connectionString: connStr,
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30_000
});

// Define timeouts por sessão
poolRead.on("connect", client => {
  client.query(`SET statement_timeout = '20s'; SET idle_in_transaction_session_timeout = '10s';`).catch(()=>{});
});

