// api/cron-health.ts
import type { IncomingMessage, ServerResponse } from "http";

export default function handler(req: IncomingMessage, res: ServerResponse) {
  console.log('[cron-health] chamada recebida', {
    ua: req.headers['user-agent'],
    vercelEnv: process.env.VERCEL_ENV,
    now: new Date().toISOString(),
  });

  res.statusCode = 200;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify({ ok: true, ts: Date.now() }));
}
