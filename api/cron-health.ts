// api/cron-health.ts
import type { VercelRequest, VercelResponse } from '@vercel/node';

export default function handler(req: VercelRequest, res: VercelResponse) {
  console.log('[cron-health] chamada recebida', {
    ua: req.headers['user-agent'],
    vercelEnv: process.env.VERCEL_ENV,
    now: new Date().toISOString(),
  });

  res.status(200).json({ ok: true, ts: Date.now() });
}
