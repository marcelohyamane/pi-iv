// api/cron-health.ts
export default async function handler(req, res) {
  console.log('[cron-health] chamada recebida', {
    ua: req.headers['user-agent'],
    vercelEnv: process.env.VERCEL_ENV,
    now: new Date().toISOString()
  });
  res.status(200).json({ ok: true, ts: Date.now() });
}
