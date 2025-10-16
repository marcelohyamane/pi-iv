import { requireAuth } from "./_lib/auth";

export const config = { runtime: "edge" };

export default async function handler(req: Request) {
  try {
    requireAuth(req); // exige token, se API_PUBLIC_OK=false
    return new Response(JSON.stringify({ ok: true, ts: new Date().toISOString() }), {
      headers: { "content-type": "application/json" }
    });
  } catch (e: any) {
    return e instanceof Response ? e : new Response("error", { status: 500 });
  }
}

