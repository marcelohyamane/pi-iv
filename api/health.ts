export const config = { runtime: "edge" }; // leve

export default async function handler() {
  return new Response(JSON.stringify({ ok: true, ts: new Date().toISOString() }), {
    headers: { "content-type": "application/json" }
  });
}
