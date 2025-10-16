export function requireAuth(req: Request) {
  const publicOk = process.env.API_PUBLIC_OK === "true";
  if (publicOk) return; // Modo público habilitado → sem checagem

  const key = req.headers.get("x-api-key");
  const valid = key && key === process.env.API_KEY;

  if (!valid) {
    throw new Response(JSON.stringify({ error: "unauthorized" }), {
      status: 401,
      headers: { "content-type": "application/json; charset=utf-8" }
    });
  }
}

