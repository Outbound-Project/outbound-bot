export default {
  async fetch(request, env) {
    if (request.method === "GET") {
      return new Response("ok", { status: 200 });
    }

    if (request.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }

    const token = request.headers.get("X-Goog-Channel-Token") || "";
    if (env.WEBHOOK_TOKEN && token !== env.WEBHOOK_TOKEN) {
      return new Response("invalid token", { status: 401 });
    }

    if (!env.FORWARD_URL) {
      return new Response("missing FORWARD_URL", { status: 500 });
    }

    const forwardUrl = new URL(env.FORWARD_URL);
    forwardUrl.pathname = "/webhook";

    const forwardHeaders = new Headers(request.headers);
    forwardHeaders.set("X-Forwarded-By", "cloudflare-worker");

    const res = await fetch(forwardUrl.toString(), {
      method: "POST",
      headers: forwardHeaders,
      body: request.body,
    });

    const body = await res.text();
    return new Response(body, { status: res.status, headers: res.headers });
  },
};
