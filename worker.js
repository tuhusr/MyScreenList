const TMDB_ORIGIN = "https://api.themoviedb.org/3/";
const RAWG_ORIGIN = "https://api.rawg.io/api/";
const VISITOR_COOKIE = "msl_vid";
const VISITOR_COOKIE_MAX_AGE = 60 * 60 * 24 * 365;

function readCookie(request, name) {
  const cookieHeader = request.headers.get("cookie") || "";
  const cookies = cookieHeader.split(/;\s*/);
  for (const cookie of cookies) {
    const [key, ...rest] = cookie.split("=");
    if (key === name) return rest.join("=");
  }
  return "";
}

function isHtmlNavigationRequest(request, url) {
  if (request.method !== "GET") return false;
  if (url.pathname.startsWith("/api/")) return false;
  if (/\.[a-z0-9]+$/i.test(url.pathname)) return false;
  const destination = request.headers.get("sec-fetch-dest");
  if (destination === "document") return true;
  const accept = request.headers.get("accept") || "";
  return accept.includes("text/html");
}

function buildVisitorCookie(visitorId) {
  return `${VISITOR_COOKIE}=${visitorId}; Max-Age=${VISITOR_COOKIE_MAX_AGE}; Path=/; HttpOnly; Secure; SameSite=Lax`;
}

async function registerVisitor(request, env) {
  const existingVisitorId = readCookie(request, VISITOR_COOKIE);
  const visitorId = existingVisitorId || crypto.randomUUID();
  const stub = env.VISITOR_COUNTER.get(env.VISITOR_COUNTER.idFromName("global"));
  await stub.fetch("https://visitor-counter.internal/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ visitorId })
  });
  return existingVisitorId ? "" : buildVisitorCookie(visitorId);
}

async function fetchVisitorStats(env) {
  const stub = env.VISITOR_COUNTER.get(env.VISITOR_COUNTER.idFromName("global"));
  return stub.fetch("https://visitor-counter.internal/stats");
}

function withAppendedCookie(response, cookieHeader) {
  if (!cookieHeader) return response;
  const headers = new Headers(response.headers);
  headers.append("Set-Cookie", cookieHeader);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}

function buildUpstreamUrl(origin, pathSuffix, originalUrl, authParam, authValue) {
  const upstream = new URL(pathSuffix.replace(/^\/+/, ""), origin);
  const sourceParams = new URL(originalUrl).searchParams;
  sourceParams.forEach((value, key) => {
    if (key !== authParam) upstream.searchParams.set(key, value);
  });
  upstream.searchParams.set(authParam, authValue);
  return upstream;
}

function buildProxyRequest(request, upstreamUrl) {
  const headers = new Headers(request.headers);
  headers.delete("host");
  const init = {
    method: request.method,
    headers,
    redirect: "follow"
  };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
  }
  return new Request(upstreamUrl, init);
}

async function proxyApi(request, env, options) {
  const keyValue = env[options.keyEnv];
  if (!keyValue) {
    return new Response(`${options.label} key is not configured.`, { status: 500 });
  }

  const url = new URL(request.url);
  const pathSuffix = url.pathname.slice(options.prefix.length);
  const upstreamUrl = buildUpstreamUrl(options.origin, pathSuffix, request.url, options.authParam, keyValue);
  const upstreamRequest = buildProxyRequest(request, upstreamUrl);
  return fetch(upstreamRequest);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/api/site-stats") {
      return fetchVisitorStats(env);
    }

    if (url.pathname.startsWith("/api/tmdb/")) {
      return proxyApi(request, env, {
        prefix: "/api/tmdb/",
        origin: TMDB_ORIGIN,
        authParam: "api_key",
        keyEnv: "TMDB_KEY",
        label: "TMDB"
      });
    }

    if (url.pathname.startsWith("/api/rawg/")) {
      return proxyApi(request, env, {
        prefix: "/api/rawg/",
        origin: RAWG_ORIGIN,
        authParam: "key",
        keyEnv: "RAWG_KEY",
        label: "RAWG"
      });
    }

    const shouldRegister = isHtmlNavigationRequest(request, url);
    const cookieHeader = shouldRegister ? await registerVisitor(request, env) : "";
    const response = await env.ASSETS.fetch(request);
    return withAppendedCookie(response, cookieHeader);
  }
};

export class VisitorCounter {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/register") {
      const { visitorId } = await request.json();
      if (!visitorId) {
        return Response.json({ error: "Missing visitorId" }, { status: 400 });
      }

      const visitorKey = `visitor:${visitorId}`;
      const seen = await this.state.storage.get(visitorKey);
      if (!seen) {
        const totalVisitors = (await this.state.storage.get("totalVisitors")) || 0;
        await this.state.storage.put(visitorKey, Date.now());
        await this.state.storage.put("totalVisitors", totalVisitors + 1);
      }

      const totalVisitors = (await this.state.storage.get("totalVisitors")) || 0;
      return Response.json({ totalVisitors });
    }

    if (request.method === "GET" && url.pathname === "/stats") {
      const totalVisitors = (await this.state.storage.get("totalVisitors")) || 0;
      return Response.json({ totalVisitors });
    }

    return new Response("Not found", { status: 404 });
  }
}
