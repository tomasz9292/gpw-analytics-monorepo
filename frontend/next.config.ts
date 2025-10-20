import path from "node:path";
import { fileURLToPath } from "node:url";
import type { NextConfig } from "next";

type RewriteRule = {
  source: string;
  destination: string;
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const workspaceRoot = path.join(__dirname, "..");

const createProxy = (apiBase: string) =>
  (source: string, destination: string): RewriteRule => ({
    source,
    destination: new URL(destination, apiBase).toString(),
  });

const nextConfig: NextConfig = {
  outputFileTracingRoot: workspaceRoot,
  env: {
    GOOGLE_CLIENT_ID_FALLBACK: process.env.GOOGLE_CLIENT_ID,
  },
  async rewrites() {
    const rawBase =
      process.env.NEXT_PUBLIC_API_BASE ||
      "https://gpw-analytics-starter-backend-1.onrender.com";
    const apiBase = rawBase.endsWith("/") ? rawBase : `${rawBase}/`;

    const proxy = createProxy(apiBase);

    return [
      // Publiczne endpointy backendu wykorzystywane bezpośrednio z klienta
      proxy("/api/score/:path*", "/score/:path*"),
      proxy("/api/backtest/:path*", "/backtest/:path*"),
      proxy("/api/companies", "/companies"),
      proxy("/api/companies/:path*", "/companies/:path*"),
      proxy("/api/symbols", "/symbols"),
      proxy("/api/symbols/:path*", "/symbols/:path*"),
      proxy("/api/quotes", "/quotes"),
      proxy("/api/quotes/:path*", "/quotes/:path*"),
    ];
  },
};

export default nextConfig;
