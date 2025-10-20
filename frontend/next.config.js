/** @type {import('next').NextConfig} */
const nextConfig = {
  env: {
    GOOGLE_CLIENT_ID_FALLBACK: process.env.GOOGLE_CLIENT_ID,
  },
  async rewrites() {
    const rawBase =
      process.env.NEXT_PUBLIC_API_BASE ||
      "https://gpw-analytics-starter-backend-1.onrender.com";
    const apiBase = rawBase.endsWith("/") ? rawBase : `${rawBase}/`;
    const proxy = (source, destination) => ({
      source,
      destination: new URL(destination, apiBase).toString(),
    });

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

module.exports = nextConfig;
