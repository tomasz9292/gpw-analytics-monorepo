/** @type {import('next').NextConfig} */
const nextConfig = {
  env: {
    GOOGLE_CLIENT_ID_FALLBACK: process.env.GOOGLE_CLIENT_ID,
  },
  async rewrites() {
    const apiBase = process.env.NEXT_PUBLIC_API_BASE ||
      "https://gpw-analytics-starter-backend-1.onrender.com";
    return [
      {
        source: "/api/:path*",
        // Użyj zmiennej środowiskowej (patrz punkt 2) albo wpisz na sztywno URL Rendera:
        destination: `${apiBase}/:path*`,
        // przykład na sztywno:
        // destination: "https://gpw-analytics-starter-backend-1.onrender.com/:path*",
      },
    ];
  },
};

module.exports = nextConfig;
