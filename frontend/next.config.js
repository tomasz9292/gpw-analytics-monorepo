/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        // Użyj zmiennej środowiskowej (patrz punkt 2) albo wpisz na sztywno URL Rendera:
        destination: `${process.env.NEXT_PUBLIC_API_BASE}/:path*`,
        // przykład na sztywno:
        // destination: "https://gpw-analytics-starter-backend-1.onrender.com/:path*",
      },
    ];
  },
};

module.exports = nextConfig;
