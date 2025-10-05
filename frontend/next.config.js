/** @type {import('next').NextConfig} */
const nextConfig = {
    reactStrictMode: true,
    async rewrites() {
        return [
            {
                source: '/api/:path*',
                destination: 'http://localhost:8001/:path*', // Tw�j backend FastAPI
            },
        ]
    },
}

module.exports = nextConfig
