# syntax=docker/dockerfile:1.7
ARG NODE_VERSION=20
ARG NEXT_PUBLIC_API_BASE=http://backend:8000

FROM node:${NODE_VERSION}-bullseye AS deps
ARG NEXT_PUBLIC_API_BASE
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM node:${NODE_VERSION}-bullseye AS builder
ARG NEXT_PUBLIC_API_BASE
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1 \
    NODE_ENV=production \
    NEXT_PUBLIC_API_BASE=${NEXT_PUBLIC_API_BASE}
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:${NODE_VERSION}-bullseye AS runner
ARG NEXT_PUBLIC_API_BASE
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    NEXT_PUBLIC_API_BASE=${NEXT_PUBLIC_API_BASE}
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
EXPOSE 3000
CMD ["npm", "run", "start"]
