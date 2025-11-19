#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env.local"
ENV_EXAMPLE="$ROOT_DIR/.env.local.example"
COMPOSE_FILE="$ROOT_DIR/docker-compose.local-dev.yml"

ensure_env_file() {
  if [[ ! -f "$ENV_FILE" && -f "$ENV_EXAMPLE" ]]; then
    echo "[setup] Creating .env.local from example..."
    cp "$ENV_EXAMPLE" "$ENV_FILE"
  fi
}

check_binary() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[error] $1 is required but was not found in PATH" >&2
    exit 1
  fi
}

check_binary docker
if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker compose)
else
  check_binary docker-compose
  DOCKER_COMPOSE=(docker-compose)
fi

ensure_env_file

echo "[info] Starting local stack using $COMPOSE_FILE"
"${DOCKER_COMPOSE[@]}" -f "$COMPOSE_FILE" up --build

echo
echo "Services exposed locally:"
echo "  • Frontend:   http://localhost:${FRONTEND_PORT:-3000}"
echo "  • Backend:    http://localhost:${BACKEND_PORT:-8000}/api/admin/ping"
echo "  • ClickHouse: http://localhost:${CLICKHOUSE_HTTP_PORT:-8123}"
