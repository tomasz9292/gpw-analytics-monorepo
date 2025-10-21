#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.local-sync.yml"
BACKEND_HEALTH_URL="${LOCAL_SYNC_HEALTHCHECK:-http://localhost:8000/api/admin/ping}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Nie znaleziono pliku $COMPOSE_FILE" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "Wymagany jest curl (https://curl.se/)." >&2
  exit 1
fi

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD=(docker-compose)
  else
    echo "Wymagany jest Docker Compose (plugin lub osobny binarny)." >&2
    exit 1
  fi
else
  echo "Wymagany jest Docker (https://docs.docker.com/get-docker/)." >&2
  exit 1
fi

cleanup() {
  echo -e "\nZatrzymywanie środowiska lokalnego..."
  "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" down --remove-orphans --volumes >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

start_stack() {
  echo "Budowanie i uruchamianie środowiska lokalnego..."
  "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" up -d --build
}

wait_for_backend() {
  echo -n "Oczekiwanie na backend pod $BACKEND_HEALTH_URL"
  for attempt in {1..60}; do
    if curl -fsS "$BACKEND_HEALTH_URL" >/dev/null 2>&1; then
      echo -e "\nBackend działa."
      return 0
    fi
    echo -n "."
    sleep 1
  done
  echo -e "\nBackend nie odpowiedział w oczekiwanym czasie." >&2
  echo "Logi usług:" >&2
  "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" logs >&2 || true
  return 1
}

start_stack
wait_for_backend

echo "\nŚrodowisko lokalne działa. Możesz teraz użyć przycisku 'Uruchom lokalnie' w panelu."
echo "Aby zakończyć, naciśnij Ctrl+C."

"${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" logs -f backend
