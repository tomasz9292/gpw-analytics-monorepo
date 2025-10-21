#!/bin/sh
set -euo pipefail

# Resolve repository root relative to this script (frontend/scripts)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR"/../.. && pwd)"

cd "$REPO_ROOT"
exec ./scripts/install-frontend.sh "$@"
