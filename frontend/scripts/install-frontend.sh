#!/bin/sh
set -eu

# Resolve repository root relative to this script (frontend/scripts)
REPO_ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

"$REPO_ROOT/scripts/install-frontend.sh"
