#!/bin/sh
set -euo pipefail

if [ -f "./frontend/package.json" ]; then
  npm install --prefix frontend
elif [ -f "./package.json" ]; then
  npm install
elif [ -f "../package.json" ]; then
  cd ..
  npm install --prefix frontend
else
  echo "Unable to locate frontend package.json" >&2
  exit 1
fi
