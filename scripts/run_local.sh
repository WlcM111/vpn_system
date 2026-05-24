#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and fill required values."
  exit 1
fi

docker compose --env-file .env -f deploy/docker-compose.dev.yml up --build "$@"