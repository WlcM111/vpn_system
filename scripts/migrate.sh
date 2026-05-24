#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Применяем в порядке: platform → billing-service → tg-bot-gateway → user-subscription-service → vpn-orchestrator-service
# Внутри каждой подпапки — алфавитная сортировка по имени файла.
SERVICE_DIRS=(
  "platform"
  "billing-service"
  "tg-bot-gateway"
  "user-subscription-service"
  "vpn-orchestrator-service"
  "crypto-billing-service"
)

for sd in "${SERVICE_DIRS[@]}"; do
  dir="$ROOT_DIR/migrations/$sd"
  if [ ! -d "$dir" ]; then
    continue
  fi
  for f in $(find "$dir" -maxdepth 1 -type f -name "*.sql" | sort); do
    echo "applying $f"
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
  done
done