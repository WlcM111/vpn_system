#!/usr/bin/env bash
#
# Поднимает временный кластер Postgres, накатывает на него все миграции проекта
# в том же порядке, что и контейнер migrate, и прогоняет через полученную схему
# каждый SQL-литерал из Go-кода.
#
# Ловит ошибки, невидимые для go build и go vet: несуществующие колонки,
# опечатки в SQL и неоднозначный вывод типов параметров (SQLSTATE 42P08).
#
# Использование:
#     scripts/check_sql.sh
#
# Возвращает 0, если все запросы подготавливаются, иначе 1.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGVER="${PGVER:-16}"
PGBIN="${PGBIN:-/usr/lib/postgresql/${PGVER}/bin}"
PGPORT="${CHECK_SQL_PORT:-5455}"
PGDATA="$(mktemp -d)/data"
PGSOCK="$(mktemp -d)"

# Postgres отказывается стартовать от root. Если скрипт запущен под root,
# перезапускаем его от непривилегированного пользователя.
if [ "$(id -u)" = "0" ]; then
  RUNAS="${CHECK_SQL_USER:-postgres}"
  if ! id "${RUNAS}" >/dev/null 2>&1; then
    echo "Запуск от root невозможен, а пользователя ${RUNAS} нет."
    echo "Создайте его или задайте CHECK_SQL_USER."
    exit 2
  fi
  WORKDIR="$(mktemp -d)"
  cp -a "${ROOT}" "${WORKDIR}/src"
  chown -R "${RUNAS}" "${WORKDIR}"
  exec su "${RUNAS}" -c "PGVER='${PGVER}' PGBIN='${PGBIN}' CHECK_SQL_PORT='${PGPORT}' bash '${WORKDIR}/src/scripts/check_sql.sh'"
fi

if [ ! -x "${PGBIN}/initdb" ]; then
  echo "Не найден initdb в ${PGBIN}."
  echo "Установите postgresql-${PGVER} или задайте PGBIN."
  exit 2
fi

cleanup() {
  "${PGBIN}/pg_ctl" -D "${PGDATA}" stop -m immediate >/dev/null 2>&1 || true
  rm -rf "${PGDATA}" "${PGSOCK}" 2>/dev/null || true
}
trap cleanup EXIT

echo "── поднимаю временный Postgres ${PGVER}"
"${PGBIN}/initdb" -D "${PGDATA}" -A trust -U vpn --locale=C --encoding=UTF8 >/dev/null
"${PGBIN}/pg_ctl" -D "${PGDATA}" -l "${PGDATA}/pg.log" \
  -o "-k ${PGSOCK} -p ${PGPORT} -c listen_addresses=" start >/dev/null
sleep 2

PSQL="${PGBIN}/psql -h ${PGSOCK} -p ${PGPORT} -U vpn"
${PSQL} -d postgres -q -c 'CREATE DATABASE vpn_platform;'

echo "── применяю миграции"
for sd in platform billing-service tg-bot-gateway user-subscription-service vpn-orchestrator-service crypto-billing-service; do
  [ -d "${ROOT}/migrations/${sd}" ] || continue
  for f in $(find "${ROOT}/migrations/${sd}" -maxdepth 1 -type f -name '*.sql' | sort); do
    if ! ${PSQL} -d vpn_platform -v ON_ERROR_STOP=1 -q -f "$f" >/dev/null 2>"${PGDATA}/mig.err"; then
      echo "✗ миграция не применилась: ${sd}/$(basename "$f")"
      head -5 "${PGDATA}/mig.err"
      exit 1
    fi
  done
done

echo "── проверяю SQL из Go-кода"
CHECK_SQL_PSQL="${PGBIN}/psql" \
CHECK_SQL_DSN="postgresql://vpn@/vpn_platform?host=${PGSOCK}&port=${PGPORT}" \
  python3 "${ROOT}/scripts/check_sql.py" "${ROOT}"