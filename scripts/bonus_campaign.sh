#!/usr/bin/env bash
# Бонусная кампания: начислить дни ушедшим пользователям и отправить им
# сообщение через штатный канал уведомлений бота.
#
#   ./scripts/bonus_campaign.sh --campaign apology-2026-11 --dry-run   # только посмотреть
#   ./scripts/bonus_campaign.sh --campaign apology-2026-11             # выполнить
#
# Кому (по умолчанию): у кого доступ закончился за последние 21 день и сейчас
# нет действующего доступа:
#   paid  — платил хотя бы раз (журнал начислений, платежи YooKassa/Platega,
#           крипто-счета, last_payment_id) и не продлил;
#   trial — был пробный период, оплат не было.
# Кроме тех, у кого в биллинге ещё действует автопродление (повторы списания,
# льготный период, запланированное списание): неуспешное списание позже
# остановило бы доступ и обрезало бонус. Они откладываются — повторный запуск
# с тем же --campaign начислит им, когда биллинг закончит (если не продлят сами).
# Выборка делается заново при каждом запуске: повторный запуск включит и тех,
# у кого доступ закончился уже после первого. Зафиксированный при первом
# запуске список держит extra/bonus_campaign_current.sh.
#
# Как: для каждого получателя в outbox ставится команда subscription.grant_bonus.
# user-subscription-service начисляет дни (журнал subscription_grants: ровно одно
# начисление на пользователя и кампанию), публикует активацию для оркестратора
# (доступ на нодах) и сообщение пользователю — одной транзакцией. Сообщение
# получает только тот, кому начисление действительно применено.
# Рассылка /internal/broadcast для этого не подходит: она шлёт всем.
#
# Безопасность: предпросмотр и список получателей в файл → подтверждение вводом
# точного числа получателей → одна пробная команда → после её обработки —
# остальные, по одной в 200 мс. Повторный запуск с тем же --campaign повторно
# ничего не начисляет и не отправляет: начисление держит журнал, а у команд
# каждого запуска свой command_id — отложенные обработчиком в прошлый раз
# (льготный период, автопродление) будут обработаны снова. Секреты не нужны и
# не выводятся.
#
# Требует: обновлённый user-subscription-service (команда subscription.grant_bonus,
# см. docs/bonus-campaign.md), запущенный стек docker compose, docker и python3.

set +x
set -Eeuo pipefail
umask 077

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
	printf '%s\n' "ОШИБКА: нужен bash 4 или новее (на сервере Ubuntu он есть)." >&2
	exit 1
fi

readonly SCRIPT_NAME="bonus_campaign.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CAMPAIGN=""
DAYS=30
WINDOW_DAYS=21
MESSAGE_FILE=""
DRY_RUN=0
PG_CONTAINER=""
STAGGER_MS=200
CANARY_TIMEOUT=180

DEFAULT_MESSAGE='🙏 Здравствуйте!

Приносим искренние извинения за неудобства, которые могли возникнуть при работе House VPN.

🎁 В знак извинения дарим вам 1 месяц бесплатного доступа ({days}) — бонус уже начислен на ваш аккаунт.

✅ Доступ активен до {until}.

🔗 Ссылка подписки осталась прежней — просто обновите подписку в приложении. Если вы её удалили, нажмите «🔗 Получить ссылку доступа» ниже.

Спасибо, что остаётесь с нами! 💙'

say() { printf '%s\n' "$*" >&2; }
die() {
	say "ОШИБКА: $*"
	exit 1
}
usage() {
	say "Использование: $SCRIPT_NAME --campaign КЛЮЧ [--dry-run] [--days 30] [--window-days 21]"
	say "                         [--message-file ФАЙЛ] [--postgres-container ИМЯ]"
	say "КЛЮЧ — строчные латинские буквы, цифры и . _ : - (до 64 символов), например apology-2026-11."
	say "Один ключ = одна кампания: повторный запуск с тем же ключом повторно не начисляет."
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--campaign) [ "$#" -ge 2 ] || { usage; exit 2; }; CAMPAIGN="$2"; shift 2 ;;
	--days) [ "$#" -ge 2 ] || { usage; exit 2; }; DAYS="$2"; shift 2 ;;
	--window-days) [ "$#" -ge 2 ] || { usage; exit 2; }; WINDOW_DAYS="$2"; shift 2 ;;
	--message-file) [ "$#" -ge 2 ] || { usage; exit 2; }; MESSAGE_FILE="$2"; shift 2 ;;
	--postgres-container) [ "$#" -ge 2 ] || { usage; exit 2; }; PG_CONTAINER="$2"; shift 2 ;;
	--dry-run) DRY_RUN=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) usage; exit 2 ;;
	esac
done

# ---------------------------------------------------------------------------
# Проверка параметров.
# ---------------------------------------------------------------------------

[[ "$CAMPAIGN" =~ ^[a-z0-9][a-z0-9._:-]{0,63}$ ]] || { usage; die "неверный или пустой --campaign"; }
[[ "$DAYS" =~ ^[0-9]{1,3}$ ]] && [ "$DAYS" -ge 1 ] && [ "$DAYS" -le 366 ] || die "--days: целое от 1 до 366"
[[ "$WINDOW_DAYS" =~ ^[0-9]{1,3}$ ]] && [ "$WINDOW_DAYS" -ge 1 ] && [ "$WINDOW_DAYS" -le 365 ] || die "--window-days: целое от 1 до 365"
command -v docker >/dev/null 2>&1 || die "нужен docker"
command -v python3 >/dev/null 2>&1 || die "нужен python3"

if [ -n "$MESSAGE_FILE" ]; then
	[ -f "$MESSAGE_FILE" ] && [ -r "$MESSAGE_FILE" ] || die "нет файла $MESSAGE_FILE"
	MESSAGE="$(cat -- "$MESSAGE_FILE")"
else
	MESSAGE="$DEFAULT_MESSAGE"
fi
# Те же правила, что у user-subscription-service (ValidateGrantBonusCommand):
# непустой UTF-8 не длиннее 3500 символов.
printf '%s' "$MESSAGE" | python3 -I -c '
import sys
raw = sys.stdin.buffer.read()
try:
    text = raw.decode("utf-8")
except UnicodeDecodeError:
    sys.exit("сообщение не в UTF-8")
if not text.strip():
    sys.exit("сообщение пустое")
if len(text) > 3500:
    sys.exit("сообщение длиннее 3500 символов")
' || die "сообщение не прошло проверку"

# ---------------------------------------------------------------------------
# Контейнер PostgreSQL проекта и доступ к БД (без паролей: локальный сокет
# внутри контейнера, пользователь и база — из окружения контейнера).
# ---------------------------------------------------------------------------

if [ -z "$PG_CONTAINER" ]; then
	mapfile -t found < <(docker ps \
		--filter "label=com.docker.compose.service=postgres" \
		--filter "label=com.docker.compose.project.working_dir=$REPO_ROOT/deploy" \
		--format '{{.Names}}')
	[ "${#found[@]}" -eq 1 ] || die "не найден ровно один запущенный контейнер postgres этого проекта (найдено: ${#found[@]}). Укажите --postgres-container ИМЯ."
	PG_CONTAINER="${found[0]}"
fi
docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null | grep -qx true || die "контейнер $PG_CONTAINER не запущен"
PG_USER="$(docker exec "$PG_CONTAINER" printenv POSTGRES_USER 2>/dev/null || true)"
PG_DB="$(docker exec "$PG_CONTAINER" printenv POSTGRES_DB 2>/dev/null || true)"
[ -n "$PG_USER" ] || die "в контейнере $PG_CONTAINER не задан POSTGRES_USER"
[ -n "$PG_DB" ] || PG_DB="$PG_USER"

psql_run() { # SQL со stdin; переменные psql — аргументами
	docker exec -i "$PG_CONTAINER" psql -X -q -v ON_ERROR_STOP=1 -U "$PG_USER" -d "$PG_DB" -At -F $'\t' \
		-v campaign="$CAMPAIGN" -v days="$DAYS" -v window="$WINDOW_DAYS" "$@"
}

# Схема на месте: журнал с источником admin, outbox, таблицы оплат.
preflight="$(psql_run <<'SQL'
SELECT
  (SELECT count(*) FROM pg_constraint WHERE conname = 'chk_subscription_grants_source'
     AND pg_get_constraintdef(oid) LIKE '%admin%'),
  (SELECT count(*) FROM information_schema.columns WHERE table_name = 'event_outbox' AND column_name = 'locked_at'),
  (SELECT count(*) FROM information_schema.tables WHERE table_name IN ('payments', 'crypto_invoices', 'user_subscriptions',
                                                                         'processed_messages', 'billing_recurring_profiles'));
SQL
)" || die "не удалось выполнить запрос к БД в контейнере $PG_CONTAINER"
[ "$preflight" = $'1\t1\t5' ] || die "схема БД не соответствует обновлённой версии (миграции из поставки применены?): $preflight"

# ---------------------------------------------------------------------------
# Выборка получателей (одинаковая для предпросмотра и постановки команд).
# ---------------------------------------------------------------------------

read -r -d '' ELIGIBLE_SQL <<'SQL' || true
churned AS (
  SELECT s.telegram_id,
         CASE WHEN p.telegram_id IS NOT NULL THEN 'paid' ELSE 'trial' END AS category,
         s.ended_at
  FROM (
    SELECT us.telegram_id, us.trial_used,
           GREATEST(us.expires_at, COALESCE(us.grace_until, us.expires_at)) AS ended_at
    FROM user_subscriptions us
    WHERE us.status <> 'none' AND us.expires_at IS NOT NULL
  ) s
  LEFT JOIN (
    SELECT telegram_id FROM subscription_grants WHERE source = 'paid'
    UNION SELECT telegram_id FROM payments WHERE status = 'succeeded' AND checkout_type = 'subscription_purchase'
    UNION SELECT telegram_id FROM crypto_invoices WHERE status = 'paid'
    UNION SELECT telegram_id FROM user_subscriptions WHERE COALESCE(last_payment_id, '') <> ''
  ) p ON p.telegram_id = s.telegram_id
  WHERE s.ended_at >= now() - make_interval(days => :window)
    AND s.ended_at <= now()
    AND (p.telegram_id IS NOT NULL OR s.trial_used)
    AND NOT EXISTS (
      SELECT 1 FROM subscription_grants g
      WHERE g.telegram_id = s.telegram_id AND g.source = 'admin' AND g.business_key = :'campaign')
),
-- Биллинг ещё может изменить подписку — то же правило, что у обработчика
-- (billingRenewalLiveTx): такие откладываются.
billing_live AS (
  SELECT c.telegram_id FROM churned c JOIN billing_recurring_profiles b USING (telegram_id)
  WHERE NOT (b.status IN ('expired', 'disabled')
             OR (b.status = 'active' AND NOT (b.auto_renew_enabled AND b.payment_method_id <> ''
                                              AND b.next_charge_at IS NOT NULL)))
),
eligible AS (
  SELECT c.* FROM churned c WHERE NOT EXISTS (SELECT 1 FROM billing_live l WHERE l.telegram_id = c.telegram_id)
)
SQL

CAMPAIGN_DIR="${HOME:-/root}/bonus-campaigns/$CAMPAIGN"
mkdir -p -- "$CAMPAIGN_DIR"
chmod 700 -- "$CAMPAIGN_DIR" "${HOME:-/root}/bonus-campaigns" 2>/dev/null || true
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TARGETS="$CAMPAIGN_DIR/targets-$STAMP.tsv"

{
	printf 'BEGIN READ ONLY;\nWITH %s\n' "$ELIGIBLE_SQL"
	cat <<'SQL'
SELECT telegram_id, category, to_char(ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI')
FROM eligible
ORDER BY telegram_id;
COMMIT;
SQL
} | psql_run >"$TARGETS" || die "не удалось получить список получателей"
chmod 600 -- "$TARGETS"
deferred_billing="$(printf 'BEGIN READ ONLY;\nWITH %s\nSELECT count(*) FROM billing_live;\nCOMMIT;\n' "$ELIGIBLE_SQL" | psql_run)" ||
	die "не удалось посчитать отложенных"

total="$(grep -c . "$TARGETS" || true)"
paid_n="$(awk -F'\t' '$2=="paid"' "$TARGETS" | grep -c . || true)"
trial_n="$(awk -F'\t' '$2=="trial"' "$TARGETS" | grep -c . || true)"
window_from="$(date -u -d "-$WINDOW_DAYS days" '+%d.%m.%Y %H:%M' 2>/dev/null || true)"

say "Кампания: $CAMPAIGN; начисление: $DAYS дн. с момента обработки; окно: последние $WINDOW_DAYS дн.${window_from:+ (с $window_from UTC)}"
say "Получателей: $total (платная закончилась и не продлена: $paid_n; закончился пробный, оплат не было: $trial_n)"
say "Список (telegram_id, категория, окончание доступа UTC): $TARGETS"
if [ "$deferred_billing" -gt 0 ]; then
	say "Отложено (в биллинге ещё действует автопродление): $deferred_billing — сейчас не начисляется; запустите"
	say "  кампанию с тем же --campaign позже: биллинг завершает повторы и льготный период за ~5 суток."
fi
say ""
say "Сообщение (пример: дата окончания подставится фактическая):"
say "-----"
example_until="$(date -u -d "+$DAYS days" '+%d.%m.%Y' 2>/dev/null || echo 'ДД.ММ.ГГГГ')"
printf '%s' "$MESSAGE" | DAYS_TEXT="$DAYS" UNTIL="$example_until" python3 -I -c '
import os, sys
n = int(os.environ["DAYS_TEXT"])
word = "дней" if 11 <= n % 100 <= 14 else ("день" if n % 10 == 1 else ("дня" if 2 <= n % 10 <= 4 else "дней"))
text = sys.stdin.read().replace("{days}", f"{n} {word}").replace("{until}", os.environ["UNTIL"])
sys.stderr.write(text + "\n")
'
say "-----"

if [ "$DRY_RUN" = 1 ]; then
	say "Режим --dry-run: ничего не начислено и не отправлено."
	exit 0
fi
if [ "$total" -eq 0 ]; then
	say "Получателей нет — делать нечего."
	exit 0
fi

# Защита от одновременного запуска одной кампании.
exec 9>"$CAMPAIGN_DIR/.lock"
command -v flock >/dev/null 2>&1 && { flock -n 9 || die "кампания $CAMPAIGN уже выполняется в другом окне"; }

[ -t 0 ] && printf '%s' "Чтобы начислить $DAYS дн. и отправить сообщение $total получателям, введите число $total: " >&2
if ! IFS= read -r answer; then
	say ""
	die "ввод прерван — ничего не сделано"
fi
[ "$answer" = "$total" ] || die "введено «$answer», ожидалось $total — ничего не сделано"

# ---------------------------------------------------------------------------
# Постановка команд: только из сохранённого списка и только тем, кто всё ещё
# подходит (за время подтверждения кто-то мог продлить подписку).
# ---------------------------------------------------------------------------

ids=()
while IFS=$'\t' read -r tg _; do
	[[ "$tg" =~ ^[0-9]{1,19}$ ]] || die "некорректная строка в $TARGETS"
	ids+=("$tg")
done <"$TARGETS"

queue_ids() { # ID... → печатает поставленные в очередь telegram_id
	local values="" id
	for id in "$@"; do
		values+="${values:+,}($id)"
	done
	{
		printf 'BEGIN;\n'
		printf 'CREATE TEMP TABLE bonus_frozen (telegram_id bigint PRIMARY KEY) ON COMMIT DROP;\n'
		printf 'INSERT INTO bonus_frozen VALUES %s;\n' "$values"
		printf 'WITH %s,\n' "$ELIGIBLE_SQL"
		cat <<'SQL'
todo AS (
  SELECT e.telegram_id, row_number() OVER (ORDER BY e.telegram_id) AS rn
  FROM eligible e JOIN bonus_frozen f USING (telegram_id)
  WHERE NOT EXISTS (
    SELECT 1 FROM event_outbox o
    WHERE o.topic = 'subscription.commands' AND o.event_type = 'subscription.grant_bonus'
      AND o.message_key = e.telegram_id::text AND o.payload->>'campaign' = :'campaign'
      AND o.status IN ('pending', 'retry', 'processing'))
)
INSERT INTO event_outbox (aggregate_type, aggregate_id, topic, message_key, event_type, payload, next_attempt_at)
SELECT 'subscription', telegram_id::text, 'subscription.commands', telegram_id::text, 'subscription.grant_bonus',
       jsonb_build_object(
         'type', 'subscription.grant_bonus',
         'command_id', 'bonus:' || :'campaign' || ':' || telegram_id || ':' || :'run',
         'telegram_id', telegram_id,
         'days', :days,
         'campaign', :'campaign',
         'message', :'message'),
       now() + (rn - 1) * make_interval(secs => :stagger / 1000.0)
FROM todo
RETURNING message_key;
COMMIT;
SQL
	} | psql_run -v message="$MESSAGE" -v stagger="$STAGGER_MS" -v run="$STAMP"
}

progress() { # → обработано<TAB>начислено среди поставленных в очередь
	local values="" id
	for id in "${queued_ids[@]}"; do
		values+="${values:+,}($id)"
	done
	{
		printf 'WITH t(telegram_id) AS (VALUES %s)\n' "$values"
		cat <<'SQL'
SELECT
  (SELECT count(*) FROM t JOIN processed_messages pm
     ON pm.message_id = 'user-subscription-service:bonus:' || :'campaign' || ':' || t.telegram_id || ':' || :'run'),
  (SELECT count(*) FROM t JOIN subscription_grants g
     ON g.telegram_id = t.telegram_id AND g.source = 'admin' AND g.business_key = :'campaign');
SQL
	} | psql_run -v run="$STAMP"
}

started_at="$(date -u '+%Y-%m-%d %H:%M:%S')"
queued_ids=()
say ""

# Пробная команда: первый из списка, кто всё ещё подходит.
canary_id=""
idx=0
while [ "$idx" -lt "${#ids[@]}" ]; do
	out="$(queue_ids "${ids[$idx]}")" || die "не удалось поставить пробную команду"
	idx=$((idx + 1))
	if [ -n "$out" ]; then
		canary_id="$out"
		break
	fi
	say "  telegram_id ${ids[$((idx - 1))]} уже не подходит (продлил подписку или бонус уже начислен) — пропущен"
done
if [ -z "$canary_id" ]; then
	say "Ни один получатель из списка больше не подходит — ничего не поставлено."
	exit 0
fi
queued_ids+=("$canary_id")
say "Пробная команда: telegram_id $canary_id — жду обработки (до ${CANARY_TIMEOUT} с)…"
deadline=$((SECONDS + CANARY_TIMEOUT))
while :; do
	state="$(printf "SELECT
  (SELECT count(*) FROM processed_messages WHERE message_id = 'user-subscription-service:bonus:' || :'campaign' || ':%s:' || :'run'),
  (SELECT count(*) FROM subscription_grants WHERE telegram_id = %s AND source = 'admin' AND business_key = :'campaign'),
  COALESCE((SELECT status FROM event_outbox WHERE topic = 'subscription.commands' AND message_key = '%s'
              AND payload->>'campaign' = :'campaign' ORDER BY id DESC LIMIT 1), '-');\n" "$canary_id" "$canary_id" "$canary_id" | psql_run -v run="$STAMP")" || state=""
	IFS=$'\t' read -r c_processed c_granted c_outbox <<<"$state"
	if [ "${c_processed:-0}" -ge 1 ] || [ "${c_granted:-0}" -ge 1 ]; then
		break
	fi
	if [ "$SECONDS" -ge "$deadline" ]; then
		say "Пробная команда не обработана за ${CANARY_TIMEOUT} с (статус в outbox: ${c_outbox:--})."
		say "  pending/retry — не работают публикаторы outbox (сервисы не запущены?);"
		say "  published     — user-subscription-service не принял команду: обновлён ли он до версии"
		say "                  с subscription.grant_bonus? Смотрите его журнал и топик subscription.commands.dlt."
		die "остальные команды НЕ поставлены"
	fi
	sleep 2
done
if [ "${c_granted:-0}" -ge 1 ]; then
	say "Пробная команда обработана: бонус начислен."
else
	say "Пробная команда обработана без начисления (льготный период или автопродление в биллинге) — конвейер работает."
fi

rest=("${ids[@]:$idx}")
if [ "${#rest[@]}" -gt 0 ]; then
	rest_out="$(queue_ids "${rest[@]}")" || die "не удалось поставить остальные команды (пробная уже выполнена; запустите скрипт ещё раз с тем же --campaign)"
	while IFS= read -r line; do
		[[ "$line" =~ ^[0-9]{1,19}$ ]] && queued_ids+=("$line")
	done <<<"$rest_out"
fi
queued="${#queued_ids[@]}"
skipped=$((total - queued))
say "Поставлено команд: $queued (по одной в ${STAGGER_MS} мс); не подходят на момент постановки: $skipped. Жду обработки…"

limit=$((180 + (queued * STAGGER_MS) / 1000 * 2))
deadline=$((SECONDS + limit))
processed=0
granted=0
while :; do
	if out="$(progress)"; then
		IFS=$'\t' read -r processed granted <<<"$out"
	fi
	say "  обработано: $processed из $queued; начислено: $granted"
	[ "$processed" -ge "$queued" ] && break
	[ "$SECONDS" -ge "$deadline" ] && break
	sleep 10
done

notif="$(
	{
		values=""
		for id in "${queued_ids[@]}"; do values+="${values:+,}('$id')"; done
		printf "SELECT COALESCE(string_agg(status || ': ' || n, ', '), 'нет') FROM (\n"
		printf "  SELECT status, count(*) AS n FROM event_outbox\n"
		printf "  WHERE topic = 'tg.notifications' AND created_at >= '%s'::timestamptz AND message_key IN (%s)\n" "$started_at+00" "$values"
		printf "  GROUP BY status) s;\n"
	} | psql_run
)" || notif="не удалось посчитать"

summary="$CAMPAIGN_DIR/summary-$STAMP.txt"
{
	printf 'campaign=%s days=%s window_days=%s started_at_utc=%s\n' "$CAMPAIGN" "$DAYS" "$WINDOW_DAYS" "$started_at"
	printf 'recipients=%s paid=%s trial=%s deferred_billing=%s queued=%s skipped=%s processed=%s granted=%s\n' "$total" "$paid_n" "$trial_n" "$deferred_billing" "$queued" "$skipped" "$processed" "$granted"
	printf 'notifications_outbox=%s\n' "$notif"
} >"$summary"
chmod 600 -- "$summary"

say ""
say "Итог: в списке $total, поставлено $queued, обработано $processed, начислено $granted."
say "Сообщения в канале уведомлений (outbox → tg.notifications): $notif"
say "Отчёт: $summary"
if [ "$processed" -lt "$queued" ]; then
	say "Обработаны не все команды. Повторный запуск с тем же --campaign безопасен: начисленным повторно"
	say "ничего не будет, оставшимся команды поставятся заново."
	exit 3
fi
if [ "$granted" -lt "$processed" ]; then
	say "Часть пользователей обработана без начисления (льготный период или автопродление в биллинге) — им"
	say "сообщение не отправлялось. Повторный запуск с тем же --campaign позже начислит им, если они не продлят сами."
fi
