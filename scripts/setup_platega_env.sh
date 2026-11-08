#!/usr/bin/env bash
# Интерактивная настройка ключей Platega в .env проекта.
#
# Запуск (из корня репозитория или откуда угодно):
#
#   ./scripts/setup_platega_env.sh                 # .env в корне репозитория
#   ./scripts/setup_platega_env.sh --env-file PATH # другой файл
#
# Обычно .env уже есть (его читает docker compose --env-file .env). Если файла
# нет, скрипт предлагает создать его из шаблона deploy/.env.example (как
# «cp deploy/.env.example .env» из README) и только по явному ответу «y».
#
# Что делает:
#   - спрашивает PLATEGA_MERCHANT_ID, PLATEGA_SECRET (ввод скрыт),
#     PLATEGA_PAYMENT_METHOD, PLATEGA_RETURN_URL, PLATEGA_FAILED_URL;
#     пустой ввод (Enter) оставляет текущее значение;
#   - проверяет формат каждого значения;
#   - собирает новый .env во временном файле, сохраняя все остальные строки
#     без изменений, и проверяет его настоящим парсером Docker Compose
#     (значения сравниваются по SHA-256 и не выводятся);
#   - только после успешной проверки делает резервную копию
#     .env.bak-ГГГГММДД-ЧЧММСС и атомарно заменяет .env;
#   - права 600 (640, если у исходного файла было чтение группой), владелец
#     исходного файла сохраняется.
#
# Чего НЕ делает: не выводит секреты, не передаёт их в аргументах команд, не
# пишет в историю shell, не перезапускает сервисы, не выполняет миграции и
# не делает деплой. Ctrl+C или Ctrl+D на любом шаге — выход без изменений.
# Права root не нужны: достаточно права записи в каталог .env.

set +x
set -Eeuo pipefail
umask 077

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
	printf '%s\n' "ОШИБКА: нужен bash 4 или новее (на сервере Ubuntu он есть)." >&2
	exit 1
fi

readonly SCRIPT_NAME="setup_platega_env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/deploy/docker-compose.dev.yml"
TEMPLATE_FILE="$REPO_ROOT/deploy/.env.example"
ENV_FILE="$REPO_ROOT/.env"
CREATE_NEW=0
NOCHANGE_MSG="Файл .env не изменён."

MANAGED_KEYS=(PLATEGA_MERCHANT_ID PLATEGA_SECRET PLATEGA_PAYMENT_METHOD PLATEGA_RETURN_URL PLATEGA_FAILED_URL)

TMP_FILE=""
LOCK_DIR=""
LOCK_FD_OPEN=0
TTY_ECHO_OFF=0

say() { printf '%s\n' "$*" >&2; }
die() {
	say "ОШИБКА: $*"
	exit 1
}

cleanup() {
	if [ "$TTY_ECHO_OFF" = 1 ] && [ -t 0 ]; then
		stty echo 2>/dev/null || true
	fi
	if [ -n "$TMP_FILE" ] && [ -e "$TMP_FILE" ]; then
		rm -f -- "$TMP_FILE"
	fi
	if [ "$LOCK_FD_OPEN" = 1 ]; then
		exec 9>&- || true
	fi
	if [ -n "$LOCK_DIR" ] && [ -d "$LOCK_DIR" ]; then
		rmdir -- "$LOCK_DIR" 2>/dev/null || true
	fi
}
on_interrupt() {
	say ""
	say "Прервано. $NOCHANGE_MSG"
	exit 130
}
trap cleanup EXIT
trap on_interrupt INT TERM HUP

usage() {
	say "Использование: $SCRIPT_NAME [--env-file PATH]"
	say "Секреты в аргументах не принимаются — скрипт спросит их сам."
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--env-file)
		[ "$#" -ge 2 ] || {
			usage
			exit 2
		}
		ENV_FILE="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage
		exit 2
		;;
	esac
done

# ---------------------------------------------------------------------------
# Проверки окружения.
# ---------------------------------------------------------------------------

if [ -L "$ENV_FILE" ]; then
	target="$(readlink -f -- "$ENV_FILE" 2>/dev/null || true)"
	[ -n "$target" ] && [ -f "$target" ] || die "$ENV_FILE — символическая ссылка на несуществующий файл"
	ENV_FILE="$target"
fi
if [ ! -e "$ENV_FILE" ]; then
	[ -d "$(dirname -- "$ENV_FILE")" ] || die "каталог для $ENV_FILE не существует"
	[ -f "$TEMPLATE_FILE" ] || die "файл $ENV_FILE не найден, и нет шаблона $TEMPLATE_FILE"
	say "Файл $ENV_FILE не найден."
	say "Проект читает настройки из него (docker compose --env-file .env …). Если .env лежит"
	say "в другом месте, прервите (Enter) и запустите скрипт с --env-file ПУТЬ."
	say "Можно создать новый файл из шаблона deploy/.env.example и сразу записать в него ключи"
	say "Platega. Остальные значения шаблона (пароли change_me, PUBLIC_DOMAIN=example.com и др.)"
	say "нужно будет заменить до запуска сервисов."
	[ -t 0 ] && printf '%s' "Создать $ENV_FILE из шаблона? [y/N]: " >&2
	if ! IFS= read -r answer; then
		say ""
		say "Ввод прерван. Файл .env не создан."
		exit 130
	fi
	case "$answer" in
	y | Y | yes | YES | д | Д | да | Да) CREATE_NEW=1 ;;
	*)
		say "Файл .env не создан."
		exit 1
		;;
	esac
	NOCHANGE_MSG="Файл .env не создан."
fi
ENV_DIR="$(cd "$(dirname -- "$ENV_FILE")" && pwd)"
ENV_FILE="$ENV_DIR/$(basename -- "$ENV_FILE")"
[ -w "$ENV_DIR" ] || die "нет права записи в каталог $ENV_DIR (нужно для атомарной замены .env)"
if [ "$CREATE_NEW" = 1 ]; then
	SOURCE_FILE="$TEMPLATE_FILE"
else
	SOURCE_FILE="$ENV_FILE"
	[ -r "$ENV_FILE" ] || die "нет права чтения $ENV_FILE"
	[ -w "$ENV_FILE" ] || die "нет права записи в $ENV_FILE"
fi
[ -r "$SOURCE_FILE" ] || die "нет права чтения $SOURCE_FILE"
command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 || die "нужна утилита sha256sum или shasum"

# Блокировка от одновременного запуска.
if command -v flock >/dev/null 2>&1; then
	exec 9>"$ENV_DIR/.env.lock"
	LOCK_FD_OPEN=1
	flock -n 9 || die "скрипт уже запущен для этого .env (занят $ENV_DIR/.env.lock)"
else
	if mkdir -- "$ENV_DIR/.env.lock.d" 2>/dev/null; then
		LOCK_DIR="$ENV_DIR/.env.lock.d"
	else
		die "скрипт уже запущен для этого .env (существует $ENV_DIR/.env.lock.d; если запуска нет — удалите каталог)"
	fi
fi

# ---------------------------------------------------------------------------
# Разбор текущего .env (только нужные ключи).
# ---------------------------------------------------------------------------

key_regex() { printf '^[[:space:]]*(export[[:space:]]+)?%s[[:space:]]*=(.*)$' "$1"; }
declare -A KEY_RE=()
for k in "${MANAGED_KEYS[@]}" PUBLIC_DOMAIN; do
	KEY_RE[$k]="$(key_regex "$k")"
done

# unquote_value — значение строки KEY=VALUE в простом виде dotenv: снимает
# одинарные/двойные кавычки и комментарий после пробела у значений без кавычек.
unquote_value() {
	local v="$1"
	v="${v#"${v%%[![:space:]]*}"}"
	if [[ "$v" == \'* ]]; then
		v="${v#\'}"
		v="${v%%\'*}"
	elif [[ "$v" == \"* ]]; then
		v="${v#\"}"
		v="${v%%\"*}"
	else
		v="${v%%[[:space:]]#*}"
		v="${v%"${v##*[![:space:]]}"}"
	fi
	printf '%s' "$v"
}

declare -A CURRENT=()
while IFS= read -r line || [ -n "$line" ]; do
	for k in "${MANAGED_KEYS[@]}" PUBLIC_DOMAIN; do
		re="${KEY_RE[$k]}"
		if [[ "$line" =~ $re ]]; then
			CURRENT[$k]="$(unquote_value "${BASH_REMATCH[2]}")"
		fi
	done
done <"$SOURCE_FILE"

# ---------------------------------------------------------------------------
# Проверки значений.
# ---------------------------------------------------------------------------

# Символы, которые нельзя однозначно записать в .env в одинарных кавычках
# (парсер Compose трактует \' особо) и которые почти наверняка — ошибка
# копирования: кавычка, обратная косая черта, пробелы, управляющие символы.
safe_chars() {
	local v="$1"
	[[ "$v" != *"'"* ]] || return 1
	[[ "$v" != *\\* ]] || return 1
	[[ ! "$v" =~ [[:space:]] ]] || return 1
	[[ ! "$v" =~ [[:cntrl:]] ]] || return 1
	return 0
}
valid_uuid() { [[ "$1" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; }
valid_method() { [[ "$1" =~ ^(2|3|11|12|13|14)$ ]]; }
valid_url() {
	[ -z "$1" ] && return 0
	safe_chars "$1" || return 1
	[[ "$1" =~ ^https://[^/?#]+ ]] || return 1
	[ "${#1}" -le 2048 ]
}
valid_secret() {
	[ -n "$1" ] && [ "${#1}" -le 512 ] && safe_chars "$1"
}

# ask KEY PROMPT HIDDEN VALIDATOR HINT — спрашивает значение до трёх раз.
# Пустой ввод оставляет текущее значение. Ctrl+D — выход без изменений.
declare -A NEW=()
ask() {
	local key="$1" prompt="$2" hidden="$3" validator="$4" hint="$5"
	local cur="${CURRENT[$key]:-}" state value attempt
	if [ -n "$cur" ]; then
		if [ "$hidden" = 1 ] || [ "$key" = PLATEGA_MERCHANT_ID ]; then
			state="сейчас задано; Enter — оставить"
		else
			state="сейчас: $cur; Enter — оставить"
		fi
	else
		state="сейчас не задано"
	fi
	for attempt in 1 2 3; do
		if [ -t 0 ]; then
			printf '%s (%s): ' "$prompt" "$state" >&2
		fi
		if [ "$hidden" = 1 ]; then
			if [ -t 0 ]; then
				TTY_ECHO_OFF=1
			fi
			if ! IFS= read -r -s value; then
				say ""
				say "Ввод прерван (Ctrl+D). $NOCHANGE_MSG"
				exit 130
			fi
			TTY_ECHO_OFF=0
			[ -t 0 ] && say ""
		else
			if ! IFS= read -r value; then
				say ""
				say "Ввод прерван (Ctrl+D). $NOCHANGE_MSG"
				exit 130
			fi
		fi
		# Пробелы по краям — почти всегда хвост копирования.
		value="${value#"${value%%[![:space:]]*}"}"
		value="${value%"${value##*[![:space:]]}"}"
		if [ -z "$value" ]; then
			if [ -n "$cur" ]; then
				NEW[$key]="$cur"
				return 0
			fi
			if [ "$validator" = valid_url ]; then
				NEW[$key]=""
				return 0
			fi
			say "  Значение обязательно. $hint"
			continue
		fi
		if "$validator" "$value"; then
			NEW[$key]="$value"
			return 0
		fi
		say "  Неверный формат. $hint"
	done
	die "$key: три неудачные попытки ввода. $NOCHANGE_MSG"
}

say "Настройка Platega в $ENV_FILE"
say "Значения секретов не выводятся. Ctrl+C — выход без изменений."
say ""

ask PLATEGA_MERCHANT_ID "ID мерчанта Platega (UUID из ЛК)" 0 valid_uuid \
	"Нужен UUID вида xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx."
ask PLATEGA_SECRET "API-ключ Platega (ввод скрыт)" 1 valid_secret \
	"Ключ не может быть пустым, длиннее 512 символов и содержать пробелы, кавычку ' или обратную косую черту."
if [ -z "${CURRENT[PLATEGA_PAYMENT_METHOD]:-}" ]; then
	CURRENT[PLATEGA_PAYMENT_METHOD]=2
fi
ask PLATEGA_PAYMENT_METHOD "Способ оплаты Platega: 2 СБП QR, 3 ЕРИП, 11 карты, 12 международная, 13 крипто, 14 Sberpay" 0 valid_method \
	"Допустимы только документированные коды: 2, 3, 11, 12, 13, 14."
ask PLATEGA_RETURN_URL "Адрес возврата после оплаты, https://… (необязательно)" 0 valid_url \
	"Нужен адрес, начинающийся с https://, без пробелов и кавычек, или пусто."
ask PLATEGA_FAILED_URL "Адрес возврата при неуспешной оплате, https://… (необязательно)" 0 valid_url \
	"Нужен адрес, начинающийся с https://, без пробелов и кавычек, или пусто."

# ---------------------------------------------------------------------------
# Сборка нового файла во временном файле рядом с .env.
# ---------------------------------------------------------------------------

TMP_FILE="$(mktemp "$ENV_DIR/.env.tmp.XXXXXX")"

declare -A WRITTEN=()
dups=0
while IFS= read -r line || [ -n "$line" ]; do
	handled=0
	for k in "${MANAGED_KEYS[@]}"; do
		re="${KEY_RE[$k]}"
		if [[ "$line" =~ $re ]]; then
			handled=1
			if [ -z "${WRITTEN[$k]:-}" ]; then
				printf "%s='%s'\n" "$k" "${NEW[$k]}" >>"$TMP_FILE"
				WRITTEN[$k]=1
			else
				dups=$((dups + 1))
			fi
			break
		fi
	done
	if [ "$handled" = 0 ]; then
		printf '%s\n' "$line" >>"$TMP_FILE"
	fi
done <"$SOURCE_FILE"

missing=()
for k in "${MANAGED_KEYS[@]}"; do
	[ -n "${WRITTEN[$k]:-}" ] || missing+=("$k")
done
if [ "${#missing[@]}" -gt 0 ]; then
	{
		printf '\n# Platega («Способ 1 - рекомендуемый»), записано %s\n' "$SCRIPT_NAME"
		for k in "${missing[@]}"; do
			printf "%s='%s'\n" "$k" "${NEW[$k]}"
		done
	} >>"$TMP_FILE"
fi

# ---------------------------------------------------------------------------
# Проверка настоящим парсером Docker Compose (до замены .env).
# ---------------------------------------------------------------------------

hash_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		printf '%s' "$1" | sha256sum | cut -d' ' -f1
	else
		printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1
	fi
}

compose_check() {
	# Чистое окружение: переменные, экспортированные в shell оператора, не
	# должны подменять значения из файла.
	local json_hashes
	if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
		say "Предупреждение: docker compose недоступен — проверка парсером Compose пропущена."
		return 0
	fi
	if ! command -v python3 >/dev/null 2>&1; then
		say "Предупреждение: нет python3 — проверка парсером Compose пропущена."
		return 0
	fi
	if ! json_hashes="$(env -i PATH="$PATH" HOME="${HOME:-/root}" ${DOCKER_HOST:+DOCKER_HOST="$DOCKER_HOST"} \
		docker compose --env-file "$TMP_FILE" -f "$COMPOSE_FILE" config --format json 2>/dev/null |
		python3 -I -c '
import hashlib, json, sys
cfg = json.load(sys.stdin)
env = cfg["services"]["billing-service"].get("environment") or {}
for k in ("PLATEGA_MERCHANT_ID", "PLATEGA_SECRET", "PLATEGA_PAYMENT_METHOD", "PLATEGA_RETURN_URL", "PLATEGA_FAILED_URL"):
    v = env.get(k)
    # В выводе config буквальный «$» экранирован как «$$»; в окружение
    # контейнера попадает одиночный «$».
    v = "" if v is None else str(v).replace("$$", "$")
    print(k + "=" + hashlib.sha256(v.encode()).hexdigest())
')"; then
		say "Docker Compose не смог разобрать новый .env вместе с deploy/docker-compose.dev.yml."
		say "Частая причина — в .env не хватает обязательных переменных проекта. $NOCHANGE_MSG"
		return 1
	fi
	local k want got
	for k in "${MANAGED_KEYS[@]}"; do
		want="$(hash_of "${NEW[$k]}")"
		got="$(printf '%s\n' "$json_hashes" | sed -n "s/^$k=//p")"
		if [ "$want" != "$got" ]; then
			say "Docker Compose прочитал $k не так, как он был введён. $NOCHANGE_MSG"
			return 1
		fi
	done
	say "Проверка Docker Compose: значения в billing-service совпадают с введёнными."
	return 0
}

compose_check || exit 1

# ---------------------------------------------------------------------------
# Права, резервная копия, атомарная замена.
# ---------------------------------------------------------------------------

file_mode() { stat -c '%a' -- "$1" 2>/dev/null || stat -f '%Lp' -- "$1"; }
file_owner() { stat -c '%u:%g' -- "$1" 2>/dev/null || stat -f '%u:%g' -- "$1"; }

new_mode=600
backup=""
if [ "$CREATE_NEW" = 1 ]; then
	chmod 600 -- "$TMP_FILE"
	# Под root новый файл отдаётся владельцу каталога проекта, иначе
	# пользователь деплоя не сможет прочитать собственный .env.
	if [ "$(id -u)" = 0 ]; then
		chown "$(file_owner "$ENV_DIR")" -- "$TMP_FILE"
	fi
	sync 2>/dev/null || true
	# ln не перезаписывает существующий файл: если .env появился, пока шёл
	# ввод, он останется нетронутым.
	ln -- "$TMP_FILE" "$ENV_FILE" 2>/dev/null || die "$ENV_FILE появился во время работы скрипта — запустите скрипт ещё раз. Файл .env не создан."
	rm -f -- "$TMP_FILE"
	TMP_FILE=""
	sync 2>/dev/null || true
else
	orig_mode="$(file_mode "$ENV_FILE")"
	# Чтение группой сохраняем (группа деплоя), чтение всеми — никогда.
	group_digit="${orig_mode: -2:1}"
	case "$group_digit" in
	4 | 5 | 6 | 7) new_mode=640 ;;
	esac
	chmod "$new_mode" -- "$TMP_FILE"
	if [ "$(id -u)" = 0 ]; then
		chown "$(file_owner "$ENV_FILE")" -- "$TMP_FILE"
	fi

	stamp="$(date +%Y%m%d-%H%M%S)"
	backup="$ENV_DIR/.env.bak-$stamp"
	n=1
	while [ -e "$backup" ]; do
		backup="$ENV_DIR/.env.bak-$stamp-$n"
		n=$((n + 1))
	done
	cp -p -- "$ENV_FILE" "$backup"
	chmod 600 -- "$backup"

	sync 2>/dev/null || true
	mv -f -- "$TMP_FILE" "$ENV_FILE"
	TMP_FILE=""
	sync 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Итог без секретов.
# ---------------------------------------------------------------------------

mask_id() {
	local v="$1"
	[ "${#v}" -ge 12 ] && printf '%s…%s' "${v:0:4}" "${v: -4}" || printf 'задан'
}
say ""
if [ "$CREATE_NEW" = 1 ]; then
	say "Готово. Создан новый файл из шаблона: $ENV_FILE (права 600)"
	say "Замените в нём значения шаблона (change_me, example.com и др.) до запуска сервисов."
else
	say "Готово. Файл обновлён: $ENV_FILE (права $new_mode)"
	say "Резервная копия: $backup (права 600)"
fi
[ "$dups" -gt 0 ] && say "Удалено дублирующихся строк Platega: $dups (действовала бы последняя из них)."
say "  PLATEGA_MERCHANT_ID    = $(mask_id "${NEW[PLATEGA_MERCHANT_ID]}")"
say "  PLATEGA_SECRET         = задан (значение не выводится)"
say "  PLATEGA_PAYMENT_METHOD = ${NEW[PLATEGA_PAYMENT_METHOD]}"
say "  PLATEGA_RETURN_URL     = ${NEW[PLATEGA_RETURN_URL]:-не задан (используется адрес из команды бота)}"
say "  PLATEGA_FAILED_URL     = ${NEW[PLATEGA_FAILED_URL]:-не задан (используется адрес возврата)}"
domain="${CURRENT[PUBLIC_DOMAIN]:-}"
if [ "$CREATE_NEW" = 0 ] && [ -n "$domain" ] && [[ "$domain" != *'$'* ]]; then
	say ""
	say "Адрес колбэка для ЛК Platega (Настройки → Callback URLs):"
	say "  https://$domain/webhooks/platega"
else
	say ""
	say "Адрес колбэка для ЛК Platega (Настройки → Callback URLs): https://<PUBLIC_DOMAIN>/webhooks/platega"
fi
say ""
say "Эти параметры читает только billing-service: они вступят в силу, когда его контейнер"
say "будет пересоздан (docker compose up -d --no-deps billing-service; restart не перечитывает .env)."
say "Сервисы не перезапускались. Дальше — шаги развёртывания из APPLY_DEPLOY_RU.md."
