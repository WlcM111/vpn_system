#!/usr/bin/env bash
# Тесты scripts/setup_platega_env.sh.
#
#   bash scripts/tests/setup_platega_env_test.sh
#
# Каждый сценарий работает во временном каталоге с копией скрипта и
# deploy/docker-compose.dev.yml; рабочий .env репозитория не трогается.
# Значения ключей здесь тестовые. Проверка «настоящим парсером» выполняется,
# если доступны docker compose и python3 (иначе скрипт сам пропускает её —
# тогда сценарии T13/T15 помечаются SKIP).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PASS=0
FAIL=0
SKIP=0
MERCHANT="1a021d91-9b26-4762-b303-5d4aac74e921"
SECRET='t3st-Secret_$HOME#x;y'
SECRET2='second-test-secret-value'

have_compose=0
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1; then
	have_compose=1
fi

new_sandbox() {
	local d
	d="$(mktemp -d)"
	mkdir -p "$d/scripts" "$d/deploy"
	cp "$ROOT/scripts/setup_platega_env.sh" "$d/scripts/"
	cp "$ROOT/deploy/docker-compose.dev.yml" "$d/deploy/"
	cp "$ROOT/deploy/.env.example" "$d/.env"
	chmod 600 "$d/.env"
	printf '%s' "$d"
}

run_script() { # dir, stdin-text → stdout+stderr в $d/out, код возврата
	local d="$1" input="$2"
	printf '%b' "$input" | "$d/scripts/setup_platega_env.sh" >"$d/out" 2>&1
}

sum() { sha256sum "$1" | cut -d' ' -f1; }
ok() {
	PASS=$((PASS + 1))
	printf 'PASS %s\n' "$1"
}
bad() {
	FAIL=$((FAIL + 1))
	printf 'FAIL %s: %s\n' "$1" "$2"
}
skip() {
	SKIP=$((SKIP + 1))
	printf 'SKIP %s: %s\n' "$1" "$2"
}

compose_value() { # dir key → значение, как его видит Docker Compose
	local d="$1" key="$2"
	env -i PATH="$PATH" HOME="$HOME" docker compose --env-file "$d/.env" -f "$d/deploy/docker-compose.dev.yml" config --format json 2>/dev/null |
		KEY="$key" python3 -I -c 'import json,os,sys; print((json.load(sys.stdin)["services"]["billing-service"]["environment"].get(os.environ["KEY"]) or "").replace("$$", "$"))'
}

no_leftovers() { # dir → нет временных файлов и каталога-блокировки
	! ls "$1"/.env.tmp.* >/dev/null 2>&1 && [ ! -d "$1/.env.lock.d" ]
}

# T1: первичная запись ключей в существующий .env.
t="T1 запись ключей, резервная копия, права, остальные строки не тронуты"
d="$(new_sandbox)"
before="$(sum "$d/.env")"
grep -v '^PLATEGA_' "$d/.env" >"$d/expect_rest"
if run_script "$d" "$MERCHANT\n$SECRET\n\nhttps://t.me/vpn_house_bot\n\n"; then
	backup="$(ls "$d"/.env.bak-* 2>/dev/null | head -1)"
	mode="$(stat -c '%a' "$d/.env")"
	grep -v '^PLATEGA_' "$d/.env" | grep -v '^# Platega' >"$d/got_rest"
	if [ -n "$backup" ] && [ "$(sum "$backup")" = "$before" ] && [ "$(stat -c '%a' "$backup")" = 600 ] &&
		[ "$mode" = 600 ] && grep -qx "PLATEGA_MERCHANT_ID='$MERCHANT'" "$d/.env" &&
		grep -qx "PLATEGA_PAYMENT_METHOD='2'" "$d/.env" && grep -qx "PLATEGA_RETURN_URL='https://t.me/vpn_house_bot'" "$d/.env" &&
		[ "$(grep -c '^PLATEGA_SECRET=' "$d/.env")" = 1 ] && diff -q <(grep -v '^$' "$d/expect_rest") <(grep -v '^$' "$d/got_rest") >/dev/null &&
		no_leftovers "$d"; then
		ok "$t"
	else
		bad "$t" "файл/копия/права не те"
	fi
else
	bad "$t" "код возврата $?: $(tail -3 "$d/out")"
fi

# T2: секрет не выводится ни в одном сценарии (проверяется по всем выводам ниже).
t="T2 секрет не попадает в вывод"
if grep -qF -- "$SECRET" "$d/out"; then bad "$t" "секрет в выводе"; else ok "$t"; fi

# T3: пустой ввод сохраняет все значения.
t="T3 Enter оставляет текущие значения"
cp "$d/.env" "$d/prev"
if run_script "$d" "\n\n\n\n\n" && diff -q <(grep '^PLATEGA_' "$d/prev") <(grep '^PLATEGA_' "$d/.env") >/dev/null; then
	ok "$t"
else
	bad "$t" "значения изменились: $(tail -2 "$d/out")"
fi

# T4: смена только секрета.
t="T4 замена одного секрета на месте"
if run_script "$d" "\n$SECRET2\n\n\n\n" && grep -qx "PLATEGA_SECRET='$SECRET2'" "$d/.env" &&
	grep -qx "PLATEGA_MERCHANT_ID='$MERCHANT'" "$d/.env" && ! grep -qF -- "$SECRET2" "$d/out"; then
	ok "$t"
else
	bad "$t" "$(tail -2 "$d/out")"
fi

# T5: неверный UUID трижды — выход без изменений.
t="T5 неверный ID мерчанта: отказ, .env не изменён"
d="$(new_sandbox)"
before="$(sum "$d/.env")"
if run_script "$d" "nope\n123\n$MERCHANT-x\n"; then
	bad "$t" "скрипт завершился успешно"
elif [ "$(sum "$d/.env")" = "$before" ] && ! ls "$d"/.env.bak-* >/dev/null 2>&1 && no_leftovers "$d"; then
	ok "$t"
else
	bad "$t" ".env изменён или остались файлы"
fi

# T6: недопустимые символы в секрете (кавычка, пробел, обратная черта).
t="T6 секрет с ' / пробелом / \\ отклоняется"
d="$(new_sandbox)"
before="$(sum "$d/.env")"
if run_script "$d" "$MERCHANT\nab'cd\nab cd\nab\\\\cd\n"; then
	bad "$t" "принят недопустимый секрет"
elif [ "$(sum "$d/.env")" = "$before" ] && no_leftovers "$d"; then
	ok "$t"
else
	bad "$t" ".env изменён"
fi

# T7: Ctrl+D (конец ввода) посреди вопросов.
t="T7 Ctrl+D: выход 130, .env не изменён, блокировка снята"
d="$(new_sandbox)"
before="$(sum "$d/.env")"
run_script "$d" "$MERCHANT\n"
rc=$?
if [ "$rc" = 130 ] && [ "$(sum "$d/.env")" = "$before" ] && no_leftovers "$d" &&
	run_script "$d" "$MERCHANT\n$SECRET\n\n\n\n"; then
	ok "$t"
else
	bad "$t" "rc=$rc $(tail -2 "$d/out")"
fi

# T8: Ctrl+C (SIGINT) во время ожидания ввода.
# Фоновый процесс неинтерактивной оболочки получает SIGINT игнорируемым, а
# игнорируемый при старте сигнал bash перехватить не может (так бывает, если
# сам тест запущен в фоне или через nohup). Поэтому скрипт запускается с
# SIGINT по умолчанию — как в терминале, где его прерывает Ctrl+C.
t="T8 Ctrl+C: выход 130, .env не изменён, временных файлов нет"
d="$(new_sandbox)"
before="$(sum "$d/.env")"
launch=()
if env --default-signal=INT true 2>/dev/null; then
	launch=(env --default-signal=INT)
elif command -v python3 >/dev/null 2>&1; then
	launch=(python3 -c 'import os, signal, sys; signal.signal(signal.SIGINT, signal.SIG_DFL); os.execvp(sys.argv[1], sys.argv[1:])')
fi
if [ "${#launch[@]}" -eq 0 ]; then
	skip "$t" "нет env --default-signal и python3"
else
	mkfifo "$d/fifo"
	("${launch[@]}" "$d/scripts/setup_platega_env.sh" <"$d/fifo" >"$d/out" 2>&1) &
	pid=$!
	exec 7>"$d/fifo"
	printf '%s\n' "$MERCHANT" >&7
	sleep 1
	pkill -INT -f "$d/scripts/setup_platega_env.sh" || kill -INT "$pid"
	# Не ждать вечно: если сигнал не подействовал, это провал, а не зависание.
	for _ in $(seq 1 100); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.1
	done
	hung=0
	if kill -0 "$pid" 2>/dev/null; then
		hung=1
		pkill -TERM -f "$d/scripts/setup_platega_env.sh" || kill -TERM "$pid"
	fi
	wait "$pid"
	rc=$?
	exec 7>&-
	if [ "$hung" = 0 ] && [ "$rc" = 130 ] && [ "$(sum "$d/.env")" = "$before" ] && no_leftovers "$d"; then
		ok "$t"
	else
		bad "$t" "rc=$rc hung=$hung $(tail -2 "$d/out")"
	fi
fi

# T9: одновременный запуск блокируется.
t="T9 второй одновременный запуск отказывает"
d="$(new_sandbox)"
if command -v flock >/dev/null 2>&1; then
	exec 8>"$d/.env.lock"
	flock -n 8
	before="$(sum "$d/.env")"
	if run_script "$d" "$MERCHANT\n$SECRET\n\n\n\n"; then
		bad "$t" "запуск прошёл при занятой блокировке"
	elif grep -q "уже запущен" "$d/out" && [ "$(sum "$d/.env")" = "$before" ]; then
		ok "$t"
	else
		bad "$t" "$(tail -2 "$d/out")"
	fi
	exec 8>&-
else
	skip "$t" "нет flock"
fi

# T10: export-префикс и дубли ключей.
t="T10 export и дубли: одна строка на месте первой"
d="$(new_sandbox)"
{
	echo ""
	echo "export PLATEGA_SECRET='old-secret'"
	echo "PLATEGA_SECRET=dup-secret"
	echo "UNRELATED=keep # comment"
} >>"$d/.env"
if run_script "$d" "$MERCHANT\n$SECRET2\n\n\n\n" && [ "$(grep -c 'PLATEGA_SECRET' "$d/.env")" = 1 ] &&
	grep -qx "PLATEGA_SECRET='$SECRET2'" "$d/.env" && grep -qx "UNRELATED=keep # comment" "$d/.env" &&
	grep -q "дублирующихся строк Platega: 2" "$d/out"; then
	ok "$t"
else
	bad "$t" "$(grep -n PLATEGA_SECRET "$d/.env" | sed 's/=.*/=…/') $(tail -2 "$d/out")"
fi

# T11: права — чтение группой сохраняется, чтение всеми убирается.
for m in 600:600 640:640 644:640 660:640; do
	from="${m%%:*}"
	want="${m##*:}"
	t="T11 права $from → $want"
	d="$(new_sandbox)"
	chmod "$from" "$d/.env"
	if run_script "$d" "$MERCHANT\n$SECRET\n\n\n\n" && [ "$(stat -c '%a' "$d/.env")" = "$want" ]; then
		ok "$t"
	else
		bad "$t" "получилось $(stat -c '%a' "$d/.env")"
	fi
done

# T12: нет .env — без явного согласия ничего не создаётся; с согласием —
# новый файл из шаблона deploy/.env.example, права 600, ключи Platega записаны.
t="T12 нет .env, ответ Enter — файл не создаётся"
d="$(new_sandbox)"
rm -f "$d/.env"
cp "$ROOT/deploy/.env.example" "$d/deploy/.env.example"
if run_script "$d" "\n$MERCHANT\n$SECRET\n\n\n\n"; then
	bad "$t" "успех без согласия"
elif [ ! -e "$d/.env" ] && grep -q "не найден" "$d/out" && grep -q "Файл .env не создан" "$d/out" && no_leftovers "$d"; then
	ok "$t"
else
	bad "$t" "$(tail -2 "$d/out")"
fi

t="T12 нет .env, ответ y — новый файл из шаблона"
d="$(new_sandbox)"
rm -f "$d/.env"
cp "$ROOT/deploy/.env.example" "$d/deploy/.env.example"
if run_script "$d" "y\n$MERCHANT\n$SECRET\n\n\n\n" && [ -f "$d/.env" ] && [ "$(stat -c '%a' "$d/.env")" = 600 ] &&
	grep -qx "PLATEGA_MERCHANT_ID='$MERCHANT'" "$d/.env" && [ "$(grep -c '^PLATEGA_SECRET=' "$d/.env")" = 1 ] &&
	diff -q <(grep -v '^PLATEGA_' "$ROOT/deploy/.env.example") <(grep -v '^PLATEGA_' "$d/.env") >/dev/null &&
	! ls "$d"/.env.bak-* >/dev/null 2>&1 && no_leftovers "$d" && ! grep -qF -- "$SECRET" "$d/out" &&
	grep -q "Создан новый файл" "$d/out"; then
	if [ "$have_compose" = 1 ] && [ "$(compose_value "$d" PLATEGA_SECRET)" != "$SECRET" ]; then
		bad "$t" "Docker Compose видит другое значение секрета"
	else
		ok "$t"
	fi
else
	bad "$t" "$(tail -3 "$d/out")"
fi

# T13: настоящий парсер Compose видит ровно введённые значения ($ и # в секрете
# не интерполируются и не обрезаются).
t="T13 Docker Compose читает значения дословно"
if [ "$have_compose" = 1 ]; then
	d="$(new_sandbox)"
	if run_script "$d" "$MERCHANT\n$SECRET\n11\nhttps://t.me/vpn_house_bot?start=paid\n\n" &&
		[ "$(compose_value "$d" PLATEGA_SECRET)" = "$SECRET" ] && [ "$(compose_value "$d" PLATEGA_MERCHANT_ID)" = "$MERCHANT" ] &&
		[ "$(compose_value "$d" PLATEGA_PAYMENT_METHOD)" = 11 ] &&
		[ "$(compose_value "$d" PLATEGA_RETURN_URL)" = "https://t.me/vpn_house_bot?start=paid" ] &&
		grep -q "Проверка Docker Compose: значения в billing-service совпадают" "$d/out"; then
		ok "$t"
	else
		bad "$t" "$(tail -3 "$d/out")"
	fi
else
	skip "$t" "нет docker compose или python3"
fi

# T14: неверные способ оплаты и адрес возврата.
t="T14 недокументированный способ и не-https адрес отклоняются"
d="$(new_sandbox)"
before="$(sum "$d/.env")"
if run_script "$d" "$MERCHANT\n$SECRET\n6\n1\n99\n"; then
	bad "$t" "принят способ 6/1/99"
elif [ "$(sum "$d/.env")" = "$before" ]; then
	d2="$(new_sandbox)"
	before2="$(sum "$d2/.env")"
	if run_script "$d2" "$MERCHANT\n$SECRET\n\nhttp://t.me/x\nt.me/x\nhttps://t.me/a b\n"; then
		bad "$t" "принят не-https адрес"
	elif [ "$(sum "$d2/.env")" = "$before2" ]; then
		ok "$t"
	else
		bad "$t" ".env изменён"
	fi
else
	bad "$t" ".env изменён"
fi

# T15: Compose не принимает итоговый файл — .env не заменяется.
t="T15 файл, который Compose не разбирает, не устанавливается"
if [ "$have_compose" = 1 ]; then
	d="$(new_sandbox)"
	sed -i '/^POSTGRES_USER=/d' "$d/.env"
	before="$(sum "$d/.env")"
	if run_script "$d" "$MERCHANT\n$SECRET\n\n\n\n"; then
		bad "$t" "успех при неразборчивом .env"
	elif [ "$(sum "$d/.env")" = "$before" ] && ! ls "$d"/.env.bak-* >/dev/null 2>&1 && no_leftovers "$d"; then
		ok "$t"
	else
		bad "$t" "$(tail -2 "$d/out")"
	fi
else
	skip "$t" "нет docker compose или python3"
fi

# T16: экспортированная в shell переменная не подменяет значение из файла
# при проверке (скрипт запускает Compose в чистом окружении).
t="T16 переменные shell не влияют на проверку"
if [ "$have_compose" = 1 ]; then
	d="$(new_sandbox)"
	if PLATEGA_SECRET=shell-value run_script "$d" "$MERCHANT\n$SECRET\n\n\n\n" && grep -qx "PLATEGA_SECRET='$SECRET'" "$d/.env"; then
		ok "$t"
	else
		bad "$t" "$(tail -2 "$d/out")"
	fi
else
	skip "$t" "нет docker compose или python3"
fi

# T17: в скрипте нет set -x и приёма секретов из аргументов.
t="T17 нет трассировки и секретов в аргументах"
if ! grep -qE '^[^#]*set -[a-zA-Z]*x' "$ROOT/scripts/setup_platega_env.sh" &&
	"$ROOT/scripts/setup_platega_env.sh" --secret x >/dev/null 2>&1; then
	bad "$t" "аргумент --secret принят"
elif ! grep -qE '^[^#]*set -[a-zA-Z]*x' "$ROOT/scripts/setup_platega_env.sh"; then
	ok "$t"
else
	bad "$t" "найден set -x"
fi

printf '\nИтого: PASS=%d FAIL=%d SKIP=%d\n' "$PASS" "$FAIL" "$SKIP"
[ "$FAIL" = 0 ]
