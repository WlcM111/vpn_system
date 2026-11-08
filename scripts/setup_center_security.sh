#!/usr/bin/env bash
# Комплексная настройка Центра (main.race-src.com):
#   1. Генерация PKI (CA, серверный PKCS12 keystore/truststore, клиентский
#      сертификат для нод) для шифрования трафика Kafka (mTLS на внешнем
#      порту 19092).
#   2. Прописывание настроек Kafka SSL, TRIAL_SERVER=RO и включение транспортов
#      (Reality, Hysteria2, XHTTP) в .env (с резервной копией, атомарно).
#   3. Закрытие лишних открытых портов в UFW и ограничение доступа к
#      Docker-порту Kafka 19092 через цепочку DOCKER-USER только для белых IP
#      нод (с systemd-юнитом).
#
# Профили нод (Reality, XHTTP, Hysteria2) в БД этот скрипт не меняет.
#
# Запуск (root, из любого каталога):
#
#   scripts/setup_center_security.sh --env-file /путь/к/.env
#
# Без --env-file берётся .env в корне репозитория, а если его нет —
# deploy/.env. Один .env никогда не копируется поверх другого.
#
# Скрипт ничего не перезапускает. Настройки вступают в силу после
# пересоздания контейнеров (kafka, vpn-orchestrator-service,
# user-subscription-service) — см. гайд по выкатке. Секреты не выводятся.
#
# Параметры окружения (необязательно):
#   CENTER_DOMAIN, CENTER_IP        — имена в сертификате брокера (к ним
#                                     добавляется PUBLIC_DOMAIN из .env)
#   ALLOWED_NODE_IPS                — дополнительные IP нод через пробел

set +x
set -Eeuo pipefail
umask 077

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
	printf '%s\n' "ОШИБКА: нужен bash 4 или новее." >&2
	exit 1
fi

die() {
	printf 'ОШИБКА: %s\n' "$*" >&2
	exit 1
}
say() { printf '%s\n' "$*"; }
trap 'die "строка $LINENO: команда завершилась с ошибкой (код $?)"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_FILE=""
while [ "$#" -gt 0 ]; do
	case "$1" in
	--env-file)
		[ "$#" -ge 2 ] || die "после --env-file нужен путь к файлу"
		ENV_FILE="$2"
		shift 2
		;;
	-h | --help)
		sed -n '2,28p' "${BASH_SOURCE[0]}"
		exit 0
		;;
	*) die "неизвестный аргумент: $1 (см. --help)" ;;
	esac
done

[ "$(id -u)" = 0 ] || die "запустите от root: скрипт пишет в /etc, iptables и systemd"
for t in openssl docker iptables systemctl awk sed grep getent mktemp stat cmp; do
	command -v "$t" >/dev/null 2>&1 || die "не найдена команда $t"
done

if [ -z "$ENV_FILE" ]; then
	if [ -f "$REPO_ROOT/.env" ]; then
		ENV_FILE="$REPO_ROOT/.env"
	elif [ -f "$REPO_ROOT/deploy/.env" ]; then
		ENV_FILE="$REPO_ROOT/deploy/.env"
	fi
fi
[ -n "$ENV_FILE" ] || die "не найден .env ни в $REPO_ROOT/.env, ни в $REPO_ROOT/deploy/.env — укажите --env-file"
if [ -L "$ENV_FILE" ]; then
	ENV_FILE="$(readlink -f -- "$ENV_FILE")"
fi
[ -f "$ENV_FILE" ] || die "нет файла $ENV_FILE"
ENV_FILE="$(cd "$(dirname -- "$ENV_FILE")" && pwd)/$(basename -- "$ENV_FILE")"

TLS_DIR="/etc/house-vpn/kafka-tls"
ALLOWED_IPS_FILE="/etc/house-vpn/allowed_node_ips.conf"
CENTER_DOMAIN="${CENTER_DOMAIN:-main.race-src.com}"
CENTER_IP="${CENTER_IP:-168.222.141.104}"
KAFKA_IMAGE="apache/kafka:3.9.1"
KAFKA_CONTAINER="vpn-platform-kafka"
PG_CONTAINER="vpn-platform-postgres"

ipv4_re='^([0-9]{1,3}\.){3}[0-9]{1,3}$'
[[ "$CENTER_IP" =~ $ipv4_re ]] || die "CENTER_IP=$CENTER_IP — нужен IPv4-адрес"

# env_get KEY — значение ключа из .env (последняя строка, кавычки сняты).
env_get() {
	sed -nE "s/^[[:space:]]*(export[[:space:]]+)?$1[[:space:]]*=(.*)$/\2/p" "$ENV_FILE" | tail -n 1 |
		sed -E "s/^[[:space:]]*//; s/[[:space:]]+#.*$//; s/^'(.*)'\$/\1/; s/^\"(.*)\"\$/\1/"
}
container_running() {
	[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" = true ]
}

PUBLIC_DOMAIN="$(env_get PUBLIC_DOMAIN)"
say "Файл .env: $ENV_FILE"
say "PUBLIC_DOMAIN из .env: ${PUBLIC_DOMAIN:-не задан}"

# ---------------------------------------------------------------------------
# 1. Сертификаты Kafka TLS (mTLS)
# ---------------------------------------------------------------------------
say "[1/3] Сертификаты Kafka mTLS в $TLS_DIR ..."
install -d -m 755 /etc/house-vpn "$TLS_DIR"

PASS_FILE="$TLS_DIR/keystore_password.txt"
if [ ! -s "$PASS_FILE" ]; then
	openssl rand -hex 16 >"$PASS_FILE"
fi
chmod 600 "$PASS_FILE"
SSL_PASS="$(cat "$PASS_FILE")"

# Имена в сертификате брокера: узлы проверяют и адрес из KAFKA_BROKERS, и
# анонсируемый брокером EXTERNAL://${PUBLIC_DOMAIN}:19092.
SAN_DNS=("$CENTER_DOMAIN")
if [ -n "$PUBLIC_DOMAIN" ] && [[ ! "$PUBLIC_DOMAIN" =~ $ipv4_re ]] && [ "$PUBLIC_DOMAIN" != "$CENTER_DOMAIN" ]; then
	SAN_DNS+=("$PUBLIC_DOMAIN")
fi
SAN_DNS+=(kafka localhost)
SAN_IP=("$CENTER_IP" 127.0.0.1)
if [ -n "$PUBLIC_DOMAIN" ] && [[ "$PUBLIC_DOMAIN" =~ $ipv4_re ]] && [ "$PUBLIC_DOMAIN" != "$CENTER_IP" ]; then
	SAN_IP+=("$PUBLIC_DOMAIN")
fi
san_line="subjectAltName = "
for n in "${SAN_DNS[@]}"; do san_line+="DNS:$n,"; done
for n in "${SAN_IP[@]}"; do san_line+="IP:$n,"; done
san_line="${san_line%,}"

# server_cert_ok — серверный сертификат есть, подписан текущим CA и покрывает
# все нужные имена.
server_cert_ok() {
	[ -s "$TLS_DIR/server.crt" ] && [ -s "$TLS_DIR/kafka.server.keystore.p12" ] || return 1
	openssl verify -CAfile "$TLS_DIR/ca.crt" "$TLS_DIR/server.crt" >/dev/null 2>&1 || return 1
	local n
	for n in "${SAN_DNS[@]}"; do
		openssl x509 -in "$TLS_DIR/server.crt" -noout -checkhost "$n" 2>/dev/null | grep -q 'does match' || return 1
	done
	for n in "${SAN_IP[@]}"; do
		openssl x509 -in "$TLS_DIR/server.crt" -noout -checkip "$n" 2>/dev/null | grep -q 'does match' || return 1
	done
}

new_ca=0
if [ ! -s "$TLS_DIR/ca.crt" ] || [ ! -s "$TLS_DIR/ca.key" ]; then
	say "  создаю CA"
	openssl req -new -x509 -nodes -days 3650 -newkey rsa:4096 \
		-keyout "$TLS_DIR/ca.key" -out "$TLS_DIR/ca.crt" \
		-subj "/CN=HouseVPN-Kafka-CA" 2>/dev/null
	new_ca=1
fi

if [ "$new_ca" = 1 ] || ! server_cert_ok; then
	say "  создаю серверный сертификат брокера: ${san_line#subjectAltName = }"
	tmpd="$(mktemp -d)"
	printf '%s\nextendedKeyUsage = serverAuth\n' "$san_line" >"$tmpd/server-ext.cnf"
	openssl req -new -nodes -newkey rsa:4096 \
		-keyout "$TLS_DIR/server.key" -out "$tmpd/server.csr" \
		-subj "/CN=${CENTER_DOMAIN}" 2>/dev/null
	openssl x509 -req -in "$tmpd/server.csr" \
		-CA "$TLS_DIR/ca.crt" -CAkey "$TLS_DIR/ca.key" -CAcreateserial \
		-out "$TLS_DIR/server.crt" -days 3650 \
		-extfile "$tmpd/server-ext.cnf" 2>/dev/null
	openssl pkcs12 -export \
		-in "$TLS_DIR/server.crt" -inkey "$TLS_DIR/server.key" \
		-chain -CAfile "$TLS_DIR/ca.crt" -name kafka-server \
		-out "$TLS_DIR/kafka.server.keystore.p12" -password "pass:${SSL_PASS}"
	rm -rf "$tmpd"
fi

if [ "$new_ca" = 1 ] || [ ! -s "$TLS_DIR/kafka.server.truststore.p12" ]; then
	say "  создаю truststore брокера (CA для проверки клиентских сертификатов)"
	rm -f "$TLS_DIR/kafka.server.truststore.p12"
	# -jdktrust есть только в OpenSSL 3.2+; в Ubuntu 22.04/24.04 (OpenSSL 3.0)
	# работает запасной путь — keytool из образа Kafka. Он запускается от root
	# (--user 0:0): пользователь образа appuser (UID 1000) не может писать в
	# каталог root с правами 755.
	if ! openssl pkcs12 -export -nokeys -in "$TLS_DIR/ca.crt" -caname kafka-ca \
		-out "$TLS_DIR/kafka.server.truststore.p12" -passout "pass:${SSL_PASS}" \
		-jdktrust anyExtendedKeyUsage 2>/dev/null; then
		rm -f "$TLS_DIR/kafka.server.truststore.p12"
		docker run --rm --user 0:0 -v "$TLS_DIR:/certs" --entrypoint keytool "$KAFKA_IMAGE" \
			-importcert -noprompt -alias kafka-ca -file /certs/ca.crt \
			-keystore /certs/kafka.server.truststore.p12 -storetype PKCS12 \
			-storepass "${SSL_PASS}" >/dev/null
	fi
fi

if [ "$new_ca" = 1 ] || [ ! -s "$TLS_DIR/client.crt" ] || [ ! -s "$TLS_DIR/client.key" ] ||
	! openssl verify -CAfile "$TLS_DIR/ca.crt" "$TLS_DIR/client.crt" >/dev/null 2>&1; then
	say "  создаю клиентский сертификат для нод (CN=vpn-node-agent)"
	tmpd="$(mktemp -d)"
	printf 'extendedKeyUsage = clientAuth\n' >"$tmpd/client-ext.cnf"
	openssl req -new -nodes -newkey rsa:4096 \
		-keyout "$TLS_DIR/client.key" -out "$tmpd/client.csr" \
		-subj "/CN=vpn-node-agent" 2>/dev/null
	openssl x509 -req -in "$tmpd/client.csr" \
		-CA "$TLS_DIR/ca.crt" -CAkey "$TLS_DIR/ca.key" -CAcreateserial \
		-out "$TLS_DIR/client.crt" -days 3650 \
		-extfile "$tmpd/client-ext.cnf" 2>/dev/null
	rm -rf "$tmpd"
fi

# Права чтения для контейнера apache/kafka:3.9.1 (UID 1000): хранилища
# защищены паролем из .env (права 600), закрытые ключи — только root.
chmod 644 "$TLS_DIR/ca.crt" "$TLS_DIR/kafka.server.keystore.p12" "$TLS_DIR/kafka.server.truststore.p12" "$TLS_DIR/client.crt"
chmod 600 "$TLS_DIR/ca.key" "$TLS_DIR/server.key" "$TLS_DIR/client.key"
if [ -f "$TLS_DIR/ca.srl" ]; then chmod 600 "$TLS_DIR/ca.srl"; fi

# Проверка тем же Java, что у брокера: хранилища читаются, в keystore —
# ключ брокера, в truststore — доверенный CA.
ks_list="$(docker run --rm --user 0:0 -v "$TLS_DIR:/certs:ro" --entrypoint keytool "$KAFKA_IMAGE" \
	-list -keystore /certs/kafka.server.keystore.p12 -storetype PKCS12 -storepass "${SSL_PASS}" 2>&1)" ||
	die "keystore не читается keytool: $ks_list"
grep -q 'PrivateKeyEntry' <<<"$ks_list" || die "в keystore нет закрытого ключа брокера"
ts_list="$(docker run --rm --user 0:0 -v "$TLS_DIR:/certs:ro" --entrypoint keytool "$KAFKA_IMAGE" \
	-list -keystore /certs/kafka.server.truststore.p12 -storetype PKCS12 -storepass "${SSL_PASS}" 2>&1)" ||
	die "truststore не читается keytool: $ts_list"
grep -q 'trustedCertEntry' <<<"$ts_list" || die "в truststore нет доверенного CA (trustedCertEntry)"
openssl verify -CAfile "$TLS_DIR/ca.crt" "$TLS_DIR/client.crt" >/dev/null || die "клиентский сертификат не подписан текущим CA"
say "  OK: keystore, truststore и клиентский сертификат проверены"

# ---------------------------------------------------------------------------
# 2. Обновление .env Центра (резервная копия, атомарная замена)
# ---------------------------------------------------------------------------
say "[2/3] Параметры в $ENV_FILE ..."
ENV_DIR="$(dirname -- "$ENV_FILE")"
declare -A WANT=(
	[KAFKA_EXTERNAL_PROTOCOL]=SSL
	[KAFKA_TLS_DIR]="$TLS_DIR"
	[KAFKA_SSL_PASSWORD]="$SSL_PASS"
	[KAFKA_SSL_CLIENT_AUTH]=required
	[TRIAL_SERVER]=RO
	[SUBSCRIPTION_DEFAULT_COUNTRY]=RO
	[CDN_CONFIG_ENABLED]=true
	[HYSTERIA_CONFIG_ENABLED]=true
	[REALITY_CONFIG_ENABLED]=true
	[XHTTP_CONFIG_ENABLED]=true
)
ORDER=(KAFKA_EXTERNAL_PROTOCOL KAFKA_TLS_DIR KAFKA_SSL_PASSWORD KAFKA_SSL_CLIENT_AUTH TRIAL_SERVER SUBSCRIPTION_DEFAULT_COUNTRY CDN_CONFIG_ENABLED HYSTERIA_CONFIG_ENABLED REALITY_CONFIG_ENABLED XHTTP_CONFIG_ENABLED)

tmp_env="$(mktemp "$ENV_DIR/.env.tmp.XXXXXX")"
declare -A WRITTEN=()
while IFS= read -r line || [ -n "$line" ]; do
	handled=0
	for k in "${ORDER[@]}"; do
		if [[ "$line" =~ ^[[:space:]]*(export[[:space:]]+)?${k}[[:space:]]*= ]]; then
			handled=1
			if [ -z "${WRITTEN[$k]:-}" ]; then
				printf '%s=%s\n' "$k" "${WANT[$k]}" >>"$tmp_env"
				WRITTEN[$k]=1
			fi
			break
		fi
	done
	[ "$handled" = 1 ] || printf '%s\n' "$line" >>"$tmp_env"
done <"$ENV_FILE"
missing=()
for k in "${ORDER[@]}"; do
	[ -n "${WRITTEN[$k]:-}" ] || missing+=("$k")
done
if [ "${#missing[@]}" -gt 0 ]; then
	{
		printf '\n# Kafka mTLS, пробный сервер и транспорты (setup_center_security.sh)\n'
		for k in "${missing[@]}"; do printf '%s=%s\n' "$k" "${WANT[$k]}"; done
	} >>"$tmp_env"
fi
if cmp -s "$tmp_env" "$ENV_FILE"; then
	rm -f "$tmp_env"
	say "  .env уже содержит нужные значения — без изменений"
else
	backup="$ENV_DIR/.env.bak-security-$(date +%Y%m%d-%H%M%S)"
	cp -p -- "$ENV_FILE" "$backup"
	chmod 600 "$backup"
	chmod "$(stat -c '%a' -- "$ENV_FILE")" "$tmp_env"
	chown "$(stat -c '%u:%g' -- "$ENV_FILE")" "$tmp_env"
	mv -f -- "$tmp_env" "$ENV_FILE"
	say "  обновлено: ${ORDER[*]}"
	say "  резервная копия: $backup (права 600)"
fi

# ---------------------------------------------------------------------------
# 3. Файрвол: UFW + DOCKER-USER для порта 19092
# ---------------------------------------------------------------------------
say "[3/3] Файрвол (UFW + DOCKER-USER для порта 19092) ..."
touch "$ALLOWED_IPS_FILE"
chmod 644 "$ALLOWED_IPS_FILE"

is_public_ipv4() {
	local ip="$1" a b c d
	[[ "$ip" =~ $ipv4_re ]] || return 1
	IFS=. read -r a b c d <<<"$ip"
	for o in "$a" "$b" "$c" "$d"; do [ "$o" -le 255 ] || return 1; done
	[ "$a" = 10 ] || [ "$a" = 127 ] || [ "$a" = 0 ] && return 1
	[ "$a" = 172 ] && [ "$b" -ge 16 ] && [ "$b" -le 31 ] && return 1
	[ "$a" = 192 ] && [ "$b" = 168 ] && return 1
	[ "$a" = 169 ] && [ "$b" = 254 ] && return 1
	[ "$a" = 100 ] && [ "$b" -ge 64 ] && [ "$b" -le 127 ] && return 1
	[ "$a" -ge 224 ] && return 1
	return 0
}
add_allowed_ip() {
	local ip="$1" why="$2"
	is_public_ipv4 "$ip" || return 0
	if ! grep -qxF "$ip" "$ALLOWED_IPS_FILE"; then
		printf '%s\n' "$ip" >>"$ALLOWED_IPS_FILE"
		say "  + $ip ($why)"
	fi
}

for ip in ${ALLOWED_NODE_IPS:-}; do add_allowed_ip "$ip" "ALLOWED_NODE_IPS"; done

# Ноды, подключённые к Kafka прямо сейчас: их адреса видны в сетевом
# пространстве контейнера брокера (DNAT сохраняет адрес клиента).
if ! command -v nsenter >/dev/null 2>&1 || ! command -v ss >/dev/null 2>&1; then
	say "  ВНИМАНИЕ: нет nsenter/ss (util-linux, iproute2) — подключённые сейчас ноды не определены,"
	say "  проверьте белый список ниже вручную."
elif container_running "$KAFKA_CONTAINER"; then
	kpid="$(docker inspect -f '{{.State.Pid}}' "$KAFKA_CONTAINER")"
	while IFS= read -r peer; do
		peer="${peer%:*}"
		peer="${peer#\[}"
		peer="${peer%\]}"
		peer="${peer#::ffff:}"
		add_allowed_ip "$peer" "подключён к 19092 сейчас"
	done < <(nsenter -t "$kpid" -n ss -Htn state established '( sport = :19092 )' 2>/dev/null | awk '{print $4}' || true)
fi

# Адреса нод из БД: IPv4 Reality-эндпоинтов и public_host серверов (имена
# разрешаются в IPv4).
if container_running "$PG_CONTAINER"; then
	db_hosts="$(docker exec -i "$PG_CONTAINER" sh -c 'exec psql -X -q -At -U "$POSTGRES_USER" -d "$POSTGRES_DB"' 2>/dev/null <<'SQL' || true
SELECT DISTINCT address FROM vpn_reality_endpoints WHERE enabled
UNION
SELECT DISTINCT public_host FROM vpn_servers WHERE enabled AND public_host <> '';
SQL
)"
	while IFS= read -r host; do
		[ -n "$host" ] || continue
		if [[ "$host" =~ $ipv4_re ]]; then
			add_allowed_ip "$host" "из БД"
		else
			while IFS= read -r ip; do add_allowed_ip "$ip" "из БД: $host"; done < <(getent ahostsv4 "$host" 2>/dev/null | awk '{print $1}' | sort -u || true)
		fi
	done <<<"$db_hosts"
fi

say "  белый список $ALLOWED_IPS_FILE:"
sed 's/^/    /' "$ALLOWED_IPS_FILE"

cat >/usr/local/sbin/house-vpn-firewall.sh <<'EOS'
#!/usr/bin/env bash
# Доступ к внешнему порту Kafka 19092 только с белых IP нод.
set -euo pipefail
ALLOWED_FILE="/etc/house-vpn/allowed_node_ips.conf"

iptables -L DOCKER-USER -n >/dev/null 2>&1 || iptables -N DOCKER-USER

# Удаляем прежние правила для порта 19092 (снимок списка, затем удаление по
# одному — без grep -q в конвейере под pipefail).
mapfile -t old_rules < <(iptables -S DOCKER-USER | grep -e '--dport 19092' || true)
for rule in "${old_rules[@]}"; do
	read -r -a spec <<<"${rule/#-A /}"
	iptables -D "${spec[@]}" || true
done

pos=1
add_return() {
	iptables -I DOCKER-USER "$pos" -p tcp --dport 19092 -s "$1" -j RETURN
	pos=$((pos + 1))
}

# Локальные и внутренние Docker-сети
add_return 127.0.0.0/8
add_return 172.16.0.0/12
add_return 10.0.0.0/8

# Белые IP нод
if [ -f "$ALLOWED_FILE" ]; then
	while IFS= read -r ip || [ -n "$ip" ]; do
		ip="${ip%%#*}"
		ip="${ip//[[:space:]]/}"
		[ -n "$ip" ] || continue
		add_return "$ip"
	done <"$ALLOWED_FILE"
fi

# Всем остальным доступ к внешнему порту Kafka 19092 закрыт
iptables -I DOCKER-USER "$pos" -p tcp --dport 19092 -j DROP
EOS
chmod 755 /usr/local/sbin/house-vpn-firewall.sh
/usr/local/sbin/house-vpn-firewall.sh

cat >/etc/systemd/system/house-vpn-firewall.service <<'EOS'
[Unit]
Description=House VPN DOCKER-USER Firewall Rules for Kafka 19092
After=network-online.target docker.service
Requires=docker.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/house-vpn-firewall.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOS
chmod 644 /etc/systemd/system/house-vpn-firewall.service
systemctl daemon-reload
systemctl enable house-vpn-firewall.service >/dev/null 2>&1 || true

if command -v ufw >/dev/null 2>&1; then
	# Порт SSH берём у самого sshd: включение UFW без него отрезало бы доступ.
	ssh_ports="$(sshd -T 2>/dev/null | awk '$1 == "port" {print $2}' | sort -u || true)"
	[ -n "$ssh_ports" ] || ssh_ports=22
	for p in $ssh_ports; do ufw allow "$p/tcp" >/dev/null 2>&1 || true; done
	ufw allow 22/tcp >/dev/null 2>&1 || true
	ufw allow 80/tcp >/dev/null 2>&1 || true
	ufw allow 443/tcp >/dev/null 2>&1 || true
	# Удаляем прямое открытие 19092/9092/5432/6379 в UFW, если они были открыты
	ufw delete allow 19092/tcp >/dev/null 2>&1 || true
	ufw delete allow 9092/tcp >/dev/null 2>&1 || true
	ufw delete allow 5432/tcp >/dev/null 2>&1 || true
	ufw delete allow 6379/tcp >/dev/null 2>&1 || true
	ufw --force enable >/dev/null 2>&1 || true
	say "  UFW: разрешены SSH (${ssh_ports//$'\n'/ }), 80, 443"
fi
say "  DOCKER-USER (19092):"
iptables -S DOCKER-USER | grep -e '--dport 19092' | sed 's/^/    /' || true

trap - ERR
say ""
say "✅ Центр настроен: сертификаты Kafka mTLS созданы, .env обновлён, порт 19092 закрыт для посторонних (DOCKER-USER)."
say "Клиентские сертификаты для нод: $TLS_DIR/{ca.crt,client.crt,client.key}"
say "Вступит в силу после пересоздания контейнеров kafka, vpn-orchestrator-service, user-subscription-service."
