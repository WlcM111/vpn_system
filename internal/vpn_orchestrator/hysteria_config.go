package vpn_orchestrator

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

// ============================================================================
// Hysteria2-эндпоинты, привязанные к серверам.
//
// Обычная нода: Hysteria2 — отдельный сервер без inbound'ов Xray, пользователь
// не регистрируется на узле заранее. Hysteria при каждом подключении
// спрашивает node-agent по HTTP, пускать ли клиента, а паролем выступает
// VLESS-UUID пользователя. InboundTag у такого эндпоинта пустой.
//
// Нода-вход каскада: Hysteria2 работает внутри Xray (общая маршрутизация с
// Reality и XHTTP). Тогда InboundTag — тег Hysteria2-инбаунда Xray, и учётка
// пользователя (тот же UUID как пароль) регистрируется в нём через агента,
// как в Reality и XHTTP.
//
// Источник — таблица vpn_hysteria_endpoints. Env-фолбэка нет: адрес, порт и
// SNI у каждой ноды свои, одной группой переменных окружения их не описать.
// ============================================================================

// HysteriaEndpoint — параметры одного Hysteria2-транспорта.
type HysteriaEndpoint struct {
	HysteriaKey string
	ServerKey   string
	Enabled     bool
	SortOrder   int

	Address      string
	Port         int
	SNI          string
	Insecure     bool
	ObfsType     string
	ObfsPassword string
	Remarks      string

	// InboundTag — тег Hysteria2-инбаунда Xray на ноде; пусто — отдельный
	// сервер Hysteria2 с HTTP-аутентификацией через агента.
	InboundTag string
}

// hysteriaConfigEnabled — глобальный рубильник выдачи Hysteria (env).
func hysteriaConfigEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("HYSTERIA_CONFIG_ENABLED")), "true")
}

// selectHysteriaForServer выбирает эндпоинт для конкретного server_key.
//
// ВАЖНО: фолбэка «любой первый» здесь НЕТ намеренно. Отдать пользователю
// эндпоинт чужой ноды — значит выдать ссылку туда, где его UUID неизвестен:
// в списке она появится, а подключение молча не заработает. Лучше не отдать
// транспорт вовсе, чем отдать нерабочий.
func selectHysteriaForServer(endpoints []HysteriaEndpoint, serverKey string) (HysteriaEndpoint, bool) {
	if len(endpoints) == 0 || serverKey == "" {
		return HysteriaEndpoint{}, false
	}
	for _, e := range endpoints {
		if e.ServerKey == serverKey {
			return e, true
		}
	}
	return HysteriaEndpoint{}, false
}

// BuildHysteriaURL строит hysteria2://-ссылку. Паролем выступает UUID
// пользователя — его же проверяет node-agent при аутентификации.
//
// Формат по официальной спецификации URI Scheme:
//
//	hysteria2://<auth>@<host>:<port>?sni=...&obfs=...#<remarks>
func BuildHysteriaURL(e HysteriaEndpoint, userUUID string) string {
	if strings.TrimSpace(e.Address) == "" || strings.TrimSpace(userUUID) == "" {
		return ""
	}

	port := e.Port
	if port <= 0 {
		port = 443
	}
	sni := strings.TrimSpace(e.SNI)
	if sni == "" {
		sni = e.Address
	}
	remarks := strings.TrimSpace(e.Remarks)
	if remarks == "" {
		remarks = "Hysteria"
	}

	params := []string{"sni=" + url.QueryEscape(sni)}
	if e.Insecure {
		params = append(params, "insecure=1")
	}
	if e.ObfsType != "" && e.ObfsPassword != "" {
		params = append(params, "obfs="+url.QueryEscape(e.ObfsType))
		params = append(params, "obfs-password="+url.QueryEscape(e.ObfsPassword))
	}

	return "hysteria2://" + url.QueryEscape(userUUID) + "@" + e.Address + ":" + strconv.Itoa(port) +
		"?" + strings.Join(params, "&") + "#" + escapeFragment(remarks)
}
