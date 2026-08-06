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
// Hysteria2 работает поверх UDP/QUIC и не имеет inbound'ов: пользователь не
// регистрируется на узле заранее. Вместо этого Hysteria при каждом подключении
// спрашивает node-agent по HTTP, пускать ли клиента, а паролем выступает
// VLESS-UUID пользователя. Поэтому здесь нет ни InboundTag, ни отдельных учёток.
//
// Источник — таблица vpn_hysteria_endpoints. Если она пуста, но задан
// HYSTERIA_ADDRESS в окружении — используется он.
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
}

// hysteriaConfigEnabled — глобальный рубильник выдачи Hysteria (env).
func hysteriaConfigEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("HYSTERIA_CONFIG_ENABLED")), "true")
}

// hysteriaEndpointFromEnv собирает эндпоинт из окружения (для одной ноды без
// записи в БД). Возвращает (endpoint, true) если задан HYSTERIA_ADDRESS.
func hysteriaEndpointFromEnv() (HysteriaEndpoint, bool) {
	addr := strings.TrimSpace(os.Getenv("HYSTERIA_ADDRESS"))
	if addr == "" {
		return HysteriaEndpoint{}, false
	}

	port := 443
	if raw := strings.TrimSpace(os.Getenv("HYSTERIA_PORT")); raw != "" {
		if p, err := strconv.Atoi(raw); err == nil && p > 0 {
			port = p
		}
	}

	return HysteriaEndpoint{
		HysteriaKey:  "env",
		ServerKey:    strings.TrimSpace(os.Getenv("HYSTERIA_SERVER_KEY")),
		Enabled:      true,
		SortOrder:    0,
		Address:      addr,
		Port:         port,
		SNI:          strings.TrimSpace(os.Getenv("HYSTERIA_SNI")),
		Insecure:     strings.EqualFold(strings.TrimSpace(os.Getenv("HYSTERIA_INSECURE")), "true"),
		ObfsType:     strings.TrimSpace(os.Getenv("HYSTERIA_OBFS_TYPE")),
		ObfsPassword: strings.TrimSpace(os.Getenv("HYSTERIA_OBFS_PASSWORD")),
		Remarks:      envOrDefault("HYSTERIA_REMARKS", "Hysteria"),
	}, true
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
		"?" + strings.Join(params, "&") + "#" + url.QueryEscape(remarks)
}
