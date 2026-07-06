package vpn_orchestrator

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

// ============================================================================
// gRPC-эндпоинты (VLESS-over-gRPC), привязанные к серверам.
//
// gRPC — НЕ отдельная нода, а альтернативный транспорт к тому же exit-узлу через
// nginx grpc_pass → xray vless-grpc-cdn-in. gRPC-ссылка добавляется в общий фид
// подписки (одна ссылка на всё). UUID берётся тот же, что у обычных конфигов.
//
// Выбор gRPC по серверу: пользователю выдаётся наименее загруженный сервер; к нему
// подбирается gRPC-эндпоинт, привязанный к этому server_key. Если персональной
// привязки нет — берётся глобальный (server_key IS NULL) либо первый доступный.
//
// Источник — таблица vpn_grpc_endpoints (через Admin API). Если таблица пуста, но
// задан GRPC_ADDRESS в окружении — используется он (обратная совместимость).
//
// Модель намеренно вынесена отдельно от CDN (vpn_cdn_endpoints), хотя структурно
// похожа: gRPC и XHTTP-CDN — разные транспорты и могут эволюционировать раздельно.
// ============================================================================

// GRPCEndpoint — параметры одного gRPC-транспорта (из БД или из окружения).
type GRPCEndpoint struct {
	GRPCKey    string
	ServerKey  string // "" = глобальный (fallback)
	Enabled    bool
	SortOrder  int
	InboundTag string // inbound на узле для регистрации пользователя

	Address     string
	ServerName  string
	Host        string
	Port        int
	ServiceName string
	Mode        string
	Fingerprint string
	ALPN        string
	Remarks     string
}

// grpcConfigEnabled — глобальный рубильник выдачи gRPC (env). Позволяет полностью
// отключить gRPC, не трогая БД.
func grpcConfigEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("GRPC_CONFIG_ENABLED")), "true")
}

// grpcEndpointFromEnv собирает gRPC-эндпоинт из окружения (обратная совместимость,
// когда таблица vpn_grpc_endpoints пуста). Возвращает (endpoint, true) если задан
// хотя бы GRPC_ADDRESS; иначе (zero, false).
func grpcEndpointFromEnv() (GRPCEndpoint, bool) {
	addr := strings.TrimSpace(os.Getenv("GRPC_ADDRESS"))
	if addr == "" {
		return GRPCEndpoint{}, false
	}

	port := 443
	if raw := strings.TrimSpace(os.Getenv("GRPC_PORT")); raw != "" {
		if p, err := strconv.Atoi(raw); err == nil && p > 0 {
			port = p
		}
	}

	return GRPCEndpoint{
		GRPCKey:     "env",
		ServerKey:   "", // глобальный
		Enabled:     true,
		SortOrder:   0,
		InboundTag:  envOrDefault("GRPC_INBOUND_TAG", "vless-grpc-cdn-in"),
		Address:     addr,
		ServerName:  strings.TrimSpace(os.Getenv("GRPC_SERVER_NAME")),
		Host:        strings.TrimSpace(os.Getenv("GRPC_HOST")),
		Port:        port,
		ServiceName: envOrDefault("GRPC_SERVICE_NAME", "api.grpc"),
		Mode:        envOrDefault("GRPC_MODE", "gun"),
		Fingerprint: envOrDefault("GRPC_FINGERPRINT", "chrome"),
		ALPN:        envOrDefault("GRPC_ALPN", "h2"),
		Remarks:     envOrDefault("GRPC_REMARKS", "race-src-grpc"),
	}, true
}

// selectGRPCForServer выбирает gRPC-эндпоинт для конкретного server_key из списка.
// Приоритет: (1) привязанный к этому серверу; (2) глобальный (ServerKey==""); (3)
// первый доступный. endpoints предполагается отфильтрованным по Enabled и
// отсортированным по SortOrder, id (так отдаёт репозиторий).
func selectGRPCForServer(endpoints []GRPCEndpoint, serverKey string) (GRPCEndpoint, bool) {
	if len(endpoints) == 0 {
		return GRPCEndpoint{}, false
	}

	// 1) привязка к конкретному серверу
	if serverKey != "" {
		for _, e := range endpoints {
			if e.ServerKey == serverKey {
				return e, true
			}
		}
	}

	// 2) глобальный (без привязки)
	for _, e := range endpoints {
		if e.ServerKey == "" {
			return e, true
		}
	}

	// 3) любой первый
	return endpoints[0], true
}

// BuildGRPCVLESSURLFromEndpoint строит gRPC vless://-ссылку из эндпоинта и UUID.
// Возвращает "" если эндпоинт невалиден (нет адреса) или UUID пуст.
//
// Формат соответствует ссылке, подтверждённой рабочей в Happ:
//
//	vless://UUID@host:port?encryption=none&security=tls&type=grpc
//	  &serviceName=api.grpc&mode=gun&sni=...&fp=chrome&alpn=h2#remarks
func BuildGRPCVLESSURLFromEndpoint(e GRPCEndpoint, userUUID string) string {
	if strings.TrimSpace(e.Address) == "" || strings.TrimSpace(userUUID) == "" {
		return ""
	}

	sni := e.ServerName
	if sni == "" {
		sni = e.Address
	}
	port := e.Port
	if port <= 0 {
		port = 443
	}
	serviceName := e.ServiceName
	if serviceName == "" {
		serviceName = "api.grpc"
	}
	mode := e.Mode
	if mode == "" {
		mode = "gun"
	}
	fp := e.Fingerprint
	if fp == "" {
		fp = "chrome"
	}
	alpn := e.ALPN
	if alpn == "" {
		alpn = "h2"
	}
	remarks := e.Remarks
	if remarks == "" {
		remarks = "race-src-grpc"
	}

	params := []string{
		"encryption=none",
		"security=tls",
		"type=grpc",
		"serviceName=" + url.QueryEscape(serviceName),
		"mode=" + url.QueryEscape(mode),
		"sni=" + url.QueryEscape(sni),
		"fp=" + url.QueryEscape(fp),
		"alpn=" + url.QueryEscape(alpn),
	}
	// host header для gRPC указываем, только если он задан и отличается от address.
	if strings.TrimSpace(e.Host) != "" && e.Host != e.Address {
		params = append(params, "host="+url.QueryEscape(e.Host))
	}

	remark := url.QueryEscape(remarks)
	return "vless://" + userUUID + "@" + e.Address + ":" + strconv.Itoa(port) +
		"?" + strings.Join(params, "&") + "#" + remark
}
