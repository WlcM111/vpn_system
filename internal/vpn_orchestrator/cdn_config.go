package vpn_orchestrator

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

// ============================================================================
// CDN-эндпоинты (VLESS-over-XHTTP фронты), привязанные к серверам.
//
// CDN — это НЕ отдельная нода, а альтернативный способ подключения к exit-узлу
// через CDN-фронт, скрывающий реальный IP. CDN-ссылка добавляется в общий фид
// подписки (одна ссылка на всё). UUID берётся тот же, что у обычных конфигов.
//
// Выбор CDN по серверу: пользователю выдаётся наименее загруженный сервер; к нему
// подбирается CDN, привязанный к этому server_key. Если персональной привязки нет
// — берётся глобальный CDN (server_key IS NULL) либо первый доступный.
//
// Источник CDN — таблица vpn_cdn_endpoints (управляется через Admin API). Если
// таблица пуста, но задан CDN_ADDRESS в окружении — используется он (обратная
// совместимость с однокдоновой конфигурацией).
// ============================================================================

// CDNEndpoint — параметры одного CDN-фронта (из БД или из окружения).
type CDNEndpoint struct {
	CDNKey     string
	ServerKey  string // "" = глобальный (fallback)
	Enabled    bool
	SortOrder  int
	InboundTag string // inbound на узле, куда регистрируется пользователь для этого CDN

	Address     string
	ServerName  string
	Host        string
	Port        int
	XHTTPPath   string
	Mode        string
	Fingerprint string
	ALPN        string
	Remarks     string

	PaddingObfsMode      bool
	PaddingPlacement     string
	PaddingKey           string
	PaddingMethod        string
	ScMaxBufferedPosts   int
	ScMinPostsIntervalMs string
}

// cdnConfigEnabled сообщает, включена ли выдача CDN глобально (рубильник в env).
// Позволяет полностью отключить CDN, не трогая БД.
func cdnConfigEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("CDN_CONFIG_ENABLED")), "true")
}

// cdnEndpointFromEnv собирает CDN-эндпоинт из окружения (обратная совместимость,
// когда таблица vpn_cdn_endpoints пуста). Возвращает (endpoint, true) если задан
// хотя бы CDN_ADDRESS; иначе (zero, false).
func cdnEndpointFromEnv() (CDNEndpoint, bool) {
	addr := strings.TrimSpace(os.Getenv("CDN_ADDRESS"))
	if addr == "" {
		return CDNEndpoint{}, false
	}

	port := 443
	if raw := strings.TrimSpace(os.Getenv("CDN_PORT")); raw != "" {
		if p, err := strconv.Atoi(raw); err == nil && p > 0 {
			port = p
		}
	}
	scMax := 256
	if raw := strings.TrimSpace(os.Getenv("CDN_SC_MAX_BUFFERED_POSTS")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			scMax = v
		}
	}

	return CDNEndpoint{
		CDNKey:               "env",
		ServerKey:            "", // глобальный
		Enabled:              true,
		SortOrder:            0,
		Address:              addr,
		ServerName:           strings.TrimSpace(os.Getenv("CDN_SERVER_NAME")),
		Host:                 strings.TrimSpace(os.Getenv("CDN_HOST")),
		Port:                 port,
		XHTTPPath:            envOrDefault("CDN_XHTTP_PATH", "/api/uploadFile/"),
		Mode:                 envOrDefault("CDN_MODE", "packet-up"),
		Fingerprint:          envOrDefault("CDN_FINGERPRINT", "chrome"),
		ALPN:                 envOrDefault("CDN_ALPN", "h2,http/1.1"),
		Remarks:              envOrDefault("CDN_REMARKS", "race-src-cdn"),
		PaddingObfsMode:      !strings.EqualFold(strings.TrimSpace(os.Getenv("CDN_PADDING_OBFS_MODE")), "false"),
		PaddingPlacement:     envOrDefault("CDN_PADDING_PLACEMENT", "cookie"),
		PaddingKey:           envOrDefault("CDN_PADDING_KEY", "ssid"),
		PaddingMethod:        envOrDefault("CDN_PADDING_METHOD", "tokenish"),
		ScMaxBufferedPosts:   scMax,
		ScMinPostsIntervalMs: envOrDefault("CDN_SC_MIN_POSTS_INTERVAL_MS", "5"),
	}, true
}

func envOrDefault(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

// selectCDNForServer выбирает CDN-эндпоинт для конкретного server_key из списка.
// Приоритет: (1) привязанный к этому серверу; (2) глобальный (ServerKey=="");
// (3) первый доступный. Возвращает (endpoint, true) если что-то нашлось.
//
// endpoints предполагается уже отфильтрованным по Enabled и отсортированным по
// SortOrder, id (так отдаёт репозиторий).
func selectCDNForServer(endpoints []CDNEndpoint, serverKey string) (CDNEndpoint, bool) {
	if len(endpoints) == 0 {
		return CDNEndpoint{}, false
	}

	// 1) привязка к конкретному серверу
	if serverKey != "" {
		for _, e := range endpoints {
			if e.ServerKey == serverKey {
				return e, true
			}
		}
	}

	// 2) глобальный CDN (без привязки) — только если он единственный вариант
	for _, e := range endpoints {
		if e.ServerKey == "" {
			return e, true
		}
	}

	// 3) У этого сервера нет CDN-эндпоинта — см. комментарий в grpc_config.go:
	// чужой эндпоинт означает ссылку на ноду, где пользователя нет.
	return CDNEndpoint{}, false
}

// BuildCDNVLESSURLFromEndpoint строит CDN vless://-ссылку из эндпоинта и UUID.
// Возвращает "" если эндпоинт невалиден (нет адреса) или UUID пуст.
func BuildCDNVLESSURLFromEndpoint(e CDNEndpoint, userUUID string) string {
	if strings.TrimSpace(e.Address) == "" || strings.TrimSpace(userUUID) == "" {
		return ""
	}

	sni := e.ServerName
	if sni == "" {
		sni = e.Address
	}
	host := e.Host
	if host == "" {
		host = e.Address
	}
	port := e.Port
	if port <= 0 {
		port = 443
	}
	path := e.XHTTPPath
	if path == "" {
		path = "/api/uploadFile/"
	}
	mode := e.Mode
	if mode == "" {
		mode = "packet-up"
	}
	fp := e.Fingerprint
	if fp == "" {
		fp = "chrome"
	}
	alpn := e.ALPN
	if alpn == "" {
		alpn = "h2,http/1.1"
	}
	remarks := e.Remarks
	if remarks == "" {
		remarks = "race-src-cdn"
	}
	placement := e.PaddingPlacement
	if placement == "" {
		placement = "cookie"
	}
	pkey := e.PaddingKey
	if pkey == "" {
		pkey = "ssid"
	}
	pmethod := e.PaddingMethod
	if pmethod == "" {
		pmethod = "tokenish"
	}
	scMax := e.ScMaxBufferedPosts
	if scMax <= 0 {
		scMax = 256
	}
	scMin := e.ScMinPostsIntervalMs
	if scMin == "" {
		scMin = "5"
	}

	// extra — JSON обфускации. Порядок ключей соответствует рабочему конфигу.
	extraJSON := "{" +
		`"xPaddingObfsMode":` + strconv.FormatBool(e.PaddingObfsMode) + "," +
		`"xPaddingPlacement":"` + placement + `",` +
		`"xPaddingKey":"` + pkey + `",` +
		`"xPaddingMethod":"` + pmethod + `",` +
		`"scMaxBufferedPosts":` + strconv.Itoa(scMax) + "," +
		`"scMinPostsIntervalMs":"` + scMin + `"` +
		"}"

	params := []string{
		"encryption=none",
		"security=tls",
		"sni=" + url.QueryEscape(sni),
		"fp=" + url.QueryEscape(fp),
		"alpn=" + url.QueryEscape(alpn),
		"type=xhttp",
		"host=" + url.QueryEscape(host),
		"path=" + url.QueryEscape(path),
		"mode=" + url.QueryEscape(mode),
		"extra=" + url.QueryEscape(extraJSON),
	}

	remark := url.QueryEscape(remarks)
	return "vless://" + userUUID + "@" + e.Address + ":" + strconv.Itoa(port) +
		"?" + strings.Join(params, "&") + "#" + remark
}
