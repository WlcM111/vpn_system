package vpn_orchestrator

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

// ============================================================================
// XHTTP-эндпоинты (VLESS + XHTTP + TLS), привязанные к серверам.
//
// Прямой вход на ноду: TLS на 443 терминирует nginx ноды с сертификатом её
// собственного домена, путь эндпоинта проксируется (grpc_pass, h2c) в
// XHTTP-инбаунд Xray на 127.0.0.1. Это не CDN: CDN-эндпоинты (таблица
// vpn_cdn_endpoints) живут отдельно, со своим email учётки и своей квотой.
//
// Пользователь регистрируется в инбаунде эндпоинта с той же учёткой (email и
// UUID), что и в Reality-инбаунде этого сервера. flow у XHTTP всегда пустой:
// xtls-rprx-vision работает только поверх TCP с TLS/REALITY напрямую.
//
// Источник — таблица vpn_xhttp_endpoints. Env-фолбэка нет: путь, адрес и
// инбаунд у каждой ноды свои.
// ============================================================================

// XHTTPEndpoint — параметры одного XHTTP-эндпоинта.
type XHTTPEndpoint struct {
	XHTTPKey   string
	ServerKey  string
	Enabled    bool
	SortOrder  int
	InboundTag string // инбаунд на узле, куда регистрируется пользователь

	Address     string
	ServerName  string
	Host        string
	Port        int
	Path        string
	Mode        string
	Fingerprint string
	ALPN        string
	Remarks     string
}

// xhttpConfigEnabled — рубильник транспорта. Выключенный XHTTP_CONFIG_ENABLED
// убирает XHTTP из фида и из желаемого состояния узлов, не трогая таблицу.
//
// По умолчанию включён, как Reality: пустая таблица сама по себе означает
// «транспорт не настроен».
func xhttpConfigEnabled() bool {
	v := strings.TrimSpace(os.Getenv("XHTTP_CONFIG_ENABLED"))
	if v == "" {
		return true
	}
	return v == "1" || strings.EqualFold(v, "true")
}

// selectXHTTPForServer возвращает эндпоинт, привязанный к конкретному серверу.
//
// Фолбэка на «глобальный» или «первый попавшийся» нет намеренно: UUID
// выписывается на пару (пользователь, item_key), и эндпоинт чужой ноды в паре
// с этим UUID дал бы ссылку, по которой нельзя подключиться.
//
// endpoints предполагается уже отфильтрованным по Enabled и отсортированным по
// sort_order, id (так отдаёт репозиторий).
func selectXHTTPForServer(endpoints []XHTTPEndpoint, serverKey string) (XHTTPEndpoint, bool) {
	if len(endpoints) == 0 || serverKey == "" {
		return XHTTPEndpoint{}, false
	}
	for _, e := range endpoints {
		if e.ServerKey == serverKey {
			return e, true
		}
	}
	return XHTTPEndpoint{}, false
}

// BuildXHTTPVLESSURLFromEndpoint собирает vless://-ссылку XHTTP + TLS.
//
// Обязательны адрес, UUID и путь: путь — единственное, по чему nginx ноды
// отличает XHTTP-запросы от обычного сайта. Без них возвращается пустая
// строка: лучше не выдать ссылку, чем выдать заведомо нерабочую.
func BuildXHTTPVLESSURLFromEndpoint(e XHTTPEndpoint, userUUID string) string {
	address := strings.TrimSpace(e.Address)
	path := strings.TrimSpace(e.Path)
	if address == "" || strings.TrimSpace(userUUID) == "" || !strings.HasPrefix(path, "/") {
		return ""
	}

	sni := strings.TrimSpace(e.ServerName)
	if sni == "" {
		sni = address
	}
	host := strings.TrimSpace(e.Host)
	if host == "" {
		host = sni
	}
	port := e.Port
	if port <= 0 {
		port = 443
	}
	mode := strings.TrimSpace(e.Mode)
	if mode == "" {
		mode = "auto"
	}
	fp := strings.TrimSpace(e.Fingerprint)
	if fp == "" {
		fp = "chrome"
	}
	alpn := strings.TrimSpace(e.ALPN)
	if alpn == "" {
		alpn = "h2,http/1.1"
	}
	remarks := strings.TrimSpace(e.Remarks)
	if remarks == "" {
		remarks = "xhttp"
	}

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
	}

	return "vless://" + userUUID + "@" + address + ":" + strconv.Itoa(port) +
		"?" + strings.Join(params, "&") + "#" + escapeFragment(remarks)
}
