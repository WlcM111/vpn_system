package vpn_orchestrator

import (
	"net/url"
	"strconv"
	"strings"
)

// ============================================================================
// Reality-эндпоинты (VLESS + TCP + Reality).
//
// Отличие от трёх остальных транспортов: Reality сам терминирует TLS и потому
// не может стоять за nginx или CDN. Ему нужен собственный порт и прямой доступ
// снаружи. Взамен он не требует ни домена, ни сертификата: клиент подключается
// по IP узла и предъявляет SNI чужого сайта.
//
// Практическая ценность для нас: WS, gRPC и XHTTP завязаны на race-src.com и
// его сертификат — при бане домена перестают работать разом. Reality от домена
// не зависит вовсе.
// ============================================================================

// RealityEndpoint — параметры одного Reality-эндпоинта.
//
// Приватного ключа здесь нет и быть не должно: он остаётся в config.json на
// узле. В ссылку уходит только публичная половина пары.
type RealityEndpoint struct {
	RealityKey string
	ServerKey  string
	Enabled    bool
	SortOrder  int
	InboundTag string

	Address     string
	Port        int
	ServerName  string
	PublicKey   string
	ShortID     string
	SpiderX     string
	Flow        string
	Fingerprint string
	Remarks     string
}

// BuildRealityVLESSURLFromEndpoint собирает vless://-ссылку для Reality.
//
// Обязательные для работы параметры — pbk (публичный ключ) и sni: без них
// клиент не сможет пройти рукопожатие. Поэтому при их отсутствии возвращается
// пустая строка: лучше не выдать ссылку вовсе, чем выдать заведомо нерабочую.
func BuildRealityVLESSURLFromEndpoint(e RealityEndpoint, userUUID string) string {
	if strings.TrimSpace(e.Address) == "" || strings.TrimSpace(userUUID) == "" {
		return ""
	}
	// Без публичного ключа и SNI ссылка бесполезна: клиент не построит
	// рукопожатие и получит ошибку соединения вместо туннеля.
	if strings.TrimSpace(e.PublicKey) == "" || strings.TrimSpace(e.ServerName) == "" {
		return ""
	}

	port := e.Port
	if port <= 0 {
		port = 8443
	}
	fp := e.Fingerprint
	if fp == "" {
		fp = "chrome"
	}
	spiderX := e.SpiderX
	if spiderX == "" {
		spiderX = "/"
	}
	remarks := e.Remarks
	if remarks == "" {
		remarks = "reality"
	}

	params := []string{
		"encryption=none",
		"security=reality",
		"type=tcp",
		"sni=" + url.QueryEscape(e.ServerName),
		"pbk=" + url.QueryEscape(e.PublicKey),
		"fp=" + url.QueryEscape(fp),
		"spx=" + url.QueryEscape(spiderX),
	}
	// shortId необязателен: узел может принимать пустой. Передаём, только
	// если задан, — пустой параметр sid некоторые клиенты трактуют как ошибку.
	if sid := strings.TrimSpace(e.ShortID); sid != "" {
		params = append(params, "sid="+url.QueryEscape(sid))
	}
	// flow передаётся только при xtls-rprx-vision. Пустое значение означает
	// обычный TLS-прокси, и параметр в ссылке в этом случае лишний.
	if flow := strings.TrimSpace(e.Flow); flow != "" {
		params = append(params, "flow="+url.QueryEscape(flow))
	}
	// ALPN для Reality не указываем: значение согласуется с сайтом-прикрытием
	// при рукопожатии, и жёсткая фиксация здесь только ломает маскировку.

	remark := escapeFragment(remarks)
	return "vless://" + userUUID + "@" + e.Address + ":" + strconv.Itoa(port) +
		"?" + strings.Join(params, "&") + "#" + remark
}

// selectRealityForServer возвращает эндпоинт, привязанный к конкретному узлу.
//
// Фолбэка на глобальный эндпоинт (ServerKey == "") здесь нет намеренно:
// address у Reality — это IP конкретной машины, и глобальный эндпоинт увёл бы
// пользователей одного узла на другой.
func selectRealityForServer(endpoints []RealityEndpoint, serverKey string) (RealityEndpoint, bool) {
	if len(endpoints) == 0 || serverKey == "" {
		return RealityEndpoint{}, false
	}
	for _, e := range endpoints {
		if e.ServerKey == serverKey {
			return e, true
		}
	}
	return RealityEndpoint{}, false
}
