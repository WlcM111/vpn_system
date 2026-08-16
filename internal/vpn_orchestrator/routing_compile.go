package vpn_orchestrator

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// ============================================================================
// Компиляция манифеста в клиентские форматы + определение клиента.
//
// Две форматные группы (см. дизайн-документ):
//   clientGroupHapp — Happ, Incy      → Happ-профиль (DirectSites/DirectIp/...)
//   clientGroupXray — v2RayTun, Streisand → Xray-JSON (rules[] с outboundTag)
//
// Обе доставляются заголовком `routing` (base64). Для Happ/Incy дополнительно
// кладём deeplink в тело подписки — он даёт АВТО-АКТИВАЦИЮ профиля (.../onadd/),
// то есть пользователю не нужно ничего включать руками.
// ============================================================================

type clientGroup string

const (
	clientGroupHapp clientGroup = "happ"
	clientGroupXray clientGroup = "xray"
)

// Стандартные гео-базы (те же, что в профиле по умолчанию Happ).
// Нужны, потому что активация профиля гейтится успешной загрузкой гео-файлов.
const (
	happGeoIPURL   = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"
	happGeoSiteURL = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat"
)

// manifestVersionStamp возвращает метку времени версии манифеста для поля
// LastUpdated. Берётся из времени изменения файла-манифеста: правка манифеста
// меняет метку, клиент видит новую версию и переприменяет профиль.
func manifestVersionStamp() int64 {
	path := routingManifestPath()
	if path == "" {
		return time.Now().Unix()
	}
	st, err := os.Stat(path)
	if err != nil {
		return time.Now().Unix()
	}
	return st.ModTime().Unix()
}

// detectClientGroup определяет формат роутинга для запроса.
// Приоритет: явный параметр ?c= (его проставляет бот по выбору пользователя),
// затем User-Agent, затем безопасный дефолт.
func detectClientGroup(r *http.Request) clientGroup {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("c"))) {
	case "happ", "incy":
		return clientGroupHapp
	case "xray", "v2raytun", "streisand":
		return clientGroupXray
	}

	ua := strings.ToLower(r.UserAgent())
	switch {
	case strings.Contains(ua, "happ"), strings.Contains(ua, "incy"):
		return clientGroupHapp
	case strings.Contains(ua, "v2raytun"), strings.Contains(ua, "streisand"),
		strings.Contains(ua, "v2ray"), strings.Contains(ua, "xray"):
		return clientGroupXray
	}

	// Неизвестный клиент: Xray-JSON — более распространённый формат.
	return clientGroupXray
}

// ---------------------------------------------------------------------------
// Xray-JSON (v2RayTun, Streisand)
// ---------------------------------------------------------------------------

type xrayRoutingRule struct {
	Type        string   `json:"type"`
	OutboundTag string   `json:"outboundTag"`
	Name        string   `json:"__name__,omitempty"`
	Domain      []string `json:"domain,omitempty"`
	IP          []string `json:"ip,omitempty"`
}

type xrayRouting struct {
	DomainStrategy string            `json:"domainStrategy"`
	DomainMatcher  string            `json:"domainMatcher"`
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Balancers      []any             `json:"balancers"`
	Rules          []xrayRoutingRule `json:"rules"`
}

// compileXrayRoutingB64 собирает Xray-совместимый объект роутинга.
// Порядок правил важен: block → proxy → direct (более специфичное раньше).
func compileXrayRoutingB64(m *RoutingManifest) string {
	if m == nil {
		return ""
	}

	rules := make([]xrayRoutingRule, 0, 4)

	if len(m.BlockDomains) > 0 {
		rules = append(rules, xrayRoutingRule{
			Type: "field", OutboundTag: "block", Name: "Block",
			Domain: m.BlockDomains,
		})
	}
	if len(m.ProxyDomains) > 0 || len(m.ProxyIPs) > 0 {
		rules = append(rules, xrayRoutingRule{
			Type: "field", OutboundTag: "proxy", Name: "Force proxy",
			Domain: m.ProxyDomains, IP: m.ProxyIPs,
		})
	}

	directDomains := append([]string{}, m.DirectDomains...)
	directIPs := append([]string{}, m.DirectIPs...)
	if m.GeoRules.Enabled {
		directDomains = append(directDomains, m.GeoRules.DirectSites...)
		directIPs = append(directIPs, m.GeoRules.DirectIPs...)
	}
	if len(directDomains) > 0 || len(directIPs) > 0 {
		rules = append(rules, xrayRoutingRule{
			Type: "field", OutboundTag: "direct", Name: "Direct Russia",
			Domain: directDomains, IP: directIPs,
		})
	}
	if len(rules) == 0 {
		return ""
	}

	name := strings.TrimSpace(m.Name)
	if name == "" {
		name = "House VPN"
	}

	payload := xrayRouting{
		// AsIs — без предварительного резолва. Наш список direct содержит только
		// .ru/.рф, поэтому при IPIfNonMatch Xray резолвил КАЖДЫЙ иностранный домен
		// через российский DNS, и заблокированные сайты не открывались.
		DomainStrategy: "AsIs",
		DomainMatcher:  "hybrid",
		ID:             stableRoutingID(name),
		Name:           name,
		Balancers:      []any{},
		Rules:          rules,
	}

	buf, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(buf)
}

// ---------------------------------------------------------------------------
// Happ-профиль (Happ, Incy)
// ---------------------------------------------------------------------------

type happRouting struct {
	Name        string `json:"Name"`
	GlobalProxy string `json:"GlobalProxy"`

	// UseChunkFiles — Happ вырезает из геофайлов только нужные секции.
	// Обязательно для iOS: там лимит памяти ядра 50 МБ, без нарезки Xray падает.
	UseChunkFiles string `json:"UseChunkFiles"`

	RemoteDNSType     string `json:"RemoteDNSType,omitempty"`
	RemoteDNSDomain   string `json:"RemoteDNSDomain,omitempty"`
	RemoteDNSIP       string `json:"RemoteDNSIP,omitempty"`
	DomesticDNSType   string `json:"DomesticDNSType,omitempty"`
	DomesticDNSDomain string `json:"DomesticDNSDomain,omitempty"`
	DomesticDNSIP     string `json:"DomesticDNSIP,omitempty"`

	Geoipurl   string `json:"Geoipurl"`
	Geositeurl string `json:"Geositeurl"`

	// LastUpdated — метка времени. По документации Happ именно она запускает
	// принудительную загрузку геофайлов, а активация профиля гейтится успешной
	// загрузкой. Без этого поля тумблер маршрутизации не включается сам.
	LastUpdated string `json:"LastUpdated"`

	// RouteOrder — порядок проверки правил. Без него порядок не определён.
	RouteOrder string `json:"RouteOrder"`

	DirectSites []string `json:"DirectSites"`
	DirectIp    []string `json:"DirectIp"`
	ProxySites  []string `json:"ProxySites"`
	ProxyIp     []string `json:"ProxyIp"`
	BlockSites  []string `json:"BlockSites"`
	BlockIp     []string `json:"BlockIp"`

	DomainStrategy string `json:"DomainStrategy"`
	FakeDNS        string `json:"FakeDNS"`
}

// happValue нормализует значение для Happ-профиля.
//
// Happ передаёт значения в Xray-ядро как есть — это видно по официальному
// примеру из документации, где в DirectSites стоит "geosite:ru" с префиксом.
// Поэтому Xray-префиксы НЕЛЬЗЯ срезать: без "regexp:" регулярка превращается
// в обычную доменную строку и правило перестаёт совпадать с чем-либо.
//
// Срезаем только "domain:" — в Xray он семантически эквивалентен записи без
// префикса (совпадение по домену и поддоменам), так что запись становится
// короче без изменения поведения.
func happValue(v string) string {
	if strings.HasPrefix(v, "domain:") {
		return strings.TrimPrefix(v, "domain:")
	}
	return v
}

func happValues(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		out = append(out, happValue(v))
	}
	return out
}

// compileHappRoutingB64 собирает профиль в формате Happ/Incy.
// Name держим стабильным: клиенты обновляют профиль с тем же именем,
// а не плодят дубликаты при каждом обновлении подписки.
func compileHappRoutingB64(m *RoutingManifest) string {
	if m == nil {
		return ""
	}

	directSites := happValues(m.DirectDomains)
	directIPs := happValues(m.DirectIPs)
	if m.GeoRules.Enabled {
		directSites = append(directSites, m.GeoRules.DirectSites...)
		directIPs = append(directIPs, m.GeoRules.DirectIPs...)
	}
	if len(directSites) == 0 && len(directIPs) == 0 &&
		len(m.ProxyDomains) == 0 && len(m.BlockDomains) == 0 {
		return ""
	}

	name := strings.TrimSpace(m.Name)
	if name == "" {
		name = "House VPN"
	}
	// Ограничение клиента на длину имени профиля.
	//
	// Режем по рунам, а не по байтам: len() для строки в Go возвращает байты,
	// и граница могла попасть в середину многобайтового символа. Кириллическое
	// имя приезжало в клиент с повреждённым символом на конце, а сам лимит
	// срабатывал вдвое раньше (25 байт — это ~12 русских букв).
	if r := []rune(name); len(r) > 25 {
		name = string(r[:25])
	}

	payload := happRouting{
		Name:          name,
		GlobalProxy:   "true",
		UseChunkFiles: "true",
		// Гео-файлы обязательны: активация профиля в Happ гейтится их успешной
		// загрузкой. Ссылки — стандартные loyalsoldier, как в профиле по умолчанию.
		Geoipurl:   happGeoIPURL,
		Geositeurl: happGeoSiteURL,
		// Метка времени = версия манифеста. Меняется при каждой правке файла,
		// поэтому клиент перекачает гео-файлы и переприменит профиль.
		LastUpdated:    strconv.FormatInt(manifestVersionStamp(), 10),
		RouteOrder:     "block-proxy-direct",
		DirectSites:    directSites,
		DirectIp:       directIPs,
		ProxySites:     happValues(m.ProxyDomains),
		ProxyIp:        happValues(m.ProxyIPs),
		BlockSites:     happValues(m.BlockDomains),
		BlockIp:        []string{},
		DomainStrategy: "AsIs",
		FakeDNS:        "false",
	}

	if m.DNS.RemoteIP != "" {
		payload.RemoteDNSIP = m.DNS.RemoteIP
		payload.RemoteDNSType = "DoH"
		payload.RemoteDNSDomain = m.DNS.RemoteDoH
	}
	if m.DNS.DomesticIP != "" {
		payload.DomesticDNSIP = m.DNS.DomesticIP
		payload.DomesticDNSType = "DoH"
		payload.DomesticDNSDomain = m.DNS.DomesticDoH
	}
	// Если в манифесте заданы свои гео-ссылки — используем их.
	if m.GeoRules.Enabled && m.GeoRules.GeoIPURL != "" {
		payload.Geoipurl = m.GeoRules.GeoIPURL
	}
	if m.GeoRules.Enabled && m.GeoRules.GeoSiteURL != "" {
		payload.Geositeurl = m.GeoRules.GeoSiteURL
	}

	buf, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(buf)
}

// ---------------------------------------------------------------------------
// Вспомогательное
// ---------------------------------------------------------------------------

// stableRoutingID — детерминированный UUID-подобный идентификатор профиля.
// Стабилен между рестартами: клиент видит «тот же профиль», а не новый.
func stableRoutingID(seed string) string {
	sum := sha1.Sum([]byte("house-vpn-routing:" + seed))
	h := hex.EncodeToString(sum[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// routingBodyLines возвращает строки, которые надо добавить В ТЕЛО подписки
// для Happ/Incy. Формат `.../onadd/` включает авто-активацию профиля —
// пользователь не нажимает ничего, роутинг применяется сам.
func routingBodyLines(group clientGroup, happB64 string) []string {
	if group != clientGroupHapp || happB64 == "" {
		return nil
	}
	return []string{
		"happ://routing/onadd/" + happB64,
		"incy://routing/onadd/" + happB64,
	}
}
