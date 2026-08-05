package vpn_orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// Манифест сплит-роутинга — единственный источник правды для правил.
//
// Источник: JSON-файл, монтируемый в контейнер (ROUTING_MANIFEST_PATH).
// Правка файла подхватывается на лету (hot-reload по mtime, проверка не чаще
// раза в manifestCheckInterval) — рестарт сервиса не требуется.
//
// Fail-open: если файл отсутствует, битый или пустой — роутинг просто не
// отдаётся, подписка работает как раньше. Роутинг это улучшение, а не
// критичный путь: сломать пользователю VPN он не должен ни при каких условиях.
// ============================================================================

const manifestCheckInterval = 10 * time.Second

// RoutingManifest — структура файла-манифеста.
type RoutingManifest struct {
	Version int    `json:"version"`
	Name    string `json:"name"`

	DNS struct {
		DomesticIP  string `json:"domestic_ip"`
		DomesticDoH string `json:"domestic_doh"`
		RemoteIP    string `json:"remote_ip"`
		RemoteDoH   string `json:"remote_doh"`
	} `json:"dns"`

	DirectDomains []string `json:"direct_domains"`
	DirectIPs     []string `json:"direct_ips"`
	ProxyDomains  []string `json:"proxy_domains"`
	ProxyIPs      []string `json:"proxy_ips"`
	BlockDomains  []string `json:"block_domains"`

	// GeoRules — задел на Вариант C (опора на внешние geo-файлы).
	// По умолчанию выключено: первый релиз строго инлайн, без зависимости
	// от наличия и версии .dat-файлов на клиенте.
	GeoRules struct {
		Enabled     bool     `json:"enabled"`
		DirectSites []string `json:"direct_sites"`
		DirectIPs   []string `json:"direct_ips"`
		GeoIPURL    string   `json:"geoip_url"`
		GeoSiteURL  string   `json:"geosite_url"`
	} `json:"geo_rules"`
}

// hasRules сообщает, есть ли в манифесте хоть одно правило. Пустой манифест
// компилировать бессмысленно — лучше отдать подписку без роутинга.
func (m *RoutingManifest) hasRules() bool {
	if m == nil {
		return false
	}
	n := len(m.DirectDomains) + len(m.DirectIPs) + len(m.ProxyDomains) +
		len(m.ProxyIPs) + len(m.BlockDomains)
	if m.GeoRules.Enabled {
		n += len(m.GeoRules.DirectSites) + len(m.GeoRules.DirectIPs)
	}
	return n > 0
}

// routingCache хранит манифест и уже скомпилированные под клиентов payload'ы.
// Компиляция выполняется один раз на версию файла (по mtime+size), дальше —
// отдача готовой строки.
type routingCache struct {
	mu sync.RWMutex

	path        string
	lastChecked time.Time
	fingerprint string // mtime+size файла — признак «файл изменился»

	manifest *RoutingManifest
	xrayB64  string // base64(Xray-JSON) — для v2RayTun / Streisand
	happB64  string // base64(Happ-профиль) — для Happ / Incy
}

var globalRoutingCache = &routingCache{}

// routingManifestPath возвращает путь к манифесту из env.
// Пустое значение = роутинг выключен (fail-open).
func routingManifestPath() string {
	return strings.TrimSpace(os.Getenv("ROUTING_MANIFEST_PATH"))
}

// load возвращает актуальные скомпилированные payload'ы. Никогда не возвращает
// ошибку наружу: при любой проблеме отдаёт пустые строки, и вызывающий код
// просто не проставляет заголовок роутинга.
func (c *routingCache) load() (xrayB64 string, happB64 string) {
	path := routingManifestPath()
	if path == "" {
		return "", ""
	}

	// Быстрый путь: кэш свеж — отдаём как есть.
	c.mu.RLock()
	if c.path == path && time.Since(c.lastChecked) < manifestCheckInterval {
		x, h := c.xrayB64, c.happB64
		c.mu.RUnlock()
		return x, h
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Повторная проверка после захвата write-lock (другая горутина могла успеть).
	if c.path == path && time.Since(c.lastChecked) < manifestCheckInterval {
		return c.xrayB64, c.happB64
	}
	c.lastChecked = time.Now()

	st, err := os.Stat(path)
	if err != nil {
		// Файла нет — роутинг выключен, подписка работает как раньше.
		c.path, c.fingerprint = path, ""
		c.manifest, c.xrayB64, c.happB64 = nil, "", ""
		return "", ""
	}

	fp := fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
	if c.path == path && c.fingerprint == fp && c.manifest != nil {
		// Файл не менялся — перекомпиляция не нужна.
		return c.xrayB64, c.happB64
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		c.path, c.fingerprint = path, ""
		c.manifest, c.xrayB64, c.happB64 = nil, "", ""
		return "", ""
	}

	var m RoutingManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		// Битый JSON: держим ПРЕДЫДУЩУЮ рабочую версию, если она была.
		// Так опечатка в манифесте не выключает роутинг всем пользователям.
		c.path, c.fingerprint = path, fp
		return c.xrayB64, c.happB64
	}
	if !m.hasRules() {
		c.path, c.fingerprint = path, fp
		c.manifest, c.xrayB64, c.happB64 = nil, "", ""
		return "", ""
	}

	c.path = path
	c.fingerprint = fp
	c.manifest = &m
	c.xrayB64 = compileXrayRoutingB64(&m)
	c.happB64 = compileHappRoutingB64(&m)
	return c.xrayB64, c.happB64
}
