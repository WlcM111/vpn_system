package vpn_orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ============================================================================
// Боевой манифест маршрутизации — строгий список исключений.
//
// Напрямую идут только перечисленные сервисы. Всё остальное, включая
// зарубежные сервисы, уходит через VPN: у Happ это GlobalProxy профиля, у
// Xray-клиентов — outbound по умолчанию. Тесты охраняют это свойство от
// случайного возврата широких правил.
// ============================================================================

func loadShippedManifest(t *testing.T) RoutingManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "routing", "routing-manifest.json"))
	if err != nil {
		t.Fatalf("манифест не читается: %v", err)
	}
	var m RoutingManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("манифест не разбирается как JSON: %v", err)
	}
	return m
}

func TestShippedManifestHasNoBroadDirectRules(t *testing.T) {
	m := loadShippedManifest(t)
	// geosite/geoip отправили бы напрямую целые категории и страны.
	if m.GeoRules.Enabled {
		t.Error("geo_rules.enabled = true: geosite/geoip уводят мимо VPN всё, что не в списке")
	}
	for _, d := range m.DirectDomains {
		// Только точные правила: domain: — домен и поддомены, full: — ровно
		// имя. Запись без префикса в Xray — поиск подстроки, а regexp: и
		// keyword: — шаблоны; любое из них ломает строгость списка.
		if !strings.HasPrefix(d, "domain:") && !strings.HasPrefix(d, "full:") {
			t.Errorf("неточное правило в direct_domains: %q", d)
		}
	}
	for _, ip := range m.DirectIPs {
		if strings.HasPrefix(ip, "geoip:") {
			t.Errorf("geoip-правило в direct_ips уводит мимо VPN целую страну: %q", ip)
		}
	}
}

func TestShippedManifestDirectDomainsAreUnique(t *testing.T) {
	m := loadShippedManifest(t)
	seen := make(map[string]bool, len(m.DirectDomains))
	for _, d := range m.DirectDomains {
		if d != strings.ToLower(strings.TrimSpace(d)) {
			t.Errorf("значение не нормализовано: %q", d)
		}
		if seen[d] {
			t.Errorf("дубль в direct_domains: %q", d)
		}
		seen[d] = true
	}
}

func TestShippedManifestCoversRequiredServices(t *testing.T) {
	m := loadShippedManifest(t)
	have := make(map[string]bool, len(m.DirectDomains))
	for _, d := range m.DirectDomains {
		have[d] = true
	}
	// По одному опорному домену на каждый сервис из списка исключений.
	for _, d := range []string{
		"domain:vtb.ru", "domain:gazprombank.ru", "domain:alfabank.ru", "domain:psbank.ru",
		"domain:ozon.ru", "domain:wildberries.ru", "domain:avito.ru", "domain:market.yandex.ru",
		"domain:max.ru", "domain:vk.com", "domain:rzd.ru", "domain:tutu.ru",
		"domain:kinopoisk.ru", "domain:okko.tv", "domain:ivi.ru", "domain:hh.ru",
		"domain:gosuslugi.ru", "domain:pochta.ru", "domain:cian.ru",
		"domain:mts.ru", "domain:megafon.ru", "domain:t2.ru", "domain:beeline.ru",
		"domain:yota.ru", "domain:lamoda.ru", "domain:taxi.yandex.ru",
	} {
		if !have[d] {
			t.Errorf("в списке исключений нет %s", d)
		}
	}
}

// Всё, что не в списке, обязано уходить в VPN: у Happ это GlobalProxy=true.
// Профиль должен собираться для обеих групп клиентов.
func TestShippedManifestCompilesWithGlobalProxy(t *testing.T) {
	m := loadShippedManifest(t)
	happ := decodeHapp(t, compileHappRoutingB64(&m))
	if happ.GlobalProxy != "true" {
		t.Errorf("GlobalProxy = %q: несовпавший трафик должен идти через VPN", happ.GlobalProxy)
	}
	if len(happ.DirectSites) != len(m.DirectDomains) {
		t.Errorf("DirectSites: %d значений, в манифесте %d", len(happ.DirectSites), len(m.DirectDomains))
	}
	if compileXrayRoutingB64(&m) == "" {
		t.Error("роутинг Xray не собрался")
	}
}

// В Xray условия одного правила должны выполниться одновременно: правило с
// domain и ip сразу при AsIs не совпадает ни с одним доменным запросом.
func TestXrayRulesNeverMixDomainAndIP(t *testing.T) {
	m := loadShippedManifest(t)
	m.ProxyIPs = []string{"203.0.113.0/24"}
	for _, r := range decodeXray(t, compileXrayRoutingB64(&m)).Rules {
		if len(r.Domain) > 0 && len(r.IP) > 0 {
			t.Errorf("правило %q смешивает domain и ip — при AsIs не сработает", r.Name)
		}
	}
}

func TestRoutingHeaderPayload(t *testing.T) {
	if got := routingHeaderPayload(clientGroupHapp, "XRAY"); got != "" {
		t.Errorf("Happ/Incy: заголовок %q, ожидался пустой — профиль едет в теле", got)
	}
	if got := routingHeaderPayload(clientGroupXray, "XRAY"); got != "XRAY" {
		t.Errorf("Xray-клиенты: заголовок %q, ожидался XRAY", got)
	}
	if got := routingHeaderPayload(clientGroupXray, ""); got != "" {
		t.Errorf("без роутинга заголовок должен быть пустым, получено %q", got)
	}
}
