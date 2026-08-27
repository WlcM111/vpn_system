package vpn_orchestrator

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

// ============================================================================
// Классификация CDN-трафика и политика квоты.
//
// Ошибка здесь означает либо списание не-CDN байтов в CDN-квоту (пользователь
// теряет доступ, за который заплатил), либо безлимитный CDN. Обе цены высокие,
// поэтому проверяется именно граница классификации, а не happy path.
// ============================================================================

func TestCDNEmailIsolatesCounter(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"обычная учётка", "tg-727840698-lt-main-1-ws@vpn-platform.local", "tg-727840698-lt-main-1-ws-cdn@vpn-platform.local"},
		{"повторное применение не удваивает суффикс", "tg-1-x-cdn@vpn-platform.local", "tg-1-x-cdn@vpn-platform.local"},
		{"пустая строка", "", ""},
		{"нет @", "tg-1-lt-main-1", ""},
		{"@ в начале", "@vpn-platform.local", ""},
		{"@ в конце", "tg-1-lt@", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cdnEmail(tt.in); got != tt.want {
				t.Errorf("cdnEmail(%q) = %q, ожидалось %q", tt.in, got, tt.want)
			}
		})
	}
}

// Учётка CDN обязана отличаться от основной, иначе Xray сложит их счётчики
// в один и разделить трафик станет невозможно.
func TestCDNEmailDiffersFromBase(t *testing.T) {
	base := "tg-42-us-main-1-ws@vpn-platform.local"
	if cdnEmail(base) == base {
		t.Fatal("email CDN-учётки совпал с основным: счётчики Xray сольются")
	}
}

func TestCDNInboundAllowlistUsesServerConfigOnly(t *testing.T) {
	endpoints := []CDNEndpoint{
		{CDNKey: "lt", InboundTag: "vless-xhttp-cdn-in"},
		{CDNKey: "us", InboundTag: ""}, // пустой тег → дефолт, как в buildCDNProfile
		{CDNKey: "ar", InboundTag: "vless-xhttp-front-in"},
	}
	got := cdnInboundAllowlist(endpoints)

	for _, tag := range []string{"vless-xhttp-cdn-in", "vless-xhttp-front-in"} {
		if _, ok := got[tag]; !ok {
			t.Errorf("тег %q должен быть в allowlist", tag)
		}
	}
	// Основной инбаунд не CDN, даже если кому-то так «кажется по названию».
	for _, tag := range []string{"vless-ws-in", "vless-grpc-cdn-in", "cdn", ""} {
		if _, ok := got[tag]; ok {
			t.Errorf("тег %q не должен попадать в allowlist", tag)
		}
	}
}

// Пустой набор эндпоинтов не должен давать «всё считается CDN».
func TestCDNInboundAllowlistEmpty(t *testing.T) {
	if got := cdnInboundAllowlist(nil); len(got) != 0 {
		t.Fatalf("пустая конфигурация дала непустой allowlist: %v", got)
	}
}

func TestPeriodKeys(t *testing.T) {
	at := time.Date(2026, 8, 26, 23, 59, 59, 0, time.UTC)
	if got, want := calendarPeriodKey(at), "cal:2026-08"; got != want {
		t.Errorf("calendarPeriodKey = %q, ожидалось %q", got, want)
	}
	// Ровно 00:00:00 UTC первого числа — уже новый период.
	next := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if got, want := calendarPeriodKey(next), "cal:2026-09"; got != want {
		t.Errorf("calendarPeriodKey на границе = %q, ожидалось %q", got, want)
	}
	// Ключ считается в UTC независимо от зоны входного значения.
	msk := time.FixedZone("MSK", 3*3600)
	if got := calendarPeriodKey(time.Date(2026, 9, 1, 2, 0, 0, 0, msk)); got != "cal:2026-08" {
		t.Errorf("календарный ключ должен считаться в UTC, получено %q", got)
	}

	if got, want := paymentPeriodKey("2f0a-b1"), "pay:2f0a-b1"; got != want {
		t.Errorf("paymentPeriodKey = %q, ожидалось %q", got, want)
	}
	if got := paymentPeriodKey("  "); got != "" {
		t.Errorf("пустой payment_id не должен открывать период, получено %q", got)
	}
	// Один и тот же платёж всегда даёт один ключ — период не создаётся дважды.
	// Значения сравниваются через переменные: два одинаковых вызова в одном
	// выражении staticcheck справедливо считает тавтологией (SA4000).
	first := paymentPeriodKey("abc")
	second := paymentPeriodKey("abc")
	if first != second {
		t.Fatalf("ключ периода по payment_id недетерминирован: %q против %q", first, second)
	}
	if first != "pay:abc" {
		t.Errorf("ключ периода = %q, ожидалось \"pay:abc\"", first)
	}
}

func TestLoadCDNQuotaPolicyFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, p CDNQuotaPolicy)
	}{
		{
			name: "по умолчанию выключено и лимит 20 GB",
			env:  map[string]string{},
			check: func(t *testing.T, p CDNQuotaPolicy) {
				if p.Enabled || p.Enforce {
					t.Error("по умолчанию квота должна быть выключена")
				}
				if p.LimitBytes != 20_000_000_000 {
					t.Errorf("лимит по умолчанию = %d, ожидалось 20000000000", p.LimitBytes)
				}
			},
		},
		{
			name:    "enforce без enabled — ошибка запуска",
			env:     map[string]string{"CDN_QUOTA_ENFORCE": "true"},
			wantErr: true,
		},
		{
			name:    "нулевой лимит — ошибка запуска",
			env:     map[string]string{"CDN_QUOTA_LIMIT_BYTES": "0"},
			wantErr: true,
		},
		{
			name:    "нечисловой лимит — ошибка запуска",
			env:     map[string]string{"CDN_QUOTA_LIMIT_BYTES": "20GB"},
			wantErr: true,
		},
		{
			name:    "неизвестный fail mode — ошибка запуска",
			env:     map[string]string{"CDN_QUOTA_FAIL_MODE": "maybe"},
			wantErr: true,
		},
		{
			name:    "fail close без порога — ошибка запуска",
			env:     map[string]string{"CDN_QUOTA_FAIL_MODE": "close"},
			wantErr: true,
		},
		{
			name: "полная валидная конфигурация",
			env: map[string]string{
				"CDN_QUOTA_ENABLED":               "true",
				"CDN_QUOTA_ENFORCE":               "true",
				"CDN_QUOTA_LIMIT_BYTES":           "1000",
				"CDN_QUOTA_TELEMETRY_STALE_AFTER": "45m",
				"CDN_QUOTA_FAIL_MODE":             "close",
			},
			check: func(t *testing.T, p CDNQuotaPolicy) {
				if !p.Enabled || !p.Enforce {
					t.Error("ожидались enabled и enforce")
				}
				if p.LimitBytes != 1000 {
					t.Errorf("лимит = %d, ожидалось 1000", p.LimitBytes)
				}
				if p.TelemetryStaleAfter != 45*time.Minute {
					t.Errorf("порог = %v, ожидалось 45m", p.TelemetryStaleAfter)
				}
				if p.FailMode != CDNQuotaFailClose {
					t.Errorf("fail mode = %q, ожидалось close", p.FailMode)
				}
			},
		},
	}

	keys := []string{
		"CDN_QUOTA_ENABLED", "CDN_QUOTA_ENFORCE", "CDN_QUOTA_LIMIT_BYTES",
		"CDN_QUOTA_TELEMETRY_STALE_AFTER", "CDN_QUOTA_FAIL_MODE",
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range keys {
				_ = os.Unsetenv(k)
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			p, err := LoadCDNQuotaPolicyFromEnv()
			if tt.wantErr {
				if err == nil {
					t.Fatal("ожидалась ошибка конфигурации, получен nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if tt.check != nil {
				tt.check(t, p)
			}
		})
	}
}

// ============================================================================
// Компиляция манифеста маршрутизации.
// ============================================================================

func ruManifest(enabled bool) *RoutingManifest {
	m := &RoutingManifest{Version: 3, Name: "House VPN RU"}
	m.DirectDomains = []string{"regexp:.*\\.ru$", "domain:vk.com", "domain:yastatic.net"}
	m.DirectIPs = []string{"10.0.0.0/8"}
	m.GeoRules.Enabled = enabled
	m.GeoRules.DirectSites = []string{"geosite:category-ru"}
	m.GeoRules.DirectIPs = []string{"geoip:ru"}
	m.GeoRules.GeoIPURL = "https://race-src.com/geo/geoip.dat"
	m.GeoRules.GeoSiteURL = "https://race-src.com/geo/geosite.dat"
	return m
}

func decodeHapp(t *testing.T, b64 string) happRouting {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("профиль Happ не декодируется из base64: %v", err)
	}
	var p happRouting
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("профиль Happ не разбирается как JSON: %v", err)
	}
	return p
}

func decodeXray(t *testing.T, b64 string) xrayRouting {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("роутинг Xray не декодируется из base64: %v", err)
	}
	var p xrayRouting
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("роутинг Xray не разбирается как JSON: %v", err)
	}
	return p
}

// Корень проблемы «российские сервисы не пускают»: без geo-правил в direct
// попадали только .ru-домены, а geoip:ru не попадал никуда — весь трафик,
// пришедший в ядро IP-адресом, уходил в туннель.
func TestGeoRulesReachDirectListWhenEnabled(t *testing.T) {
	happ := decodeHapp(t, compileHappRoutingB64(ruManifest(true)))

	if !containsString(happ.DirectSites, "geosite:category-ru") {
		t.Errorf("geosite:category-ru не попал в DirectSites: %v", happ.DirectSites)
	}
	if !containsString(happ.DirectIp, "geoip:ru") {
		t.Errorf("geoip:ru не попал в DirectIp: %v", happ.DirectIp)
	}

	xray := decodeXray(t, compileXrayRoutingB64(ruManifest(true)))
	var direct *xrayRoutingRule
	for i := range xray.Rules {
		if xray.Rules[i].OutboundTag == "direct" {
			direct = &xray.Rules[i]
		}
	}
	if direct == nil {
		t.Fatal("правило direct отсутствует в роутинге Xray")
	}
	if !containsString(direct.Domain, "geosite:category-ru") {
		t.Errorf("geosite:category-ru не попал в domain правила direct: %v", direct.Domain)
	}
	if !containsString(direct.IP, "geoip:ru") {
		t.Errorf("geoip:ru не попал в ip правила direct: %v", direct.IP)
	}
}

func TestGeoRulesAbsentWhenDisabled(t *testing.T) {
	happ := decodeHapp(t, compileHappRoutingB64(ruManifest(false)))
	if containsString(happ.DirectSites, "geosite:category-ru") {
		t.Error("geo-правила не должны применяться при geo_rules.enabled=false")
	}
	if containsString(happ.DirectIp, "geoip:ru") {
		t.Error("geoip:ru не должен применяться при geo_rules.enabled=false")
	}
}

// Гео-ссылки нужны всегда, потому что активация профиля гейтится загрузкой
// файлов. Дефолт на GitHub из России режется — должно подставляться зеркало.
func TestGeoURLsAlwaysTakenFromManifest(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		happ := decodeHapp(t, compileHappRoutingB64(ruManifest(enabled)))
		if happ.Geoipurl != "https://race-src.com/geo/geoip.dat" {
			t.Errorf("enabled=%v: Geoipurl = %q, ожидалось зеркало ноды", enabled, happ.Geoipurl)
		}
		if happ.Geositeurl != "https://race-src.com/geo/geosite.dat" {
			t.Errorf("enabled=%v: Geositeurl = %q, ожидалось зеркало ноды", enabled, happ.Geositeurl)
		}
	}
}

// Метка LastUpdated обязана быть детерминированной: иначе каждый передеплой
// заставляет всех клиентов перекачивать геобазы, и на время загрузки
// сплит-роутинг не применяется.
func TestLastUpdatedIsStableAcrossRestarts(t *testing.T) {
	m := ruManifest(true)
	first := decodeHapp(t, compileHappRoutingB64(m)).LastUpdated
	second := decodeHapp(t, compileHappRoutingB64(m)).LastUpdated
	if first != second {
		t.Fatalf("LastUpdated недетерминирован: %q против %q", first, second)
	}

	want := strconv.FormatInt(routingGeoEpoch+3*routingGeoVersionStep, 10)
	if first != want {
		t.Errorf("LastUpdated = %q, ожидалось %q (эпоха + version*шаг)", first, want)
	}

	// Поднятие версии обязано увеличивать метку — иначе клиент не перекачает
	// геобазы, когда это действительно нужно.
	newer := ruManifest(true)
	newer.Version = 4
	got := decodeHapp(t, compileHappRoutingB64(newer)).LastUpdated
	if got <= first {
		t.Errorf("LastUpdated не вырос при поднятии версии: было %q, стало %q", first, got)
	}
}

// Порядок правил определяет исход: block специфичнее proxy, proxy специфичнее
// direct. Перестановка сделала бы исключения недостижимыми.
func TestXrayRuleOrder(t *testing.T) {
	m := ruManifest(true)
	m.BlockDomains = []string{"domain:ads.example"}
	m.ProxyDomains = []string{"domain:youtube.com"}

	xray := decodeXray(t, compileXrayRoutingB64(m))
	if len(xray.Rules) != 3 {
		t.Fatalf("ожидалось 3 правила, получено %d", len(xray.Rules))
	}
	want := []string{"block", "proxy", "direct"}
	for i, tag := range want {
		if xray.Rules[i].OutboundTag != tag {
			t.Errorf("правило %d имеет outboundTag %q, ожидалось %q", i, xray.Rules[i].OutboundTag, tag)
		}
	}
}

// Fail-open: пустой манифест не должен ломать подписку.
func TestEmptyManifestCompilesToNothing(t *testing.T) {
	empty := &RoutingManifest{Version: 1, Name: "empty"}
	if got := compileHappRoutingB64(empty); got != "" {
		t.Errorf("пустой манифест дал профиль Happ: %q", got)
	}
	if got := compileXrayRoutingB64(empty); got != "" {
		t.Errorf("пустой манифест дал роутинг Xray: %q", got)
	}
	if compileHappRoutingB64(nil) != "" || compileXrayRoutingB64(nil) != "" {
		t.Error("nil-манифест должен компилироваться в пустую строку")
	}
}

// happValue срезает только domain:, потому что для Xray это эквивалентная
// запись. Любой другой префикс срезать нельзя: regexp: без префикса перестаёт
// быть регуляркой и правило молча перестаёт совпадать.
func TestHappValueKeepsMeaningfulPrefixes(t *testing.T) {
	tests := map[string]string{
		"domain:vk.com":       "vk.com",
		"regexp:.*\\.ru$":     "regexp:.*\\.ru$",
		"geosite:category-ru": "geosite:category-ru",
		"full:ya.ru":          "full:ya.ru",
		"ext:geosite.dat:ru":  "ext:geosite.dat:ru",
	}
	for in, want := range tests {
		if got := happValue(in); got != want {
			t.Errorf("happValue(%q) = %q, ожидалось %q", in, got, want)
		}
	}
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
