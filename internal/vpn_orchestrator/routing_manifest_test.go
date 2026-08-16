package vpn_orchestrator

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// ============================================================================
// Компиляция манифеста маршрутизации.
//
// Здесь мы обожглись дважды: срезанный префикс regexp: молча ломал правила
// для всех .ru-доменов, а QueryEscape давал плюсы вместо пробелов в имени
// профиля. Оба случая закрыты тестами.
// ============================================================================

func manifestWithDirect(domains ...string) *RoutingManifest {
	m := &RoutingManifest{Name: "Test Profile"}
	m.DirectDomains = domains
	m.DirectIPs = []string{"10.0.0.0/8"}
	m.DNS.DomesticIP = "77.88.8.8"
	m.DNS.DomesticDoH = "https://77.88.8.8/dns-query"
	m.DNS.RemoteIP = "8.8.8.8"
	m.DNS.RemoteDoH = "https://8.8.8.8/dns-query"
	return m
}

func TestManifestHasRules(t *testing.T) {
	tests := []struct {
		name     string
		manifest *RoutingManifest
		want     bool
	}{
		{"nil", nil, false},
		{"пустой", &RoutingManifest{}, false},
		{"есть direct-домен", manifestWithDirect("domain:example.ru"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.manifest.hasRules(); got != tt.want {
				t.Errorf("hasRules() = %v, ожидалось %v", got, tt.want)
			}
		})
	}

	t.Run("только выключенные geo-правила не считаются", func(t *testing.T) {
		m := &RoutingManifest{}
		m.GeoRules.Enabled = false
		m.GeoRules.DirectSites = []string{"geosite:ru"}
		if m.hasRules() {
			t.Error("выключенные geo-правила не должны считаться правилами")
		}
	})

	t.Run("включённые geo-правила считаются", func(t *testing.T) {
		m := &RoutingManifest{}
		m.GeoRules.Enabled = true
		m.GeoRules.DirectSites = []string{"geosite:ru"}
		if !m.hasRules() {
			t.Error("включённые geo-правила должны считаться правилами")
		}
	})
}

func TestCompileXrayRouting(t *testing.T) {
	t.Run("nil даёт пустой результат", func(t *testing.T) {
		if got := compileXrayRoutingB64(nil); got != "" {
			t.Errorf("ожидалась пустая строка, получено %q", got)
		}
	})

	t.Run("результат — валидный base64 с валидным JSON", func(t *testing.T) {
		m := manifestWithDirect("regexp:.*\\.ru$", "domain:vk.com")

		raw, err := base64.StdEncoding.DecodeString(compileXrayRoutingB64(m))
		if err != nil {
			t.Fatalf("результат не декодируется: %v", err)
		}

		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("результат не разбирается как JSON: %v\n%s", err, raw)
		}
		if parsed["domainStrategy"] == nil {
			t.Error("в профиле нет domainStrategy")
		}
		if parsed["rules"] == nil {
			t.Error("в профиле нет rules")
		}
	})

	// Xray отличает регулярку от обычного домена только по префиксу.
	// Без него правило перестаёт совпадать с чем-либо — это и был баг.
	t.Run("префикс regexp сохраняется", func(t *testing.T) {
		m := manifestWithDirect("regexp:.*\\.ru$")

		raw, _ := base64.StdEncoding.DecodeString(compileXrayRoutingB64(m))
		if !strings.Contains(string(raw), "regexp:") {
			t.Errorf("префикс regexp: потерян — правило не сработает:\n%s", raw)
		}
	})
}

func TestCompileHappRouting(t *testing.T) {
	t.Run("nil даёт пустой результат", func(t *testing.T) {
		if got := compileHappRoutingB64(nil); got != "" {
			t.Errorf("ожидалась пустая строка, получено %q", got)
		}
	})

	t.Run("обязательные поля профиля на месте", func(t *testing.T) {
		m := manifestWithDirect("regexp:.*\\.ru$")

		raw, err := base64.StdEncoding.DecodeString(compileHappRoutingB64(m))
		if err != nil {
			t.Fatalf("результат не декодируется: %v", err)
		}

		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("результат не разбирается как JSON: %v", err)
		}

		// Активация профиля в клиенте гейтится успешной загрузкой гео-файлов,
		// а её запускает непустой LastUpdated. Без этих полей тумблер
		// маршрутизации не включается.
		for _, field := range []string{"Name", "RouteOrder", "LastUpdated", "Geoipurl", "Geositeurl", "DirectSites"} {
			if _, ok := parsed[field]; !ok {
				t.Errorf("в профиле нет поля %s", field)
			}
		}
	})

	t.Run("префикс regexp сохраняется и здесь", func(t *testing.T) {
		m := manifestWithDirect("regexp:.*\\.ru$")

		raw, _ := base64.StdEncoding.DecodeString(compileHappRoutingB64(m))
		if !strings.Contains(string(raw), "regexp:") {
			t.Errorf("префикс regexp: потерян:\n%s", raw)
		}
	})

	// Регрессия: имя резалось по байтам, из-за чего кириллица рвалась
	// посередине символа, а сам лимит срабатывал вдвое раньше.
	t.Run("имя профиля обрезается по рунам, а не по байтам", func(t *testing.T) {
		m := manifestWithDirect("domain:example.ru")
		m.Name = strings.Repeat("Очень длинное имя ", 10)

		raw, _ := base64.StdEncoding.DecodeString(compileHappRoutingB64(m))
		var parsed struct {
			Name string `json:"Name"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("не разбирается: %v", err)
		}

		if runes := []rune(parsed.Name); len(runes) > 25 {
			t.Errorf("длина имени = %d рун, лимит клиента 25: %q", len(runes), parsed.Name)
		}
		// Символ-замена означает, что обрезка разорвала многобайтовый символ.
		if strings.ContainsRune(parsed.Name, '\uFFFD') {
			t.Errorf("имя содержит повреждённый символ — обрезка режет по байтам: %q", parsed.Name)
		}
	})

	t.Run("короткое имя не обрезается", func(t *testing.T) {
		m := manifestWithDirect("domain:example.ru")
		m.Name = "House VPN"

		raw, _ := base64.StdEncoding.DecodeString(compileHappRoutingB64(m))
		var parsed struct {
			Name string `json:"Name"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("не разбирается: %v", err)
		}
		if parsed.Name != "House VPN" {
			t.Errorf("имя = %q, ожидалось %q", parsed.Name, "House VPN")
		}
	})
}

func TestHappValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// domain: в Xray эквивалентен записи без префикса — срезаем.
		{"domain: срезается", "domain:example.ru", "example.ru"},
		// А regexp: несёт смысл и обязан остаться.
		{"regexp: сохраняется", "regexp:.*\\.ru$", "regexp:.*\\.ru$"},
		{"geosite: сохраняется", "geosite:category-ru", "geosite:category-ru"},
		{"geoip: сохраняется", "geoip:private", "geoip:private"},
		{"без префикса не меняется", "example.ru", "example.ru"},
		{"CIDR не меняется", "10.0.0.0/8", "10.0.0.0/8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := happValue(tt.in); got != tt.want {
				t.Errorf("happValue(%q) = %q, ожидалось %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestHappValuesSkipsEmpty(t *testing.T) {
	got := happValues([]string{"domain:a.ru", "", "   ", "b.ru"})
	if len(got) != 2 {
		t.Fatalf("получено %d значений (%v), ожидалось 2", len(got), got)
	}
	if got[0] != "a.ru" || got[1] != "b.ru" {
		t.Errorf("значения = %v, ожидалось [a.ru b.ru]", got)
	}
}

// Идентификатор должен быть одинаковым между рестартами: иначе клиент
// увидит новый профиль вместо обновления существующего.
func TestStableRoutingIDIsDeterministic(t *testing.T) {
	first := stableRoutingID("House VPN RU")
	for i := 0; i < 10; i++ {
		if got := stableRoutingID("House VPN RU"); got != first {
			t.Fatalf("прогон %d дал %q, первый — %q", i, got, first)
		}
	}
	if stableRoutingID("другое имя") == first {
		t.Error("разные имена дали одинаковый идентификатор")
	}
	if len(first) != 36 {
		t.Errorf("длина идентификатора = %d, ожидалось 36 (формат UUID)", len(first))
	}
}

func TestRoutingBodyLines(t *testing.T) {
	t.Run("для happ отдаются deeplink'и", func(t *testing.T) {
		got := routingBodyLines(clientGroupHapp, "SOMEBASE64")
		if len(got) == 0 {
			t.Fatal("для группы happ ожидались deeplink'и")
		}
		joined := strings.Join(got, "\n")
		// Схема с onadd активирует профиль сразу при импорте.
		if !strings.Contains(joined, "happ://routing/onadd/") {
			t.Errorf("нет deeplink'а с авто-активацией:\n%s", joined)
		}
	})

	t.Run("для xray deeplink'и не нужны", func(t *testing.T) {
		if got := routingBodyLines(clientGroupXray, "SOMEBASE64"); len(got) != 0 {
			t.Errorf("для группы xray ожидался пустой список, получено %v", got)
		}
	})

	t.Run("без payload ничего не отдаём", func(t *testing.T) {
		if got := routingBodyLines(clientGroupHapp, ""); len(got) != 0 {
			t.Errorf("при пустом payload ожидался пустой список, получено %v", got)
		}
	})
}
