package vpn_orchestrator

import (
	"strings"
	"testing"
)

// ============================================================================
// CDN-транспорт: выбор эндпоинта и генерация ссылки.
//
// Регрессия на архитектурный дефект: раньше выбор имел фолбэк «взять первый
// попавшийся». Пользователь ноды без CDN получал ссылку на ЧУЖУЮ ноду, где
// его UUID не зарегистрирован: ссылка в списке есть, подключение молча не
// работает — худший вид отказа, потому что выглядит как проблема клиента.
// ============================================================================

func TestSelectCDNForServer(t *testing.T) {
	bound := []CDNEndpoint{
		{CDNKey: "lt", ServerKey: "lt-1", Address: "lt.cdn.example", Enabled: true},
		{CDNKey: "de", ServerKey: "de-1", Address: "de.cdn.example", Enabled: true},
	}
	withGlobal := []CDNEndpoint{
		{CDNKey: "global", ServerKey: "", Address: "any.cdn.example", Enabled: true},
		{CDNKey: "lt", ServerKey: "lt-1", Address: "lt.cdn.example", Enabled: true},
	}

	tests := []struct {
		name      string
		endpoints []CDNEndpoint
		serverKey string
		wantKey   string
		wantOK    bool
	}{
		{"эндпоинт своей ноды", bound, "lt-1", "lt", true},
		{"эндпоинт другой своей ноды", bound, "de-1", "de", true},
		{"нода без CDN — чужой эндпоинт НЕ отдаём", bound, "fr-1", "", false},
		{"пустой список", nil, "lt-1", "", false},
		{"привязка приоритетнее глобального", withGlobal, "lt-1", "lt", true},
		{"глобальный, когда своего нет", withGlobal, "fr-1", "global", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := selectCDNForServer(tt.endpoints, tt.serverKey)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, ожидалось %v", ok, tt.wantOK)
			}
			if ok && got.CDNKey != tt.wantKey {
				t.Errorf("выбран %q, ожидался %q", got.CDNKey, tt.wantKey)
			}
		})
	}
}

func TestBuildCDNVLESSURLFromEndpoint(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"

	t.Run("пустой адрес даёт пустую ссылку", func(t *testing.T) {
		if got := BuildCDNVLESSURLFromEndpoint(CDNEndpoint{Address: ""}, uuid); got != "" {
			t.Errorf("ожидалась пустая строка, получено %q", got)
		}
	})

	t.Run("пустой uuid даёт пустую ссылку", func(t *testing.T) {
		e := CDNEndpoint{Address: "cdn.example"}
		if got := BuildCDNVLESSURLFromEndpoint(e, ""); got != "" {
			t.Errorf("ожидалась пустая строка, получено %q", got)
		}
	})

	t.Run("умолчания подставляются", func(t *testing.T) {
		// Задан только адрес — остальное должно взяться из умолчаний.
		got := BuildCDNVLESSURLFromEndpoint(CDNEndpoint{Address: "cdn.example"}, uuid)

		mustContain(t, got, []string{
			"vless://" + uuid + "@cdn.example:443",
			"encryption=none",
			"security=tls",
			"type=xhttp",
			"fp=chrome",
			"mode=packet-up",
		})
	})

	t.Run("явные значения переопределяют умолчания", func(t *testing.T) {
		e := CDNEndpoint{
			Address:     "cdn.example",
			ServerName:  "sni.example",
			Host:        "host.example",
			Port:        8443,
			XHTTPPath:   "/custom/",
			Mode:        "stream-up",
			Fingerprint: "firefox",
			Remarks:     "My CDN",
		}
		got := BuildCDNVLESSURLFromEndpoint(e, uuid)

		mustContain(t, got, []string{
			"@cdn.example:8443",
			"sni=sni.example",
			"fp=firefox",
			"mode=stream-up",
		})
	})

	t.Run("во фрагменте нет плюсов вместо пробелов", func(t *testing.T) {
		e := CDNEndpoint{Address: "cdn.example", Remarks: "Если обычные ссылки не работают"}
		got := BuildCDNVLESSURLFromEndpoint(e, uuid)

		idx := strings.Index(got, "#")
		if idx < 0 {
			t.Fatalf("в ссылке нет фрагмента: %q", got)
		}
		fragment := got[idx:]
		if strings.Contains(fragment, "+") {
			t.Errorf("фрагмент содержит '+', клиент покажет его буквально: %q", fragment)
		}
		if !strings.Contains(fragment, "%20") {
			t.Errorf("пробелы во фрагменте закодированы неверно: %q", fragment)
		}
	})
}

func TestEnvOrDefault(t *testing.T) {
	const key = "TEST_CDN_ENV_OR_DEFAULT"

	tests := []struct {
		name  string
		value string
		set   bool
		want  string
	}{
		{"переменная не задана", "", false, "fallback"},
		{"пустое значение", "", true, "fallback"},
		{"только пробелы", "   ", true, "fallback"},
		{"заданное значение", "custom", true, "custom"},
		{"пробелы обрезаются", "  custom  ", true, "custom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			}
			if got := envOrDefault(key, "fallback"); got != tt.want {
				t.Errorf("envOrDefault() = %q, ожидалось %q", got, tt.want)
			}
		})
	}
}

func TestCDNConfigEnabled(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"false", false},
		{"", false},
		{"yes", false},
		{"1", false},
	}

	for _, tt := range tests {
		t.Run("значение="+tt.value, func(t *testing.T) {
			t.Setenv("CDN_CONFIG_ENABLED", tt.value)
			if got := cdnConfigEnabled(); got != tt.want {
				t.Errorf("cdnConfigEnabled() = %v при %q, ожидалось %v", got, tt.value, tt.want)
			}
		})
	}
}

// mustContain проверяет, что строка содержит все подстроки.
func mustContain(t *testing.T, got string, want []string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("ссылка не содержит %q:\n  %s", w, got)
		}
	}
}
