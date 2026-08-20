package vpn_orchestrator

import (
	"strings"
	"testing"
)

// ============================================================================
// gRPC-транспорт: выбор эндпоинта и генерация ссылки.
// Тот же дефект с фолбэком, что и у CDN, — закрываем симметрично.
// ============================================================================

func TestSelectGRPCForServer(t *testing.T) {
	bound := []GRPCEndpoint{
		{GRPCKey: "lt", ServerKey: "lt-1", Address: "lt.example", Enabled: true},
		{GRPCKey: "de", ServerKey: "de-1", Address: "de.example", Enabled: true},
	}
	withGlobal := []GRPCEndpoint{
		{GRPCKey: "global", ServerKey: "", Address: "any.example", Enabled: true},
		{GRPCKey: "lt", ServerKey: "lt-1", Address: "lt.example", Enabled: true},
	}

	tests := []struct {
		name      string
		endpoints []GRPCEndpoint
		serverKey string
		wantKey   string
		wantOK    bool
	}{
		{"эндпоинт своей ноды", bound, "lt-1", "lt", true},
		{"нода без gRPC — чужой эндпоинт НЕ отдаём", bound, "fr-1", "", false},
		{"пустой список", nil, "lt-1", "", false},
		{"привязка приоритетнее глобального", withGlobal, "lt-1", "lt", true},
		{"глобальный НЕ выдаётся ноде без привязки", withGlobal, "fr-1", "", false},
		{"пустой serverKey", bound, "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := selectGRPCForServer(tt.endpoints, tt.serverKey)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, ожидалось %v", ok, tt.wantOK)
			}
			if ok && got.GRPCKey != tt.wantKey {
				t.Errorf("выбран %q, ожидался %q", got.GRPCKey, tt.wantKey)
			}
		})
	}
}

func TestBuildGRPCVLESSURLFromEndpoint(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"

	t.Run("пустой адрес даёт пустую ссылку", func(t *testing.T) {
		if got := BuildGRPCVLESSURLFromEndpoint(GRPCEndpoint{Address: ""}, uuid); got != "" {
			t.Errorf("ожидалась пустая строка, получено %q", got)
		}
	})

	t.Run("пустой uuid даёт пустую ссылку", func(t *testing.T) {
		if got := BuildGRPCVLESSURLFromEndpoint(GRPCEndpoint{Address: "a.example"}, ""); got != "" {
			t.Errorf("ожидалась пустая строка, получено %q", got)
		}
	})

	t.Run("умолчания подставляются", func(t *testing.T) {
		got := BuildGRPCVLESSURLFromEndpoint(GRPCEndpoint{Address: "grpc.example"}, uuid)

		mustContain(t, got, []string{
			"vless://" + uuid + "@grpc.example:443",
			"encryption=none",
			"security=tls",
			"type=grpc",
			"fp=chrome",
			"alpn=h2",
		})
	})

	// host передаём только когда он отличается от адреса: одинаковые значения
	// в ссылке — лишний шум, который некоторые клиенты трактуют по-своему.
	t.Run("host не дублирует адрес", func(t *testing.T) {
		e := GRPCEndpoint{Address: "grpc.example", Host: "grpc.example"}
		got := BuildGRPCVLESSURLFromEndpoint(e, uuid)
		if strings.Contains(got, "host=") {
			t.Errorf("host совпадает с адресом и не должен попадать в ссылку: %s", got)
		}
	})

	t.Run("host передаётся, когда отличается", func(t *testing.T) {
		e := GRPCEndpoint{Address: "grpc.example", Host: "other.example"}
		got := BuildGRPCVLESSURLFromEndpoint(e, uuid)
		if !strings.Contains(got, "host=other.example") {
			t.Errorf("отличающийся host должен попасть в ссылку: %s", got)
		}
	})

	t.Run("во фрагменте нет плюсов", func(t *testing.T) {
		e := GRPCEndpoint{Address: "grpc.example", Remarks: "Литва - Маленький Пинг"}
		got := BuildGRPCVLESSURLFromEndpoint(e, uuid)

		idx := strings.Index(got, "#")
		if idx < 0 {
			t.Fatalf("в ссылке нет фрагмента: %q", got)
		}
		if strings.Contains(got[idx:], "+") {
			t.Errorf("фрагмент содержит '+': %q", got[idx:])
		}
	})
}

func TestGRPCConfigEnabled(t *testing.T) {
	t.Setenv("GRPC_CONFIG_ENABLED", "true")
	if !grpcConfigEnabled() {
		t.Error("при GRPC_CONFIG_ENABLED=true ожидалось true")
	}

	t.Setenv("GRPC_CONFIG_ENABLED", "false")
	if grpcConfigEnabled() {
		t.Error("при GRPC_CONFIG_ENABLED=false ожидалось false")
	}
}
