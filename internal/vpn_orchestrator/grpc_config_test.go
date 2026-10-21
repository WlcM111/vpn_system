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

// Выключенная строка таблицы выключает gRPC на своём сервере, и env-фолбэк её
// не возвращает: он действует только для пустой таблицы.
func TestGRPCEndpointsForIssue(t *testing.T) {
	withEnv := func() (GRPCEndpoint, bool) {
		return GRPCEndpoint{GRPCKey: "env", ServerKey: "lt-1", Address: "env.example", Enabled: true}, true
	}
	noEnv := func() (GRPCEndpoint, bool) { return GRPCEndpoint{}, false }

	lt := GRPCEndpoint{GRPCKey: "lt", ServerKey: "lt-1", Address: "lt.example", Enabled: true}
	de := GRPCEndpoint{GRPCKey: "de", ServerKey: "de-1", Address: "de.example", Enabled: true}
	ltOff, deOff := lt, de
	ltOff.Enabled = false
	deOff.Enabled = false

	tests := []struct {
		name    string
		rows    []GRPCEndpoint
		fromEnv func() (GRPCEndpoint, bool)
		want    []string
	}{
		{"все строки выключены — gRPC нет, окружение его не возвращает", []GRPCEndpoint{ltOff, deOff}, withEnv, nil},
		{"выключен один сервер — второй остаётся", []GRPCEndpoint{ltOff, de}, withEnv, []string{"de"}},
		{"все строки включены", []GRPCEndpoint{lt, de}, withEnv, []string{"lt", "de"}},
		{"пустая таблица — работает env-фолбэк", nil, withEnv, []string{"env"}},
		{"пустая таблица без окружения — gRPC нет", nil, noEnv, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := grpcEndpointsForIssue(tt.rows, tt.fromEnv)
			keys := make([]string, 0, len(got))
			for _, e := range got {
				keys = append(keys, e.GRPCKey)
			}
			if strings.Join(keys, ",") != strings.Join(tt.want, ",") {
				t.Fatalf("к выдаче %v, ожидалось %v", keys, tt.want)
			}
		})
	}
}

// Выключенная строка убирает gRPC сразу из обоих мест выдачи: из подписки и из
// желаемого состояния узла. Включённая возвращает его без правки кода. Эндпоинт
// окружения привязан к тому же серверу — самый опасный случай для фолбэка.
func TestDisabledGRPCEndpointIsNotIssued(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{{
		PoolItem:   PoolItem{ItemKey: "lt", ServerKey: "lt-main-1"},
		Credential: UserCredential{VLESSUUID: "uuid-1"},
		URL:        "vless://uuid-1@lt.race-src.com:443#LT",
	}}
	fromEnv := func() (GRPCEndpoint, bool) {
		return GRPCEndpoint{GRPCKey: "env", ServerKey: "lt-main-1", Address: "race-src.com", Enabled: true}, true
	}
	row := GRPCEndpoint{
		GRPCKey: "lt-grpc", ServerKey: "lt-main-1", Enabled: true,
		InboundTag: "vless-grpc-cdn-in", Address: "race-src.com",
	}

	issued := func(rows []GRPCEndpoint) (inFeed, onNode bool) {
		endpoints := grpcEndpointsForIssue(rows, fromEnv)
		lines, _ := svc.buildGroupedFeedLinesWithEndpoints(items, nil, endpoints, nil, nil)
		for _, l := range lines {
			if strings.Contains(l, "type=grpc") {
				inFeed = true
			}
		}
		for _, p := range svc.buildUserProfiles(baseProfile(), "lt-main-1", nil, endpoints, nil, false) {
			if p.InboundTag == "vless-grpc-cdn-in" {
				onNode = true
			}
		}
		return inFeed, onNode
	}

	if inFeed, onNode := issued([]GRPCEndpoint{row}); !inFeed || !onNode {
		t.Fatalf("включённая строка: ссылка в подписке %v, учётка на узле %v — ожидалось и то и другое", inFeed, onNode)
	}
	row.Enabled = false
	if inFeed, onNode := issued([]GRPCEndpoint{row}); inFeed || onNode {
		t.Fatalf("выключенная строка: ссылка в подписке %v, учётка на узле %v — gRPC выдаваться не должен", inFeed, onNode)
	}
}
