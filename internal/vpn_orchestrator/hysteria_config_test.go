package vpn_orchestrator

import (
	"strings"
	"testing"
)

// ============================================================================
// Регрессия на архитектурный дефект: раньше выбор эндпоинта имел фолбэк
// «любой первый». Пользователь ноды без Hysteria получал ссылку на ЧУЖУЮ
// ноду, где его UUID не зарегистрирован: ссылка в списке есть, подключение
// молча не работает. Правильное поведение — не отдавать транспорт вовсе.
// ============================================================================

func TestSelectHysteriaForServer(t *testing.T) {
	endpoints := []HysteriaEndpoint{
		{HysteriaKey: "lt", ServerKey: "lt-1", Address: "lt.example.com", Enabled: true},
		{HysteriaKey: "de", ServerKey: "de-1", Address: "de.example.com", Enabled: true},
	}

	tests := []struct {
		name      string
		endpoints []HysteriaEndpoint
		serverKey string
		wantKey   string
		wantOK    bool
	}{
		{"своя нода", endpoints, "lt-1", "lt", true},
		{"другая своя нода", endpoints, "de-1", "de", true},
		{"нода без Hysteria — НЕ отдаём чужой эндпоинт", endpoints, "fr-1", "", false},
		{"пустой список", nil, "lt-1", "", false},
		{"пустой serverKey", endpoints, "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := selectHysteriaForServer(tt.endpoints, tt.serverKey)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, ожидалось %v", ok, tt.wantOK)
			}
			if ok && got.HysteriaKey != tt.wantKey {
				t.Errorf("выбран %q, ожидался %q", got.HysteriaKey, tt.wantKey)
			}
		})
	}
}

func TestBuildHysteriaURL(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"

	tests := []struct {
		name     string
		endpoint HysteriaEndpoint
		uuid     string
		want     string
	}{
		{
			name:     "базовый случай",
			endpoint: HysteriaEndpoint{Address: "race-src.com", Port: 443, SNI: "race-src.com", Remarks: "Litva"},
			uuid:     uuid,
			want:     "hysteria2://" + uuid + "@race-src.com:443?sni=race-src.com#Litva",
		},
		{
			name:     "порт по умолчанию при нуле",
			endpoint: HysteriaEndpoint{Address: "race-src.com", Port: 0, Remarks: "X"},
			uuid:     uuid,
			want:     "hysteria2://" + uuid + "@race-src.com:443?sni=race-src.com#X",
		},
		{
			name:     "insecure добавляет флаг",
			endpoint: HysteriaEndpoint{Address: "h.example.com", Port: 8443, SNI: "h.example.com", Insecure: true, Remarks: "T"},
			uuid:     uuid,
			want:     "hysteria2://" + uuid + "@h.example.com:8443?sni=h.example.com&insecure=1#T",
		},
		{"пустой адрес — пустая ссылка", HysteriaEndpoint{Address: "", Port: 443}, uuid, ""},
		{"пустой uuid — пустая ссылка", HysteriaEndpoint{Address: "a.com", Port: 443}, "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildHysteriaURL(tt.endpoint, tt.uuid)
			if got != tt.want {
				t.Errorf("BuildHysteriaURL() =\n  %q\nожидалось\n  %q", got, tt.want)
			}
		})
	}
}

// Название профиля не должно приезжать с плюсами вместо пробелов.
func TestBuildHysteriaURLRemarksUseSpaces(t *testing.T) {
	e := HysteriaEndpoint{
		Address: "race-src.com", Port: 443, SNI: "race-src.com",
		Remarks: "Литва - Маленький пинг",
	}
	got := BuildHysteriaURL(e, "11111111-2222-3333-4444-555555555555")
	idx := strings.Index(got, "#")
	if idx < 0 {
		t.Fatalf("в ссылке нет фрагмента: %q", got)
	}
	if strings.Contains(got[idx:], "+") {
		t.Errorf("фрагмент содержит '+': %q", got[idx:])
	}
	if !strings.Contains(got[idx:], "%20") {
		t.Errorf("фрагмент не содержит %%20 — пробелы закодированы неверно: %q", got[idx:])
	}
}
