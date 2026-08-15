package vpn_orchestrator

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// ============================================================================
// Определение формата правил маршрутизации по клиенту.
//
// Ошибка здесь означает, что Happ получит Xray-JSON (и молча не применит
// правила) или наоборот. Реальные User-Agent взяты из логов nginx.
// ============================================================================

func TestDetectClientGroup(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		userAgent string
		want      clientGroup
	}{
		// явный параметр приоритетнее User-Agent
		{"параметр c=happ", "?c=happ", "", clientGroupHapp},
		{"параметр c=incy", "?c=incy", "", clientGroupHapp},
		{"параметр c=xray", "?c=xray", "", clientGroupXray},
		{"параметр важнее UA", "?c=xray", "Happ/4.13.0/ios", clientGroupXray},

		// реальные UA из логов nginx
		{"Happ macOS", "", "Happ/2.17.1/macOS/2606011224503", clientGroupHapp},
		{"Happ iOS", "", "Happ/4.13.0/ios/2606221809551", clientGroupHapp},
		{"Happ Android", "", "Happ/3.26.3/Android/17839452147361875576", clientGroupHapp},
		{"Incy", "", "Incy/1.0", clientGroupHapp},
		{"v2raytun ios", "", "v2raytun/ios", clientGroupXray},
		{"v2raytun windows", "", "v2raytun/windows", clientGroupXray},
		{"Streisand", "", "Streisand/1.0", clientGroupXray},

		// фолбэк
		{"неизвестный UA", "", "okhttp/4.12.0", clientGroupXray},
		{"пустой UA", "", "", clientGroupXray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/sub/token"+tt.query, nil)
			if tt.userAgent != "" {
				r.Header.Set("User-Agent", tt.userAgent)
			}
			if got := detectClientGroup(r); got != tt.want {
				t.Errorf("detectClientGroup() = %v, ожидалось %v", got, tt.want)
			}
		})
	}
}
