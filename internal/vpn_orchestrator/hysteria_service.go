package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Загрузка Hysteria2-эндпоинтов.
//
// У Hysteria нет inbound'ов: пользователь не регистрируется на узле заранее,
// вместо этого сервер при каждом подключении спрашивает node-agent по HTTP, а
// паролем выступает VLESS-UUID пользователя. Именно поэтому привязка эндпоинта
// к server_key критична: агент ищет UUID только в состоянии СВОЕЙ ноды.
//
// Источник — таблица vpn_hysteria_endpoints; при пустой таблице работает
// env-фолбэк (HYSTERIA_ADDRESS + HYSTERIA_SERVER_KEY).
// ============================================================================

// loadHysteriaEndpoints отдаёт включённые Hysteria-эндпоинты с учётом
// рубильника HYSTERIA_CONFIG_ENABLED и env-фолбэка.
func (s *Service) loadHysteriaEndpoints(ctx context.Context) []HysteriaEndpoint {
	if !hysteriaConfigEnabled() {
		return nil
	}
	endpoints, err := s.repo.ListEnabledHysteriaEndpoints(ctx)
	if err != nil {
		slog.Error("load hysteria endpoints failed", "err", err)
		endpoints = nil
	}
	if len(endpoints) == 0 {
		if envEndpoint, ok := hysteriaEndpointFromEnv(); ok {
			endpoints = []HysteriaEndpoint{envEndpoint}
		}
	}
	return endpoints
}
