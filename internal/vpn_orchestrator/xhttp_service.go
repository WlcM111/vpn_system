package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Загрузка XHTTP-эндпоинтов (VLESS + XHTTP + TLS).
//
// Источник — таблица vpn_xhttp_endpoints. Эндпоинт ОБЯЗАН быть привязан к
// server_key (см. selectXHTTPForServer): UUID пользователя у каждого сервера
// свой.
// ============================================================================

// loadXHTTPEndpoints отдаёт включённые XHTTP-эндпоинты с учётом рубильника
// XHTTP_CONFIG_ENABLED.
//
// Ошибка чтения не критична: возвращается пустой список, и фид собирается без
// XHTTP-строк. Остальные транспорты при этом работают.
func (s *Service) loadXHTTPEndpoints(ctx context.Context) []XHTTPEndpoint {
	if !xhttpConfigEnabled() {
		return nil
	}
	endpoints, err := s.repo.ListEnabledXHTTPEndpoints(ctx)
	if err != nil {
		slog.Error("load xhttp endpoints failed", "err", err)
		return nil
	}
	return endpoints
}
