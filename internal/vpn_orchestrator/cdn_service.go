package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Загрузка CDN-эндпоинтов (VLESS-over-XHTTP фронтов).
//
// CDN — не отдельная нода, а альтернативный вход на тот же exit-узел через
// фронт, скрывающий реальный IP. Эндпоинт ОБЯЗАН быть привязан к server_key:
// UUID пользователя выписывается на пару (пользователь, item_key), поэтому
// ссылка на чужую ноду в паре с этим UUID никогда не подключится.
//
// Источник — таблица vpn_cdn_endpoints. Если она пуста, но задан CDN_ADDRESS
// в окружении — используется он (привязка задаётся CDN_SERVER_KEY).
//
// Сборкой строк фида занимается buildGroupedFeedLines (feed_builder.go):
// там же гарантируется, что каждому эндпоинту достаётся UUID именно того
// сервера, к которому он привязан.
// ============================================================================

// loadCDNEndpoints отдаёт включённые CDN-эндпоинты с учётом глобального
// рубильника CDN_CONFIG_ENABLED и env-фолбэка. Ошибки не возвращает: CDN —
// дополнительный транспорт, из-за него подписка падать не должна.
func (s *Service) loadCDNEndpoints(ctx context.Context) []CDNEndpoint {
	if !cdnConfigEnabled() {
		return nil
	}
	endpoints, err := s.repo.ListEnabledCDNEndpoints(ctx)
	if err != nil {
		slog.Error("load cdn endpoints failed", "err", err)
		endpoints = nil
	}
	if len(endpoints) == 0 {
		if envEndpoint, ok := cdnEndpointFromEnv(); ok {
			endpoints = []CDNEndpoint{envEndpoint}
		}
	}
	return endpoints
}
