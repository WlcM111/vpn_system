package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Выбор и добавление CDN-ссылок в фид подписки с привязкой к серверам.
//
// Логика: пользователю выдаётся набор серверов (по одному на страну, наименее
// загруженные). Для КАЖДОГО уникального сервера подбирается привязанный к нему
// CDN. Если у сервера нет персонального CDN — используется глобальный (без
// привязки) либо первый доступный. Каждый уникальный CDN добавляется в фид один
// раз (без дублей), с UUID пользователя.
//
// Источник CDN — таблица vpn_cdn_endpoints. Если она пуста, но задан CDN_ADDRESS
// в окружении — используется он (обратная совместимость с одним CDN).
// ============================================================================

// cdnLinesForFeed формирует CDN vless://-ссылки для фида пользователя.
// feedItems — выбранные для пользователя пункты (с ServerKey и Credential).
// Возвращает список готовых ссылок (может быть пустым, если CDN выключен/не задан).
func (s *Service) cdnLinesForFeed(ctx context.Context, feedItems []FeedItem) []string {
	// Глобальный рубильник: CDN_CONFIG_ENABLED=false полностью отключает выдачу.
	if !cdnConfigEnabled() || len(feedItems) == 0 {
		return nil
	}

	// UUID пользователя (одинаков для подбора любого CDN — он валиден на узле).
	userUUID := feedItems[0].Credential.VLESSUUID
	if userUUID == "" {
		return nil
	}

	// Загружаем CDN-эндпоинты из БД; при отсутствии — пробуем env-fallback.
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
	if len(endpoints) == 0 {
		return nil
	}

	// Собираем уникальные server_key из выданных пользователю пунктов.
	seenServers := make(map[string]struct{})
	serverKeys := make([]string, 0, len(feedItems))
	for _, item := range feedItems {
		sk := item.PoolItem.ServerKey
		if _, ok := seenServers[sk]; ok {
			continue
		}
		seenServers[sk] = struct{}{}
		serverKeys = append(serverKeys, sk)
	}

	// Для каждого сервера подбираем CDN; дедупим итоговые ссылки по cdn_key,
	// чтобы один и тот же CDN не попал в фид дважды.
	seenCDN := make(map[string]struct{})
	lines := make([]string, 0, len(serverKeys))
	for _, sk := range serverKeys {
		endpoint, ok := selectCDNForServer(endpoints, sk)
		if !ok {
			continue
		}
		if _, dup := seenCDN[endpoint.CDNKey]; dup {
			continue
		}
		url := BuildCDNVLESSURLFromEndpoint(endpoint, userUUID)
		if url == "" {
			continue
		}
		seenCDN[endpoint.CDNKey] = struct{}{}
		lines = append(lines, url)
	}
	return lines
}
