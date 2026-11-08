package vpn_orchestrator

import (
	"context"
	"strings"
)

// ============================================================================
// Сборка строк подписки.
//
// ГРУППИРОВКА ПО СЕРВЕРАМ. Строки идут блоками: по каждому выделенному серверу
// — ссылки его эндпоинтов в постоянном порядке Reality → XHTTP → Hysteria2 →
// CDN, затем следующий сервер. Какие ссылки есть у сервера, определяют только
// его эндпоинты в БД (vpn_reality_endpoints, vpn_xhttp_endpoints,
// vpn_hysteria_endpoints, vpn_cdn_endpoints); отдельной «основной» ссылки из
// полей сервера нет. CDN идёт последним: это запасной путь «если обычные
// ссылки не работают».
//
// UUID. Учётка выписывается на пару (пользователь, item_key), то есть на каждом
// сервере у пользователя СВОЙ UUID (см. Repository.EnsureCredentialsForItemsTx).
// Поэтому ссылка любого транспорта строится с UUID того самого FeedItem, чей
// сервер выбрал эндпоинт.
// ============================================================================

// buildGroupedFeedLines собирает все строки подписки пользователя.
func (s *Service) buildGroupedFeedLines(ctx context.Context, feedItems []FeedItem) ([]string, map[string]struct{}) {
	return s.buildGroupedFeedLinesWithEndpoints(
		feedItems,
		s.loadCDNEndpoints(ctx),
		s.loadXHTTPEndpoints(ctx),
		s.loadHysteriaEndpoints(ctx),
		s.loadRealityEndpoints(ctx),
	)
}

// buildGroupedFeedLinesWithEndpoints — та же сборка, но эндпоинты передаются
// готовыми.
func (s *Service) buildGroupedFeedLinesWithEndpoints(
	feedItems []FeedItem,
	cdnEndpoints []CDNEndpoint,
	xhttpEndpoints []XHTTPEndpoint,
	hysteriaEndpoints []HysteriaEndpoint,
	realityEndpoints []RealityEndpoint,
) ([]string, map[string]struct{}) {
	cdnServers := make(map[string]struct{}, len(feedItems))
	if len(feedItems) == 0 {
		return nil, cdnServers
	}

	lines := make([]string, 0, len(feedItems)*4)
	seenServers := make(map[string]struct{}, len(feedItems))

	for _, item := range feedItems {
		serverKey := item.PoolItem.ServerKey
		userUUID := strings.TrimSpace(item.Credential.VLESSUUID)
		if serverKey == "" || userUUID == "" {
			continue
		}
		// Один и тот же сервер может стоять за несколькими пул-айтемами:
		// ссылки сервера добавляем только один раз.
		if _, dup := seenServers[serverKey]; dup {
			continue
		}
		seenServers[serverKey] = struct{}{}

		if endpoint, ok := selectRealityForServer(realityEndpoints, serverKey); ok {
			if url := BuildRealityVLESSURLFromEndpoint(endpoint, userUUID); url != "" {
				lines = append(lines, url)
			}
		}
		if endpoint, ok := selectXHTTPForServer(xhttpEndpoints, serverKey); ok {
			if url := BuildXHTTPVLESSURLFromEndpoint(endpoint, userUUID); url != "" {
				lines = append(lines, url)
			}
		}
		if endpoint, ok := selectHysteriaForServer(hysteriaEndpoints, serverKey); ok {
			if url := BuildHysteriaURL(endpoint, userUUID); url != "" {
				lines = append(lines, url)
			}
		}
		if endpoint, ok := selectCDNForServer(cdnEndpoints, serverKey); ok {
			if url := BuildCDNVLESSURLFromEndpoint(endpoint, userUUID); url != "" {
				lines = append(lines, url)
				// Узел засчитывается ровно там, где ссылка добавлена в фид.
				// Пустой URL (невалидный эндпоинт) узел не засчитывает.
				cdnServers[serverKey] = struct{}{}
			}
		}
	}

	return lines, cdnServers
}
