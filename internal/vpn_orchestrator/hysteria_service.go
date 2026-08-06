package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Добавление Hysteria2-ссылок в фид подписки.
//
// Для каждого уникального сервера из выданных пользователю пунктов подбирается
// привязанный к нему Hysteria-эндпоинт. Если у сервера его нет — транспорт
// просто не выдаётся (никаких подстановок чужих нод).
// ============================================================================

// hysteriaLinesForFeed формирует hysteria2://-ссылки для фида пользователя.
func (s *Service) hysteriaLinesForFeed(ctx context.Context, feedItems []FeedItem) []string {
	if !hysteriaConfigEnabled() || len(feedItems) == 0 {
		return nil
	}

	// UUID пользователя — он же пароль Hysteria: его проверяет node-agent.
	userUUID := feedItems[0].Credential.VLESSUUID
	if userUUID == "" {
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
	if len(endpoints) == 0 {
		return nil
	}

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

	seenHysteria := make(map[string]struct{})
	lines := make([]string, 0, len(serverKeys))
	for _, sk := range serverKeys {
		endpoint, ok := selectHysteriaForServer(endpoints, sk)
		if !ok {
			continue
		}
		if _, dup := seenHysteria[endpoint.HysteriaKey]; dup {
			continue
		}
		url := BuildHysteriaURL(endpoint, userUUID)
		if url == "" {
			continue
		}
		seenHysteria[endpoint.HysteriaKey] = struct{}{}
		lines = append(lines, url)
	}
	return lines
}
