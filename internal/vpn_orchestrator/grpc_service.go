package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Выбор и добавление gRPC-ссылок в фид подписки с привязкой к серверам.
//
// Логика идентична CDN: пользователю выдаётся набор серверов (по одному на страну,
// наименее загруженные). Для КАЖДОГО уникального сервера подбирается привязанный к
// нему gRPC-эндпоинт (или глобальный/первый, если персональной привязки нет).
// Каждый уникальный эндпоинт добавляется в фид один раз (дедуп по grpc_key).
//
// Источник — таблица vpn_grpc_endpoints. Если она пуста, но задан GRPC_ADDRESS в
// окружении — используется он (обратная совместимость с одним эндпоинтом).
// ============================================================================

// grpcLinesForFeed формирует gRPC vless://-ссылки для фида пользователя.
// feedItems — выбранные для пользователя пункты (с ServerKey и Credential).
// Возвращает список готовых ссылок (может быть пустым, если gRPC выключен/не задан).
func (s *Service) grpcLinesForFeed(ctx context.Context, feedItems []FeedItem) []string {
	// Глобальный рубильник: GRPC_CONFIG_ENABLED=false полностью отключает выдачу.
	if !grpcConfigEnabled() || len(feedItems) == 0 {
		return nil
	}

	// UUID пользователя (одинаков для подбора любого эндпоинта — он валиден на узле).
	userUUID := feedItems[0].Credential.VLESSUUID
	if userUUID == "" {
		return nil
	}

	// Загружаем эндпоинты из БД; при отсутствии — пробуем env-fallback.
	endpoints, err := s.repo.ListEnabledGRPCEndpoints(ctx)
	if err != nil {
		slog.Error("load grpc endpoints failed", "err", err)
		endpoints = nil
	}
	if len(endpoints) == 0 {
		if envEndpoint, ok := grpcEndpointFromEnv(); ok {
			endpoints = []GRPCEndpoint{envEndpoint}
		}
	}
	if len(endpoints) == 0 {
		return nil
	}

	// Уникальные server_key из выданных пользователю пунктов.
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

	// Для каждого сервера подбираем gRPC-эндпоинт; дедупим по grpc_key.
	seenGRPC := make(map[string]struct{})
	lines := make([]string, 0, len(serverKeys))
	for _, sk := range serverKeys {
		endpoint, ok := selectGRPCForServer(endpoints, sk)
		if !ok {
			continue
		}
		if _, dup := seenGRPC[endpoint.GRPCKey]; dup {
			continue
		}
		url := BuildGRPCVLESSURLFromEndpoint(endpoint, userUUID)
		if url == "" {
			continue
		}
		seenGRPC[endpoint.GRPCKey] = struct{}{}
		lines = append(lines, url)
	}
	return lines
}
