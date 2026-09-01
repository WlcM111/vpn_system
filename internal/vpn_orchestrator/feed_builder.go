package vpn_orchestrator

import (
	"context"
	"strings"
)

// ============================================================================
// Сборка строк подписки.
//
// ГРУППИРОВКА ПО СТРАНАМ. Строки идут блоками: сначала основной профиль страны,
// сразу за ним — все альтернативные транспорты ЭТОГО ЖЕ сервера (CDN → gRPC →
// Hysteria), затем следующая страна. Раньше фид склеивался по транспортам
// (все основные, потом все CDN, потом все gRPC...), и в клиенте конфиги стран
// шли вперемешку.
//
// UUID. Учётка выписывается на пару (пользователь, item_key), то есть на каждом
// сервере у пользователя СВОЙ UUID (см. Repository.EnsureCredentialsForItemsTx).
// Поэтому ссылка любого транспорта строится с UUID того самого FeedItem, чей
// сервер выбрал эндпоинт. Брать UUID «первого элемента фида» нельзя: на второй
// ноде такой UUID не зарегистрирован, и ссылка молча не подключается.
// ============================================================================

// buildGroupedFeedLines собирает все строки подписки пользователя.
// feedItems ожидаются отсортированными по sort_order (так отдаёт Allocator) —
// этот порядок и определяет порядок стран в клиенте.
//
// Вторым значением возвращается множество server_key узлов, для которых в фид
// РЕАЛЬНО попала CDN-ссылка. Это единственный допустимый источник величины n
// для заголовка subscription-userinfo: она получается не отдельным запросом к
// БД, а по построению — из той же ветки, которая добавляет строку. Поэтому
// «показали объём, но ссылку не выдали» и обратное невозможны в принципе.
//
// Множество, а не счётчик: у одного узла может быть несколько транспортов и
// несколько пул-айтемов, но в n он обязан войти ровно один раз.
func (s *Service) buildGroupedFeedLines(ctx context.Context, feedItems []FeedItem) ([]string, map[string]struct{}) {
	// Таблицы эндпоинтов читаем ОДИН раз на весь фид, а не на каждую страну.
	return s.buildGroupedFeedLinesWithEndpoints(
		feedItems,
		s.loadCDNEndpoints(ctx),
		s.loadGRPCEndpoints(ctx),
		s.loadHysteriaEndpoints(ctx),
	)
}

// buildGroupedFeedLinesWithEndpoints — та же сборка, но эндпоинты передаются
// готовыми. Выделено, чтобы правило подсчёта n проверялось unit-тестом без
// поднятого Postgres: именно здесь решается, попадёт ли узел и в фид, и в n.
func (s *Service) buildGroupedFeedLinesWithEndpoints(
	feedItems []FeedItem,
	cdnEndpoints []CDNEndpoint,
	grpcEndpoints []GRPCEndpoint,
	hysteriaEndpoints []HysteriaEndpoint,
) ([]string, map[string]struct{}) {
	cdnServers := make(map[string]struct{}, len(feedItems))
	if len(feedItems) == 0 {
		return nil, cdnServers
	}

	lines := make([]string, 0, len(feedItems)*4)
	seenServers := make(map[string]struct{}, len(feedItems))

	for _, item := range feedItems {
		if url := strings.TrimSpace(item.URL); url != "" {
			lines = append(lines, url)
		}

		serverKey := item.PoolItem.ServerKey
		userUUID := item.Credential.VLESSUUID
		if serverKey == "" || userUUID == "" {
			continue
		}
		// Один и тот же сервер может стоять за несколькими пул-айтемами:
		// альтернативные транспорты добавляем только один раз.
		if _, dup := seenServers[serverKey]; dup {
			continue
		}
		seenServers[serverKey] = struct{}{}

		if endpoint, ok := selectCDNForServer(cdnEndpoints, serverKey); ok {
			if url := BuildCDNVLESSURLFromEndpoint(endpoint, userUUID); url != "" {
				lines = append(lines, url)
				// Узел засчитывается ровно там, где ссылка добавлена в фид.
				// Пустой URL (невалидный эндпоинт) узел не засчитывает.
				cdnServers[serverKey] = struct{}{}
			}
		}
		if endpoint, ok := selectGRPCForServer(grpcEndpoints, serverKey); ok {
			if url := BuildGRPCVLESSURLFromEndpoint(endpoint, userUUID); url != "" {
				lines = append(lines, url)
			}
		}
		if endpoint, ok := selectHysteriaForServer(hysteriaEndpoints, serverKey); ok {
			if url := BuildHysteriaURL(endpoint, userUUID); url != "" {
				lines = append(lines, url)
			}
		}
	}

	return lines, cdnServers
}
