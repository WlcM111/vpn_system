package vpn_orchestrator

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// ============================================================================
// Загрузка Reality-эндпоинтов.
//
// Источник — таблица vpn_reality_endpoints. Env-фолбэка, как у gRPC, здесь нет
// намеренно: у Reality на каждый узел свой публичный ключ и свой адрес, одной
// парой переменных окружения это не описать.
// ============================================================================

// realityConfigEnabled — рубильник транспорта. Выключенный REALITY_CONFIG_ENABLED
// убирает Reality из фида и из желаемого состояния узлов, не трогая таблицу.
//
// По умолчанию включён: таблица пуста до первой записи, и пустой список
// эндпоинтов сам по себе означает «транспорт не настроен».
func realityConfigEnabled() bool {
	v := strings.TrimSpace(os.Getenv("REALITY_CONFIG_ENABLED"))
	if v == "" {
		return true
	}
	return v == "1" || strings.EqualFold(v, "true")
}

// loadRealityEndpoints отдаёт включённые Reality-эндпоинты.
//
// Ошибка чтения не критична: возвращается пустой список, и фид собирается без
// Reality-строк. Остальные транспорты при этом работают — принцип тот же, что
// у CDN и gRPC.
func (s *Service) loadRealityEndpoints(ctx context.Context) []RealityEndpoint {
	if !realityConfigEnabled() {
		return nil
	}
	endpoints, err := s.repo.ListEnabledRealityEndpoints(ctx)
	if err != nil {
		slog.Error("load reality endpoints failed", "err", err)
		return nil
	}
	return endpoints
}
