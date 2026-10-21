package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Загрузка gRPC-эндпоинтов (VLESS-over-gRPC).
//
// gRPC — не отдельная нода, а альтернативный транспорт к тому же exit-узлу
// через nginx grpc_pass → xray vless-grpc-cdn-in. Эндпоинт ОБЯЗАН быть привязан
// к server_key (см. selectGRPCForServer).
//
// Источник — таблица vpn_grpc_endpoints; при пустой таблице работает
// env-фолбэк (GRPC_ADDRESS + GRPC_SERVER_KEY).
// ============================================================================

// loadGRPCEndpoints отдаёт включённые gRPC-эндпоинты с учётом рубильника
// GRPC_CONFIG_ENABLED и env-фолбэка.
//
// Ошибка чтения таблицы даёт пустой список, а не фолбэк: недоступность БД не
// должна подменять настройки таблицы переменными окружения.
func (s *Service) loadGRPCEndpoints(ctx context.Context) []GRPCEndpoint {
	if !grpcConfigEnabled() {
		return nil
	}
	rows, err := s.repo.ListGRPCEndpoints(ctx)
	if err != nil {
		slog.Error("load grpc endpoints failed", "err", err)
		return nil
	}
	return grpcEndpointsForIssue(rows, grpcEndpointFromEnv)
}

// grpcEndpointsForIssue оставляет из строк таблицы только включённые.
//
// Env-фолбэк (GRPC_ADDRESS) применяется только к ПУСТОЙ таблице. Строка с
// enabled = FALSE означает «на этом сервере gRPC выключен», и окружение не
// должно отменять это решение. Раньше фолбэк срабатывал, когда не находилось
// ни одной включённой строки, поэтому выключение всех строк возвращало в
// выдачу эндпоинт из окружения.
func grpcEndpointsForIssue(rows []GRPCEndpoint, fromEnv func() (GRPCEndpoint, bool)) []GRPCEndpoint {
	if len(rows) == 0 {
		if e, ok := fromEnv(); ok {
			return []GRPCEndpoint{e}
		}
		return nil
	}
	out := make([]GRPCEndpoint, 0, len(rows))
	for _, e := range rows {
		if e.Enabled {
			out = append(out, e)
		}
	}
	return out
}
