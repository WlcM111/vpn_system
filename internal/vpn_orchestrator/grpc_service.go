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
func (s *Service) loadGRPCEndpoints(ctx context.Context) []GRPCEndpoint {
	if !grpcConfigEnabled() {
		return nil
	}
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
	return endpoints
}
