package vpn_orchestrator

import (
	"context"
	"log/slog"
)

// ============================================================================
// Загрузка Hysteria2-эндпоинтов.
//
// У Hysteria нет inbound'ов: пользователь не регистрируется на узле заранее,
// вместо этого сервер при каждом подключении спрашивает node-agent по HTTP, а
// паролем выступает VLESS-UUID пользователя. Именно поэтому привязка эндпоинта
// к server_key критична: агент ищет UUID только в состоянии СВОЕЙ ноды.
//
// Источник — таблица vpn_hysteria_endpoints.
// ============================================================================

// loadHysteriaEndpoints отдаёт включённые Hysteria-эндпоинты с учётом
// рубильника HYSTERIA_CONFIG_ENABLED.
//
// Ошибка чтения таблицы даёт пустой список: фид собирается без Hysteria-строк,
// остальные транспорты работают.
func (s *Service) loadHysteriaEndpoints(ctx context.Context) []HysteriaEndpoint {
	if !hysteriaConfigEnabled() {
		return nil
	}
	rows, err := s.repo.ListHysteriaEndpoints(ctx)
	if err != nil {
		slog.Error("load hysteria endpoints failed", "err", err)
		return nil
	}
	return hysteriaEndpointsForIssue(rows)
}

// hysteriaEndpointsForIssue оставляет из строк таблицы только включённые.
// Строка с enabled = FALSE означает «на этом сервере Hysteria выключена».
func hysteriaEndpointsForIssue(rows []HysteriaEndpoint) []HysteriaEndpoint {
	out := make([]HysteriaEndpoint, 0, len(rows))
	for _, e := range rows {
		if e.Enabled {
			out = append(out, e)
		}
	}
	return out
}
