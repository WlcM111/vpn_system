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
// Источник — таблица vpn_hysteria_endpoints; при пустой таблице работает
// env-фолбэк (HYSTERIA_ADDRESS + HYSTERIA_SERVER_KEY).
// ============================================================================

// loadHysteriaEndpoints отдаёт включённые Hysteria-эндпоинты с учётом
// рубильника HYSTERIA_CONFIG_ENABLED и env-фолбэка.
//
// Ошибка чтения таблицы даёт пустой список, а не фолбэк: недоступность БД не
// должна подменять настройки таблицы переменными окружения (как у gRPC).
func (s *Service) loadHysteriaEndpoints(ctx context.Context) []HysteriaEndpoint {
	if !hysteriaConfigEnabled() {
		return nil
	}
	rows, err := s.repo.ListHysteriaEndpoints(ctx)
	if err != nil {
		slog.Error("load hysteria endpoints failed", "err", err)
		return nil
	}
	return hysteriaEndpointsForIssue(rows, hysteriaEndpointFromEnv)
}

// hysteriaEndpointsForIssue оставляет из строк таблицы только включённые.
//
// Env-фолбэк (HYSTERIA_ADDRESS) применяется только к ПУСТОЙ таблице, как и
// описано в шапке файла. Строка с enabled = FALSE означает «на этом сервере
// Hysteria выключена», и окружение не должно отменять это решение. Раньше
// фолбэк срабатывал, когда не находилось ни одной включённой строки (и при
// ошибке чтения), поэтому выключение всех строк возвращало в подписку
// эндпоинт из окружения (дефект D11; для gRPC то же исправлено ранее).
func hysteriaEndpointsForIssue(rows []HysteriaEndpoint, fromEnv func() (HysteriaEndpoint, bool)) []HysteriaEndpoint {
	if len(rows) == 0 {
		if e, ok := fromEnv(); ok {
			return []HysteriaEndpoint{e}
		}
		return nil
	}
	out := make([]HysteriaEndpoint, 0, len(rows))
	for _, e := range rows {
		if e.Enabled {
			out = append(out, e)
		}
	}
	return out
}
