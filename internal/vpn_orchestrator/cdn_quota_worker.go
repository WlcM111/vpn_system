package vpn_orchestrator

import (
	"context"
	"log/slog"
	"time"

	commonmetrics "vpn-platform/internal/common/metrics"
)

// ============================================================================
// Периодические задачи квоты CDN.
//
//	1. Плановый календарный сброс — 00:00:00 UTC первого числа месяца.
//	   Реализован не «будильником на полночь», а сверкой ключа периода: строка
//	   с period_key <> cal:<текущий месяц> подлежит сбросу. Поэтому пропуск
//	   полуночи из-за рестарта, деплоя или сдвига часов не теряет сброс — его
//	   выполнит ближайший проход. По той же причине повторный запуск job'а
//	   идемпотентен: после первого прохода ключи совпадают и UPDATE не находит
//	   строк.
//
//	2. Экспорт метрик потребления и свежести телеметрии.
//
// Многоинстансовость: проход под advisory-lock, как и reconcile.
// ============================================================================

const cdnQuotaLockKey int64 = 776644221101

// RunCDNQuotaWorker запускает периодический проход. Блокирующий — вызывать в
// горутине. Выключенная политика превращает воркер в no-op.
func (s *Service) RunCDNQuotaWorker(ctx context.Context, interval time.Duration, batchSize int) {
	if !s.cfg.CDNQuota.Enabled {
		slog.Info("[cdn-quota] worker disabled by policy")
		return
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if batchSize <= 0 {
		batchSize = 500
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(45 * time.Second):
	}
	s.cdnQuotaPassOnce(ctx, batchSize)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cdnQuotaPassOnce(ctx, batchSize)
		}
	}
}

func (s *Service) cdnQuotaPassOnce(ctx context.Context, batchSize int) {
	var locked bool
	if err := s.repo.TryAdvisoryLock(ctx, cdnQuotaLockKey, &locked); err != nil {
		slog.Warn("[cdn-quota] advisory lock check failed", "err", err)
		return
	}
	if !locked {
		return
	}
	defer func() {
		if err := s.repo.AdvisoryUnlock(ctx, cdnQuotaLockKey); err != nil {
			slog.Warn("[cdn-quota] advisory unlock failed", "err", err)
		}
	}()

	s.runCalendarReset(ctx, batchSize)
	s.exportQuotaMetrics(ctx)
}

// runCalendarReset переводит все отставшие строки в текущий календарный период.
//
// После сброса CDN-доступ восстанавливается не здесь, а обычным идемпотентным
// reconcile: строка снова active, поэтому ближайший проход положит CDN-профиль
// в желаемое состояние. Прямая выдача доступа из job'а сброса была бы вторым
// путём записи в desired state — ровно тем, чего архитектура избегает.
func (s *Service) runCalendarReset(ctx context.Context, batchSize int) {
	now := time.Now().UTC()
	periodKey := calendarPeriodKey(now)

	var (
		afterID   int64
		afterNode string
		resetRows int
	)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rows, err := s.repo.ListQuotasForCalendarReset(ctx, periodKey, afterID, afterNode, batchSize)
		if err != nil {
			commonmetrics.CDNQuotaResetErrorsTotal.WithLabelValues("calendar").Inc()
			slog.Error("[cdn-quota] list rows for calendar reset failed", "err", err)
			return
		}
		if len(rows) == 0 {
			break
		}

		for i := range rows {
			row := rows[i]
			afterID, afterNode = row.TelegramID, row.NodeID

			tx, err := s.repo.pool.Begin(ctx)
			if err != nil {
				commonmetrics.CDNQuotaResetErrorsTotal.WithLabelValues("calendar").Inc()
				slog.Error("[cdn-quota] begin tx failed", "err", err)
				return
			}
			done, err := s.repo.ResetQuotaTx(ctx, tx, row.TelegramID, row.NodeID, periodKey, s.cfg.CDNQuota.LimitBytes, now)
			if err != nil {
				_ = tx.Rollback(ctx)
				commonmetrics.CDNQuotaResetErrorsTotal.WithLabelValues("calendar").Inc()
				slog.Warn("[cdn-quota] calendar reset failed",
					"telegram_id", row.TelegramID, "node_id", row.NodeID, "err", err)
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				commonmetrics.CDNQuotaResetErrorsTotal.WithLabelValues("calendar").Inc()
				slog.Warn("[cdn-quota] calendar reset commit failed",
					"telegram_id", row.TelegramID, "node_id", row.NodeID, "err", err)
				continue
			}
			if done {
				resetRows++
				commonmetrics.CDNQuotaResetTotal.WithLabelValues(row.NodeID, "calendar").Inc()
			}
		}

		if len(rows) < batchSize {
			break
		}
	}

	if resetRows > 0 {
		slog.Info("[cdn-quota] calendar reset done", "period", periodKey, "rows", resetRows)
	}
}

// exportQuotaMetrics выставляет агрегаты по узлам.
func (s *Service) exportQuotaMetrics(ctx context.Context) {
	summaries, err := s.repo.SummarizeQuotas(ctx, s.cfg.CDNQuota.TelemetryStaleAfter)
	if err != nil {
		slog.Warn("[cdn-quota] summarize failed", "err", err)
		return
	}
	for _, sum := range summaries {
		commonmetrics.CDNQuotaRowsTotal.WithLabelValues(sum.NodeID).Set(float64(sum.Rows))
		commonmetrics.CDNQuotaExhaustedRows.WithLabelValues(sum.NodeID).Set(float64(sum.ExhaustedRows))
		commonmetrics.CDNQuotaUsedBytes.WithLabelValues(sum.NodeID).Set(float64(sum.UsedBytesTotal))
		commonmetrics.CDNQuotaStaleRows.WithLabelValues(sum.NodeID).Set(float64(sum.StaleRows))

		if s.cfg.CDNQuota.TelemetryStaleAfter > 0 && sum.StaleRows > 0 {
			slog.Warn("[cdn-quota] telemetry is stale",
				"node_id", sum.NodeID, "stale_rows", sum.StaleRows,
				"threshold", s.cfg.CDNQuota.TelemetryStaleAfter.String(),
				"fail_mode", string(s.cfg.CDNQuota.FailMode))
		}
	}
}
