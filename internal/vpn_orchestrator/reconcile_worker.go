package vpn_orchestrator

import (
	"context"
	"log/slog"
	"time"
)

// RunReconcileWorker периодически пересобирает желаемое состояние нод: берёт все
// активные доступы (trial/active/grace) и для каждого досылает node_sync_user.
// Это самовосстановление после сетевых сбоев — если нода была отрезана, после
// восстановления связи пользователи в течение ≤ interval вернутся на узел.
//
// Идемпотентность: node-agent игнорирует команды с access_rev не новее уже
// применённого, поэтому повторная рассылка не создаёт лишней работы на узле.
//
// Многоинстансовость: проход защищён advisory-lock в Postgres — если оркестратор
// запущен в нескольких репликах, reconcile выполняет только одна за раз (остальные
// тихо пропускают проход). Это безопасно при горизонтальном масштабировании.
//
// Блокирующий — вызывать в горутине.
func (s *Service) RunReconcileWorker(ctx context.Context, interval time.Duration, batchSize int) {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if batchSize <= 0 {
		batchSize = 200
	}

	// первый проход — с небольшой задержкой после старта, чтобы не конкурировать
	// с начальной инициализацией консьюмеров.
	select {
	case <-ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}
	s.reconcileOnce(ctx, batchSize)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileOnce(ctx, batchSize)
		}
	}
}

// reconcileLockKey — произвольный стабильный ключ advisory-lock для reconcile.
const reconcileLockKey int64 = 776644221100

func (s *Service) reconcileOnce(ctx context.Context, batchSize int) {
	// advisory-lock: одна реплика за раз. pg_try_advisory_lock не блокирует —
	// если лок занят другой репликой, просто пропускаем проход.
	var locked bool
	if err := s.repo.TryAdvisoryLock(ctx, reconcileLockKey, &locked); err != nil {
		slog.Warn("[reconcile] advisory lock check failed", "err", err)
		return
	}
	if !locked {
		slog.Debug("[reconcile] skipped: lock held by another instance")
		return
	}
	defer func() {
		if err := s.repo.AdvisoryUnlock(ctx, reconcileLockKey); err != nil {
			slog.Warn("[reconcile] advisory unlock failed", "err", err)
		}
	}()

	var (
		after   int64
		total   int
		failed  int
		batches int
	)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		accesses, err := s.repo.ListActiveAccessesForReconcile(ctx, after, batchSize)
		if err != nil {
			slog.Error("[reconcile] list active accesses failed", "after", after, "err", err)
			return
		}
		if len(accesses) == 0 {
			break
		}
		batches++

		for i := range accesses {
			a := accesses[i]
			after = a.TelegramID // keyset-пагинация
			if _, err := s.ensureUserCredentialsAndSync(ctx, &a); err != nil {
				failed++
				slog.Warn("[reconcile] sync failed", "telegram_id", a.TelegramID, "err", err)
			}
			total++
		}

		if len(accesses) < batchSize {
			break
		}
	}

	if total > 0 {
		slog.Info("[reconcile] pass done", "synced", total, "failed", failed, "batches", batches)
	}
}
