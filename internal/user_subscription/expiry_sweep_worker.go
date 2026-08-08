package user_subscription

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Воркер принудительного истечения подписок.
//
// ЗАЧЕМ. Раньше отзыв доступа происходил ТОЛЬКО через биллинг: воркер
// LockExpiredGraceProfiles выбирал людей из billing_recurring_profiles со
// статусом grace/expiring. У кого рекуррентного профиля в этих статусах нет —
// доступ не отзывался никогда. Плюс expireIfNeededTx (ленивое истечение при
// чтении строки) меняет статус, но НЕ публикует событие, поэтому нода об
// отзыве не узнаёт и UUID остаётся зарегистрированным.
//
// ЧТО ДЕЛАЕТ. Периодически ищет подписки с истёкшим сроком и переводит их в
// expired через ту же MarkSuspendedTx + SubscriptionSuspendedEvent, что
// использует биллинг. Дальше срабатывает существующая цепочка:
//   SubscriptionSuspendedEvent → ApplySuspended → publishRevokeCommands → нода.
//
// ОТСРОЧКИ. Наивное «истёк → отозвать» сломало бы автопродление: биллинг
// делает ретраи (15м/6ч/24ч) и держит grace, и всё это время статус остаётся
// active. Поэтому:
//   - auto_renew_enabled = false → короткая отсрочка (биллинг их не продлевает);
//   - auto_renew_enabled = true  → длинная отсрочка, только как страховочная
//     сетка на случай, если биллинг сломался;
//   - grace с истёкшим grace_until → короткая отсрочка (grace уже закончился).
//
// УВЕДОМЛЕНИЯ НЕ ШЛЁТ: об окончании подписки сообщает RunExpirationReminderWorker
// (стадия stageExpired). Дублировать не нужно.
// ============================================================================

const (
	defaultExpirySweepInterval    = 5 * time.Minute
	defaultExpirySweepPlainMargin = 15 * time.Minute
	defaultExpirySweepRenewMargin = 7 * 24 * time.Hour
	defaultExpirySweepBatchSize   = 200
)

// RunExpirySweepWorker — блокирующий вызов, запускать в горутине из main.
func RunExpirySweepWorker(ctx context.Context, svc *Service) {
	interval := durationEnv("EXPIRY_SWEEP_INTERVAL", defaultExpirySweepInterval)
	plainMargin := durationEnv("EXPIRY_SWEEP_MARGIN", defaultExpirySweepPlainMargin)
	renewMargin := durationEnv("EXPIRY_SWEEP_AUTORENEW_MARGIN", defaultExpirySweepRenewMargin)
	batchSize := intEnv("EXPIRY_SWEEP_BATCH_SIZE", defaultExpirySweepBatchSize)

	slog.Info("user-subscription expiry sweep worker started",
		"interval", interval, "margin", plainMargin,
		"autorenew_margin", renewMargin, "batch", batchSize)

	// Первый прогон с задержкой, чтобы не конкурировать с инициализацией
	// консьюмеров и дать биллингу шанс отработать своей цепочкой.
	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("user-subscription expiry sweep worker stopped")
			return
		case <-timer.C:
			if err := svc.SweepExpiredSubscriptions(ctx, plainMargin, renewMargin, batchSize); err != nil {
				slog.Error("user-subscription expiry sweep failed", "err", err)
			}
			timer.Reset(interval)
		}
	}
}

// SweepExpiredSubscriptions — один проход воркера.
func (s *Service) SweepExpiredSubscriptions(ctx context.Context, plainMargin, renewMargin time.Duration, limit int) error {
	candidates, err := s.repo.FindExpiredForSweep(ctx, plainMargin, renewMargin, limit)
	if err != nil {
		return fmt.Errorf("find expired for sweep: %w", err)
	}
	if len(candidates) == 0 {
		return nil
	}

	var suspended, failed int
	for _, c := range candidates {
		if err := s.suspendExpiredOne(ctx, c); err != nil {
			slog.Error("expiry sweep: suspend failed",
				"telegram_id", c.TelegramID, "prev_status", c.Status, "err", err)
			failed++
			continue
		}
		suspended++
	}

	slog.Info("[expiry-sweep] pass done", "suspended", suspended, "failed", failed)
	return nil
}

// suspendExpiredOne отзывает доступ одному пользователю в отдельной транзакции.
// Использует тот же путь, что и биллинг при истечении grace, — новой логики
// отзыва не вводим.
func (s *Service) suspendExpiredOne(ctx context.Context, c ExpiredCandidate) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	reason := "subscription_expired"
	if c.Status == string(StatusGrace) {
		reason = "grace_period_expired"
	}

	state, err := s.repo.MarkSuspendedTx(ctx, tx, c.TelegramID, s.defaultCountry, reason)
	if err != nil {
		return err
	}

	if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionSuspendedEvent{
		Type:        kafkacontracts.SubscriptionEventSuspended,
		TelegramID:  c.TelegramID,
		SuspendedAt: time.Now().UTC(),
		Reason:      reason,
		AccessRev:   state.AccessRev,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// чтение настроек из окружения
// ---------------------------------------------------------------------------

func durationEnv(key string, fallback time.Duration) time.Duration {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
