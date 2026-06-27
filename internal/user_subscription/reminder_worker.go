package user_subscription

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5"
)

// ============================================================================
// Воркер напоминаний об истечении подписки.
//
// Периодически сканирует активные подписки и шлёт пользователю напоминания:
//   • Платная подписка (status='active'): за 7, 3, 1 день до конца и в момент
//     окончания. К каждому — кнопка «Купить подписку».
//   • Пробный период (status='trial'): только за 1 день до конца и в момент
//     окончания, с текстом про окончание пробного периода и кнопкой покупки.
//
// Дедупликация: в user_subscriptions есть last_reminder_stage и reminder_anchor_at.
// last_reminder_stage хранит последнюю отправленную «веху» ('d7'/'d3'/'d1'/'expired'),
// reminder_anchor_at — на какой expires_at эти вехи навешаны. При продлении
// expires_at меняется → воркер сбрасывает стадию и начинает цикл заново.
// ============================================================================

// reminderCandidate — строка из выборки кандидатов на напоминание.
type reminderCandidate struct {
	TelegramID        int64
	Status            string
	ExpiresAt         time.Time
	LastReminderStage string // может быть пустым
	ReminderAnchorSet bool
	ReminderAnchorAt  time.Time
}

// reminderStage — веха напоминания.
type reminderStage string

const (
	stageNone    reminderStage = ""
	stageD7      reminderStage = "d7"
	stageD3      reminderStage = "d3"
	stageD1      reminderStage = "d1"
	stageExpired reminderStage = "expired"
)

// RunExpirationReminderWorker периодически проверяет подписки и шлёт напоминания.
// Блокирующий вызов — запускать в горутине из main. Период берётся из аргумента.
func RunExpirationReminderWorker(ctx context.Context, svc *Service, interval time.Duration) {
	if interval <= 0 {
		interval = 1 * time.Hour
	}

	// первый прогон с небольшой задержкой, чтобы дать сервису подняться
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("user-subscription reminder worker stopped")
			return
		case <-timer.C:
			if err := svc.processExpirationReminders(ctx); err != nil {
				slog.Error("user-subscription reminder run failed", "err", err)
			}
			timer.Reset(interval)
		}
	}
}

// processExpirationReminders обрабатывает один проход воркера.
func (s *Service) processExpirationReminders(ctx context.Context) error {
	candidates, err := s.repo.FindReminderCandidates(ctx)
	if err != nil {
		return fmt.Errorf("find reminder candidates: %w", err)
	}

	now := time.Now().UTC()
	var sent int
	for _, c := range candidates {
		stage := decideReminderStage(c, now)
		if stage == stageNone {
			continue
		}

		if err := s.sendReminder(ctx, c, stage); err != nil {
			slog.Error("user-subscription send reminder failed",
				"telegram_id", c.TelegramID, "stage", stage, "err", err)
			continue
		}
		sent++
	}

	if sent > 0 {
		slog.Info("user-subscription reminders sent", "count", sent)
	}
	return nil
}

// decideReminderStage решает, какую веху нужно отправить для кандидата сейчас,
// учитывая статус (trial/active), сколько осталось до конца и что уже отправлено.
// Возвращает stageNone, если отправлять нечего.
func decideReminderStage(c reminderCandidate, now time.Time) reminderStage {
	// если истёкший anchor не совпадает с текущим expires_at — значит подписку
	// продлили, прошлые вехи неактуальны (их сбросит репозиторий при записи).
	lastStage := reminderStage(c.LastReminderStage)
	if c.ReminderAnchorSet && !c.ReminderAnchorAt.Equal(c.ExpiresAt) {
		lastStage = stageNone
	}

	// сколько целых дней до окончания (может быть отрицательным, если уже прошло)
	remaining := c.ExpiresAt.Sub(now)

	isTrial := c.Status == string(StatusTrial)

	// Уже истекла?
	if !c.ExpiresAt.After(now) {
		if lastStage == stageExpired {
			return stageNone // уже уведомляли об окончании
		}
		return stageExpired
	}

	// Ещё активна — считаем дни до конца (округляем вверх: <24ч = «1 день»).
	days := int(remaining.Hours() / 24)
	if remaining%(24*time.Hour) > 0 {
		days++
	}

	if isTrial {
		// Для триала — только за 1 день. Стадии d7/d3 пропускаем.
		if days <= 1 && !stageAlreadySent(lastStage, stageD1) {
			return stageD1
		}
		return stageNone
	}

	// Платная подписка: 7 / 3 / 1 день.
	switch {
	case days <= 1 && !stageAlreadySent(lastStage, stageD1):
		return stageD1
	case days <= 3 && days > 1 && !stageAlreadySent(lastStage, stageD3):
		return stageD3
	case days <= 7 && days > 3 && !stageAlreadySent(lastStage, stageD7):
		return stageD7
	}
	return stageNone
}

// stageAlreadySent сообщает, была ли уже отправлена веха target (или более поздняя).
// Порядок вех по «свежести»: d7 < d3 < d1 < expired.
func stageAlreadySent(last, target reminderStage) bool {
	return stageRank(last) >= stageRank(target)
}

func stageRank(s reminderStage) int {
	switch s {
	case stageD7:
		return 1
	case stageD3:
		return 2
	case stageD1:
		return 3
	case stageExpired:
		return 4
	default:
		return 0
	}
}

// sendReminder формирует текст и в одной транзакции публикует уведомление
// (через outbox) и фиксирует отправленную веху (dedup).
func (s *Service) sendReminder(ctx context.Context, c reminderCandidate, stage reminderStage) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	isTrial := c.Status == string(StatusTrial)
	text := reminderText(isTrial, stage, c.ExpiresAt)

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: c.TelegramID,
		Message:    text,
		ParseMode:  "Markdown",
		Keyboard:   kafkacontracts.TgKeyboardTrialOrBuy, // содержит «Купить подписку»
	}); err != nil {
		return err
	}

	if err := s.repo.MarkReminderSentTx(ctx, tx, c.TelegramID, string(stage), c.ExpiresAt); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// reminderText формирует текст напоминания.
func reminderText(isTrial bool, stage reminderStage, expiresAt time.Time) string {
	if isTrial {
		switch stage {
		case stageExpired:
			return "⌛ Твой *пробный период* закончился.\n\n" +
				"Чтобы продолжить пользоваться сервисом, оформи подписку 👇"
		default: // stageD1
			return "⏳ Твой *пробный период* заканчивается завтра.\n\n" +
				"Чтобы не потерять доступ, оформи подписку 👇"
		}
	}

	// Платная подписка.
	switch stage {
	case stageExpired:
		return "⌛ Твоя подписка закончилась.\n\n" +
			"Продли её, чтобы снова пользоваться сервисом 👇"
	case stageD1:
		return "⏳ Твоя подписка заканчивается *завтра*.\n\n" +
			"Продли её заранее, чтобы не потерять доступ 👇"
	case stageD3:
		return "⏳ Твоя подписка заканчивается через *3 дня*.\n\n" +
			"Можно продлить уже сейчас 👇"
	case stageD7:
		return "⏳ Твоя подписка заканчивается через *7 дней*.\n\n" +
			"Можно продлить заранее 👇"
	default:
		return "⏳ Не забудь продлить подписку 👇"
	}
}

// ----------------------------------------------------------------------------
// Методы репозитория для воркера напоминаний.
// (Размещены здесь же, чтобы доработка была в одном файле; используют тот же
// тип Repository.)
// ----------------------------------------------------------------------------

// FindReminderCandidates выбирает активные подписки с заданным сроком окончания.
// Берём только status IN ('trial','active') и непустой expires_at.
func (r *Repository) FindReminderCandidates(ctx context.Context) ([]reminderCandidate, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT telegram_id, status, expires_at, last_reminder_stage, reminder_anchor_at
		FROM user_subscriptions
		WHERE status IN ('trial', 'active')
		  AND expires_at IS NOT NULL
		  AND expires_at <= now() + interval '8 days'
		  AND (
		        last_reminder_stage IS DISTINCT FROM 'expired'
		        OR reminder_anchor_at IS DISTINCT FROM expires_at
		      )
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []reminderCandidate
	for rows.Next() {
		var (
			c         reminderCandidate
			lastStage *string
			anchorAt  *time.Time
		)
		if err := rows.Scan(&c.TelegramID, &c.Status, &c.ExpiresAt, &lastStage, &anchorAt); err != nil {
			return nil, err
		}
		if lastStage != nil {
			c.LastReminderStage = *lastStage
		}
		if anchorAt != nil {
			c.ReminderAnchorSet = true
			c.ReminderAnchorAt = anchorAt.UTC()
		}
		c.ExpiresAt = c.ExpiresAt.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

// MarkReminderSentTx фиксирует отправленную веху и привязывает её к текущему
// expires_at (anchor). Если expires_at позже изменится (продление), несовпадение
// anchor'а заставит воркер начать цикл заново.
func (r *Repository) MarkReminderSentTx(ctx context.Context, tx pgx.Tx, telegramID int64, stage string, expiresAt time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET last_reminder_stage = $2,
		    reminder_anchor_at = $3,
		    updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, stage, expiresAt)
	return err
}
