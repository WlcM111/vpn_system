package user_subscription

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Перевыпуск ссылки подписки.
//
// Токен подписки постоянный: создаётся при первой подписке и живёт с
// пользователем, меняется только срок действия. Это удобно (не надо
// переустанавливать подписку после каждой оплаты), но означает, что утёкшую
// ссылку нельзя обезвредить.
//
// Ротация выдаёт новое значение токена: старая ссылка сразу перестаёт
// открываться, потому что поиск доступа идёт строго по текущему значению.
// Причина ротации пишется в rotate_reason — остаётся след для разбора.
// ============================================================================

// RotateSubscriptionTokenTx выдаёт пользователю новый токен подписки.
// Возвращает новый токен и срок его действия.
// Если активной подписки нет, возвращает ok=false — перевыпускать нечего.
func (r *Repository) RotateSubscriptionTokenTx(ctx context.Context, tx pgx.Tx, telegramID int64, reason string) (string, *time.Time, bool, error) {
	state, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return "", nil, false, err
	}
	// Перевыпускаем только при живой подписке: иначе человек получит ссылку,
	// которая всё равно ничего не отдаст.
	if state == nil || state.ExpiresAt == nil || !state.ExpiresAt.After(time.Now().UTC()) {
		return "", nil, false, nil
	}
	switch state.Status {
	case StatusTrial, StatusActive, StatusGrace:
	default:
		return "", nil, false, nil
	}

	newToken := uuid.NewString()

	tag, err := tx.Exec(ctx, `
		UPDATE subscription_tokens
		SET token = $2,
		    revoked_at = NULL,
		    rotate_reason = $3,
		    last_issued_at = now(),
		    updated_at = now(),
		    expires_at = $4
		WHERE telegram_id = $1
	`, telegramID, newToken, reason, state.ExpiresAt)
	if err != nil {
		return "", nil, false, fmt.Errorf("rotate subscription token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Строки токена ещё нет — создаём.
		if _, err := r.ensureTokenTx(ctx, tx, telegramID, state.ExpiresAt); err != nil {
			return "", nil, false, err
		}
		if err := tx.QueryRow(ctx,
			`SELECT token FROM subscription_tokens WHERE telegram_id = $1`, telegramID,
		).Scan(&newToken); err != nil {
			return "", nil, false, fmt.Errorf("read token after ensure: %w", err)
		}
	}

	// Смена токена — это смена доступа: увеличиваем ревизию, чтобы ноды
	// пересинхронизировались и старые данные не считались актуальными.
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET access_rev = access_rev + 1, updated_at = now()
		WHERE telegram_id = $1
	`, telegramID); err != nil {
		return "", nil, false, fmt.Errorf("bump access rev on rotate: %w", err)
	}

	return newToken, state.ExpiresAt, true, nil
}

// HandleRotateToken обрабатывает команду перевыпуска ссылки из бота.
func (s *Service) HandleRotateToken(ctx context.Context, cmd *kafkacontracts.RotateTokenCommand) error {
	if cmd == nil || cmd.TelegramID == 0 {
		return nil
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	reason := cmd.Reason
	if reason == "" {
		reason = "user_request"
	}

	newToken, expiresAt, ok, err := s.repo.RotateSubscriptionTokenTx(ctx, tx, cmd.TelegramID, reason)
	if err != nil {
		return err
	}

	if !ok {
		if nErr := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			ParseMode:  "Markdown",
			Message: "Перевыпустить ссылку можно только при активной подписке.\n\n" +
				"Оформите подписку в меню — и получите новую ссылку доступа.",
		}); nErr != nil {
			return nErr
		}
		return tx.Commit(ctx)
	}

	slog.Info("subscription token rotated", "telegram_id", cmd.TelegramID, "reason", reason)

	link := s.publicBaseURL + newToken
	msg := "🔑 *Готово! Вот ваша новая ссылка доступа:*\n\n" +
		"`" + link + "`\n\n" +
		"Действует до: *" + expiresAt.Format("02.01.2006") + "*\n\n" +
		"⚠️ *Старая ссылка больше не работает.* Обновите подписку во всех " +
		"приложениях, где она была добавлена:\n\n" +
		"1️⃣ Нажмите на ссылку выше — она скопируется\n" +
		"2️⃣ В приложении удалите старую подписку\n" +
		"3️⃣ Добавьте новую из буфера обмена\n\n" +
		"Не делитесь ссылкой: она привязана к вашей подписке."

	if nErr := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		ParseMode:  "Markdown",
		Message:    msg,
	}); nErr != nil {
		return nErr
	}

	return tx.Commit(ctx)
}
