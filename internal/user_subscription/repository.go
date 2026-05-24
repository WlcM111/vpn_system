package user_subscription

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func computeDaysLeft(expiresAt *time.Time) int {
	if expiresAt == nil {
		return 0
	}
	now := time.Now().UTC()
	if !expiresAt.After(now) {
		return 0
	}
	return int(expiresAt.Sub(now).Hours()/24) + 1
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}

func (r *Repository) ensureUserAndStateTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string) error {
	if country == "" {
		country = "LT"
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO telegram_users (telegram_id)
		VALUES ($1)
		ON CONFLICT (telegram_id) DO NOTHING
	`, telegramID); err != nil {
		return fmt.Errorf("insert telegram user: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO user_subscriptions (
			telegram_id, status, country_code, trial_used,
			auto_renew_enabled, cancel_at_period_end, access_rev
		)
		VALUES ($1, $2, $3, false, false, false, 0)
		ON CONFLICT (telegram_id) DO NOTHING
	`, telegramID, string(StatusNone), country); err != nil {
		return fmt.Errorf("insert subscription state: %w", err)
	}

	return nil
}

func (r *Repository) scanState(row pgx.Row) (*SubscriptionState, error) {
	var s SubscriptionState
	var currentPlan sql.NullString
	var startedAt sql.NullTime
	var expiresAt sql.NullTime
	var graceUntil sql.NullTime
	var canceledAt sql.NullTime
	var lastPaymentID sql.NullString
	var country sql.NullString

	err := row.Scan(
		&s.TelegramID,
		&s.Status,
		&currentPlan,
		&s.TrialUsed,
		&startedAt,
		&expiresAt,
		&graceUntil,
		&canceledAt,
		&lastPaymentID,
		&country,
		&s.AutoRenewEnabled,
		&s.CancelAtPeriodEnd,
		&s.AccessRev,
	)
	if err != nil {
		return nil, err
	}

	if currentPlan.Valid {
		s.CurrentPlanCode = currentPlan.String
	}
	if startedAt.Valid {
		t := startedAt.Time.UTC()
		s.StartedAt = &t
	}
	if expiresAt.Valid {
		t := expiresAt.Time.UTC()
		s.ExpiresAt = &t
	}
	if graceUntil.Valid {
		t := graceUntil.Time.UTC()
		s.GraceUntil = &t
	}
	if canceledAt.Valid {
		t := canceledAt.Time.UTC()
		s.CanceledAt = &t
	}
	if lastPaymentID.Valid {
		s.LastPaymentID = lastPaymentID.String
	}
	if country.Valid {
		s.CountryCode = country.String
	}

	if s.Status == StatusGrace && s.GraceUntil != nil {
		s.DaysLeft = computeDaysLeft(s.GraceUntil)
	} else {
		s.DaysLeft = computeDaysLeft(s.ExpiresAt)
	}

	return &s, nil
}

func (r *Repository) getStateForUpdateTx(ctx context.Context, tx pgx.Tx, telegramID int64) (*SubscriptionState, error) {
	row := tx.QueryRow(ctx, `
		SELECT
			telegram_id,
			status,
			current_plan_code,
			trial_used,
			started_at,
			expires_at,
			grace_until,
			canceled_at,
			last_payment_id,
			country_code,
			auto_renew_enabled,
			cancel_at_period_end,
			access_rev
		FROM user_subscriptions
		WHERE telegram_id = $1
		FOR UPDATE
	`, telegramID)

	s, err := r.scanState(row)
	if err != nil {
		return nil, fmt.Errorf("select subscription state: %w", err)
	}

	if err := r.expireIfNeededTx(ctx, tx, s); err != nil {
		return nil, err
	}

	return s, nil
}

func (r *Repository) expireIfNeededTx(ctx context.Context, tx pgx.Tx, s *SubscriptionState) error {
	now := time.Now().UTC()

	shouldExpire := false
	if (s.Status == StatusTrial || s.Status == StatusActive) && s.ExpiresAt != nil && !s.ExpiresAt.After(now) {
		shouldExpire = true
	}
	if s.Status == StatusGrace && s.GraceUntil != nil && !s.GraceUntil.After(now) {
		shouldExpire = true
	}

	if !shouldExpire {
		return nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			auto_renew_enabled = false,
			cancel_at_period_end = true,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, s.TelegramID, string(StatusExpired)); err != nil {
		return fmt.Errorf("expire subscription: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE subscription_tokens
		SET expires_at = $2,
			updated_at = now()
		WHERE telegram_id = $1
	`, s.TelegramID, now); err != nil {
		return fmt.Errorf("expire token: %w", err)
	}

	s.Status = StatusExpired
	s.AutoRenewEnabled = false
	s.CancelAtPeriodEnd = true
	s.DaysLeft = 0
	s.AccessRev++
	return nil
}

func (r *Repository) GetStatus(ctx context.Context, telegramID int64, country string) (*SubscriptionState, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return s, nil
}

func (r *Repository) StartTrial(ctx context.Context, telegramID int64, country string, trialDays int) (*SubscriptionState, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, false, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if s.TrialUsed || s.Status == StatusTrial || s.Status == StatusActive || s.Status == StatusGrace {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit tx: %w", err)
		}
		return s, false, nil
	}

	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(trialDays) * 24 * time.Hour)

	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			current_plan_code = $3,
			trial_used = true,
			started_at = $4,
			expires_at = $5,
			grace_until = NULL,
			canceled_at = NULL,
			last_payment_id = NULL,
			auto_renew_enabled = false,
			cancel_at_period_end = false,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, string(StatusTrial), TrialPlanCode, now, expiresAt); err != nil {
		return nil, false, fmt.Errorf("update trial subscription: %w", err)
	}

	if _, err := r.ensureTokenTx(ctx, tx, telegramID, &expiresAt); err != nil {
		return nil, false, err
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit tx: %w", err)
	}

	return s, true, nil
}

func (r *Repository) ActivatePaid(ctx context.Context, telegramID int64, country, planCode string, durationDays int, paymentID string, autoRenewEnabled bool) (*SubscriptionState, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, false, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if paymentID != "" {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO processed_messages (message_id, source, message_type)
			VALUES ($1, 'billing-service', 'billing.payment_succeeded')
			ON CONFLICT (message_id) DO NOTHING
			RETURNING true
		`, "payment:"+paymentID).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			if err := tx.Commit(ctx); err != nil {
				return nil, false, fmt.Errorf("commit duplicate payment tx: %w", err)
			}
			return s, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("dedup payment message: %w", err)
		}
	}

	if paymentID != "" && s.LastPaymentID == paymentID {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit tx: %w", err)
		}
		return s, false, nil
	}

	now := time.Now().UTC()
	base := now
	if s.ExpiresAt != nil && s.ExpiresAt.After(now) {
		base = *s.ExpiresAt
	}
	newExpiresAt := base.Add(time.Duration(durationDays) * 24 * time.Hour)

	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			current_plan_code = $3,
			started_at = COALESCE(started_at, $4),
			expires_at = $5,
			grace_until = NULL,
			canceled_at = NULL,
			last_payment_id = $6,
			auto_renew_enabled = $7,
			cancel_at_period_end = false,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, string(StatusActive), planCode, now, newExpiresAt, paymentID, autoRenewEnabled); err != nil {
		return nil, false, fmt.Errorf("activate paid subscription: %w", err)
	}

	if _, err := r.ensureTokenTx(ctx, tx, telegramID, &newExpiresAt); err != nil {
		return nil, false, err
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit tx: %w", err)
	}

	return s, true, nil
}

func (r *Repository) Cancel(ctx context.Context, telegramID int64, country string) (*SubscriptionState, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, false, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if s.Status != StatusTrial && s.Status != StatusActive && s.Status != StatusGrace {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit tx: %w", err)
		}
		return s, false, nil
	}

	now := time.Now().UTC()

	if s.Status == StatusTrial {
		if _, err := tx.Exec(ctx, `
			UPDATE user_subscriptions
			SET status = $2,
				expires_at = $3,
				grace_until = NULL,
				canceled_at = $3,
				auto_renew_enabled = false,
				cancel_at_period_end = true,
				access_rev = access_rev + 1,
				updated_at = now()
			WHERE telegram_id = $1
		`, telegramID, string(StatusExpired), now); err != nil {
			return nil, false, fmt.Errorf("cancel trial subscription: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE subscription_tokens
			SET expires_at = $2,
				updated_at = now()
			WHERE telegram_id = $1
		`, telegramID, now); err != nil {
			return nil, false, fmt.Errorf("expire token: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE user_subscriptions
			SET auto_renew_enabled = false,
				cancel_at_period_end = true,
				canceled_at = $2,
				updated_at = now()
			WHERE telegram_id = $1
		`, telegramID, now); err != nil {
			return nil, false, fmt.Errorf("cancel paid auto-renew: %w", err)
		}
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit tx: %w", err)
	}

	return s, true, nil
}

func (r *Repository) MarkAutoRenewDisabled(ctx context.Context, telegramID int64, country string) (*SubscriptionState, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET auto_renew_enabled = false,
			cancel_at_period_end = CASE WHEN status IN ('active', 'grace') THEN true ELSE cancel_at_period_end END,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID); err != nil {
		return nil, fmt.Errorf("mark auto-renew disabled: %w", err)
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return s, nil
}

func (r *Repository) MarkGraceStarted(ctx context.Context, telegramID int64, country string, graceUntil time.Time, reason string) (*SubscriptionState, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			grace_until = $3,
			updated_at = now()
		WHERE telegram_id = $1
		  AND status IN ('active', 'grace', 'expired')
	`, telegramID, string(StatusGrace), graceUntil.UTC()); err != nil {
		return nil, fmt.Errorf("mark grace started: %w", err)
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	_ = reason
	return s, nil
}

func (r *Repository) MarkSuspended(ctx context.Context, telegramID int64, country string, reason string) (*SubscriptionState, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			expires_at = CASE WHEN expires_at IS NULL OR expires_at > $3 THEN $3 ELSE expires_at END,
			grace_until = NULL,
			auto_renew_enabled = false,
			cancel_at_period_end = true,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, string(StatusExpired), now); err != nil {
		return nil, fmt.Errorf("mark suspended: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE subscription_tokens
		SET expires_at = $2,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, now); err != nil {
		return nil, fmt.Errorf("expire token after suspend: %w", err)
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	_ = reason
	return s, nil
}

func (r *Repository) EnsureToken(ctx context.Context, telegramID int64, expiresAt *time.Time) (string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	token, err := r.ensureTokenTx(ctx, tx, telegramID, expiresAt)
	if err != nil {
		return "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit tx: %w", err)
	}

	return token, nil
}

func (r *Repository) ensureTokenTx(ctx context.Context, tx pgx.Tx, telegramID int64, expiresAt *time.Time) (string, error) {
	newToken := uuid.NewString()

	if _, err := tx.Exec(ctx, `
		INSERT INTO subscription_tokens (telegram_id, token, expires_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (telegram_id) DO NOTHING
	`, telegramID, newToken, expiresAt); err != nil {
		return "", fmt.Errorf("insert token: %w", err)
	}

	row := tx.QueryRow(ctx, `
		SELECT token
		FROM subscription_tokens
		WHERE telegram_id = $1
		FOR UPDATE
	`, telegramID)

	var token string
	if err := row.Scan(&token); err != nil {
		return "", fmt.Errorf("select token: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE subscription_tokens
		SET expires_at = $2,
			last_issued_at = now(),
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, expiresAt); err != nil {
		return "", fmt.Errorf("update token: %w", err)
	}

	return token, nil
}

func (r *Repository) RotateToken(ctx context.Context, telegramID int64, expiresAt *time.Time, reason string) (string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE subscription_tokens
		SET revoked_at = now(), rotate_reason = $2, updated_at = now()
		WHERE telegram_id = $1 AND revoked_at IS NULL
	`, telegramID, reason); err != nil {
		return "", fmt.Errorf("revoke old token: %w", err)
	}

	newToken := uuid.NewString()
	if _, err := tx.Exec(ctx, `
		INSERT INTO subscription_tokens (telegram_id, token, expires_at)
		VALUES ($1,$2,$3)
		ON CONFLICT (telegram_id) DO UPDATE SET
			token = EXCLUDED.token,
			expires_at = EXCLUDED.expires_at,
			revoked_at = NULL,
			rotate_reason = '',
			last_issued_at = now(),
			updated_at = now()
	`, telegramID, newToken, expiresAt); err != nil {
		return "", fmt.Errorf("insert rotated token: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit tx: %w", err)
	}
	return newToken, nil
}

// Tx-варианты для совмещения с outbox.AddTx в одной транзакции.

func (r *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

func (r *Repository) StartTrialTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string, trialDays int) (*SubscriptionState, bool, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, false, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if s.TrialUsed || s.Status == StatusTrial || s.Status == StatusActive || s.Status == StatusGrace {
		return s, false, nil
	}

	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(trialDays) * 24 * time.Hour)

	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			current_plan_code = $3,
			trial_used = true,
			started_at = $4,
			expires_at = $5,
			grace_until = NULL,
			canceled_at = NULL,
			last_payment_id = NULL,
			auto_renew_enabled = false,
			cancel_at_period_end = false,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, string(StatusTrial), TrialPlanCode, now, expiresAt); err != nil {
		return nil, false, fmt.Errorf("update trial subscription tx: %w", err)
	}

	if _, err := r.ensureTokenTx(ctx, tx, telegramID, &expiresAt); err != nil {
		return nil, false, err
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

func (r *Repository) ActivatePaidTx(ctx context.Context, tx pgx.Tx, telegramID int64, country, planCode string, durationDays int, paymentID string, autoRenewEnabled bool) (*SubscriptionState, bool, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, false, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if paymentID != "" {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO processed_messages (message_id, source, message_type)
			VALUES ($1, 'billing-service', 'billing.payment_succeeded')
			ON CONFLICT (message_id) DO NOTHING
			RETURNING true
		`, "payment:"+paymentID).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			return s, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("dedup payment message tx: %w", err)
		}
	}

	if paymentID != "" && s.LastPaymentID == paymentID {
		return s, false, nil
	}

	now := time.Now().UTC()
	base := now
	if s.ExpiresAt != nil && s.ExpiresAt.After(now) {
		base = *s.ExpiresAt
	}
	newExpiresAt := base.Add(time.Duration(durationDays) * 24 * time.Hour)

	// Семантика: успешный платёж может только ВКЛЮЧИТЬ автопродление и сбросить
	// cancel_at_period_end, но не может их выключить. Это нужно, чтобы платёж криптой
	// (где AutoRenewEnabled=false) не выключал YooKassa-автопродление, если пользователь
	// его раньше включил. Явное выключение автопродления идёт через
	// HandleBillingAutoRenewDisabled / HandleBillingPaymentMethodUnbound / HandleCancel.
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			current_plan_code = $3,
			started_at = COALESCE(started_at, $4),
			expires_at = $5,
			grace_until = NULL,
			canceled_at = NULL,
			last_payment_id = $6,
			auto_renew_enabled = auto_renew_enabled OR $7,
			cancel_at_period_end = CASE WHEN $7 THEN false ELSE cancel_at_period_end END,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, string(StatusActive), planCode, now, newExpiresAt, paymentID, autoRenewEnabled); err != nil {
		return nil, false, fmt.Errorf("activate paid subscription tx: %w", err)
	}

	if _, err := r.ensureTokenTx(ctx, tx, telegramID, &newExpiresAt); err != nil {
		return nil, false, err
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

func (r *Repository) CancelTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string) (*SubscriptionState, bool, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, false, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}

	if s.Status != StatusTrial && s.Status != StatusActive && s.Status != StatusGrace {
		return s, false, nil
	}

	now := time.Now().UTC()

	if s.Status == StatusTrial {
		if _, err := tx.Exec(ctx, `
			UPDATE user_subscriptions
			SET status = $2,
				expires_at = $3,
				grace_until = NULL,
				canceled_at = $3,
				auto_renew_enabled = false,
				cancel_at_period_end = true,
				access_rev = access_rev + 1,
				updated_at = now()
			WHERE telegram_id = $1
		`, telegramID, string(StatusExpired), now); err != nil {
			return nil, false, fmt.Errorf("cancel trial subscription tx: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE subscription_tokens
			SET expires_at = $2, updated_at = now()
			WHERE telegram_id = $1
		`, telegramID, now); err != nil {
			return nil, false, fmt.Errorf("expire token tx: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE user_subscriptions
			SET auto_renew_enabled = false,
				cancel_at_period_end = true,
				canceled_at = $2,
				updated_at = now()
			WHERE telegram_id = $1
		`, telegramID, now); err != nil {
			return nil, false, fmt.Errorf("cancel paid auto-renew tx: %w", err)
		}
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

func (r *Repository) MarkAutoRenewDisabledTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string) (*SubscriptionState, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET auto_renew_enabled = false,
			cancel_at_period_end = CASE WHEN status IN ('active', 'grace') THEN true ELSE cancel_at_period_end END,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID); err != nil {
		return nil, fmt.Errorf("mark auto-renew disabled tx: %w", err)
	}
	return r.getStateForUpdateTx(ctx, tx, telegramID)
}

func (r *Repository) MarkGraceStartedTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string, graceUntil time.Time, reason string) (*SubscriptionState, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			grace_until = $3,
			updated_at = now()
		WHERE telegram_id = $1
		  AND status IN ('active', 'grace', 'expired')
	`, telegramID, string(StatusGrace), graceUntil.UTC()); err != nil {
		return nil, fmt.Errorf("mark grace started tx: %w", err)
	}
	_ = reason
	return r.getStateForUpdateTx(ctx, tx, telegramID)
}

func (r *Repository) MarkSuspendedTx(ctx context.Context, tx pgx.Tx, telegramID int64, country, reason string) (*SubscriptionState, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			expires_at = CASE WHEN expires_at IS NULL OR expires_at > $3 THEN $3 ELSE expires_at END,
			grace_until = NULL,
			auto_renew_enabled = false,
			cancel_at_period_end = true,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, string(StatusExpired), now); err != nil {
		return nil, fmt.Errorf("mark suspended tx: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE subscription_tokens
		SET expires_at = $2, updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, now); err != nil {
		return nil, fmt.Errorf("expire token after suspend tx: %w", err)
	}
	_ = reason
	return r.getStateForUpdateTx(ctx, tx, telegramID)
}

func (r *Repository) GetStatusTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string) (*SubscriptionState, error) {
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return nil, err
	}
	return r.getStateForUpdateTx(ctx, tx, telegramID)
}
