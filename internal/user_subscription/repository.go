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

// Tx-варианты для совмещения с outbox.AddTx в одной транзакции.

func (r *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// StartTrialTx выдаёт пробный период. Ключевые инварианты:
//
//  1. Триал выдаётся РОВНО ОДИН РАЗ за всё время. Защита двухуровневая: флаг
//     trial_used под FOR UPDATE (сериализует параллельные запросы на строке) и
//     UNIQUE (telegram_id, source, business_key) в subscription_grants —
//     инвариант держит БД, а не последовательность if-ов в Go.
//
//  2. Дни триала СКЛАДЫВАЮТСЯ с оплаченным сроком, а не заменяют его. База
//     отсчёта — GREATEST(текущий expires_at, now), поэтому эффективный срок
//     никогда не уменьшается: ни при позднем событии, ни при повторе, ни при
//     выдаче триала поверх действующей подписки.
//
//  3. Платная подписка не понижается до триала. Если у пользователя активная
//     оплата, статус и current_plan_code остаются платными, а триал только
//     добавляет дни: иначе автопродление и отчётность увидели бы «триал» там,
//     где на самом деле оплата.
//
// Льготный период (grace) — сознательное исключение: это состояние принадлежит
// платёжному контуру (ретраи + grace_until), и вмешательство в него из триала
// смешало бы два владельца одного жизненного цикла. Триал остаётся невыданным
// и доступным позже — он не «сгорает».
func (r *Repository) StartTrialTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string, trialDays int) (TrialGrantResult, error) {
	if trialDays <= 0 {
		return TrialGrantResult{}, fmt.Errorf("start trial tx: non-positive trial days %d", trialDays)
	}
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return TrialGrantResult{}, err
	}

	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return TrialGrantResult{}, err
	}

	res := TrialGrantResult{State: s, TrialDays: trialDays}

	if s.TrialUsed {
		res.Outcome = TrialAlreadyUsed
		res.PaidActiveBefore = s.Status == StatusActive
		return res, nil
	}
	if s.Status == StatusGrace {
		res.Outcome = TrialDeferredGrace
		res.PaidActiveBefore = true
		return res, nil
	}

	now := time.Now().UTC()

	// Единственная авторитетная точка отсчёта — БД: GREATEST внутри SQL, а не
	// вычисление в Go по прочитанному ранее значению. Так между чтением и
	// записью не может вклиниться конкурирующее продление.
	paidActive := s.Status == StatusActive && s.ExpiresAt != nil && s.ExpiresAt.After(now)
	res.PaidActiveBefore = paidActive

	newStatus := string(StatusTrial)
	if paidActive {
		newStatus = string(StatusActive)
	}

	var newExpiresAt time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			-- Платный план не затирается триалом: подменять его значило бы
			-- потерять сведения о том, что именно оплатил пользователь.
			current_plan_code = CASE WHEN $6 THEN current_plan_code ELSE $3 END,
			trial_used = true,
			started_at = COALESCE(started_at, $4),
			expires_at = GREATEST(COALESCE(expires_at, $4), $4) + make_interval(days => $5),
			grace_until = NULL,
			canceled_at = CASE WHEN $6 THEN canceled_at ELSE NULL END,
			last_payment_id = CASE WHEN $6 THEN last_payment_id ELSE NULL END,
			auto_renew_enabled = CASE WHEN $6 THEN auto_renew_enabled ELSE false END,
			cancel_at_period_end = CASE WHEN $6 THEN cancel_at_period_end ELSE false END,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
		RETURNING expires_at
	`, telegramID, newStatus, TrialPlanCode, now, trialDays, paidActive).Scan(&newExpiresAt); err != nil {
		return TrialGrantResult{}, fmt.Errorf("update trial subscription tx: %w", err)
	}

	// Запись в журнал — часть ТОЙ ЖЕ транзакции. UNIQUE не даст создать вторую
	// запись даже при гонке; конфликт означает, что триал уже выдан, и весь
	// апдейт откатывается вместе с транзакцией.
	tag, err := tx.Exec(ctx, `
		INSERT INTO subscription_grants (
			telegram_id, source, business_key, duration_days,
			effective_from, effective_until, granted_at
		)
		VALUES ($1, 'trial', 'trial', $2, $3, $4, $3)
		ON CONFLICT (telegram_id, source, business_key) DO NOTHING
	`, telegramID, trialDays, now, newExpiresAt.UTC())
	if err != nil {
		return TrialGrantResult{}, fmt.Errorf("record trial grant tx: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Журнал уже содержит триал, а флаг в подписке его не показывал —
		// значит строка подписки была пересоздана. Начисление отменяем.
		return TrialGrantResult{}, fmt.Errorf("trial grant already recorded for tg=%d: refusing to grant twice", telegramID)
	}

	if _, err := r.ensureTokenTx(ctx, tx, telegramID, &newExpiresAt); err != nil {
		return TrialGrantResult{}, err
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return TrialGrantResult{}, err
	}
	res.State = s
	if paidActive {
		res.Outcome = TrialGrantedOnTopOfPaid
	} else {
		res.Outcome = TrialGranted
	}
	return res, nil
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

	if durationDays <= 0 {
		return nil, false, fmt.Errorf("activate paid tx: non-positive duration %d", durationDays)
	}

	now := time.Now().UTC()

	// Журнал начислений — источник инварианта «один платёж = одно начисление».
	// Пишем ДО обновления срока: конфликт по UNIQUE означает, что этот платёж
	// уже учтён, и продлевать второй раз нельзя.
	if paymentID != "" {
		tag, gErr := tx.Exec(ctx, `
			INSERT INTO subscription_grants (
				telegram_id, source, business_key, duration_days,
				effective_from, effective_until, granted_at
			)
			VALUES ($1, 'paid', $2, $3, $4, $4 + make_interval(days => $3), $4)
			ON CONFLICT (telegram_id, source, business_key) DO NOTHING
		`, telegramID, paymentID, durationDays, now)
		if gErr != nil {
			return nil, false, fmt.Errorf("record paid grant tx: %w", gErr)
		}
		if tag.RowsAffected() == 0 {
			return s, false, nil
		}
	}

	// Семантика: успешный платёж может только ВКЛЮЧИТЬ автопродление и сбросить
	// cancel_at_period_end, но не может их выключить. Это нужно, чтобы платёж криптой
	// (где AutoRenewEnabled=false) не выключал YooKassa-автопродление, если пользователь
	// его раньше включил. Явное выключение автопродления идёт через
	// HandleBillingAutoRenewDisabled / HandleBillingPaymentMethodUnbound / HandleCancel.
	// Продление считается в SQL от GREATEST(expires_at, now): база берётся из
	// строки под блокировкой, а не из значения, прочитанного выше. Иначе две
	// одновременно подтверждённые покупки могли бы посчитаться от одной базы,
	// и одна из длительностей потерялась бы (lost update).
	var newExpiresAt time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			current_plan_code = $3,
			started_at = COALESCE(started_at, $4),
			expires_at = GREATEST(COALESCE(expires_at, $4), $4) + make_interval(days => $5),
			grace_until = NULL,
			canceled_at = NULL,
			last_payment_id = $6,
			auto_renew_enabled = auto_renew_enabled OR $7,
			cancel_at_period_end = CASE WHEN $7 THEN false ELSE cancel_at_period_end END,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
		RETURNING expires_at
	`, telegramID, string(StatusActive), planCode, now, durationDays, paymentID, autoRenewEnabled).Scan(&newExpiresAt); err != nil {
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
	// ВАЖНО: в grace можно перейти только из active или grace. Истёкшую (expired) подписку
	// оживлять нельзя — иначе неуспешный recurring-charge выдаст бесплатный доступ тому,
	// у кого подписка уже закончилась.
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2,
			grace_until = $3,
			updated_at = now()
		WHERE telegram_id = $1
		  AND status IN ('active', 'grace')
	`, telegramID, string(StatusGrace), graceUntil.UTC()); err != nil {
		return nil, fmt.Errorf("mark grace started tx: %w", err)
	}
	_ = reason
	return r.getStateForUpdateTx(ctx, tx, telegramID)
}

// ExpiredCandidate — кандидат на принудительное истечение (sweep-воркер).
type ExpiredCandidate struct {
	TelegramID int64
	Status     string
}

// FindExpiredForSweep ищет подписки с истёкшим сроком, которые ещё не переведены
// в expired.
//
// Отсрочки разные, чтобы не сломать автопродление:
//   - plainMargin — для тех, у кого автопродление выключено: биллинг их
//     продлевать не будет, отзываем почти сразу;
//   - renewMargin — для тех, у кого автопродление включено: биллинг владеет
//     ими (ретраи + grace), поэтому здесь только страховочная сетка на случай,
//     если биллинг сломался;
//   - для grace смотрим на grace_until, а не на expires_at.
//
// SKIP LOCKED — чтобы параллельные проходы не конкурировали за одни строки.
func (r *Repository) FindExpiredForSweep(ctx context.Context, plainMargin, renewMargin time.Duration, limit int) ([]ExpiredCandidate, error) {
	if limit <= 0 {
		limit = 200
	}
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT telegram_id, status
		FROM user_subscriptions
		WHERE (
		        status IN ('trial', 'active')
		        AND expires_at IS NOT NULL
		        AND expires_at <= now() - (
		              CASE WHEN auto_renew_enabled THEN $2::interval ELSE $1::interval END
		            )
		      )
		   OR (
		        status = 'grace'
		        AND grace_until IS NOT NULL
		        AND grace_until <= now() - $1::interval
		      )
		ORDER BY expires_at NULLS LAST
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, plainMargin.String(), renewMargin.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("find expired for sweep: %w", err)
	}
	defer rows.Close()

	var out []ExpiredCandidate
	for rows.Next() {
		var c ExpiredCandidate
		if err := rows.Scan(&c.TelegramID, &c.Status); err != nil {
			return nil, fmt.Errorf("scan expired candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
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
