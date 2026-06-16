package billing

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

type PaymentRecord struct {
	OrderID            string
	TelegramID         int64
	CheckoutType       string
	PlanCode           string
	DurationDays       int
	PaymentID          string
	Status             string
	AmountValue        string
	Currency           string
	Description        string
	ConfirmationURL    string
	IdempotenceKey     string
	SavePaymentMethod  bool
	PaymentMethodID    string
	CancellationReason string
	Metadata           map[string]string
	RawResponse        json.RawMessage
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CommandID          string
}

type RecurringProfile struct {
	TelegramID        int64
	PlanCode          kafkacontracts.PlanCode
	DurationDays      int
	AmountValue       string
	Currency          string
	PaymentMethodID   string
	AutoRenewEnabled  bool
	Status            string
	NextChargeAt      *time.Time
	RetryCount        int
	GraceUntil        *time.Time
	LastPaymentID     string
	LastFailureReason string
	UpdatedAt         time.Time
}

func (r *Repository) InsertPayment(ctx context.Context, p *PaymentRecord) error {
	meta, _ := json.Marshal(p.Metadata)
	raw := []byte("{}")
	if len(p.RawResponse) > 0 {
		raw = p.RawResponse
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO payments (
			order_id, command_id, telegram_id, checkout_type, plan_code, duration_days,
			payment_id, status, amount_value, currency, description,
			confirmation_url, idempotence_key, save_payment_method,
			payment_method_id, cancellation_reason, metadata, raw_response
		)
		VALUES (
			$1,$2,$3,$4,$5,$6,
			$7,$8,$9,$10,$11,
			$12,$13,$14,
			$15,$16,$17::jsonb,$18::jsonb
		)
		ON CONFLICT (order_id) DO NOTHING
	`,
		p.OrderID, p.CommandID, p.TelegramID, p.CheckoutType, p.PlanCode, p.DurationDays,
		p.PaymentID, p.Status, p.AmountValue, p.Currency, p.Description,
		p.ConfirmationURL, p.IdempotenceKey, p.SavePaymentMethod,
		p.PaymentMethodID, p.CancellationReason, string(meta), string(raw),
	)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}
	return nil
}

func (r *Repository) UpdatePaymentCreated(
	ctx context.Context,
	orderID string,
	paymentID string,
	status string,
	confirmationURL string,
	paymentMethodID string,
	raw json.RawMessage,
) error {
	rawStr := "{}"
	if len(raw) > 0 {
		rawStr = string(raw)
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE payments
		SET payment_id = $2,
			status = $3,
			confirmation_url = $4,
			payment_method_id = COALESCE(NULLIF($5, ''), payment_method_id),
			raw_response = $6::jsonb,
			updated_at = now()
		WHERE order_id = $1
	`, orderID, paymentID, status, confirmationURL, paymentMethodID, rawStr)
	if err != nil {
		return fmt.Errorf("update payment created: %w", err)
	}
	return nil
}

func (r *Repository) UpdatePaymentWebhookState(
	ctx context.Context,
	paymentID string,
	status string,
	paymentMethodID string,
	cancellationReason string,
	raw json.RawMessage,
) error {
	rawStr := "{}"
	if len(raw) > 0 {
		rawStr = string(raw)
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE payments
		SET status = $2,
			payment_method_id = COALESCE(NULLIF($3, ''), payment_method_id),
			cancellation_reason = COALESCE(NULLIF($4, ''), cancellation_reason),
			raw_response = $5::jsonb,
			updated_at = now()
		WHERE payment_id = $1
	`, paymentID, status, paymentMethodID, cancellationReason, rawStr)
	if err != nil {
		return fmt.Errorf("update payment webhook state: %w", err)
	}
	return nil
}

func (r *Repository) GetPaymentByPaymentID(ctx context.Context, paymentID string) (*PaymentRecord, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT
			order_id, telegram_id, checkout_type, plan_code, duration_days,
			payment_id, status, amount_value::text, currency, description,
			confirmation_url, idempotence_key, save_payment_method,
			COALESCE(payment_method_id, ''),
			COALESCE(cancellation_reason, ''),
			metadata, raw_response, created_at, updated_at
		FROM payments
		WHERE payment_id = $1
	`, paymentID)

	return r.scanPayment(row, "get payment by payment_id")
}

func (r *Repository) scanPayment(row pgx.Row, op string) (*PaymentRecord, error) {
	var p PaymentRecord
	var metaRaw []byte
	var rawResp []byte

	if err := row.Scan(
		&p.OrderID, &p.TelegramID, &p.CheckoutType, &p.PlanCode, &p.DurationDays,
		&p.PaymentID, &p.Status, &p.AmountValue, &p.Currency, &p.Description,
		&p.ConfirmationURL, &p.IdempotenceKey, &p.SavePaymentMethod,
		&p.PaymentMethodID, &p.CancellationReason,
		&metaRaw, &rawResp, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	_ = json.Unmarshal(metaRaw, &p.Metadata)
	p.RawResponse = rawResp

	return &p, nil
}

func (r *Repository) UpdateNextChargeAt(ctx context.Context, telegramID int64, activeUntil time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE billing_recurring_profiles
		SET next_charge_at = $2,
			status = CASE WHEN auto_renew_enabled THEN 'active' ELSE status END,
			updated_at = now()
		WHERE telegram_id = $1
		  AND auto_renew_enabled = true
	`, telegramID, activeUntil.UTC())
	if err != nil {
		return fmt.Errorf("update next charge at: %w", err)
	}
	return nil
}

func (r *Repository) LockDueRenewals(ctx context.Context, now time.Time, limit int) ([]RecurringProfile, error) {
	rows, err := r.pool.Query(ctx, `
		WITH picked AS (
			SELECT telegram_id
			FROM billing_recurring_profiles
			WHERE (
				auto_renew_enabled = true
				AND payment_method_id <> ''
				AND status IN ('active', 'retry')
				AND next_charge_at IS NOT NULL
				AND next_charge_at <= $1
			) OR (
				status = 'processing'
				AND locked_at IS NOT NULL
				AND locked_at < $1 - INTERVAL '30 minutes'
			)
			ORDER BY next_charge_at ASC NULLS FIRST
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE billing_recurring_profiles p
		SET status = 'processing',
			locked_at = now(),
			updated_at = now()
		FROM picked
		WHERE p.telegram_id = picked.telegram_id
		RETURNING
			p.telegram_id, p.plan_code, p.duration_days, p.amount_value::text, p.currency,
			p.payment_method_id, p.auto_renew_enabled, p.status, p.next_charge_at,
			p.retry_count, p.grace_until, COALESCE(p.last_payment_id, ''), COALESCE(p.last_failure_reason, ''), p.updated_at
	`, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim due renewals: %w", err)
	}
	defer rows.Close()

	profiles, err := scanRecurringProfiles(rows)
	if err != nil {
		return nil, err
	}

	return profiles, nil
}

func (r *Repository) LockExpiredGraceProfiles(ctx context.Context, now time.Time, limit int) ([]RecurringProfile, error) {
	rows, err := r.pool.Query(ctx, `
		WITH picked AS (
			SELECT telegram_id
			FROM billing_recurring_profiles
			WHERE (
				status = 'grace'
				AND grace_until IS NOT NULL
				AND grace_until <= $1
			) OR (
				status = 'expiring'
				AND locked_at IS NOT NULL
				AND locked_at < $1 - INTERVAL '30 minutes'
			)
			ORDER BY grace_until ASC NULLS FIRST
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE billing_recurring_profiles p
		SET status = 'expiring',
			locked_at = now(),
			updated_at = now()
		FROM picked
		WHERE p.telegram_id = picked.telegram_id
		RETURNING
			p.telegram_id, p.plan_code, p.duration_days, p.amount_value::text, p.currency,
			p.payment_method_id, p.auto_renew_enabled, p.status, p.next_charge_at,
			p.retry_count, p.grace_until, COALESCE(p.last_payment_id, ''), COALESCE(p.last_failure_reason, ''), p.updated_at
	`, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim expired grace profiles: %w", err)
	}
	defer rows.Close()

	profiles, err := scanRecurringProfiles(rows)
	if err != nil {
		return nil, err
	}

	return profiles, nil
}

func scanRecurringProfiles(rows pgx.Rows) ([]RecurringProfile, error) {
	profiles := make([]RecurringProfile, 0)
	for rows.Next() {
		var p RecurringProfile
		var planCode string
		var nextChargeAt sql.NullTime
		var graceUntil sql.NullTime

		if err := rows.Scan(
			&p.TelegramID,
			&planCode,
			&p.DurationDays,
			&p.AmountValue,
			&p.Currency,
			&p.PaymentMethodID,
			&p.AutoRenewEnabled,
			&p.Status,
			&nextChargeAt,
			&p.RetryCount,
			&graceUntil,
			&p.LastPaymentID,
			&p.LastFailureReason,
			&p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan recurring profile: %w", err)
		}

		p.PlanCode = kafkacontracts.PlanCode(planCode)
		if nextChargeAt.Valid {
			t := nextChargeAt.Time.UTC()
			p.NextChargeAt = &t
		}
		if graceUntil.Valid {
			t := graceUntil.Time.UTC()
			p.GraceUntil = &t
		}

		profiles = append(profiles, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recurring profiles: %w", err)
	}
	return profiles, nil
}

type PendingRefund struct {
	PaymentID     string
	AmountValue   string
	Currency      string
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
}

func (r *Repository) RecordWebhookFingerprint(ctx context.Context, paymentID, eventType, fingerprint string, raw json.RawMessage) (bool, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		raw = json.RawMessage(`{}`)
	}
	ct, err := r.pool.Exec(ctx, `
		INSERT INTO billing_webhook_events (event_type, payment_id, fingerprint, raw_payload)
		VALUES ($1,$2,$3,$4::jsonb)
		ON CONFLICT (provider, fingerprint) DO NOTHING
	`, eventType, paymentID, fingerprint, string(raw))
	if err != nil {
		return false, fmt.Errorf("record webhook fingerprint: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

func (r *Repository) LockDueRefunds(ctx context.Context, now time.Time, limit int) ([]PendingRefund, error) {
	rows, err := r.pool.Query(ctx, `
		WITH picked AS (
			SELECT payment_id
			FROM billing_pending_refunds
			WHERE attempts < 10
			  AND next_attempt_at <= $1
			ORDER BY next_attempt_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE billing_pending_refunds r
		SET attempts = attempts + 1,
			updated_at = now()
		FROM picked
		WHERE r.payment_id = picked.payment_id
		RETURNING r.payment_id, r.amount_value, r.currency, r.attempts, r.next_attempt_at, r.last_error
	`, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("lock due refunds: %w", err)
	}
	defer rows.Close()

	out := make([]PendingRefund, 0)
	for rows.Next() {
		var p PendingRefund
		if err := rows.Scan(&p.PaymentID, &p.AmountValue, &p.Currency, &p.Attempts, &p.NextAttemptAt, &p.LastError); err != nil {
			return nil, fmt.Errorf("scan pending refund: %w", err)
		}
		out = append(out, p)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Repository) MarkRefundSucceeded(ctx context.Context, paymentID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM billing_pending_refunds WHERE payment_id=$1`, paymentID)
	return err
}

func (r *Repository) MarkRefundRetry(ctx context.Context, paymentID, errText string, attempts int) error {
	delayMinutes := 1
	if attempts > 0 {
		delayMinutes = 1 << minInt(attempts, 8)
	}
	_, err := r.pool.Exec(ctx, `
		UPDATE billing_pending_refunds
		SET last_error=$2,
			next_attempt_at = now() + ($3 || ' minutes')::interval,
			updated_at=now()
		WHERE payment_id=$1
	`, paymentID, errText, delayMinutes)
	return err
}

func (r *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

func (r *Repository) SaveCustomerBindingTx(ctx context.Context, tx pgx.Tx, telegramID int64, paymentMethodID, methodType, last4, expiryMonth, expiryYear string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO billing_customers (
			telegram_id, payment_method_id, method_type, card_last4, card_expiry_month, card_expiry_year, bound_at
		) VALUES ($1,$2,$3,$4,$5,$6,now())
		ON CONFLICT (telegram_id) DO UPDATE SET
			payment_method_id = EXCLUDED.payment_method_id,
			method_type = EXCLUDED.method_type,
			card_last4 = EXCLUDED.card_last4,
			card_expiry_month = EXCLUDED.card_expiry_month,
			card_expiry_year = EXCLUDED.card_expiry_year,
			bound_at = now(),
			updated_at = now()
	`, telegramID, paymentMethodID, methodType, last4, expiryMonth, expiryYear)
	if err != nil {
		return fmt.Errorf("save customer binding tx: %w", err)
	}
	return nil
}

func (r *Repository) ClearCustomerBindingTx(ctx context.Context, tx pgx.Tx, telegramID int64) error {
	_, err := tx.Exec(ctx, `DELETE FROM billing_customers WHERE telegram_id = $1`, telegramID)
	if err != nil {
		return fmt.Errorf("clear customer binding tx: %w", err)
	}
	return nil
}

func (r *Repository) UpsertRecurringProfileTx(ctx context.Context, tx pgx.Tx, p *RecurringProfile) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO billing_recurring_profiles (
			telegram_id, plan_code, duration_days, amount_value, currency,
			payment_method_id, auto_renew_enabled, status, next_charge_at,
			retry_count, grace_until, last_payment_id, last_failure_reason
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,NULL,$10,'')
		ON CONFLICT (telegram_id) DO UPDATE SET
			plan_code = EXCLUDED.plan_code,
			duration_days = EXCLUDED.duration_days,
			amount_value = EXCLUDED.amount_value,
			currency = EXCLUDED.currency,
			payment_method_id = EXCLUDED.payment_method_id,
			auto_renew_enabled = EXCLUDED.auto_renew_enabled,
			status = EXCLUDED.status,
			next_charge_at = EXCLUDED.next_charge_at,
			retry_count = 0,
			grace_until = NULL,
			last_payment_id = EXCLUDED.last_payment_id,
			last_failure_reason = '',
			updated_at = now()
	`,
		p.TelegramID, string(p.PlanCode), p.DurationDays, p.AmountValue, p.Currency,
		p.PaymentMethodID, p.AutoRenewEnabled, p.Status, p.NextChargeAt, p.LastPaymentID,
	)
	if err != nil {
		return fmt.Errorf("upsert recurring profile tx: %w", err)
	}
	return nil
}

func (r *Repository) DisableAutoRenewTx(ctx context.Context, tx pgx.Tx, telegramID int64, reason string) error {
	_, err := tx.Exec(ctx, `
		UPDATE billing_recurring_profiles
		SET auto_renew_enabled = false,
			locked_at = NULL,
			status = 'disabled',
			last_failure_reason = $2,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, reason)
	if err != nil {
		return fmt.Errorf("disable auto-renew tx: %w", err)
	}
	return nil
}

func (r *Repository) ScheduleRenewalRetryTx(ctx context.Context, tx pgx.Tx, telegramID int64, nextRetryAt time.Time, retryCount int, reason, paymentID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE billing_recurring_profiles
		SET status = 'retry',
			locked_at = NULL,
			next_charge_at = $2,
			retry_count = $3,
			last_failure_reason = $4,
			last_payment_id = COALESCE(NULLIF($5, ''), last_payment_id),
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, nextRetryAt.UTC(), retryCount, reason, paymentID)
	if err != nil {
		return fmt.Errorf("schedule renewal retry tx: %w", err)
	}
	return nil
}

func (r *Repository) StartGracePeriodTx(ctx context.Context, tx pgx.Tx, telegramID int64, graceUntil time.Time, reason, paymentID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE billing_recurring_profiles
		SET status = 'grace',
			locked_at = NULL,
			grace_until = $2,
			next_charge_at = NULL,
			last_failure_reason = $3,
			last_payment_id = COALESCE(NULLIF($4, ''), last_payment_id),
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, graceUntil.UTC(), reason, paymentID)
	if err != nil {
		return fmt.Errorf("start grace period tx: %w", err)
	}
	return nil
}

func (r *Repository) MarkRecurringSuccessTx(ctx context.Context, tx pgx.Tx, telegramID int64, paymentID string, fallbackNextChargeAt time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE billing_recurring_profiles
		SET status = 'active',
			locked_at = NULL,
			retry_count = 0,
			grace_until = NULL,
			last_payment_id = $2,
			last_failure_reason = '',
			next_charge_at = $3,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, paymentID, fallbackNextChargeAt.UTC())
	if err != nil {
		return fmt.Errorf("mark recurring success tx: %w", err)
	}
	return nil
}

func (r *Repository) SuspendExpiredGraceTx(ctx context.Context, tx pgx.Tx, telegramID int64, reason string) error {
	_, err := tx.Exec(ctx, `
		UPDATE billing_recurring_profiles
		SET auto_renew_enabled = false,
			locked_at = NULL,
			status = 'expired',
			next_charge_at = NULL,
			last_failure_reason = $2,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, reason)
	if err != nil {
		return fmt.Errorf("suspend expired grace tx: %w", err)
	}
	return nil
}

func (r *Repository) AddPendingRefundTx(ctx context.Context, tx pgx.Tx, paymentID, amountValue, currency, errText string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO billing_pending_refunds (payment_id, amount_value, currency, last_error)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (payment_id) DO UPDATE SET
			amount_value = EXCLUDED.amount_value,
			currency = EXCLUDED.currency,
			last_error = EXCLUDED.last_error,
			next_attempt_at = LEAST(billing_pending_refunds.next_attempt_at, now()),
			updated_at = now()
	`, paymentID, amountValue, currency, errText)
	if err != nil {
		return fmt.Errorf("add pending refund tx: %w", err)
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
