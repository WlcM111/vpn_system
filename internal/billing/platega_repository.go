package billing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"vpn-platform/internal/billing/platega"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5"
)

// ============================================================================
// Хранение заказов Platega — в той же таблице payments (provider='platega').
// Все изменения статуса заказа выполняются в транзакциях вместе с записью
// событий и уведомлений в outbox.
// ============================================================================

const providerPlatega = string(kafkacontracts.BillingPaymentProviderPlatega)

// plategaOrder — строка payments заказа Platega.
type plategaOrder struct {
	OrderID         string
	TelegramID      int64
	CheckoutType    string
	PlanCode        string
	DurationDays    int
	PaymentID       string
	Status          platega.LocalStatus
	ProviderStatus  platega.Status
	ProviderPayload string
	AmountValue     string
	Currency        string
	Description     string
	ConfirmationURL string
	ExpiresAt       *time.Time
	PaidAt          *time.Time
	CheckAttempts   int
	Metadata        map[string]string
	CreatedAt       time.Time
}

const plategaOrderColumns = `order_id, telegram_id, checkout_type, plan_code, duration_days,
	payment_id, status, provider_status, provider_payload, amount_value::text, currency,
	description, confirmation_url, expires_at, paid_at, check_attempts, metadata, created_at`

func scanPlategaOrder(row pgx.Row) (*plategaOrder, error) {
	var (
		o        plategaOrder
		status   string
		provider string
		meta     []byte
	)
	if err := row.Scan(
		&o.OrderID, &o.TelegramID, &o.CheckoutType, &o.PlanCode, &o.DurationDays,
		&o.PaymentID, &status, &provider, &o.ProviderPayload, &o.AmountValue, &o.Currency,
		&o.Description, &o.ConfirmationURL, &o.ExpiresAt, &o.PaidAt, &o.CheckAttempts, &meta, &o.CreatedAt,
	); err != nil {
		return nil, err
	}
	o.Status = platega.LocalStatus(status)
	o.ProviderStatus = platega.Status(provider)
	if len(meta) > 0 {
		_ = json.Unmarshal(meta, &o.Metadata)
	}
	return &o, nil
}

// snapshot переводит строку в вход машины состояний.
func (o *plategaOrder) snapshot() (platega.Snapshot, error) {
	amount, err := platega.ParseDecimalMinor(o.AmountValue)
	if err != nil {
		return platega.Snapshot{}, fmt.Errorf("order %s: %w", o.OrderID, err)
	}
	expected := o.ProviderPayload
	if expected == "" {
		expected = o.OrderID
	}
	return platega.Snapshot{
		OrderID:         o.OrderID,
		ExpectedPayload: expected,
		PaymentID:       o.PaymentID,
		Status:          o.Status,
		ProviderStatus:  o.ProviderStatus,
		AmountMinor:     amount,
		Currency:        o.Currency,
		Granted:         o.PaidAt != nil,
		LinkSent:        o.ConfirmationURL != "",
		CreatedAt:       o.CreatedAt,
	}, nil
}

// pgInterval — длительность в виде литерала interval PostgreSQL.
func pgInterval(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10) + " milliseconds"
}

type reserveOutcome int

const (
	// reserveNew — заказ создан (или это повтор той же команды до обращения к
	// Platega): можно создавать транзакцию.
	reserveNew reserveOutcome = iota
	// reserveRedelivered — повтор команды, заказ уже продвинулся дальше: его
	// переходы сами уведомили пользователя, делать ничего не нужно.
	reserveRedelivered
	// reserveReused — у пользователя есть действующая ссылка на тот же тариф.
	reserveReused
	// reserveInFlight — другой счёт этого пользователя прямо сейчас создаётся.
	reserveInFlight
)

type reserveResult struct {
	Outcome  reserveOutcome
	Existing *plategaOrder
}

// ReservePlategaCheckout атомарно (под advisory-lock на пользователя)
// решает, создавать ли новый счёт, и при необходимости вставляет заказ.
func (r *Repository) ReservePlategaCheckout(ctx context.Context, rec *PaymentRecord, reuseMinRemaining time.Duration) (*reserveResult, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended('billing:platega-checkout:' || $1::bigint::text, 0))`,
		rec.TelegramID); err != nil {
		return nil, fmt.Errorf("platega checkout lock: %w", err)
	}

	var existingStatus, existingProvider string
	err = tx.QueryRow(ctx, `SELECT status, provider FROM payments WHERE order_id = $1`, rec.OrderID).
		Scan(&existingStatus, &existingProvider)
	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		if existingProvider == providerPlatega && existingStatus == string(platega.LocalCreating) {
			return &reserveResult{Outcome: reserveNew}, nil
		}
		return &reserveResult{Outcome: reserveRedelivered}, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("platega checkout lookup: %w", err)
	}

	// Один запрос — один снимок данных. Если проверять «создаётся ли счёт» и
	// «есть ли готовая ссылка» двумя запросами, счёт может перейти из
	// provider_creating в pending между ними и не попасть ни под одно условие
	// (так находил гонку интеграционный тест с параллельными нажатиями).
	existing, err := scanPlategaOrder(tx.QueryRow(ctx, `
		SELECT `+plategaOrderColumns+`
		FROM payments
		WHERE telegram_id = $1
		  AND provider = 'platega'
		  AND (
		        status IN ('local_creating', 'provider_creating')
		     OR (status = 'pending'
		         AND checkout_type = $2
		         AND confirmation_url <> ''
		         AND plan_code = $3
		         AND amount_value = $4::numeric
		         AND expires_at > now() + $5::interval)
		  )
		ORDER BY (status IN ('local_creating', 'provider_creating')) DESC, created_at DESC
		LIMIT 1
	`, rec.TelegramID, rec.CheckoutType, rec.PlanCode, rec.AmountValue, pgInterval(reuseMinRemaining)))
	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		if existing.Status == platega.LocalPending {
			return &reserveResult{Outcome: reserveReused, Existing: existing}, nil
		}
		return &reserveResult{Outcome: reserveInFlight, Existing: existing}, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("platega checkout lookup: %w", err)
	}

	meta, err := json.Marshal(rec.Metadata)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO payments (
			order_id, command_id, telegram_id, checkout_type, plan_code, duration_days,
			payment_id, status, amount_value, currency, description,
			confirmation_url, idempotence_key, save_payment_method, metadata,
			provider, provider_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			'', 'local_creating', $7::numeric, $8, $9,
			'', $10, false, $11::jsonb,
			'platega', $1
		)
	`, rec.OrderID, rec.CommandID, rec.TelegramID, rec.CheckoutType, rec.PlanCode, rec.DurationDays,
		rec.AmountValue, rec.Currency, rec.Description, rec.IdempotenceKey, string(meta)); err != nil {
		return nil, fmt.Errorf("insert platega order: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &reserveResult{Outcome: reserveNew}, nil
}

// MarkPlategaProviderCreating — отметка «запрос создания отправляется».
// Делается и фиксируется ДО HTTP-вызова: если процесс упадёт посреди
// запроса, повтор команды увидит этот статус и не создаст второй счёт.
func (r *Repository) MarkPlategaProviderCreating(ctx context.Context, orderID string) (bool, error) {
	ct, err := r.pool.Exec(ctx, `
		UPDATE payments
		SET status = 'provider_creating', updated_at = now()
		WHERE order_id = $1 AND provider = 'platega' AND status = 'local_creating'
	`, orderID)
	if err != nil {
		return false, fmt.Errorf("mark platega provider_creating: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// LockPlategaOrderTx читает заказ под блокировкой строки.
func (r *Repository) LockPlategaOrderTx(ctx context.Context, tx pgx.Tx, orderID string) (*plategaOrder, error) {
	o, err := scanPlategaOrder(tx.QueryRow(ctx, `
		SELECT `+plategaOrderColumns+`
		FROM payments
		WHERE order_id = $1 AND provider = 'platega'
		FOR UPDATE
	`, orderID))
	if err != nil {
		return nil, fmt.Errorf("lock platega order %s: %w", orderID, err)
	}
	return o, nil
}

// FindPlategaOrderByPaymentIDTx — заказ по ID транзакции Platega.
func (r *Repository) FindPlategaOrderByPaymentIDTx(ctx context.Context, tx pgx.Tx, paymentID string) (*plategaOrder, error) {
	return scanPlategaOrder(tx.QueryRow(ctx, `
		SELECT `+plategaOrderColumns+`
		FROM payments
		WHERE payment_id = $1 AND provider = 'platega'
		FOR UPDATE
	`, paymentID))
}

// FindPlategaOrderByPayloadTx — исходный заказ по payload колбэка
// (payload = order_id исходного заказа).
func (r *Repository) FindPlategaOrderByPayloadTx(ctx context.Context, tx pgx.Tx, payload string) (*plategaOrder, error) {
	return scanPlategaOrder(tx.QueryRow(ctx, `
		SELECT `+plategaOrderColumns+`
		FROM payments
		WHERE order_id = $1 AND provider = 'platega'
		FOR UPDATE
	`, payload))
}

// SetPlategaCreatedTx записывает результат успешного создания транзакции.
// sendLink=true — ссылка будет отправлена пользователю в этой же транзакции.
func (r *Repository) SetPlategaCreatedTx(ctx context.Context, tx pgx.Tx, orderID string, status platega.LocalStatus, paymentID, confirmationURL string, providerStatus platega.Status, expiresAt time.Time, raw json.RawMessage, nextCheck time.Duration) error {
	rawStr := "{}"
	if len(raw) > 0 && json.Valid(raw) {
		rawStr = string(raw)
	}
	_, err := tx.Exec(ctx, `
		UPDATE payments
		SET status = $2,
			payment_id = $3,
			confirmation_url = $4,
			provider_status = $5,
			expires_at = $6,
			raw_response = $7::jsonb,
			next_check_at = now() + $8::interval,
			check_attempts = 0,
			last_error = '',
			updated_at = now()
		WHERE order_id = $1 AND provider = 'platega'
	`, orderID, string(status), paymentID, confirmationURL, string(providerStatus), expiresAt.UTC(), rawStr, pgInterval(nextCheck))
	if err != nil {
		return fmt.Errorf("set platega created: %w", err)
	}
	return nil
}

// SetPlategaCreateErrorTx фиксирует неудачу создания (create_failed или
// create_unknown), только если заказ всё ещё в provider_creating.
func (r *Repository) SetPlategaCreateErrorTx(ctx context.Context, tx pgx.Tx, orderID string, status platega.LocalStatus, errText string) (bool, error) {
	ct, err := tx.Exec(ctx, `
		UPDATE payments
		SET status = $2, last_error = $3, updated_at = now()
		WHERE order_id = $1 AND provider = 'platega' AND status = 'provider_creating'
	`, orderID, string(status), truncateText(errText, 500))
	if err != nil {
		return false, fmt.Errorf("set platega create error: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// staleOrder — заказ, переведённый из зависшего статуса создания.
type staleOrder struct {
	OrderID         string
	TelegramID      int64
	ConfirmationURL string
}

// SweepStalePlategaCreatingTx переводит заказы, зависшие в статусе from
// дольше olderThan, в статус to. SKIP LOCKED — параллельные проходы не
// конкурируют, а заказ, который прямо сейчас дописывает обработчик, не
// трогается.
func (r *Repository) SweepStalePlategaCreatingTx(ctx context.Context, tx pgx.Tx, from, to platega.LocalStatus, olderThan time.Duration, reason string) ([]staleOrder, error) {
	rows, err := tx.Query(ctx, `
		WITH stale AS (
			SELECT order_id
			FROM payments
			WHERE provider = 'platega'
			  AND status = $1
			  AND updated_at < now() - $2::interval
			ORDER BY updated_at
			LIMIT 100
			FOR UPDATE SKIP LOCKED
		)
		UPDATE payments p
		SET status = $3, last_error = $4, updated_at = now()
		FROM stale
		WHERE p.order_id = stale.order_id
		RETURNING p.order_id, p.telegram_id, p.confirmation_url
	`, string(from), pgInterval(olderThan), string(to), reason)
	if err != nil {
		return nil, fmt.Errorf("sweep stale platega %s: %w", from, err)
	}
	defer rows.Close()
	var out []staleOrder
	for rows.Next() {
		var s staleOrder
		if err := rows.Scan(&s.OrderID, &s.TelegramID, &s.ConfirmationURL); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// dueCheck — заказ, захваченный для сверки статуса.
type dueCheck struct {
	OrderID   string
	PaymentID string
	Attempts  int
}

// ClaimDuePlategaChecks захватывает заказы, которым пора сверить статус.
// Захват — это сдвиг next_check_at на время аренды и учёт попытки: если
// обработчик упадёт, заказ вернётся в очередь после истечения аренды.
func (r *Repository) ClaimDuePlategaChecks(ctx context.Context, limit int, lease time.Duration) ([]dueCheck, error) {
	rows, err := r.pool.Query(ctx, `
		WITH due AS (
			SELECT order_id
			FROM payments
			WHERE next_check_at IS NOT NULL
			  AND next_check_at <= now()
			  AND provider = 'platega'
			  AND payment_id <> ''
			ORDER BY next_check_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE payments p
		SET next_check_at = now() + $2::interval,
			check_attempts = p.check_attempts + 1,
			updated_at = now()
		FROM due
		WHERE p.order_id = due.order_id
		RETURNING p.order_id, p.payment_id, p.check_attempts
	`, limit, pgInterval(lease))
	if err != nil {
		return nil, fmt.Errorf("claim due platega checks: %w", err)
	}
	defer rows.Close()
	var out []dueCheck
	for rows.Next() {
		var d dueCheck
		if err := rows.Scan(&d.OrderID, &d.PaymentID, &d.Attempts); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ClaimPlategaForCheckTx ставит заказ на немедленную проверку с арендой.
func (r *Repository) ClaimPlategaForCheckTx(ctx context.Context, tx pgx.Tx, orderID string, lease time.Duration) error {
	_, err := tx.Exec(ctx, `
		UPDATE payments
		SET next_check_at = now() + $2::interval, updated_at = now()
		WHERE order_id = $1 AND provider = 'platega' AND payment_id <> ''
	`, orderID, pgInterval(lease))
	if err != nil {
		return fmt.Errorf("claim platega for check: %w", err)
	}
	return nil
}

// AttachPlategaTransactionTx связывает заказ без сохранённого ID транзакции
// с транзакцией из колбэка (ответ на создание потерялся или ещё не записан).
func (r *Repository) AttachPlategaTransactionTx(ctx context.Context, tx pgx.Tx, orderID, paymentID string) (bool, error) {
	ct, err := tx.Exec(ctx, `
		UPDATE payments
		SET payment_id = $2, updated_at = now()
		WHERE order_id = $1 AND provider = 'platega' AND payment_id = ''
	`, orderID, paymentID)
	if err != nil {
		return false, fmt.Errorf("attach platega transaction: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// InsertPlategaDuplicateTx заводит отдельную строку для второй транзакции
// Platega с payload того же заказа. Политика: каждая подтверждённая
// транзакция с суммой заказа — отдельная покупка; деньги не теряются, а
// подписка не выдаётся дважды за одну транзакцию.
func (r *Repository) InsertPlategaDuplicateTx(ctx context.Context, tx pgx.Tx, orig *plategaOrder, paymentID string) (string, error) {
	orderID := orig.OrderID + ":tx:" + paymentID
	meta := map[string]string{}
	for k, v := range orig.Metadata {
		meta[k] = v
	}
	meta["order_id"] = orderID
	meta["duplicate_of"] = orig.OrderID
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	payload := orig.ProviderPayload
	if payload == "" {
		payload = orig.OrderID
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO payments (
			order_id, command_id, telegram_id, checkout_type, plan_code, duration_days,
			payment_id, status, amount_value, currency, description,
			confirmation_url, idempotence_key, save_payment_method, metadata,
			provider, provider_payload, next_check_at
		) VALUES (
			$1, '', $2, $3, $4, $5,
			$6, 'pending', $7::numeric, $8, $9,
			'', $10, false, $11::jsonb,
			'platega', $12, now()
		)
		ON CONFLICT (order_id) DO NOTHING
	`, orderID, orig.TelegramID, orig.CheckoutType, orig.PlanCode, orig.DurationDays,
		paymentID, orig.AmountValue, orig.Currency, orig.Description,
		"platega-dup:"+paymentID, string(metaRaw), payload)
	if err != nil {
		return "", fmt.Errorf("insert platega duplicate: %w", err)
	}
	return orderID, nil
}

// ApplyPlategaDecisionTx записывает решение машины состояний.
func (r *Repository) ApplyPlategaDecisionTx(ctx context.Context, tx pgx.Tx, orderID string, d platega.Decision) error {
	var next any
	if d.NextCheckIn > 0 {
		next = pgInterval(d.NextCheckIn)
	}
	_, err := tx.Exec(ctx, `
		UPDATE payments
		SET status = $2,
			provider_status = $3,
			paid_at = CASE WHEN $4 THEN COALESCE(paid_at, now()) ELSE paid_at END,
			next_check_at = CASE WHEN $5::text IS NULL THEN NULL ELSE now() + ($5::text)::interval END,
			check_attempts = 0,
			last_error = '',
			review_reason = CASE WHEN $2 = 'review_required' THEN $6 ELSE review_reason END,
			updated_at = now()
		WHERE order_id = $1 AND provider = 'platega'
	`, orderID, string(d.Status), string(d.ProviderStatus), d.Grant, next, d.Reason)
	if err != nil {
		return fmt.Errorf("apply platega decision: %w", err)
	}
	return nil
}

// MarkPlategaCheckError переносит проверку после ошибки обращения к API.
func (r *Repository) MarkPlategaCheckError(ctx context.Context, orderID string, delay time.Duration, errText string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE payments
		SET next_check_at = now() + $2::interval, last_error = $3, updated_at = now()
		WHERE order_id = $1 AND provider = 'platega' AND next_check_at IS NOT NULL
	`, orderID, pgInterval(delay), truncateText(errText, 500))
	if err != nil {
		return fmt.Errorf("mark platega check error: %w", err)
	}
	return nil
}

// MarkPlategaReviewTx переводит заказ в ручную проверку (без решения машины
// состояний — например, транзакция пропала у провайдера).
func (r *Repository) MarkPlategaReviewTx(ctx context.Context, tx pgx.Tx, orderID, reason string) error {
	_, err := tx.Exec(ctx, `
		UPDATE payments
		SET status = 'review_required', review_reason = $2, next_check_at = NULL, updated_at = now()
		WHERE order_id = $1 AND provider = 'platega'
	`, orderID, reason)
	if err != nil {
		return fmt.Errorf("mark platega review: %w", err)
	}
	return nil
}

// RecordPlategaCallbackTx — журнал принятых колбэков (аудит). Дубли
// отбрасываются по отпечатку; обработка от этого не зависит.
func (r *Repository) RecordPlategaCallbackTx(ctx context.Context, tx pgx.Tx, paymentID, eventType, fingerprint string, sanitized json.RawMessage) error {
	if len(sanitized) == 0 || !json.Valid(sanitized) {
		sanitized = json.RawMessage(`{}`)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO billing_webhook_events (provider, event_type, payment_id, fingerprint, raw_payload)
		VALUES ('platega', $1, $2, $3, $4::jsonb)
		ON CONFLICT (provider, fingerprint) DO NOTHING
	`, eventType, paymentID, fingerprint, string(sanitized))
	if err != nil {
		return fmt.Errorf("record platega callback: %w", err)
	}
	return nil
}

// HasNewerSubscriptionOrderTx — есть ли у пользователя заказ подписки новее
// данного (тогда об отмене старого счёта сообщать не нужно).
func (r *Repository) HasNewerSubscriptionOrderTx(ctx context.Context, tx pgx.Tx, telegramID int64, orderID string, createdAt time.Time) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM payments
			WHERE telegram_id = $1
			  AND checkout_type = $2
			  AND order_id <> $3
			  AND created_at > $4
		)
	`, telegramID, string(kafkacontracts.BillingCheckoutTypeSubscription), orderID, createdAt).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check newer orders: %w", err)
	}
	return exists, nil
}

// CountPlategaByStatus — срез заказов Platega по статусу для метрик.
func (r *Repository) CountPlategaByStatus(ctx context.Context) (map[string]float64, float64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT status, count(*) FROM payments WHERE provider = 'platega' GROUP BY status
	`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var s string
		var n float64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, 0, err
		}
		out[s] = n
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var overdue float64
	if err := r.pool.QueryRow(ctx, `
		SELECT count(*) FROM payments
		WHERE provider = 'platega'
		  AND next_check_at IS NOT NULL
		  AND next_check_at < now() - interval '10 minutes'
	`).Scan(&overdue); err != nil {
		return nil, 0, err
	}
	return out, overdue, nil
}

// FindReusableYooKassaPayment — неоплаченный платёж YooKassa на тот же тариф,
// созданный недавно: повторное нажатие отдаёт его ссылку, а не плодит счета.
func (r *Repository) FindReusableYooKassaPayment(ctx context.Context, telegramID int64, planCode, amountValue string, window time.Duration) (string, string, error) {
	var orderID, url string
	err := r.pool.QueryRow(ctx, `
		SELECT order_id, confirmation_url
		FROM payments
		WHERE telegram_id = $1
		  AND provider = 'yookassa'
		  AND checkout_type = $2
		  AND status = 'pending'
		  AND confirmation_url <> ''
		  AND plan_code = $3
		  AND amount_value = $4::numeric
		  AND created_at > now() - $5::interval
		  AND COALESCE(metadata->>'charge_source', 'initial') = 'initial'
		ORDER BY created_at DESC
		LIMIT 1
	`, telegramID, string(kafkacontracts.BillingCheckoutTypeSubscription), planCode, amountValue, pgInterval(window)).Scan(&orderID, &url)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("find reusable yookassa payment: %w", err)
	}
	return orderID, url, nil
}

// MarkYooKassaCreateErrorTx фиксирует неудачу создания платежа YooKassa
// (раньше строка навсегда оставалась в local_creating).
func (r *Repository) MarkYooKassaCreateErrorTx(ctx context.Context, tx pgx.Tx, orderID, status, errText string) error {
	_, err := tx.Exec(ctx, `
		UPDATE payments
		SET status = $2, last_error = $3, updated_at = now()
		WHERE order_id = $1 AND provider = 'yookassa' AND status = 'local_creating'
	`, orderID, status, truncateText(errText, 500))
	if err != nil {
		return fmt.Errorf("mark yookassa create error: %w", err)
	}
	return nil
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Не режем посреди UTF-8 символа.
	for n > 0 && n < len(s) && (s[n]&0xC0) == 0x80 {
		n--
	}
	return s[:n]
}
