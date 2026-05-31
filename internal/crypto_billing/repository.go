package crypto_billing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository инкапсулирует доступ к таблицам crypto_invoices и crypto_webhook_events.
// Pool() экспонируется потому, что outbox-worker (RunPublisher) принимает *pgxpool.Pool
// напрямую — это решение всего проекта, не специфика этого сервиса.
type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// CryptoInvoice — доменная модель инвойса. Соответствует структуре таблицы crypto_invoices.
type CryptoInvoice struct {
	OrderID      string
	CommandID    string
	TelegramID   int64
	PlanCode     string
	DurationDays int
	Asset        string
	AmountValue  string
	Description  string
	InvoiceID    string
	PayURL       string
	Status       string
	ExpiresAt    *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	PaidAt       *time.Time
}

// InsertInvoiceTx создаёт запись инвойса в статусе 'creating'.
// ON CONFLICT (order_id) DO NOTHING обеспечивает идемпотентность на случай, если эта
// транзакция выполняется повторно по одному и тому же order_id (не должно случаться,
// но защититься дёшево).
// Возвращает true, если запись была вставлена; false — если такой order_id уже есть.
func (r *Repository) InsertInvoiceTx(ctx context.Context, tx pgx.Tx, inv *CryptoInvoice) (bool, error) {
	ct, err := tx.Exec(ctx, `
		INSERT INTO crypto_invoices (
			order_id, command_id, telegram_id,
			plan_code, duration_days,
			asset, amount_value, description,
			status, expires_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (order_id) DO NOTHING
	`,
		inv.OrderID, inv.CommandID, inv.TelegramID,
		inv.PlanCode, inv.DurationDays,
		inv.Asset, inv.AmountValue, inv.Description,
		"creating", inv.ExpiresAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert crypto invoice tx: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// MarkInvoiceActiveTx переводит инвойс из 'creating' в 'active' и записывает
// invoice_id + pay_url, полученные от CryptoBot. raw_create_response сохраняет
// весь ответ для будущего аудита/отладки.
func (r *Repository) MarkInvoiceActiveTx(ctx context.Context, tx pgx.Tx, orderID, invoiceID, payURL string, rawCreate json.RawMessage) error {
	raw := "{}"
	if len(rawCreate) > 0 && json.Valid(rawCreate) {
		raw = string(rawCreate)
	}
	_, err := tx.Exec(ctx, `
		UPDATE crypto_invoices
		SET invoice_id = $2,
		    pay_url = $3,
		    status = 'active',
		    raw_create_response = $4::jsonb,
		    updated_at = now()
		WHERE order_id = $1
	`, orderID, invoiceID, payURL, raw)
	if err != nil {
		return fmt.Errorf("mark crypto invoice active tx: %w", err)
	}
	return nil
}

// MarkInvoiceFailedTx — если CryptoBot.createInvoice упал, фиксируем причину.
// Это полезно для оператора (видно в БД), и для пользователя — мы шлём TG-нотификацию
// в той же транзакции (см. service.go).
func (r *Repository) MarkInvoiceFailedTx(ctx context.Context, tx pgx.Tx, orderID, errText string) error {
	_, err := tx.Exec(ctx, `
		UPDATE crypto_invoices
		SET status = 'failed',
		    raw_create_response = jsonb_build_object('error', $2::text),
		    updated_at = now()
		WHERE order_id = $1
	`, orderID, errText)
	if err != nil {
		return fmt.Errorf("mark crypto invoice failed tx: %w", err)
	}
	return nil
}

// GetInvoiceByInvoiceIDTx находит инвойс по invoice_id из CryptoBot — это то, что
// нам приходит в webhook. FOR UPDATE блокирует строку до конца транзакции, чтобы
// при параллельных вебхуках одного и того же инвойса (теоретически возможно при
// ретраях CryptoBot) обработчики не наступали друг другу на пятки.
func (r *Repository) GetInvoiceByInvoiceIDTx(ctx context.Context, tx pgx.Tx, invoiceID string) (*CryptoInvoice, error) {
	row := tx.QueryRow(ctx, `
		SELECT order_id, command_id, telegram_id, plan_code, duration_days,
		       asset, amount_value, description,
		       invoice_id, pay_url, status, expires_at, created_at, updated_at, paid_at
		FROM crypto_invoices
		WHERE invoice_id = $1
		FOR UPDATE
	`, invoiceID)

	var inv CryptoInvoice
	var expiresAt, paidAt *time.Time
	if err := row.Scan(
		&inv.OrderID, &inv.CommandID, &inv.TelegramID, &inv.PlanCode, &inv.DurationDays,
		&inv.Asset, &inv.AmountValue, &inv.Description,
		&inv.InvoiceID, &inv.PayURL, &inv.Status, &expiresAt, &inv.CreatedAt, &inv.UpdatedAt, &paidAt,
	); err != nil {
		return nil, fmt.Errorf("get crypto invoice tx: %w", err)
	}
	inv.ExpiresAt = expiresAt
	inv.PaidAt = paidAt
	return &inv, nil
}

// MarkInvoicePaidTx переводит инвойс в 'paid'. Защищён условием status <> 'paid',
// поэтому второй вызов вернёт RowsAffected=0 — это сигнал "уже обработано, не публикуй
// событие повторно". Так мы получаем идемпотентность даже на уровне SQL.
func (r *Repository) MarkInvoicePaidTx(ctx context.Context, tx pgx.Tx, orderID string, rawWebhook json.RawMessage) (bool, error) {
	raw := "{}"
	if len(rawWebhook) > 0 && json.Valid(rawWebhook) {
		raw = string(rawWebhook)
	}
	ct, err := tx.Exec(ctx, `
		UPDATE crypto_invoices
		SET status = 'paid',
		    paid_at = now(),
		    raw_last_webhook = $2::jsonb,
		    updated_at = now()
		WHERE order_id = $1
		  AND status <> 'paid'
	`, orderID, raw)
	if err != nil {
		return false, fmt.Errorf("mark crypto invoice paid tx: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// RecordWebhookFingerprintTx дедуплицирует webhook по sha256 от raw body.
// Возвращает true, если запись была вставлена (новый webhook); false — если такой
// fingerprint уже был.
func (r *Repository) RecordWebhookFingerprintTx(ctx context.Context, tx pgx.Tx, updateType, invoiceID, fingerprint string, raw json.RawMessage) (bool, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		raw = json.RawMessage(`{}`)
	}
	ct, err := tx.Exec(ctx, `
		INSERT INTO crypto_webhook_events (provider, update_type, invoice_id, fingerprint, raw_payload)
		VALUES ('cryptobot', $1, $2, $3, $4::jsonb)
		ON CONFLICT (provider, fingerprint) DO NOTHING
	`, updateType, invoiceID, fingerprint, string(raw))
	if err != nil {
		return false, fmt.Errorf("record crypto webhook fingerprint tx: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// StuckInvoice — сжатая форма записи crypto_invoices, нужная для recovery-воркера.
// Не несёт всех полей — только то, что нужно для пометки failed и уведомления юзера.
type StuckInvoice struct {
	OrderID    string
	TelegramID int64
	CreatedAt  time.Time
}

// LockStuckCreatingInvoicesTx находит инвойсы в статусе 'creating', созданные раньше
// cutoff, и блокирует их FOR UPDATE SKIP LOCKED, чтобы два параллельных воркера
// (теоретически: при горизонтальном масштабировании сервиса) не наступали друг другу.
// На v1 сервис в одном экземпляре, но SKIP LOCKED — дешёвая страховка.
func (r *Repository) LockStuckCreatingInvoicesTx(ctx context.Context, tx pgx.Tx, cutoff time.Time, limit int) ([]StuckInvoice, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := tx.Query(ctx, `
		SELECT order_id, telegram_id, created_at
		FROM crypto_invoices
		WHERE status = 'creating'
		  AND created_at < $1
		ORDER BY id ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("lock stuck creating invoices tx: %w", err)
	}
	defer rows.Close()

	var out []StuckInvoice
	for rows.Next() {
		var s StuckInvoice
		if err := rows.Scan(&s.OrderID, &s.TelegramID, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan stuck invoice: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stuck invoices: %w", err)
	}
	return out, nil
}
