package crypto_billing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/outbox"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Sentinel ошибки. ErrDuplicateWebhook ловится в HTTP-хендлере и превращается в 200 OK
// (для CryptoBot это значит "не ретраить"). ErrInvoiceNotFound — наоборот, возвращаем 4xx,
// чтобы CryptoBot повторил позже на случай гонки (вдруг инвойс ещё не докоммитился).
var (
	ErrDuplicateWebhook = errors.New("duplicate cryptobot webhook")
	ErrInvoiceNotFound  = errors.New("crypto invoice not found")
)

type Service struct {
	cfg      Config
	repo     *Repository
	client   *CryptoBotClient
	producer *commonkafka.Producer
	rates    *RatesCache // кэш курсов для динамического ценообразования (crypto-режим)
}

func NewService(cfg Config, repo *Repository, client *CryptoBotClient, producer *commonkafka.Producer) *Service {
	return &Service{cfg: cfg, repo: repo, client: client, producer: producer}
}

// SetRatesCache подключает кэш курсов (вызывается из main после создания воркера).
// Может быть nil — тогда crypto-режим использует статичные цены как fallback.
func (s *Service) SetRatesCache(rc *RatesCache) {
	s.rates = rc
}

// ============================================================================
// Сценарий 1: обработка команды create_checkout из Kafka топика crypto.commands
// ============================================================================

// HandleCreateCheckout — главный entry-point для команды. Структура такая:
//  1. Транзакция №1: dedup по command_id + insert инвойса в статусе 'creating'
//  2. Внешний вызов CryptoBot.createInvoice (вне транзакции, иначе блокируем коннект пула)
//  3. Транзакция №2: обновляем инвойс на 'active' и кладём в outbox уведомление пользователю
//
// Между шагами 2 и 3 — единственная "дыра", где краш приведёт к висящему инвойсу.
// В v1 принимаем; см. service.go в шапке файла.
func (s *Service) HandleCreateCheckout(ctx context.Context, cmd *kafkacontracts.CreateCryptoCheckoutCommand) error {
	if cmd == nil {
		return fmt.Errorf("nil create crypto checkout command")
	}

	plan, ok := s.cfg.Plans[cmd.PlanCode]
	if !ok {
		return fmt.Errorf("unsupported plan code: %s", cmd.PlanCode)
	}

	// Определяем цену инвойса по выбранному режиму (crypto/fiat).
	// pricing инкапсулирует: динамический расчёт из рублей по курсу, fallback
	// на статичную цену, либо фиатную сумму для fiat-режима. См. pricing.go.
	pricing, err := s.resolvePricing(plan, cmd.Asset)
	if err != nil {
		return err
	}
	// Для совместимости с остальным кодом метода: asset/amount — то, что пишем в БД.
	// crypto-режим: реальные крипто-значения; fiat-режим: рубли (RUB/200),
	// webhook затем сверит фактические paid_asset/paid_amount.
	asset := kafkacontracts.CryptoAsset(pricing.DBAsset)
	amount := pricing.DBAmount

	// order_id — наш собственный идентификатор инвойса; кладём его в payload CryptoBot,
	// чтобы при необходимости найти инвойс по нему в API CryptoBot. CryptoBot ничего
	// специально с этим не делает — просто возвращает обратно в вебхуке.
	orderID := "crypto-" + uuid.NewString()
	expiresAt := time.Now().UTC().Add(s.cfg.InvoiceExpires)

	// ----- Транзакция 1: dedup команды + создание записи в статусе 'creating' -----
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// processed_messages — единая для всего проекта таблица идемпотентности.
	// Ключ message_id строится как "crypto-billing-service:<command_id>".
	// Если такой command_id уже обрабатывался — выходим, ничего не делая.
	inserted, err := outbox.MarkProcessed(ctx, tx, "crypto-billing-service", cmd.CommandID, string(cmd.Type))
	if err != nil {
		return err
	}
	if !inserted {
		slog.Info("crypto-billing duplicate command ignored",
			"command_id", cmd.CommandID,
			"telegram_id", cmd.TelegramID,
		)
		return tx.Commit(ctx)
	}

	invRec := &CryptoInvoice{
		OrderID:      orderID,
		CommandID:    cmd.CommandID,
		TelegramID:   cmd.TelegramID,
		PlanCode:     string(cmd.PlanCode),
		DurationDays: plan.DurationDays,
		Asset:        string(asset),
		AmountValue:  amount,
		Description:  plan.Title,
		ExpiresAt:    &expiresAt,
	}
	if _, err := s.repo.InsertInvoiceTx(ctx, tx, invRec); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// ----- Внешний вызов CryptoBot API (вне транзакции) -----
	// В fiat-режиме создаём инвойс в рублях (CryptoBot сам конвертирует в крипту
	// по курсу на момент оплаты). В crypto-режиме — обычный крипто-инвойс с
	// заранее посчитанной суммой.
	var (
		inv       *CryptoBotInvoice
		rawCreate json.RawMessage
		createErr error
	)
	if pricing.IsFiat {
		inv, rawCreate, createErr = s.client.CreateInvoiceFiat(
			ctx,
			pricing.FiatCurrency,
			pricing.FiatAmount,
			s.cfg.AcceptedAssetsCSV(),
			plan.Title,
			orderID,
			s.cfg.PaidBtnName,
			s.cfg.PaidBtnURL,
			s.cfg.InvoiceExpires,
		)
	} else {
		inv, rawCreate, createErr = s.client.CreateInvoice(
			ctx,
			asset,
			amount,
			plan.Title,
			orderID, // payload — вернётся в webhook'е
			s.cfg.PaidBtnName,
			s.cfg.PaidBtnURL,
			s.cfg.InvoiceExpires,
		)
	}

	// Если создание упало — фиксируем причину в БД и шлём пользователю сообщение.
	if createErr != nil {
		slog.Error("cryptobot createInvoice failed",
			"order_id", orderID,
			"telegram_id", cmd.TelegramID,
			"err", createErr,
		)

		failTx, txErr := s.repo.BeginTx(ctx)
		if txErr != nil {
			return createErr // отдадим исходную ошибку, чтобы Kafka сделал retry
		}
		defer func() { _ = failTx.Rollback(ctx) }()

		if err := s.repo.MarkInvoiceFailedTx(ctx, failTx, orderID, createErr.Error()); err != nil {
			return err
		}
		if err := s.notifyTx(ctx, failTx, kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			Message:    "Не удалось создать счёт на оплату криптой. Попробуй позже или выбери оплату картой.",
			Keyboard:   kafkacontracts.TgKeyboardBuyMenu,
		}); err != nil {
			return err
		}
		return failTx.Commit(ctx)
	}

	payURL := PickPayURL(inv)
	invoiceIDStr := fmt.Sprint(inv.InvoiceID)

	// ----- Транзакция 2: обновляем инвойс на 'active' + outbox-уведомление пользователю -----
	tx, err = s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := s.repo.MarkInvoiceActiveTx(ctx, tx, orderID, invoiceIDStr, payURL, rawCreate); err != nil {
		return err
	}

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		ParseMode:  "Markdown",
		Message:    s.buildPayLinkMessage(plan.Title, amount, string(asset), payURL, expiresAt),
		Keyboard:   kafkacontracts.TgKeyboardMainMenuWithBack,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// buildPayLinkMessage собирает Markdown-сообщение со ссылкой оплаты.
// Ссылка остаётся "голой" (не embedded в Markdown-ссылку), чтобы её можно было
// явно скопировать на мобильном Telegram-клиенте, если автооткрытие не сработает.
func (s *Service) buildPayLinkMessage(title, amount, asset, payURL string, expiresAt time.Time) string {
	var sb strings.Builder
	sb.WriteString("🪙 *Оплата криптовалютой*\n\n")
	sb.WriteString("*" + title + "*\n")
	sb.WriteString(fmt.Sprintf("Сумма: *%s %s*\n", amount, asset))
	sb.WriteString(fmt.Sprintf("Счёт действует до: *%s UTC*\n\n", expiresAt.Format("02.01.2006 15:04")))
	sb.WriteString("Перейди по ссылке и подтверди оплату:\n")
	sb.WriteString(payURL + "\n\n")
	sb.WriteString("Как только платёж пройдёт, бот пришлёт подтверждение и ключи доступа.")
	return sb.String()
}

// ============================================================================
// Сценарий 2: обработка webhook'а от CryptoBot
// ============================================================================

// CryptoBotWebhookUpdate — корневая обёртка webhook'а CryptoBot.
type CryptoBotWebhookUpdate struct {
	UpdateID    int64           `json:"update_id"`
	UpdateType  string          `json:"update_type"`
	RequestDate string          `json:"request_date"`
	Payload     json.RawMessage `json:"payload"`
}

type CryptoBotInvoicePayload struct {
	InvoiceID  int64  `json:"invoice_id"`
	Status     string `json:"status"`
	Hash       string `json:"hash"`
	Asset      string `json:"asset"`
	Amount     string `json:"amount"`
	Payload    string `json:"payload"` // совпадает с нашим order_id
	PaidAt     string `json:"paid_at"`
	PaidAnonym bool   `json:"paid_anonymously"`
	// Поля фиат-инвойса (currency_type=fiat). При оплате CryptoBot заполняет
	// фактически уплаченные крипто-значения и фиатную базу.
	CurrencyType string `json:"currency_type"`
	Fiat         string `json:"fiat"`
	PaidAsset    string `json:"paid_asset"`
	PaidAmount   string `json:"paid_amount"`
}

// ProcessWebhook — entry-point для HTTP-хендлера webhook'а. HMAC + URL-токен уже
// проверены до вызова. fingerprint — sha256(raw_body) для дедупа.
func (s *Service) ProcessWebhook(ctx context.Context, raw []byte, fingerprint string) error {
	var update CryptoBotWebhookUpdate
	if err := json.Unmarshal(raw, &update); err != nil {
		return fmt.Errorf("decode webhook: %w", err)
	}

	// Replay-защита: CryptoBot рекомендует проверять request_date и отбрасывать
	// устаревшие вебхуки. HMAC защищает подлинность, но не повторное проигрывание.
	// Окно 15 минут — с запасом на легитимные задержки и расхождение часов.
	if update.RequestDate != "" {
		if reqTime, err := time.Parse(time.RFC3339, update.RequestDate); err == nil {
			if age := time.Since(reqTime); age > 15*time.Minute || age < -15*time.Minute {
				slog.Warn("crypto-billing webhook rejected: stale request_date",
					"request_date", update.RequestDate, "age_seconds", age.Seconds())
				// Возвращаем nil (а не ошибку), чтобы CryptoBot не ретраил бесконечно
				// заведомо устаревший запрос. Это не наша ошибка обработки.
				return nil
			}
		} else {
			slog.Warn("crypto-billing webhook: cannot parse request_date, proceeding",
				"request_date", update.RequestDate, "err", err)
		}
	}

	// На v1 нас интересует только "invoice_paid". Другие типы (invoice_expired, и т.д.)
	// можно будет добавить позже.
	if update.UpdateType != "invoice_paid" {
		slog.Debug("crypto-billing webhook ignored", "update_type", update.UpdateType)
		return nil
	}

	var ip CryptoBotInvoicePayload
	if err := json.Unmarshal(update.Payload, &ip); err != nil {
		return fmt.Errorf("decode invoice payload: %w", err)
	}
	invoiceIDStr := fmt.Sprint(ip.InvoiceID)
	if invoiceIDStr == "" || invoiceIDStr == "0" {
		return fmt.Errorf("empty invoice_id in webhook")
	}

	// ----- Всё в одной транзакции -----
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Шаг 1: дедуп по fingerprint. Если вебхук уже видели — выходим с ErrDuplicateWebhook,
	// который HTTP-хендлер превратит в 200 OK.
	inserted, err := s.repo.RecordWebhookFingerprintTx(ctx, tx, update.UpdateType, invoiceIDStr, fingerprint, raw)
	if err != nil {
		return err
	}
	if !inserted {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return ErrDuplicateWebhook
	}

	inv, err := s.repo.GetInvoiceByInvoiceIDTx(ctx, tx, invoiceIDStr)
	if err != nil {
		return fmt.Errorf("%w: invoice_id=%s err=%v", ErrInvoiceNotFound, invoiceIDStr, err)
	}

	// Валидация содержимого подписанного payload: статус должен быть paid, актив и сумма
	// должны совпадать с тем, что мы создавали. HMAC защищает от подделки источника,
	// эта проверка — от логических расхождений. Если не совпало — фиксируем вебхук как
	// обработанный (fingerprint уже записан), но НЕ активируем подписку.
	if ip.Status != "" && ip.Status != "paid" {
		slog.Warn("crypto-billing webhook with non-paid status, skipping activation",
			"invoice_id", invoiceIDStr, "order_id", inv.OrderID, "status", ip.Status)
		return tx.Commit(ctx)
	}
	// Сверка суммы/актива. Для крипто-инвойса (currency_type != "fiat") сверяем
	// строго, как и раньше: asset и amount в вебхуке должны совпасть с тем, что
	// мы создавали. Для фиат-инвойса крипто-значения определяются только при
	// оплате (paid_asset/paid_amount), а в БД у нас лежит фиат — поэтому строгую
	// крипто-сверку пропускаем (целостность источника уже гарантирована HMAC).
	isFiatInvoice := strings.EqualFold(ip.CurrencyType, "fiat") || ip.Fiat != "" || ip.PaidAsset != ""
	if !isFiatInvoice {
		if ip.Asset != "" && !strings.EqualFold(ip.Asset, inv.Asset) {
			slog.Error("crypto-billing webhook asset mismatch",
				"invoice_id", invoiceIDStr, "expected", inv.Asset, "got", ip.Asset)
			return tx.Commit(ctx)
		}
		if ip.Amount != "" && !amountsEqual(ip.Amount, inv.AmountValue) {
			slog.Error("crypto-billing webhook amount mismatch",
				"invoice_id", invoiceIDStr, "expected", inv.AmountValue, "got", ip.Amount)
			return tx.Commit(ctx)
		}
	} else {
		slog.Info("crypto-billing fiat invoice paid",
			"invoice_id", invoiceIDStr, "fiat", ip.Fiat, "fiat_amount", ip.Amount,
			"paid_asset", ip.PaidAsset, "paid_amount", ip.PaidAmount)
	}

	// Шаг 3: пометить как paid. Если уже был paid — UPDATE не сработает (RowsAffected=0).
	updated, err := s.repo.MarkInvoicePaidTx(ctx, tx, inv.OrderID, raw)
	if err != nil {
		return err
	}
	if !updated {
		// Уже был paid (например, повторный webhook с другим body, но тот же invoice_id).
		// Защитная сеть: коммитим транзакцию (fingerprint мы уже записали, и это нормально)
		// и не публикуем дублирующее событие.
		slog.Info("crypto-billing invoice already paid, skipping event publish",
			"invoice_id", invoiceIDStr, "order_id", inv.OrderID,
		)
		return tx.Commit(ctx)
	}

	// Шаг 4: опубликовать в billing.events событие совместимого с YooKassa формата.
	// user-subscription-service и vpn-orchestrator не различают провайдеров на уровне обработки.
	paidAt := time.Now().UTC()
	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingPaymentSucceededEvent{
		Type:         kafkacontracts.BillingEventPaymentSucceeded,
		CheckoutType: kafkacontracts.BillingCheckoutTypeSubscription,
		ChargeSource: kafkacontracts.BillingChargeSourceInitial,
		TelegramID:   inv.TelegramID,
		OrderID:      inv.OrderID,
		// Префикс "crypto:" защищает от теоретической коллизии invoice_id с UUID YooKassa
		// в общей таблице processed_messages, где user-sub дедупит по "payment:<paymentID>".
		PaymentID:        "crypto:" + invoiceIDStr,
		PlanCode:         kafkacontracts.PlanCode(inv.PlanCode),
		DurationDays:     inv.DurationDays,
		AmountValue:      inv.AmountValue,
		Currency:         inv.Asset,
		PaymentMethodID:  "",
		AttemptNo:        0,
		AutoRenewEnabled: false,
		PaidAt:           paidAt,
		PaymentProvider:  string(kafkacontracts.BillingPaymentProviderCryptoBot),
	}); err != nil {
		return err
	}

	// Шаг 5: дополнительная нотификация пользователю "оплата получена".
	// user-subscription пришлёт своё сообщение "подписка активна" чуть позже, но мы хотим,
	// чтобы пользователь сразу видел подтверждение факта оплаты. Это улучшает UX.
	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: inv.TelegramID,
		Message: fmt.Sprintf(
			"✅ Оплата %s %s получена. Активирую подписку...",
			inv.AmountValue, inv.Asset,
		),
		Keyboard: kafkacontracts.TgKeyboardMainMenuWithBack,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ProcessStuckCreatingInvoices — один проход recovery-воркера.
// Находит инвойсы, висящие в статусе 'creating' дольше threshold, помечает их
// failed и шлёт пользователю TG-уведомление через outbox. CryptoBot-инвойс на их
// стороне истекает сам через CRYPTOBOT_INVOICE_EXPIRES_IN, ничего отзывать не надо.
//
// Все операции одной транзакции — UPDATE статусов и AddTx outbox-уведомлений —
// чтобы либо всё применилось, либо ничего. SKIP LOCKED в Lock... защищает от
// параллельных воркеров (на случай будущего горизонтального масштабирования).
func (s *Service) ProcessStuckCreatingInvoices(ctx context.Context, threshold time.Duration, limit int) error {
	cutoff := time.Now().UTC().Add(-threshold)

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	stuck, err := s.repo.LockStuckCreatingInvoicesTx(ctx, tx, cutoff, limit)
	if err != nil {
		return err
	}
	if len(stuck) == 0 {
		return tx.Commit(ctx)
	}

	for _, inv := range stuck {
		reason := fmt.Sprintf("stuck in creating since %s (recovered by worker)", inv.CreatedAt.UTC().Format(time.RFC3339))
		if err := s.repo.MarkInvoiceFailedTx(ctx, tx, inv.OrderID, reason); err != nil {
			return err
		}
		if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: inv.TelegramID,
			Message: "⚠️ Не удалось создать счёт на оплату криптой (внутренний сбой). " +
				"Попробуй ещё раз через меню «Купить подписку» — или выбери оплату картой.",
			Keyboard: kafkacontracts.TgKeyboardBuyMenu,
		}); err != nil {
			return err
		}
		slog.Warn("crypto-billing stuck invoice marked failed",
			"order_id", inv.OrderID,
			"telegram_id", inv.TelegramID,
			"created_at", inv.CreatedAt)
	}
	return tx.Commit(ctx)
}

// ============================================================================
// Outbox helpers — обёртки, чтобы handler-функции выглядели читаемо
// ============================================================================

// notifyTx кладёт TgNotification в outbox в рамках открытой транзакции.
// outbox-worker возьмёт его и опубликует в Kafka топик tg.notifications.
func (s *Service) notifyTx(ctx context.Context, tx pgx.Tx, n kafkacontracts.TgNotification) error {
	if n.TelegramID == 0 {
		return nil
	}
	return outbox.AddTx(ctx, tx, outbox.Event{
		AggregateType: "telegram",
		AggregateID:   fmt.Sprint(n.TelegramID),
		Topic:         commonkafka.TopicTgNotifications,
		MessageKey:    fmt.Sprint(n.TelegramID),
		EventType:     "tg.notification",
		Payload:       n,
	})
}

// publishBillingEventTx кладёт BillingPaymentSucceededEvent в outbox.
// Тот же топик billing.events, который слушает user-subscription-service.
// Ключ сообщения — telegram_id, чтобы все события одного пользователя попадали в одну партицию
// и обрабатывались по порядку.
func (s *Service) publishBillingEventTx(ctx context.Context, tx pgx.Tx, event *kafkacontracts.BillingPaymentSucceededEvent) error {
	key := fmt.Sprint(event.TelegramID)
	return outbox.AddTx(ctx, tx, outbox.Event{
		AggregateType: "billing",
		AggregateID:   key,
		Topic:         commonkafka.TopicBillingEvents,
		MessageKey:    key,
		EventType:     string(event.Type),
		Payload:       event,
	})
}

// amountsEqual сравнивает две суммы-строки численно с небольшой толерантностью.
// Нужна, потому что CryptoBot может вернуть "5" там, где мы отправили "5.00".
func amountsEqual(a, b string) bool {
	fa, errA := strconv.ParseFloat(strings.TrimSpace(a), 64)
	fb, errB := strconv.ParseFloat(strings.TrimSpace(b), 64)
	if errA != nil || errB != nil {
		// если не парсится — падаем на строковое сравнение
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	diff := fa - fb
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.000001
}
