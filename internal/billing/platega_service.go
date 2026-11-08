package billing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"vpn-platform/internal/billing/platega"
	commonkafka "vpn-platform/internal/common/kafka"
	commonmetrics "vpn-platform/internal/common/metrics"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5"
)

// ============================================================================
// Platega в billing-service.
//
// Путь платежа:
//   бот (тариф → «Способ 1 - рекомендуемый») → billing.commands
//   → HandleCreateSubscriptionCheckout → handleCreatePlategaCheckout
//   → payments (provider='platega') → POST /transaction/process
//   → ссылка пользователю (outbox → tg.notifications)
//   → колбэк /webhooks/platega и/или воркер сверки → GET /transaction/{id}
//   → platega.Decide → BillingPaymentSucceededEvent (outbox → billing.events)
//   → user-subscription (выдача, идемпотентно по payment_id) → оркестратор → ноды.
//
// Оплату подтверждает только GET /transaction/{id} по ключам магазина.
// Колбэк, возврат на return-URL и любые действия пользователя оплату не
// подтверждают.
// ============================================================================

// Подписи способов оплаты — те же, что на кнопках бота.
const (
	methodLabelPlatega  = "«Способ 1 - рекомендуемый»"
	methodLabelYooKassa = "«Способ 2»"
)

var errPlategaDisabled = errors.New("platega is not configured")

// normalizeProvider приводит провайдера из команды к известному значению.
// Пустое значение — команда старого бота (до Platega был только YooKassa).
func normalizeProvider(p kafkacontracts.BillingPaymentProvider) (kafkacontracts.BillingPaymentProvider, bool) {
	switch strings.ToLower(strings.TrimSpace(string(p))) {
	case "", string(kafkacontracts.BillingPaymentProviderYooKassa):
		return kafkacontracts.BillingPaymentProviderYooKassa, true
	case string(kafkacontracts.BillingPaymentProviderPlatega):
		return kafkacontracts.BillingPaymentProviderPlatega, true
	default:
		return "", false
	}
}

// planSummary — «30 дней — 89 ₽».
func planSummary(durationDays int, amountValue string) string {
	return fmt.Sprintf("%d дней — %s ₽", durationDays, humanRub(amountValue))
}

// humanRub — «89.00» → «89», «89.50» → «89,50».
func humanRub(v string) string {
	minor, err := platega.ParseDecimalMinor(v)
	if err != nil {
		return strings.TrimSpace(v)
	}
	if minor%100 == 0 {
		return strconv.FormatInt(minor/100, 10)
	}
	return fmt.Sprintf("%d,%02d", minor/100, minor%100)
}

func minutesLeft(until *time.Time, now time.Time) int {
	if until == nil {
		return 0
	}
	m := int(until.Sub(now).Minutes())
	if m < 1 {
		m = 1
	}
	return m
}

func (s *Service) plategaReturnURLs(cmdReturnURL string) (string, string) {
	ret := firstNotEmpty(s.plategaCfg.ReturnURL, cmdReturnURL, s.cfg.ReturnURL)
	failed := firstNotEmpty(s.plategaCfg.FailedURL, ret)
	return ret, failed
}

// handleCreatePlategaCheckout создаёт (или переиспользует) счёт Platega.
func (s *Service) handleCreatePlategaCheckout(ctx context.Context, cmd *kafkacontracts.CreateSubscriptionCheckoutCommand, plan Plan, commandID string) error {
	if s.platega == nil {
		commonmetrics.BillingPlategaRequestsTotal.WithLabelValues("create", "disabled").Inc()
		slog.Warn("platega checkout requested but platega is not configured", "telegram_id", cmd.TelegramID)
		return s.notifyMethodUnavailable(ctx, cmd.TelegramID, kafkacontracts.BillingPaymentProviderPlatega)
	}
	amountMinor, err := platega.ParseDecimalMinor(plan.AmountValue)
	if err != nil || amountMinor <= 0 {
		slog.Error("platega checkout: invalid plan price", "plan", plan.Code, "err", err)
		return s.notifyMethodUnavailable(ctx, cmd.TelegramID, kafkacontracts.BillingPaymentProviderPlatega)
	}

	orderID := commandID
	record := &PaymentRecord{
		OrderID:        orderID,
		TelegramID:     cmd.TelegramID,
		CheckoutType:   string(kafkacontracts.BillingCheckoutTypeSubscription),
		PlanCode:       string(plan.Code),
		DurationDays:   plan.DurationDays,
		AmountValue:    platega.FormatMinor(amountMinor),
		Currency:       plan.Currency,
		Description:    plan.Title,
		IdempotenceKey: "platega:" + commandID,
		CommandID:      commandID,
		Metadata: map[string]string{
			"order_id":         orderID,
			"telegram_id":      strconv.FormatInt(cmd.TelegramID, 10),
			"plan_code":        string(plan.Code),
			"duration_days":    strconv.Itoa(plan.DurationDays),
			"checkout_type":    string(kafkacontracts.BillingCheckoutTypeSubscription),
			"charge_source":    string(kafkacontracts.BillingChargeSourceInitial),
			"attempt_no":       "0",
			"payment_provider": providerPlatega,
		},
	}

	res, err := s.repo.ReservePlategaCheckout(ctx, record, s.plategaCfg.ReuseMinRemaining)
	if err != nil {
		// До обращения к Platega — повтор команды безопасен.
		return err
	}
	switch res.Outcome {
	case reserveRedelivered:
		slog.Info("platega checkout: command already handled", "order_id", orderID)
		return nil
	case reserveReused:
		slog.Info("platega checkout: reusing open invoice", "order_id", res.Existing.OrderID, "telegram_id", cmd.TelegramID)
		commonmetrics.BillingPlategaRequestsTotal.WithLabelValues("create", "reused").Inc()
		return s.publishNotification(ctx, s.plategaLinkNotification(res.Existing, res.Existing.ConfirmationURL, res.Existing.ExpiresAt, true))
	case reserveInFlight:
		slog.Info("platega checkout: another invoice is being created", "order_id", res.Existing.OrderID, "telegram_id", cmd.TelegramID)
		return s.publishNotification(ctx, &kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			Message: "⏳ Счёт на оплату уже создаётся — ссылка придёт отдельным сообщением в течение минуты.\n\n" +
				"Если она не придёт, выберите способ оплаты ещё раз.",
		})
	}

	marked, err := s.repo.MarkPlategaProviderCreating(ctx, orderID)
	if err != nil {
		return err
	}
	if !marked {
		// Заказ уже обработан другим путём (например, воркер признал его
		// зависшим и уведомил пользователя).
		return nil
	}

	returnURL, failedURL := s.plategaReturnURLs(cmd.ReturnURL)
	created, cerr := s.platega.CreateTransaction(ctx, platega.CreateRequest{
		AmountMinor: amountMinor,
		Currency:    plan.Currency,
		Description: plan.Title,
		ReturnURL:   returnURL,
		FailedURL:   failedURL,
		Payload:     orderID,
		OrderID:     orderID,
		UserEmail:   platega.UserEmailForTelegram(cmd.TelegramID),
		UserID:      strconv.FormatInt(cmd.TelegramID, 10),
	})

	// Результат обращения к Platega обязан попасть в БД, даже если контекст
	// команды уже отменён (остановка сервиса).
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()

	if cerr != nil {
		ambiguous := platega.IsAmbiguous(cerr)
		result := "rejected"
		if ambiguous {
			result = "ambiguous"
		}
		commonmetrics.BillingPlategaRequestsTotal.WithLabelValues("create", result).Inc()
		slog.Error("platega create transaction failed",
			"order_id", orderID, "telegram_id", cmd.TelegramID, "ambiguous", ambiguous, "err", cerr)
		return s.withSaveRetry(saveCtx, func(c context.Context) error {
			return s.finishPlategaCreateError(c, orderID, ambiguous, cerr)
		})
	}
	commonmetrics.BillingPlategaRequestsTotal.WithLabelValues("create", "ok").Inc()
	slog.Info("platega transaction created", "order_id", orderID, "payment_id", created.TransactionID, "telegram_id", cmd.TelegramID)
	return s.withSaveRetry(saveCtx, func(c context.Context) error {
		return s.finishPlategaCreated(c, orderID, created)
	})
}

// withSaveRetry повторяет запись результата при сбое БД: ответ Platega
// второй раз не получить (повторный запрос создал бы второй счёт).
func (s *Service) withSaveRetry(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
	}
	return err
}

func (s *Service) finishPlategaCreated(ctx context.Context, orderID string, created *platega.CreateResult) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	o, err := s.repo.LockPlategaOrderTx(ctx, tx, orderID)
	if err != nil {
		return err
	}
	expiresIn := created.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = s.plategaCfg.DefaultExpiresIn
	}
	expiresAt := time.Now().UTC().Add(expiresIn)
	firstCheck := time.Minute

	switch {
	case o.PaymentID != "" && !platega.SameID(o.PaymentID, created.TransactionID):
		// К заказу уже привязана другая транзакция (пришла колбэком по
		// payload). Нашу ведём отдельной строкой — ничего не теряется.
		dupID, err := s.repo.InsertPlategaDuplicateTx(ctx, tx, o, created.TransactionID)
		if err != nil {
			return err
		}
		slog.Warn("platega: second transaction for one order", "order_id", orderID, "duplicate_order_id", dupID)
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("duplicate_tx").Inc()

	case o.Status == platega.LocalProviderCreating ||
		(o.Status == platega.LocalPending && o.ConfirmationURL == ""):
		if err := s.repo.SetPlategaCreatedTx(ctx, tx, orderID, platega.LocalPending, created.TransactionID,
			created.RedirectURL, created.Status, expiresAt, created.Raw, firstCheck); err != nil {
			return err
		}
		o.ConfirmationURL = created.RedirectURL
		if err := s.publishNotificationTx(ctx, tx, s.plategaLinkNotification(o, created.RedirectURL, &expiresAt, false)); err != nil {
			return err
		}

	case o.Status == platega.LocalCreateUnknown || o.Status == platega.LocalCreateFailed:
		// Пользователю уже сообщили, что счёт не создан. Ссылку не шлём,
		// но транзакцию сохраняем и сверяем: если её всё же оплатят, оплата
		// не потеряется.
		if err := s.repo.SetPlategaCreatedTx(ctx, tx, orderID, platega.LocalPending, created.TransactionID,
			"", created.Status, expiresAt, created.Raw, firstCheck); err != nil {
			return err
		}
		slog.Warn("platega: transaction created after the order was given up", "order_id", orderID, "status", o.Status)

	default:
		// Заказ уже продвинулся по колбэку раньше, чем записался ответ.
		if o.PaymentID == "" {
			if _, err := s.repo.AttachPlategaTransactionTx(ctx, tx, orderID, created.TransactionID); err != nil {
				return err
			}
		}
		slog.Info("platega: create response arrived after the order progressed", "order_id", orderID, "status", o.Status)
	}
	return tx.Commit(ctx)
}

func (s *Service) finishPlategaCreateError(ctx context.Context, orderID string, ambiguous bool, cause error) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	o, err := s.repo.LockPlategaOrderTx(ctx, tx, orderID)
	if err != nil {
		return err
	}
	status := platega.LocalCreateFailed
	if ambiguous {
		status = platega.LocalCreateUnknown
	}
	if _, err := s.repo.SetPlategaCreateErrorTx(ctx, tx, orderID, status, cause.Error()); err != nil {
		return err
	}
	if o.ConfirmationURL == "" {
		if err := s.publishNotificationTx(ctx, tx, plategaCreateFailedNotification(o.TelegramID, ambiguous)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func plategaCreateFailedNotification(telegramID int64, ambiguous bool) *kafkacontracts.TgNotification {
	msg := "❌ Не удалось создать счёт для " + methodLabelPlatega + ". Деньги не списывались.\n\n" +
		"Оплатите " + methodLabelYooKassa + " или попробуйте этот способ ещё раз чуть позже 👇"
	if ambiguous {
		msg = "⚠️ Не удалось получить ссылку на оплату для " + methodLabelPlatega + ". " +
			"Без ссылки оплатить этот счёт нельзя, деньги не списывались.\n\n" +
			"Выберите способ оплаты ещё раз — " + methodLabelYooKassa + " или " + methodLabelPlatega + " 👇"
	}
	return &kafkacontracts.TgNotification{
		TelegramID: telegramID,
		Message:    msg,
		Keyboard:   kafkacontracts.TgKeyboardPaymentMethod,
	}
}

func (s *Service) plategaLinkNotification(o *plategaOrder, url string, expiresAt *time.Time, reused bool) *kafkacontracts.TgNotification {
	var b strings.Builder
	if reused {
		b.WriteString("💳 У вас уже есть неоплаченный счёт на этот тариф — новый не создавался, вот та же ссылка.\n\n")
	} else {
		b.WriteString("💳 Счёт на оплату готов — " + methodLabelPlatega + ".\n\n")
	}
	b.WriteString("Тариф: " + planSummary(o.DurationDays, o.AmountValue) + ".\n")
	if m := minutesLeft(expiresAt, time.Now()); m > 0 {
		fmt.Fprintf(&b, "Ссылка действует ещё примерно %d мин.\n", m)
	}
	b.WriteString("\nПодписка активируется автоматически, как только платёж подтвердится. Нажмите кнопку ниже 👇")
	return &kafkacontracts.TgNotification{
		TelegramID: o.TelegramID,
		Message:    b.String(),
		PayURL:     url,
	}
}

// notifyMethodUnavailable сообщает, что выбранный способ сейчас недоступен,
// и предлагает другой — явно, без скрытого переключения провайдера.
func (s *Service) notifyMethodUnavailable(ctx context.Context, telegramID int64, provider kafkacontracts.BillingPaymentProvider) error {
	this, other := methodLabelPlatega, methodLabelYooKassa
	if provider == kafkacontracts.BillingPaymentProviderYooKassa {
		this, other = methodLabelYooKassa, methodLabelPlatega
	}
	return s.publishNotification(ctx, &kafkacontracts.TgNotification{
		TelegramID: telegramID,
		Message: "😔 " + this + " сейчас недоступен. Деньги не списывались.\n\n" +
			"Оплатите " + other + " — тариф и цена те же 👇",
		Keyboard: kafkacontracts.TgKeyboardPaymentMethod,
	})
}

// RegisterPlategaCallback надёжно фиксирует колбэк: находит заказ, при
// необходимости связывает его с транзакцией и ставит на немедленную сверку.
// После успешного коммита колбэк не потеряется, даже если сверка сразу не
// удастся: её повторит воркер. Возвращает заказ для сверки ("" — колбэк не
// относится к нашим заказам).
func (s *Service) RegisterPlategaCallback(ctx context.Context, cb *platega.Callback, fingerprint string) (string, string, error) {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventType := "platega." + strings.ToLower(firstNotEmpty(string(cb.Status), "unknown"))
	lease := s.plategaCfg.CheckLease

	o, err := s.repo.FindPlategaOrderByPaymentIDTx(ctx, tx, cb.ID)
	switch {
	case err == nil:
		if err := s.repo.ClaimPlategaForCheckTx(ctx, tx, o.OrderID, lease); err != nil {
			return "", "", err
		}
		if err := s.repo.RecordPlategaCallbackTx(ctx, tx, cb.ID, eventType, fingerprint, cb.SanitizedJSON(true)); err != nil {
			return "", "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", "", err
		}
		return o.OrderID, cb.ID, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return "", "", err
	}

	// Транзакция не известна по ID. Сопоставляем по payload — так находится
	// заказ, ответ на создание которого потерялся или ещё не записан.
	if cb.SubscriptionID != "" || cb.Payload == "" {
		return "", "", s.ignorePlategaCallback(ctx, tx, cb, eventType, fingerprint, "foreign")
	}
	orig, err := s.repo.FindPlategaOrderByPayloadTx(ctx, tx, cb.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", s.ignorePlategaCallback(ctx, tx, cb, eventType, fingerprint, "unknown_payload")
	}
	if err != nil {
		return "", "", err
	}

	target := orig.OrderID
	if orig.PaymentID == "" {
		if _, err := s.repo.AttachPlategaTransactionTx(ctx, tx, orig.OrderID, cb.ID); err != nil {
			return "", "", err
		}
		slog.Info("platega callback attached transaction to order", "order_id", orig.OrderID, "payment_id", cb.ID)
	} else {
		dupID, err := s.repo.InsertPlategaDuplicateTx(ctx, tx, orig, cb.ID)
		if err != nil {
			return "", "", err
		}
		target = dupID
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("duplicate_tx").Inc()
		slog.Warn("platega callback: second transaction for one order", "order_id", orig.OrderID, "duplicate_order_id", dupID)
	}
	if err := s.repo.ClaimPlategaForCheckTx(ctx, tx, target, lease); err != nil {
		return "", "", err
	}
	if err := s.repo.RecordPlategaCallbackTx(ctx, tx, cb.ID, eventType, fingerprint, cb.SanitizedJSON(true)); err != nil {
		return "", "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return target, cb.ID, nil
}

func (s *Service) ignorePlategaCallback(ctx context.Context, tx pgx.Tx, cb *platega.Callback, eventType, fingerprint, reason string) error {
	if err := s.repo.RecordPlategaCallbackTx(ctx, tx, cb.ID, eventType+".ignored", fingerprint, cb.SanitizedJSON(false)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("ignored").Inc()
	slog.Info("platega callback ignored: not our order", "payment_id", cb.ID, "reason", reason)
	return nil
}

// CheckPlategaPayment сверяет заказ с Platega (GET /transaction/{id}) и
// применяет решение машины состояний. attempt — номер попытки для паузы
// перед повтором при ошибке API.
func (s *Service) CheckPlategaPayment(ctx context.Context, orderID, paymentID string, attempt int) error {
	if s.platega == nil {
		return errPlategaDisabled
	}
	reqCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	ptx, err := s.platega.GetTransaction(reqCtx, paymentID)
	cancel()
	if err != nil {
		result := "error"
		if errors.Is(err, platega.ErrNotFound) {
			result = "not_found"
		}
		commonmetrics.BillingPlategaRequestsTotal.WithLabelValues("status", result).Inc()
		saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelSave()
		if errors.Is(err, platega.ErrNotFound) {
			if done, rerr := s.reviewIfLongNotFound(saveCtx, orderID); rerr != nil || done {
				return err
			}
		}
		if merr := s.repo.MarkPlategaCheckError(saveCtx, orderID, platega.RetryDelay(attempt), err.Error()); merr != nil {
			slog.Error("platega: failed to reschedule check", "order_id", orderID, "err", merr)
		}
		return err
	}
	commonmetrics.BillingPlategaRequestsTotal.WithLabelValues("status", "ok").Inc()
	applyCtx, cancelApply := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancelApply()
	return s.applyPlategaTransaction(applyCtx, orderID, ptx)
}

// reviewIfLongNotFound: транзакция, которую Platega не находит дольше трёх
// суток после создания заказа, уходит в ручную проверку вместо бесконечных
// повторов.
func (s *Service) reviewIfLongNotFound(ctx context.Context, orderID string) (bool, error) {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := s.repo.LockPlategaOrderTx(ctx, tx, orderID)
	if err != nil {
		return false, err
	}
	if time.Since(o.CreatedAt) < 72*time.Hour || o.Status == platega.LocalReviewRequired {
		return false, nil
	}
	if err := s.repo.MarkPlategaReviewTx(ctx, tx, orderID, "not_found_at_provider"); err != nil {
		return false, err
	}
	slog.Warn("platega: transaction not found at provider, sent to review", "order_id", orderID)
	return true, tx.Commit(ctx)
}

func (s *Service) applyPlategaTransaction(ctx context.Context, orderID string, ptx *platega.Transaction) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	o, err := s.repo.LockPlategaOrderTx(ctx, tx, orderID)
	if err != nil {
		return err
	}
	snap, err := o.snapshot()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	d := platega.Decide(snap, *ptx, now)
	if err := s.repo.ApplyPlategaDecisionTx(ctx, tx, orderID, d); err != nil {
		return err
	}

	if d.Changed {
		slog.Info("platega order transition",
			"order_id", orderID, "payment_id", o.PaymentID,
			"from", o.Status, "to", d.Status,
			"provider_status", d.ProviderStatus, "grant", d.Grant, "reason", d.Reason)
	}

	if d.Grant {
		if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingPaymentSucceededEvent{
			Type:             kafkacontracts.BillingEventPaymentSucceeded,
			CheckoutType:     kafkacontracts.BillingCheckoutTypeSubscription,
			ChargeSource:     kafkacontracts.BillingChargeSourceInitial,
			TelegramID:       o.TelegramID,
			OrderID:          o.OrderID,
			PaymentID:        o.PaymentID,
			PlanCode:         kafkacontracts.PlanCode(o.PlanCode),
			DurationDays:     o.DurationDays,
			AmountValue:      o.AmountValue,
			Currency:         o.Currency,
			AutoRenewEnabled: false,
			PaidAt:           now,
			PaymentProvider:  providerPlatega,
		}); err != nil {
			return err
		}
	}

	if d.NotifyCanceled {
		newer, err := s.repo.HasNewerSubscriptionOrderTx(ctx, tx, o.TelegramID, o.OrderID, o.CreatedAt)
		if err != nil {
			return err
		}
		if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingPaymentCanceledEvent{
			Type:            kafkacontracts.BillingEventPaymentCanceled,
			CheckoutType:    kafkacontracts.BillingCheckoutTypeSubscription,
			ChargeSource:    kafkacontracts.BillingChargeSourceInitial,
			TelegramID:      o.TelegramID,
			OrderID:         o.OrderID,
			PaymentID:       o.PaymentID,
			Reason:          "platega_canceled",
			CanceledAt:      now,
			PaymentProvider: providerPlatega,
		}); err != nil {
			return err
		}
		if !newer {
			if err := s.publishNotificationTx(ctx, tx, &kafkacontracts.TgNotification{
				TelegramID: o.TelegramID,
				Message: "Счёт " + methodLabelPlatega + " на тариф " + planSummary(o.DurationDays, o.AmountValue) +
					" отменён или истёк — оплата по нему не поступила.\n\n" +
					"Если деньги всё же списались, ничего делать не нужно: подписка активируется автоматически, как только платёж подтвердится.\n\n" +
					"Чтобы оплатить заново, выберите тариф 👇",
				Keyboard: kafkacontracts.TgKeyboardBuyMenu,
			}); err != nil {
				return err
			}
		}
	}

	if d.NotifyReview {
		if err := s.publishNotificationTx(ctx, tx, &kafkacontracts.TgNotification{
			TelegramID: o.TelegramID,
			Message: "🔎 Платёж по счёту " + methodLabelPlatega + " получен, но требует ручной проверки. " +
				"Подписка будет активирована после неё.\n\nЕсли есть вопросы — напишите в поддержку.",
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if d.Grant {
		commonmetrics.BillingPaymentsTotal.WithLabelValues("succeeded").Inc()
	}
	if d.NotifyCanceled {
		commonmetrics.BillingPaymentsTotal.WithLabelValues("canceled").Inc()
	}
	if d.Status == platega.LocalReviewRequired && d.Changed {
		slog.Warn("platega order needs manual review", "order_id", orderID, "reason", d.Reason)
	}
	return nil
}

// ReconcilePlatega — один проход воркера сверки: закрыть зависшие создания,
// сверить заказы по расписанию, обновить метрики.
func (s *Service) ReconcilePlatega(ctx context.Context) error {
	var firstErr error
	if err := s.sweepStalePlategaCreating(ctx); err != nil {
		firstErr = err
		slog.Error("platega: sweep of stale creations failed", "err", err)
	}

	if s.platega != nil {
		due, err := s.repo.ClaimDuePlategaChecks(ctx, s.plategaCfg.ReconcileBatch, s.plategaCfg.CheckLease)
		if err != nil {
			slog.Error("platega: claim due checks failed", "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
		for _, d := range due {
			if ctx.Err() != nil {
				break
			}
			if err := s.CheckPlategaPayment(ctx, d.OrderID, d.PaymentID, d.Attempts); err != nil {
				slog.Warn("platega: status check failed", "order_id", d.OrderID, "attempt", d.Attempts, "err", err)
			}
		}
	}

	s.exportPlategaMetrics(ctx)
	commonmetrics.BillingPlategaReconcileLastRun.SetToCurrentTime()
	return firstErr
}

func (s *Service) sweepStalePlategaCreating(ctx context.Context) error {
	steps := []struct {
		from, to  platega.LocalStatus
		olderThan time.Duration
		reason    string
		ambiguous bool
	}{
		// Запрос к Platega из local_creating не отправлялся: счёт точно не создан.
		{platega.LocalCreating, platega.LocalCreateFailed, 5 * time.Minute, "stale_local_creating", false},
		// Запрос мог дойти: исход неизвестен, повторно не создаём.
		{platega.LocalProviderCreating, platega.LocalCreateUnknown, s.plategaCfg.StaleCreating, "stale_provider_creating", true},
	}
	for _, st := range steps {
		tx, err := s.repo.BeginTx(ctx)
		if err != nil {
			return err
		}
		rows, err := s.repo.SweepStalePlategaCreatingTx(ctx, tx, st.from, st.to, st.olderThan, st.reason)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		for _, r := range rows {
			if r.ConfirmationURL != "" {
				continue
			}
			if err := s.publishNotificationTx(ctx, tx, plategaCreateFailedNotification(r.TelegramID, st.ambiguous)); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		for _, r := range rows {
			slog.Warn("platega: stale creation closed", "order_id", r.OrderID, "from", st.from, "to", st.to)
		}
	}
	return nil
}

var plategaMetricStatuses = []platega.LocalStatus{
	platega.LocalCreating, platega.LocalProviderCreating, platega.LocalCreateFailed, platega.LocalCreateUnknown,
	platega.LocalPending, platega.LocalSucceeded, platega.LocalCanceled, platega.LocalReviewRequired,
	platega.LocalDisputed, platega.LocalChargebacked, platega.LocalPartiallyRefunded,
}

func (s *Service) exportPlategaMetrics(ctx context.Context) {
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	counts, overdue, err := s.repo.CountPlategaByStatus(qctx)
	if err != nil {
		slog.Warn("platega: metrics query failed", "err", err)
		return
	}
	for _, st := range plategaMetricStatuses {
		commonmetrics.BillingPlategaPaymentsByStatus.WithLabelValues(string(st)).Set(counts[string(st)])
	}
	commonmetrics.BillingPlategaOverdueChecks.Set(overdue)
}

// rejectUnknownProvider — команда с неизвестным провайдером: сообщаем
// пользователю и уводим команду в DLT для разбора (без повторов).
func (s *Service) rejectUnknownProvider(ctx context.Context, cmd *kafkacontracts.CreateSubscriptionCheckoutCommand) error {
	slog.Error("billing: unsupported payment provider in checkout command",
		"telegram_id", cmd.TelegramID, "provider", truncateText(string(cmd.PaymentProvider), 32))
	if err := s.publishNotification(ctx, &kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		Message:    "Не удалось начать оплату. Выберите способ оплаты ещё раз 👇",
		Keyboard:   kafkacontracts.TgKeyboardPaymentMethod,
	}); err != nil {
		return err
	}
	return fmt.Errorf("%w: unsupported payment provider", commonkafka.ErrSkip)
}
