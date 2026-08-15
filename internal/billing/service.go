package billing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	commonmetrics "vpn-platform/internal/common/metrics"
	"vpn-platform/internal/common/outbox"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Service struct {
	repo          *Repository
	producer      *commonkafka.Producer
	client        *http.Client
	cfg           ServiceConfig
	plans         map[kafkacontracts.PlanCode]Plan
	retrySchedule []time.Duration
}

type ServiceConfig struct {
	YooKassaAPIBase string
	ShopID          string
	SecretKey       string
	ReturnURL       string
	BindAmountRUB   string
	GracePeriod     time.Duration
}

type Plan struct {
	Code         kafkacontracts.PlanCode
	Title        string
	AmountValue  string
	Currency     string
	DurationDays int
}

var ErrDuplicateWebhook = errors.New("duplicate yookassa webhook")

func NewService(repo *Repository, producer *commonkafka.Producer) *Service {
	cfg := ServiceConfig{
		YooKassaAPIBase: "https://api.yookassa.ru/v3",
		ShopID:          strings.TrimSpace(os.Getenv("YOOKASSA_SHOP_ID")),
		SecretKey:       strings.TrimSpace(os.Getenv("YOOKASSA_SECRET_KEY")),
		ReturnURL:       strings.TrimSpace(os.Getenv("YOOKASSA_RETURN_URL")),
		BindAmountRUB:   strings.TrimSpace(os.Getenv("YOOKASSA_BIND_AMOUNT_RUB")),
		GracePeriod:     parseDurationEnv("BILLING_GRACE_PERIOD", 72*time.Hour),
	}

	if cfg.BindAmountRUB == "" {
		cfg.BindAmountRUB = "1.00"
	}
	if cfg.ReturnURL == "" {
		cfg.ReturnURL = "https://race-src.com/payment-return"
	}

	if strings.TrimSpace(os.Getenv("YOOKASSA_API_BASE")) != "" {
		cfg.YooKassaAPIBase = strings.TrimSpace(os.Getenv("YOOKASSA_API_BASE"))
	}
	if cfg.ShopID == "" || cfg.SecretKey == "" {
		log.Println("[billing] WARNING: YOOKASSA_SHOP_ID or YOOKASSA_SECRET_KEY is empty; payment creation will fail")
	}

	monthlyPrice := strings.TrimSpace(os.Getenv("PLAN_MONTHLY_PRICE_RUB"))
	if monthlyPrice == "" {
		monthlyPrice = "89.00"
	}
	quarterlyPrice := strings.TrimSpace(os.Getenv("PLAN_QUARTERLY_PRICE_RUB"))
	if quarterlyPrice == "" {
		quarterlyPrice = "249.00"
	}
	semiannualPrice := strings.TrimSpace(os.Getenv("PLAN_SEMIANNUAL_PRICE_RUB"))
	if semiannualPrice == "" {
		semiannualPrice = "439.00"
	}
	annualPrice := strings.TrimSpace(os.Getenv("PLAN_ANNUAL_PRICE_RUB"))
	if annualPrice == "" {
		annualPrice = "799.00"
	}

	return &Service{
		repo:     repo,
		producer: producer,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		cfg:           cfg,
		retrySchedule: parseRetryScheduleEnv("BILLING_RETRY_SCHEDULE", []time.Duration{15 * time.Minute, 6 * time.Hour, 24 * time.Hour}),
		plans: map[kafkacontracts.PlanCode]Plan{
			kafkacontracts.PlanCodeMonthly: {
				Code:         kafkacontracts.PlanCodeMonthly,
				Title:        "Подписка на сервис защищённого соединения (30 дней)",
				AmountValue:  monthlyPrice,
				Currency:     "RUB",
				DurationDays: 30,
			},
			kafkacontracts.PlanCodeQuarterly: {
				Code:         kafkacontracts.PlanCodeQuarterly,
				Title:        "Подписка на сервис защищённого соединения (90 дней)",
				AmountValue:  quarterlyPrice,
				Currency:     "RUB",
				DurationDays: 90,
			},
			kafkacontracts.PlanCodeSemiannual: {
				Code:         kafkacontracts.PlanCodeSemiannual,
				Title:        "Подписка на сервис защищённого соединения (180 дней)",
				AmountValue:  semiannualPrice,
				Currency:     "RUB",
				DurationDays: 180,
			},
			kafkacontracts.PlanCodeAnnual: {
				Code:         kafkacontracts.PlanCodeAnnual,
				Title:        "Подписка на сервис защищённого соединения (360 дней)",
				AmountValue:  annualPrice,
				Currency:     "RUB",
				DurationDays: 360,
			},
		},
	}
}

func (s *Service) HandleCreateSubscriptionCheckout(
	ctx context.Context,
	cmd *kafkacontracts.CreateSubscriptionCheckoutCommand,
) error {
	if cmd == nil {
		return fmt.Errorf("nil create checkout command")
	}

	plan, ok := s.plans[cmd.PlanCode]
	if !ok {
		return fmt.Errorf("unsupported plan code: %s", cmd.PlanCode)
	}

	commandID := strings.TrimSpace(cmd.CommandID)
	if commandID == "" {
		commandID = uuid.NewString()
	}
	orderID := commandID
	idempotenceKey := "checkout:" + commandID
	savePaymentMethod := cmd.SavePaymentMethod

	record := &PaymentRecord{
		OrderID:           orderID,
		TelegramID:        cmd.TelegramID,
		CheckoutType:      string(kafkacontracts.BillingCheckoutTypeSubscription),
		PlanCode:          string(cmd.PlanCode),
		DurationDays:      plan.DurationDays,
		Status:            "local_creating",
		AmountValue:       plan.AmountValue,
		Currency:          plan.Currency,
		Description:       plan.Title,
		IdempotenceKey:    idempotenceKey,
		SavePaymentMethod: savePaymentMethod,
		CommandID:         commandID,
		Metadata: map[string]string{
			"order_id":      orderID,
			"telegram_id":   strconv.FormatInt(cmd.TelegramID, 10),
			"plan_code":     string(cmd.PlanCode),
			"duration_days": strconv.Itoa(plan.DurationDays),
			"checkout_type": string(kafkacontracts.BillingCheckoutTypeSubscription),
			"charge_source": string(kafkacontracts.BillingChargeSourceInitial),
			"attempt_no":    "0",
		},
	}

	if err := s.repo.InsertPayment(ctx, record); err != nil {
		return err
	}

	resp, raw, err := s.createYooKassaPayment(ctx, &yooKassaCreatePaymentRequest{
		Amount: yooKassaAmount{
			Value:    plan.AmountValue,
			Currency: plan.Currency,
		},
		Capture: true,
		Confirmation: &yooKassaConfirmation{
			Type:      "redirect",
			ReturnURL: firstNotEmpty(cmd.ReturnURL, s.cfg.ReturnURL),
		},
		Description:       plan.Title,
		SavePaymentMethod: savePaymentMethod,
		Metadata:          record.Metadata,
	}, idempotenceKey)
	if err != nil {
		// YooKassa недоступна по любой причине — уведомляем пользователя и
		// прекращаем обработку без retry-шторма (см. notifyCardPaymentUnavailable).
		return s.notifyCardPaymentUnavailable(ctx, cmd.TelegramID, err)
	}

	if err := s.repo.UpdatePaymentCreated(
		ctx,
		orderID,
		resp.ID,
		resp.Status,
		resp.Confirmation.ConfirmationURL,
		resp.PaymentMethod.ID,
		raw,
	); err != nil {
		return err
	}

	if resp.Status == "succeeded" || resp.Paid {
		record.PaymentID = resp.ID
		record.Status = resp.Status
		return s.handlePaymentSucceeded(ctx, record, &yooKassaWebhookNotification{
			Type:  "notification",
			Event: "payment.succeeded",
			Object: yooKassaWebhookObject{
				ID:            resp.ID,
				Status:        resp.Status,
				Paid:          resp.Paid,
				PaymentMethod: resp.PaymentMethod,
			},
		})
	}

	return s.publishNotification(ctx, &kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		Message:    "💳 Ссылка на оплату готова. Нажмите кнопку ниже, чтобы перейти к оплате 👇",
		PayURL:     resp.Confirmation.ConfirmationURL,
	})
}

func (s *Service) HandleBindCard(
	ctx context.Context,
	cmd *kafkacontracts.BindCardCommand,
) error {
	if cmd == nil {
		return fmt.Errorf("nil bind card command")
	}

	commandID := strings.TrimSpace(cmd.CommandID)
	if commandID == "" {
		commandID = uuid.NewString()
	}
	orderID := commandID
	idempotenceKey := "bind-card:" + commandID

	record := &PaymentRecord{
		OrderID:           orderID,
		TelegramID:        cmd.TelegramID,
		CheckoutType:      string(kafkacontracts.BillingCheckoutTypeBindCard),
		Status:            "local_creating",
		AmountValue:       s.cfg.BindAmountRUB,
		Currency:          "RUB",
		Description:       "Привязка карты для будущих списаний",
		IdempotenceKey:    idempotenceKey,
		SavePaymentMethod: true,
		CommandID:         commandID,
		Metadata: map[string]string{
			"order_id":      orderID,
			"telegram_id":   strconv.FormatInt(cmd.TelegramID, 10),
			"checkout_type": string(kafkacontracts.BillingCheckoutTypeBindCard),
		},
	}

	if err := s.repo.InsertPayment(ctx, record); err != nil {
		return err
	}

	resp, raw, err := s.createYooKassaPayment(ctx, &yooKassaCreatePaymentRequest{
		Amount: yooKassaAmount{
			Value:    s.cfg.BindAmountRUB,
			Currency: "RUB",
		},
		Capture: true,
		Confirmation: &yooKassaConfirmation{
			Type:      "redirect",
			ReturnURL: firstNotEmpty(cmd.ReturnURL, s.cfg.ReturnURL),
		},
		Description:       "Привязка карты для будущих списаний",
		SavePaymentMethod: true,
		Metadata:          record.Metadata,
	}, idempotenceKey)
	if err != nil {
		// YooKassa недоступна по любой причине — уведомляем пользователя и
		// прекращаем обработку без retry-шторма (см. notifyCardPaymentUnavailable).
		return s.notifyCardPaymentUnavailable(ctx, cmd.TelegramID, err)
	}

	if err := s.repo.UpdatePaymentCreated(
		ctx,
		orderID,
		resp.ID,
		resp.Status,
		resp.Confirmation.ConfirmationURL,
		resp.PaymentMethod.ID,
		raw,
	); err != nil {
		return err
	}

	text := fmt.Sprintf(
		"🔗 Ссылка для привязки карты готова.\n\nПосле успешной привязки сервис автоматически вернет %s ₽.\n\n%s",
		s.cfg.BindAmountRUB,
		resp.Confirmation.ConfirmationURL,
	)
	return s.publishNotification(ctx, &kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		Message:    text,
	})
}

func (s *Service) HandleUnbindCard(
	ctx context.Context,
	cmd *kafkacontracts.UnbindCardCommand,
) error {
	if cmd == nil {
		return fmt.Errorf("nil unbind card command")
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := s.repo.ClearCustomerBindingTx(ctx, tx, cmd.TelegramID); err != nil {
		return err
	}
	if err := s.repo.DisableAutoRenewTx(ctx, tx, cmd.TelegramID, "payment_method_unbound"); err != nil {
		return err
	}
	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingPaymentMethodUnboundEvent{
		Type:       kafkacontracts.BillingEventPaymentMethodGone,
		TelegramID: cmd.TelegramID,
		UnboundAt:  time.Now().UTC(),
	}); err != nil {
		return err
	}
	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingAutoRenewDisabledEvent{
		Type:       kafkacontracts.BillingEventAutoRenewDisabled,
		TelegramID: cmd.TelegramID,
		Reason:     "payment_method_unbound",
		DisabledAt: time.Now().UTC(),
	}); err != nil {
		return err
	}
	if err := s.publishNotificationTx(ctx, tx, &kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		Message:    "✅ Сохраненный способ оплаты удален. Автопродление отключено, уже оплаченный период сохранится.",
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleDisableAutoRenew(ctx context.Context, cmd *kafkacontracts.DisableAutoRenewCommand) error {
	if cmd == nil {
		return fmt.Errorf("nil disable auto-renew command")
	}
	reason := firstNotEmpty(cmd.Reason, "disabled_by_command")

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := s.repo.DisableAutoRenewTx(ctx, tx, cmd.TelegramID, reason); err != nil {
		return err
	}
	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingAutoRenewDisabledEvent{
		Type:       kafkacontracts.BillingEventAutoRenewDisabled,
		TelegramID: cmd.TelegramID,
		Reason:     reason,
		DisabledAt: time.Now().UTC(),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) ProcessWebhook(ctx context.Context, n *yooKassaWebhookNotification, raw json.RawMessage, fingerprint string) error {
	if n == nil {
		return fmt.Errorf("nil webhook")
	}
	if n.Type != "notification" {
		return fmt.Errorf("unexpected webhook type: %s", n.Type)
	}
	if n.Object.ID == "" {
		return fmt.Errorf("empty payment id in webhook")
	}

	inserted, err := s.repo.RecordWebhookFingerprint(ctx, n.Object.ID, n.Event, fingerprint, raw)
	if err != nil {
		return err
	}
	if !inserted {
		return ErrDuplicateWebhook
	}

	// Обработку выносим в отдельный метод: если она упадёт транзиентно (таймаут
	// assertYooKassaPaymentPaid, блип БД), удаляем отпечаток, иначе повторный вебхук
	// YooKassa придёт с тем же fingerprint, попадёт в ErrDuplicateWebhook и событие
	// будет потеряно навсегда (деньги списаны, подписка не активирована).
	if err := s.processWebhookPayload(ctx, n, raw); err != nil {
		if delErr := s.repo.DeleteWebhookFingerprint(ctx, n.Object.ID, n.Event, fingerprint); delErr != nil {
			slog.Error("failed to delete webhook fingerprint after processing error",
				"payment_id", n.Object.ID, "event", n.Event, "delete_err", delErr, "orig_err", err)
		}
		return err
	}
	return nil
}

// processWebhookPayload выполняет фактическую обработку вебхука. Вызывается из
// ProcessWebhook уже после дедупликации; при ошибке вызывающий удаляет отпечаток,
// чтобы повторная доставка вебхука была обработана заново.
func (s *Service) processWebhookPayload(ctx context.Context, n *yooKassaWebhookNotification, raw json.RawMessage) error {
	record, err := s.repo.GetPaymentByPaymentID(ctx, n.Object.ID)
	if err != nil {
		return err
	}

	if err := s.repo.UpdatePaymentWebhookState(
		ctx,
		n.Object.ID,
		n.Object.Status,
		n.Object.PaymentMethod.ID,
		n.Object.CancellationDetails.Reason,
		raw,
	); err != nil {
		return err
	}

	switch n.Event {
	case "payment.succeeded":
		return s.handlePaymentSucceeded(ctx, record, n)

	case "payment.canceled":
		return s.handlePaymentCanceled(ctx, record, n)

	default:
		log.Printf("[billing] webhook ignored event=%s payment_id=%s", n.Event, n.Object.ID)
		return nil
	}
}

func (s *Service) handlePaymentSucceeded(
	ctx context.Context,
	record *PaymentRecord,
	n *yooKassaWebhookNotification,
) error {
	if record == nil || n == nil {
		return fmt.Errorf("nil payment success data")
	}
	commonmetrics.BillingPaymentsTotal.WithLabelValues("succeeded").Inc()

	switch record.CheckoutType {
	case string(kafkacontracts.BillingCheckoutTypeSubscription):
		return s.handleSubscriptionPaymentSucceeded(ctx, record, n)

	case string(kafkacontracts.BillingCheckoutTypeBindCard):
		return s.handleBindCardPaymentSucceeded(ctx, record, n)

	default:
		return fmt.Errorf("unknown checkout type: %s", record.CheckoutType)
	}
}

// assertYooKassaPaymentPaid запрашивает платёж у YooKassa и проверяет, что он реально
// оплачен. Возвращает ошибку, если статус не succeeded/paid — тогда обработка вебхука
// прерывается, и поддельная активация невозможна.
func (s *Service) assertYooKassaPaymentPaid(ctx context.Context, paymentID string) error {
	if strings.TrimSpace(paymentID) == "" {
		return fmt.Errorf("empty payment id for verification")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.YooKassaAPIBase+"/payments/"+paymentID, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.cfg.ShopID, s.cfg.SecretKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("verify yookassa payment http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("verify yookassa payment status http=%d body=%s", resp.StatusCode, string(raw))
	}

	var parsed yooKassaPaymentResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("decode verify payment: %w", err)
	}
	if parsed.Status != "succeeded" && !parsed.Paid {
		return fmt.Errorf("payment %s not actually paid: status=%s paid=%v", paymentID, parsed.Status, parsed.Paid)
	}
	return nil
}

func (s *Service) handleSubscriptionPaymentSucceeded(ctx context.Context, record *PaymentRecord, n *yooKassaWebhookNotification) error {
	// Authoritative-проверка: перепрашиваем статус платежа напрямую у YooKassa по нашему
	// secret key. Это защита от поддельного вебхука при утечке webhook-токена — YooKassa
	// тело вебхука не подписывает, поэтому доверять полю status из вебхука нельзя.
	if err := s.assertYooKassaPaymentPaid(ctx, record.PaymentID); err != nil {
		return err
	}

	paidAt := time.Now().UTC()
	chargeSource := billingChargeSourceFromMetadata(record.Metadata)
	attemptNo := intFromMetadata(record.Metadata, "attempt_no")
	paymentMethodID := firstNotEmpty(n.Object.PaymentMethod.ID, record.PaymentMethodID)
	// Автопродление включаем ТОЛЬКО если метод реально сохранён (saved=true).
	// YooKassa всегда присылает payment_method.id, но при save_payment_method=false
	// метод не сохранён и повторное списание по нему невозможно — иначе получим
	// recurring-профиль, который гарантированно упадёт при следующем списании.
	autoRenewEnabled := paymentMethodID != "" && n.Object.PaymentMethod.Saved
	fallbackNextChargeAt := paidAt.Add(time.Duration(record.DurationDays) * 24 * time.Hour)

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if autoRenewEnabled {
		if err := s.repo.SaveCustomerBindingTx(ctx, tx, record.TelegramID, paymentMethodID,
			n.Object.PaymentMethod.Type,
			n.Object.PaymentMethod.Card.Last4,
			n.Object.PaymentMethod.Card.ExpiryMonth,
			n.Object.PaymentMethod.Card.ExpiryYear,
		); err != nil {
			return err
		}

		if err := s.repo.UpsertRecurringProfileTx(ctx, tx, &RecurringProfile{
			TelegramID:       record.TelegramID,
			PlanCode:         kafkacontracts.PlanCode(record.PlanCode),
			DurationDays:     record.DurationDays,
			AmountValue:      record.AmountValue,
			Currency:         record.Currency,
			PaymentMethodID:  paymentMethodID,
			AutoRenewEnabled: true,
			Status:           "active",
			NextChargeAt:     &fallbackNextChargeAt,
			LastPaymentID:    record.PaymentID,
		}); err != nil {
			return err
		}

		if chargeSource == kafkacontracts.BillingChargeSourceRecurring {
			if err := s.repo.MarkRecurringSuccessTx(ctx, tx, record.TelegramID, record.PaymentID, fallbackNextChargeAt); err != nil {
				return err
			}
		}
	} else {
		if err := s.repo.DisableAutoRenewTx(ctx, tx, record.TelegramID, "payment_method_id_missing"); err != nil {
			return err
		}
	}

	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingPaymentSucceededEvent{
		Type:             kafkacontracts.BillingEventPaymentSucceeded,
		CheckoutType:     kafkacontracts.BillingCheckoutTypeSubscription,
		ChargeSource:     chargeSource,
		TelegramID:       record.TelegramID,
		OrderID:          record.OrderID,
		PaymentID:        record.PaymentID,
		PlanCode:         kafkacontracts.PlanCode(record.PlanCode),
		DurationDays:     record.DurationDays,
		AmountValue:      record.AmountValue,
		Currency:         record.Currency,
		PaymentMethodID:  paymentMethodID,
		AttemptNo:        attemptNo,
		AutoRenewEnabled: autoRenewEnabled,
		PaidAt:           paidAt,
		PaymentProvider:  string(kafkacontracts.BillingPaymentProviderYooKassa),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) handleBindCardPaymentSucceeded(ctx context.Context, record *PaymentRecord, n *yooKassaWebhookNotification) error {
	// Refund мы делаем вне транзакции с БД и outbox — у YooKassa собственная идемпотентность,
	// а если refund упал, мы кладём запись в billing_pending_refunds (тоже в основной tx).
	var refundErr error
	if record.AmountValue != "" && record.AmountValue != "0" && record.AmountValue != "0.00" {
		refundErr = s.createRefund(ctx, record.PaymentID, record.AmountValue, record.Currency)
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if n.Object.PaymentMethod.ID != "" {
		if err := s.repo.SaveCustomerBindingTx(ctx, tx,
			record.TelegramID,
			n.Object.PaymentMethod.ID,
			n.Object.PaymentMethod.Type,
			n.Object.PaymentMethod.Card.Last4,
			n.Object.PaymentMethod.Card.ExpiryMonth,
			n.Object.PaymentMethod.Card.ExpiryYear,
		); err != nil {
			return err
		}
	}

	if refundErr != nil {
		if addErr := s.repo.AddPendingRefundTx(ctx, tx, record.PaymentID, record.AmountValue, record.Currency, refundErr.Error()); addErr != nil {
			return addErr
		}
	}

	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingPaymentMethodBoundEvent{
		Type:            kafkacontracts.BillingEventPaymentMethodBound,
		TelegramID:      record.TelegramID,
		OrderID:         record.OrderID,
		PaymentID:       record.PaymentID,
		PaymentMethodID: n.Object.PaymentMethod.ID,
		Last4:           n.Object.PaymentMethod.Card.Last4,
		BoundAt:         time.Now().UTC(),
	}); err != nil {
		return err
	}
	if err := s.publishNotificationTx(ctx, tx, &kafkacontracts.TgNotification{
		TelegramID: record.TelegramID,
		Message:    "✅ Карта успешно привязана. Теперь можно оформить подписку.",
		Keyboard:   kafkacontracts.TgKeyboardBuyMenu,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) handlePaymentCanceled(ctx context.Context, record *PaymentRecord, n *yooKassaWebhookNotification) error {
	commonmetrics.BillingPaymentsTotal.WithLabelValues("canceled").Inc()
	chargeSource := billingChargeSourceFromMetadata(record.Metadata)
	attemptNo := intFromMetadata(record.Metadata, "attempt_no")
	reason := firstNotEmpty(n.Object.CancellationDetails.Reason, "payment_canceled")

	var nextRetryAt *time.Time
	var graceUntil *time.Time

	if record.CheckoutType == string(kafkacontracts.BillingCheckoutTypeSubscription) && chargeSource == kafkacontracts.BillingChargeSourceRecurring {
		retryAt, graceAt, err := s.handleRecurringPaymentFailed(ctx, record.TelegramID, kafkacontracts.PlanCode(record.PlanCode), record.PaymentID, attemptNo, reason)
		if err != nil {
			return err
		}
		nextRetryAt = retryAt
		graceUntil = graceAt
	}

	if err := s.publishBillingEvent(ctx, &kafkacontracts.BillingPaymentCanceledEvent{
		Type:            kafkacontracts.BillingEventPaymentCanceled,
		CheckoutType:    kafkacontracts.BillingCheckoutType(record.CheckoutType),
		ChargeSource:    chargeSource,
		TelegramID:      record.TelegramID,
		OrderID:         record.OrderID,
		PaymentID:       record.PaymentID,
		Reason:          reason,
		AttemptNo:       attemptNo,
		NextRetryAt:     nextRetryAt,
		GraceUntil:      graceUntil,
		CanceledAt:      time.Now().UTC(),
		PaymentProvider: string(kafkacontracts.BillingPaymentProviderYooKassa),
	}); err != nil {
		return err
	}

	if chargeSource == kafkacontracts.BillingChargeSourceRecurring {
		return nil
	}

	return s.publishNotification(ctx, &kafkacontracts.TgNotification{
		TelegramID: record.TelegramID,
		Message:    "Платеж был отменен или не завершен. Вернись в бота и попробуй еще раз.",
		Keyboard:   kafkacontracts.TgKeyboardBuyMenu,
	})
}

func (s *Service) ProcessDueRenewals(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 50
	}
	profiles, err := s.repo.LockDueRenewals(ctx, time.Now().UTC(), limit)
	if err != nil {
		return err
	}

	for _, p := range profiles {
		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := s.chargeRecurringProfile(opCtx, &p); err != nil {
			log.Printf("[billing] recurring charge failed telegram_id=%d err=%v", p.TelegramID, err)
		}
		cancel()
	}

	return nil
}

func (s *Service) ProcessExpiredGrace(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 50
	}
	profiles, err := s.repo.LockExpiredGraceProfiles(ctx, time.Now().UTC(), limit)
	if err != nil {
		return err
	}

	for _, p := range profiles {
		reason := firstNotEmpty(p.LastFailureReason, "grace_period_expired")

		tx, err := s.repo.BeginTx(ctx)
		if err != nil {
			slog.Error("billing begin tx for suspend failed", "telegram_id", p.TelegramID, "err", err)
			continue
		}
		if err := s.repo.SuspendExpiredGraceTx(ctx, tx, p.TelegramID, reason); err != nil {
			slog.Error("billing suspend grace profile failed", "telegram_id", p.TelegramID, "err", err)
			_ = tx.Rollback(ctx)
			continue
		}
		if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingAccessExpiredEvent{
			Type:       kafkacontracts.BillingEventAccessExpired,
			TelegramID: p.TelegramID,
			Reason:     reason,
			ExpiredAt:  time.Now().UTC(),
		}); err != nil {
			slog.Error("billing publish access expired enqueue failed", "telegram_id", p.TelegramID, "err", err)
			_ = tx.Rollback(ctx)
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			slog.Error("billing commit suspend grace tx failed", "telegram_id", p.TelegramID, "err", err)
		}
	}

	return nil
}

func (s *Service) HandleSubscriptionActivated(ctx context.Context, event *kafkacontracts.SubscriptionActivatedEvent) error {
	if event == nil || !event.AutoRenewEnabled {
		return nil
	}
	return s.repo.UpdateNextChargeAt(ctx, event.TelegramID, event.ActiveUntil)
}

func (s *Service) chargeRecurringProfile(ctx context.Context, p *RecurringProfile) error {
	if p == nil {
		return fmt.Errorf("nil recurring profile")
	}
	if p.PaymentMethodID == "" || !p.AutoRenewEnabled {
		return nil
	}

	attemptNo := p.RetryCount + 1
	orderID := fmt.Sprintf("renewal:%d:%s:%d", p.TelegramID, p.PlanCode, attemptNo)
	idempotenceKey := orderID
	description := fmt.Sprintf("Автопродление подписки на сервис защищённого соединения (%s)", p.PlanCode)

	record := &PaymentRecord{
		OrderID:           orderID,
		TelegramID:        p.TelegramID,
		CheckoutType:      string(kafkacontracts.BillingCheckoutTypeSubscription),
		PlanCode:          string(p.PlanCode),
		DurationDays:      p.DurationDays,
		Status:            "local_creating",
		AmountValue:       p.AmountValue,
		Currency:          p.Currency,
		Description:       description,
		IdempotenceKey:    idempotenceKey,
		SavePaymentMethod: false,
		PaymentMethodID:   p.PaymentMethodID,
		Metadata: map[string]string{
			"order_id":      orderID,
			"telegram_id":   strconv.FormatInt(p.TelegramID, 10),
			"plan_code":     string(p.PlanCode),
			"duration_days": strconv.Itoa(p.DurationDays),
			"checkout_type": string(kafkacontracts.BillingCheckoutTypeSubscription),
			"charge_source": string(kafkacontracts.BillingChargeSourceRecurring),
			"attempt_no":    strconv.Itoa(attemptNo),
		},
	}

	if err := s.repo.InsertPayment(ctx, record); err != nil {
		return err
	}

	resp, raw, err := s.createYooKassaPayment(ctx, &yooKassaCreatePaymentRequest{
		Amount: yooKassaAmount{
			Value:    p.AmountValue,
			Currency: p.Currency,
		},
		Capture:         true,
		PaymentMethodID: p.PaymentMethodID,
		Description:     description,
		Metadata:        record.Metadata,
	}, idempotenceKey)
	if err != nil {
		_, _, failErr := s.handleRecurringPaymentFailed(ctx, p.TelegramID, p.PlanCode, "", attemptNo, err.Error())
		if failErr != nil {
			return failErr
		}
		return err
	}

	if err := s.repo.UpdatePaymentCreated(
		ctx,
		orderID,
		resp.ID,
		resp.Status,
		resp.Confirmation.ConfirmationURL,
		resp.PaymentMethod.ID,
		raw,
	); err != nil {
		return err
	}

	record.PaymentID = resp.ID
	record.Status = resp.Status
	if resp.Status == "succeeded" || resp.Paid {
		return s.handlePaymentSucceeded(ctx, record, &yooKassaWebhookNotification{
			Type:  "notification",
			Event: "payment.succeeded",
			Object: yooKassaWebhookObject{
				ID:            resp.ID,
				Status:        resp.Status,
				Paid:          resp.Paid,
				PaymentMethod: firstPaymentMethod(resp.PaymentMethod, yooKassaPaymentMethodRef{ID: p.PaymentMethodID}),
			},
		})
	}

	if resp.Status == "canceled" {
		_, _, err := s.handleRecurringPaymentFailed(ctx, p.TelegramID, p.PlanCode, resp.ID, attemptNo, "payment_canceled")
		return err
	}

	return nil
}

func (s *Service) handleRecurringPaymentFailed(ctx context.Context, telegramID int64, planCode kafkacontracts.PlanCode, paymentID string, attemptNo int, reason string) (*time.Time, *time.Time, error) {
	now := time.Now().UTC()

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if attemptNo <= len(s.retrySchedule) {
		nextRetryAt := now.Add(s.retrySchedule[attemptNo-1])
		if err := s.repo.ScheduleRenewalRetryTx(ctx, tx, telegramID, nextRetryAt, attemptNo, reason, paymentID); err != nil {
			return nil, nil, err
		}
		if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingRenewalRetryScheduledEvent{
			Type:        kafkacontracts.BillingEventRenewalRetryScheduled,
			TelegramID:  telegramID,
			PlanCode:    planCode,
			PaymentID:   paymentID,
			Reason:      reason,
			AttemptNo:   attemptNo,
			NextRetryAt: nextRetryAt,
			CreatedAt:   now,
		}); err != nil {
			return nil, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, err
		}
		return &nextRetryAt, nil, nil
	}

	graceUntil := now.Add(s.cfg.GracePeriod)
	if err := s.repo.StartGracePeriodTx(ctx, tx, telegramID, graceUntil, reason, paymentID); err != nil {
		return nil, nil, err
	}
	if err := s.publishBillingEventTx(ctx, tx, &kafkacontracts.BillingGraceStartedEvent{
		Type:       kafkacontracts.BillingEventGraceStarted,
		TelegramID: telegramID,
		PlanCode:   planCode,
		PaymentID:  paymentID,
		Reason:     reason,
		GraceUntil: graceUntil,
		StartedAt:  now,
	}); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return nil, &graceUntil, nil
}

func (s *Service) ProcessPendingRefunds(ctx context.Context, limit int) error {
	items, err := s.repo.LockDueRefunds(ctx, time.Now().UTC(), limit)
	if err != nil {
		return err
	}
	for _, item := range items {
		opCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := s.createRefund(opCtx, item.PaymentID, item.AmountValue, item.Currency)
		cancel()
		if err != nil {
			_ = s.repo.MarkRefundRetry(ctx, item.PaymentID, err.Error(), item.Attempts)
			continue
		}
		_ = s.repo.MarkRefundSucceeded(ctx, item.PaymentID)
	}
	return nil
}

func (s *Service) createYooKassaPayment(
	ctx context.Context,
	reqBody *yooKassaCreatePaymentRequest,
	idempotenceKey string,
) (*yooKassaPaymentResponse, json.RawMessage, error) {
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.YooKassaAPIBase+"/payments", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, nil, err
	}

	req.SetBasicAuth(s.cfg.ShopID, s.cfg.SecretKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotence-Key", idempotenceKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, raw, fmt.Errorf("yookassa create payment http=%d body=%s", resp.StatusCode, string(raw))
	}

	var parsed yooKassaPaymentResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, raw, err
	}

	return &parsed, raw, nil
}

func (s *Service) createRefund(
	ctx context.Context,
	paymentID string,
	amountValue string,
	currency string,
) error {
	body, err := json.Marshal(map[string]any{
		"payment_id": paymentID,
		"amount": map[string]string{
			"value":    amountValue,
			"currency": currency,
		},
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.YooKassaAPIBase+"/refunds", bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.SetBasicAuth(s.cfg.ShopID, s.cfg.SecretKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotence-Key", "refund:"+paymentID)

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("yookassa refund http=%d body=%s", resp.StatusCode, string(raw))
	}

	return nil
}

func (s *Service) publishNotificationTx(ctx context.Context, tx pgx.Tx, n *kafkacontracts.TgNotification) error {
	if n == nil {
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

func (s *Service) publishNotification(ctx context.Context, n *kafkacontracts.TgNotification) error {
	if n == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.publishNotificationTx(ctx, tx, n); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func billingChargeSourceFromMetadata(meta map[string]string) kafkacontracts.BillingChargeSource {
	if meta == nil {
		return kafkacontracts.BillingChargeSourceInitial
	}
	if meta["charge_source"] == string(kafkacontracts.BillingChargeSourceRecurring) {
		return kafkacontracts.BillingChargeSourceRecurring
	}
	return kafkacontracts.BillingChargeSourceInitial
}

func intFromMetadata(meta map[string]string, key string) int {
	if meta == nil {
		return 0
	}
	v, err := strconv.Atoi(meta[key])
	if err != nil {
		return 0
	}
	return v
}

func firstPaymentMethod(primary, fallback yooKassaPaymentMethodRef) yooKassaPaymentMethodRef {
	if primary.ID != "" {
		return primary
	}
	return fallback
}

func firstNotEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("[billing] invalid duration env %s=%q, use fallback %s", key, v, fallback)
		return fallback
	}
	return d
}

func parseRetryScheduleEnv(key string, fallback []time.Duration) []time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	result := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || d <= 0 {
			log.Printf("[billing] invalid retry schedule env %s=%q, use fallback", key, v)
			return fallback
		}
		result = append(result, d)
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}

type yooKassaCreatePaymentRequest struct {
	Amount            yooKassaAmount        `json:"amount"`
	Capture           bool                  `json:"capture"`
	Confirmation      *yooKassaConfirmation `json:"confirmation,omitempty"`
	PaymentMethodID   string                `json:"payment_method_id,omitempty"`
	Description       string                `json:"description,omitempty"`
	SavePaymentMethod bool                  `json:"save_payment_method,omitempty"`
	Metadata          map[string]string     `json:"metadata,omitempty"`
}

type yooKassaAmount struct {
	Value    string `json:"value"`
	Currency string `json:"currency"`
}

type yooKassaConfirmation struct {
	Type      string `json:"type"`
	ReturnURL string `json:"return_url,omitempty"`
}

type yooKassaPaymentResponse struct {
	ID            string                   `json:"id"`
	Status        string                   `json:"status"`
	Paid          bool                     `json:"paid"`
	Confirmation  yooKassaConfirmationResp `json:"confirmation"`
	PaymentMethod yooKassaPaymentMethodRef `json:"payment_method"`
}

type yooKassaConfirmationResp struct {
	Type            string `json:"type"`
	ConfirmationURL string `json:"confirmation_url"`
}

type yooKassaPaymentMethodRef struct {
	ID    string                    `json:"id"`
	Type  string                    `json:"type"`
	Saved bool                      `json:"saved"`
	Card  yooKassaPaymentMethodCard `json:"card"`
}

type yooKassaPaymentMethodCard struct {
	Last4       string `json:"last4"`
	ExpiryMonth string `json:"expiry_month"`
	ExpiryYear  string `json:"expiry_year"`
}

type yooKassaWebhookNotification struct {
	Type   string                `json:"type"`
	Event  string                `json:"event"`
	Object yooKassaWebhookObject `json:"object"`
}

type yooKassaWebhookObject struct {
	ID                  string                      `json:"id"`
	Status              string                      `json:"status"`
	Paid                bool                        `json:"paid"`
	PaymentMethod       yooKassaPaymentMethodRef    `json:"payment_method"`
	CancellationDetails yooKassaCancellationDetails `json:"cancellation_details"`
}

type yooKassaCancellationDetails struct {
	Reason string `json:"reason"`
	Party  string `json:"party"`
}

func (s *Service) publishBillingEventTx(ctx context.Context, tx pgx.Tx, event any) error {
	key := "billing"
	eventType := "billing.event"

	switch e := event.(type) {
	case *kafkacontracts.BillingPaymentSucceededEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingPaymentCanceledEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingPaymentMethodBoundEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingPaymentMethodUnboundEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingRenewalRetryScheduledEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingGraceStartedEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingAccessExpiredEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.BillingAutoRenewDisabledEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	}

	return outbox.AddTx(ctx, tx, outbox.Event{
		AggregateType: "billing",
		AggregateID:   key,
		Topic:         commonkafka.TopicBillingEvents,
		MessageKey:    key,
		EventType:     eventType,
		Payload:       event,
	})
}

func (s *Service) publishBillingEvent(ctx context.Context, event any) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.publishBillingEventTx(ctx, tx, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// notifyCardPaymentUnavailable уведомляет пользователя о временной недоступности
// оплаты картой и возвращает ошибку, обёрнутую в ErrSkip.
//
// Вызывается, когда обращение к YooKassa завершилось ошибкой по ЛЮБОЙ причине
// (пустые креды, API недоступен, сеть, ошибка статуса). Возврат ErrSkip важен:
// он предотвращает повторные попытки consumer'а (иначе пользователь получил бы
// несколько одинаковых уведомлений). Команда уйдёт в DLT для аудита, а денег
// у пользователя не списалось (платёж в YooKassa не создан).
//
// Когда YooKassa снова станет доступна, createYooKassaPayment начнёт возвращать
// успех, и это уведомление само перестанет отправляться — без правок кода.
func (s *Service) notifyCardPaymentUnavailable(ctx context.Context, telegramID int64, cause error) error {
	slog.Error("billing: card payment unavailable",
		"telegram_id", telegramID, "err", cause)

	// ВРЕМЕННО ОТКЛЮЧЕНО: уведомление пользователю о недоступности карты теперь
	// показывает сам бот (с деталями тарифа и фолбэк-ссылкой). Чтобы вернуть
	// отправку из billing — раскомментируй блок ниже.
	//
	// const text = "💳 Оплата картой временно недоступна.\n\n" +
	// 	"Пожалуйста, попробуйте позже или воспользуйтесь другим способом оплаты."
	//
	// if notifyErr := s.publishNotification(ctx, &kafkacontracts.TgNotification{
	// 	TelegramID: telegramID,
	// 	Message:    text,
	// 	Keyboard:   kafkacontracts.TgKeyboardBuyMenu,
	// }); notifyErr != nil {
	// 	slog.Error("billing: failed to send card-unavailable notification",
	// 		"telegram_id", telegramID, "err", notifyErr)
	// }

	// Возврат ErrSkip сохранён: если billing-команда всё же попадёт в очередь,
	// она не будет ретраиться 20 раз, а уйдёт в DLT.
	return fmt.Errorf("%w: card payment unavailable: %w", commonkafka.ErrSkip, cause)
}
