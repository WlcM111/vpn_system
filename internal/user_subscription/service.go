package user_subscription

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/outbox"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Service struct {
	repo                  *Repository
	producer              *commonkafka.Producer
	publicBaseURL         string
	defaultCountry        string
	trialDays             int
	referralUsersPerMonth int
	referralDaysPerMonth  int
}

func NewService(repo *Repository, publicBaseURL, defaultCountry string) *Service {
	if defaultCountry == "" {
		defaultCountry = "LT"
	}
	publicBaseURL = normalizeBaseURL(publicBaseURL)
	if publicBaseURL == "" {
		publicBaseURL = "http://localhost:8084/sub/"
	}

	// Длительность триала из env, по умолчанию 1 день. Короткий триал снижает
	// ценность фарма через новые Telegram-аккаунты (главный фрод-вектор сервиса).
	trialDays := 1
	if raw := strings.TrimSpace(os.Getenv("TRIAL_DAYS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			trialDays = n
		}
	}

	// N приглашённых на 1 бесплатный месяц и длина месяца в днях — из env.
	referralUsersPerMonth := 1
	if raw := strings.TrimSpace(os.Getenv("REFERRAL_USERS_PER_MONTH")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			referralUsersPerMonth = n
		}
	}
	referralDaysPerMonth := 30
	if raw := strings.TrimSpace(os.Getenv("REFERRAL_DAYS_PER_MONTH")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			referralDaysPerMonth = n
		}
	}

	return &Service{
		repo:                  repo,
		publicBaseURL:         publicBaseURL,
		defaultCountry:        defaultCountry,
		trialDays:             trialDays,
		referralUsersPerMonth: referralUsersPerMonth,
		referralDaysPerMonth:  referralDaysPerMonth,
	}
}

func (s *Service) SetProducer(producer *commonkafka.Producer) {
	s.producer = producer
}

// HandleReferralAttribute фиксирует переход приглашённого по коду реферера (pending).
func (s *Service) HandleReferralAttribute(ctx context.Context, cmd *kafkacontracts.ReferralAttributeCommand) error {
	if cmd == nil || cmd.RefereeTelegramID == 0 || cmd.ReferrerCode == "" {
		return nil
	}
	referrerID, attributed, err := s.repo.AttributeReferral(ctx, cmd.RefereeTelegramID, cmd.ReferrerCode)
	if err != nil {
		return err
	}
	if attributed {
		slog.Info("referral attributed", "referrer", referrerID, "referee", cmd.RefereeTelegramID)
	}
	return nil
}

// HandleReferralRedeem начисляет доступные бесплатные месяцы (или сообщает, что их нет).
func (s *Service) HandleReferralRedeem(ctx context.Context, cmd *kafkacontracts.ReferralRedeemCommand) error {
	if cmd == nil || cmd.TelegramID == 0 {
		return nil
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	granted, newExpiresAt, err := s.repo.RedeemReferralMonthsTx(
		ctx, tx, cmd.TelegramID, s.defaultCountry, s.referralUsersPerMonth, s.referralDaysPerMonth,
	)
	if err != nil {
		return err
	}

	if granted == 0 {
		hint := fmt.Sprintf(
			"Приглашайте друзей: за каждые *%d* оплативших подписку — *1 месяц* бесплатно.",
			s.referralUsersPerMonth,
		)
		if s.referralUsersPerMonth == 1 {
			hint = "Приглашайте друзей: каждый друг, оформивший платную подписку, — это *1 месяц бесплатно* для вас."
		}
		if nErr := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			ParseMode:  "Markdown",
			Message:    "У вас пока нет доступных бесплатных месяцев.\n\n" + hint,
		}); nErr != nil {
			return nErr
		}
		return tx.Commit(ctx)
	}

	if nErr := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		ParseMode:  "Markdown",
		Message: fmt.Sprintf(
			"🎁 Начислено бесплатных месяцев: *%d*!\n\nПодписка активна до *%s*.",
			granted, newExpiresAt.Format("02.01.2006"),
		),
	}); nErr != nil {
		return nErr
	}

	// Событие активации — чтобы оркестратор продлил доступ на нодах под новый срок.
	// Читаем актуальное состояние в той же транзакции.
	state, stErr := s.repo.getStateForUpdateTx(ctx, tx, cmd.TelegramID)
	if stErr == nil && state != nil && state.ExpiresAt != nil {
		if evErr := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionActivatedEvent{
			Type:        kafkacontracts.SubscriptionEventActivated,
			TelegramID:  cmd.TelegramID,
			PlanCode:    state.CurrentPlanCode,
			ActivatedAt: time.Now().UTC(),
			ActiveUntil: *state.ExpiresAt,
			DaysLeft:    state.DaysLeft,
			Country:     state.CountryCode,
			Source:      "referral_reward",
			AccessRev:   state.AccessRev,
		}); evErr != nil {
			return evErr
		}
	}

	return tx.Commit(ctx)
}

func normalizeBaseURL(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if !strings.HasSuffix(v, "/") {
		v += "/"
	}
	return v
}

func (s *Service) HandleStartTrial(ctx context.Context, cmd *kafkacontracts.StartTrialCommand) error {
	if cmd == nil {
		return nil
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, activated, err := s.repo.StartTrialTx(ctx, tx, cmd.TelegramID, s.defaultCountry, s.trialDays)
	if err != nil {
		return err
	}

	if !activated {
		if state.Status == StatusTrial || state.Status == StatusActive || state.Status == StatusGrace {
			if err := s.sendStatusNotificationTx(ctx, tx, state); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
		if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			Message:    "Похоже, ты уже использовал пробный период ранее.\n\nТеперь доступ можно получить только через платную подписку 👇",
			Keyboard:   kafkacontracts.TgKeyboardTrialOrBuy,
		}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if state.ExpiresAt != nil {
		if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionTrialStartedEvent{
			Type:       kafkacontracts.SubscriptionEventTrialStarted,
			TelegramID: cmd.TelegramID,
			TrialUntil: *state.ExpiresAt,
			DaysLeft:   state.DaysLeft,
			Country:    state.CountryCode,
			AccessRev:  state.AccessRev,
		}); err != nil {
			return err
		}
	}

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		ParseMode:  "Markdown",
		Message: fmt.Sprintf(
			"✨ Пробный период на *%d дн.* активирован!\nПрофиль подключения по умолчанию: 🇱🇹 Литва\nДоступность профилей может меняться по техническим причинам.\nДействует до: *%s*.\n\nСейчас пришлю ссылку доступа для настройки приложения.",
			s.trialDays,
			state.ExpiresAt.Format("02.01.2006"),
		),
		Keyboard: kafkacontracts.TgKeyboardMySubscriptionConfig,
	}); err != nil {
		return err
	}

	if err := s.sendLinksNotificationTx(ctx, tx, state); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleGetStatus(ctx context.Context, cmd *kafkacontracts.GetStatusCommand) error {
	if cmd == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := s.repo.GetStatusTx(ctx, tx, cmd.TelegramID, s.defaultCountry)
	if err != nil {
		return err
	}
	if err := s.sendStatusNotificationTx(ctx, tx, state); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleGetLinks(ctx context.Context, cmd *kafkacontracts.GetLinksCommand) error {
	if cmd == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := s.repo.GetStatusTx(ctx, tx, cmd.TelegramID, s.defaultCountry)
	if err != nil {
		return err
	}
	if err := s.sendLinksNotificationTx(ctx, tx, state); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleCancel(ctx context.Context, cmd *kafkacontracts.CancelSubscriptionCommand) error {
	if cmd == nil {
		return nil
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, changed, err := s.repo.CancelTx(ctx, tx, cmd.TelegramID, s.defaultCountry)
	if err != nil {
		return err
	}

	if !changed {
		if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			Message:    "Похоже, у тебя нет активной подписки, которую можно отменить.",
			Keyboard:   kafkacontracts.TgKeyboardMainMenuWithBack,
		}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	cancelAtPeriodEnd := state.CancelAtPeriodEnd && state.Status != StatusExpired
	if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionCanceledEvent{
		Type:              kafkacontracts.SubscriptionEventCanceled,
		TelegramID:        cmd.TelegramID,
		CanceledAt:        time.Now().UTC(),
		AccessUntil:       cloneTime(state.ExpiresAt),
		CancelAtPeriodEnd: cancelAtPeriodEnd,
		AccessRev:         state.AccessRev,
	}); err != nil {
		return err
	}

	if cancelAtPeriodEnd {
		if err := s.publishBillingCommandTx(ctx, tx, cmd.TelegramID, &kafkacontracts.DisableAutoRenewCommand{
			Type:       kafkacontracts.BillingCommandDisableAutoRenew,
			CommandID:  uuid.NewString(),
			TelegramID: cmd.TelegramID,
			Reason:     "user_cancelled_subscription",
		}); err != nil {
			return err
		}

		until := "конца оплаченного периода"
		if state.ExpiresAt != nil {
			until = state.ExpiresAt.Format("02.01.2006")
		}
		if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: cmd.TelegramID,
			ParseMode:  "Markdown",
			Message:    fmt.Sprintf("✅ Автопродление отключено.\nДоступ сохранится до *%s*.\nНовых автоматических списаний не будет.", until),
			Keyboard:   kafkacontracts.TgKeyboardMainMenuWithBack,
		}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		Message:    "✅ Пробный период остановлен. Доступ отключен.",
		Keyboard:   kafkacontracts.TgKeyboardMainMenu,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleBillingPaymentSucceeded(ctx context.Context, event *kafkacontracts.BillingPaymentSucceededEvent) error {
	if event == nil || event.CheckoutType != kafkacontracts.BillingCheckoutTypeSubscription {
		return nil
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, applied, err := s.repo.ActivatePaidTx(ctx, tx,
		event.TelegramID, s.defaultCountry, string(event.PlanCode),
		event.DurationDays, event.PaymentID, event.AutoRenewEnabled,
	)
	if err != nil {
		return err
	}

	if !applied {
		slog.Info("user-subscription duplicate payment ignored", "telegram_id", event.TelegramID, "payment_id", event.PaymentID)
		return tx.Commit(ctx)
	}

	// Реферальная атрибуция: если пользователь был приглашён и это его ПЕРВАЯ платная
	// активация (applied=true), засчитываем конверсию рефереру в той же транзакции —
	// атомарно и ровно один раз. Триал сюда не попадает (для него ActivatePaidTx не
	// вызывается). Ошибка реферальной логики не должна ломать активацию: логируем.
	if referrerID, converted, refErr := s.repo.MarkReferralConvertedTx(ctx, tx, event.TelegramID); refErr != nil {
		slog.Error("referral conversion failed", "err", refErr, "referee", event.TelegramID)
	} else if converted {
		slog.Info("referral converted", "referrer", referrerID, "referee", event.TelegramID)
		if nErr := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: referrerID,
			ParseMode:  "Markdown",
			Message: "🎉 По вашей реферальной ссылке оформлена подписка!\n\n" +
				"Загляните в раздел *Реферальная программа* — возможно, доступен бесплатный месяц.",
		}); nErr != nil {
			slog.Error("notify referrer failed", "err", nErr, "referrer", referrerID)
		}
	}

	if state.ExpiresAt != nil {
		if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionActivatedEvent{
			Type:             kafkacontracts.SubscriptionEventActivated,
			TelegramID:       event.TelegramID,
			PlanCode:         string(event.PlanCode),
			ActivatedAt:      time.Now().UTC(),
			ActiveUntil:      *state.ExpiresAt,
			DaysLeft:         state.DaysLeft,
			Country:          state.CountryCode,
			Source:           string(event.ChargeSource),
			PaymentID:        event.PaymentID,
			AutoRenewEnabled: event.AutoRenewEnabled,
			AccessRev:        state.AccessRev,
		}); err != nil {
			return err
		}
	}

	messagePrefix := "✅ Оплата прошла успешно."
	if event.ChargeSource == kafkacontracts.BillingChargeSourceRecurring {
		messagePrefix = "✅ Автопродление прошло успешно."
	}

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: event.TelegramID,
		ParseMode:  "Markdown",
		Message: fmt.Sprintf(
			"%s\nПодписка активна до *%s*.\nОсталось дней: *%d*.\n\nСейчас пришлю ссылку доступа для настройки приложения.",
			messagePrefix,
			state.ExpiresAt.Format("02.01.2006"),
			state.DaysLeft,
		),
		Keyboard: kafkacontracts.TgKeyboardMySubscriptionConfig,
	}); err != nil {
		return err
	}

	if err := s.sendLinksNotificationTx(ctx, tx, state); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleBillingPaymentMethodUnbound(ctx context.Context, event *kafkacontracts.BillingPaymentMethodUnboundEvent) error {
	if event == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := s.repo.MarkAutoRenewDisabledTx(ctx, tx, event.TelegramID, s.defaultCountry)
	if err != nil {
		return err
	}
	if err := s.notifyAutoRenewDisabledTx(ctx, tx, state, "✅ Карта отвязана. Автопродление отключено."); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleBillingAutoRenewDisabled(ctx context.Context, event *kafkacontracts.BillingAutoRenewDisabledEvent) error {
	if event == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := s.repo.MarkAutoRenewDisabledTx(ctx, tx, event.TelegramID, s.defaultCountry)
	if err != nil {
		return err
	}
	if err := s.notifyAutoRenewDisabledTx(ctx, tx, state, "✅ Автопродление отключено."); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleBillingGraceStarted(ctx context.Context, event *kafkacontracts.BillingGraceStartedEvent) error {
	if event == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := s.repo.MarkGraceStartedTx(ctx, tx, event.TelegramID, s.defaultCountry, event.GraceUntil, event.Reason)
	if err != nil {
		return err
	}

	if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionGraceStartedEvent{
		Type:       kafkacontracts.SubscriptionEventGraceStarted,
		TelegramID: event.TelegramID,
		GraceUntil: event.GraceUntil,
		Reason:     event.Reason,
		AccessRev:  state.AccessRev,
	}); err != nil {
		return err
	}

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: event.TelegramID,
		ParseMode:  "Markdown",
		Message: fmt.Sprintf(
			"⚠️ Не удалось автоматически продлить подписку.\nМы временно сохранили доступ до *%s*.\nЧтобы доступ не остановился, оформи оплату повторно в меню бота.",
			event.GraceUntil.Format("02.01.2006"),
		),
		Keyboard: kafkacontracts.TgKeyboardBuyMenu,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) HandleBillingAccessExpired(ctx context.Context, event *kafkacontracts.BillingAccessExpiredEvent) error {
	if event == nil {
		return nil
	}
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := s.repo.MarkSuspendedTx(ctx, tx, event.TelegramID, s.defaultCountry, event.Reason)
	if err != nil {
		return err
	}

	if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionSuspendedEvent{
		Type:        kafkacontracts.SubscriptionEventSuspended,
		TelegramID:  event.TelegramID,
		SuspendedAt: event.ExpiredAt,
		Reason:      event.Reason,
		AccessRev:   state.AccessRev,
	}); err != nil {
		return err
	}

	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: event.TelegramID,
		Message:    "⛔ Доступ остановлен, потому что оплату не удалось продлить вовремя.\n\nЧтобы снова включить доступ, оформи подписку заново.",
		Keyboard:   kafkacontracts.TgKeyboardTrialOrBuy,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) notifyAutoRenewDisabledTx(ctx context.Context, tx pgx.Tx, state *SubscriptionState, prefix string) error {
	if state == nil {
		return nil
	}
	if state.Status == StatusActive || state.Status == StatusGrace {
		until := "конца оплаченного периода"
		if state.ExpiresAt != nil {
			until = state.ExpiresAt.Format("02.01.2006")
		}
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			ParseMode:  "Markdown",
			Message:    fmt.Sprintf("%s\nДоступ сохранится до *%s*.", prefix, until),
			Keyboard:   kafkacontracts.TgKeyboardMainMenuWithBack,
		})
	}
	return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: state.TelegramID,
		Message:    prefix,
		Keyboard:   kafkacontracts.TgKeyboardMainMenuWithBack,
	})
}

func (s *Service) sendStatusNotificationTx(ctx context.Context, tx pgx.Tx, state *SubscriptionState) error {
	if state == nil {
		return nil
	}

	switch state.Status {
	case StatusNone:
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			Message: "Пока у тебя нет активной подписки.\n\n" +
				"Ты можешь:\n" +
				fmt.Sprintf("• оформить пробный период на %d дн.,\n", s.trialDays) +
				"• или купить подписку.\n\n" +
				"Выбирай 👇",
			Keyboard: kafkacontracts.TgKeyboardTrialOrBuy,
		})

	case StatusTrial:
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			ParseMode:  "Markdown",
			Message: fmt.Sprintf(
				"✨ У тебя активен пробный период.\nПрофиль подключения по умолчанию: 🇱🇹 Литва\nДоступность профилей может меняться по техническим причинам.\nДействует до: *%s*\nОсталось дней: *%d*.\n\nНажми «🔗 Получить ссылку доступа», чтобы получить ссылку для настройки приложения.",
				state.ExpiresAt.Format("02.01.2006"),
				state.DaysLeft,
			),
			Keyboard: kafkacontracts.TgKeyboardMySubscriptionConfig,
		})

	case StatusActive:
		postfix := "Автопродление включено."
		if !state.AutoRenewEnabled || state.CancelAtPeriodEnd {
			postfix = "Автопродление отключено. Доступ сохранится до конца оплаченного периода."
		}
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			ParseMode:  "Markdown",
			Message: fmt.Sprintf(
				"✅ У тебя активная подписка.\nПрофиль подключения по умолчанию: 🇱🇹 Литва\nДоступность профилей может меняться по техническим причинам.\nДействует до: *%s*\nОсталось дней: *%d*.\n%s\n\nНажми «🔗 Получить ссылку доступа», чтобы получить ссылку для настройки приложения.",
				state.ExpiresAt.Format("02.01.2006"),
				state.DaysLeft,
				postfix,
			),
			Keyboard: kafkacontracts.TgKeyboardMySubscriptionConfig,
		})

	case StatusGrace:
		graceUntil := "-"
		if state.GraceUntil != nil {
			graceUntil = state.GraceUntil.Format("02.01.2006")
		}
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			ParseMode:  "Markdown",
			Message: fmt.Sprintf(
				"⚠️ Подписка находится в льготном периоде.\nПоследнее автопродление не прошло, но доступ временно сохранён до *%s*.\n\nЧтобы доступ не остановился, оформи оплату повторно.",
				graceUntil,
			),
			Keyboard: kafkacontracts.TgKeyboardBuyMenu,
		})

	case StatusExpired:
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			Message:    "Подписка истекла.\nТы можешь продлить её или оформить новую.",
			Keyboard:   kafkacontracts.TgKeyboardTrialOrBuy,
		})
	}

	return nil
}

func (s *Service) sendLinksNotificationTx(ctx context.Context, tx pgx.Tx, state *SubscriptionState) error {
	if state == nil {
		return nil
	}

	if state.Status != StatusTrial && state.Status != StatusActive && state.Status != StatusGrace {
		return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
			TelegramID: state.TelegramID,
			Message: "Похоже, у тебя нет активного триала или подписки.\n\n" +
				"Сначала активируй доступ, а потом запроси ссылку ещё раз.",
			Keyboard: kafkacontracts.TgKeyboardTrialOrBuy,
		})
	}

	link, err := s.buildSubscriptionLinkTx(ctx, tx, state)
	if err != nil {
		return err
	}

	if state.ExpiresAt != nil {
		if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionLinksIssuedEvent{
			Type:       kafkacontracts.SubscriptionEventLinksIssued,
			TelegramID: state.TelegramID,
			ExpiresAt:  *state.ExpiresAt,
			Links: []kafkacontracts.SubscriptionAccessLink{{
				Kind:  link.Kind,
				Title: link.Title,
				URL:   link.URL,
			}},
		}); err != nil {
			return err
		}
	}

	expireStr := "-"
	if link.ExpiresAt != nil {
		expireStr = link.ExpiresAt.Format("02.01.2006")
	}

	var sb strings.Builder
	sb.WriteString("🎉 Вот твоя *универсальная ссылка доступа* для v2RayTun:\n\n")
	sb.WriteString("*" + link.Title + "*\n")
	sb.WriteString("`" + link.URL + "`\n")
	sb.WriteString("Срок действия доступа: *" + expireStr + "*\n\n")
	sb.WriteString("Эта ссылка загружает доступные технические профили подключения, предусмотренные твоей подпиской.\n\n")
	sb.WriteString("1️⃣ Скопируй ссылку целиком.\n")
	sb.WriteString("2️⃣ Открой *v2RayTun* → вкладка *Connect*.\n")
	sb.WriteString("3️⃣ Нажми ➕ → *Enter link* или *Import from clipboard*.\n")
	sb.WriteString("4️⃣ Вставь ссылку и подтверди.\n\n")
	sb.WriteString("⚠️ Не делись этой ссылкой: она привязана к твоей подписке.")

	return s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: state.TelegramID,
		ParseMode:  "Markdown",
		Message:    sb.String(),
		Keyboard:   kafkacontracts.TgKeyboardMySubscriptionConfig,
	})
}

func (s *Service) buildSubscriptionLinkTx(ctx context.Context, tx pgx.Tx, state *SubscriptionState) (*SubscriptionLink, error) {
	if state == nil {
		return nil, fmt.Errorf("nil subscription state")
	}

	expiresAt := state.ExpiresAt
	if state.Status == StatusGrace && state.GraceUntil != nil {
		expiresAt = state.GraceUntil
	}

	token, err := s.repo.ensureTokenTx(ctx, tx, state.TelegramID, expiresAt)
	if err != nil {
		return nil, err
	}

	return &SubscriptionLink{
		Kind:      "subscription_feed",
		Title:     "Единая ссылка доступа",
		URL:       s.publicBaseURL + token,
		ExpiresAt: cloneTime(expiresAt),
	}, nil
}

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

func (s *Service) publishSubscriptionEventTx(ctx context.Context, tx pgx.Tx, event any) error {
	key := "subscription"
	eventType := "subscription.event"
	switch e := event.(type) {
	case *kafkacontracts.SubscriptionTrialStartedEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.SubscriptionActivatedEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.SubscriptionCanceledEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.SubscriptionGraceStartedEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.SubscriptionSuspendedEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	case *kafkacontracts.SubscriptionLinksIssuedEvent:
		key = fmt.Sprint(e.TelegramID)
		eventType = string(e.Type)
	}

	return outbox.AddTx(ctx, tx, outbox.Event{
		AggregateType: "subscription",
		AggregateID:   key,
		Topic:         commonkafka.TopicSubscriptionEvents,
		MessageKey:    key,
		EventType:     eventType,
		Payload:       event,
	})
}

func (s *Service) publishBillingCommandTx(ctx context.Context, tx pgx.Tx, telegramID int64, cmd any) error {
	if telegramID == 0 || cmd == nil {
		return nil
	}
	return outbox.AddTx(ctx, tx, outbox.Event{
		AggregateType: "billing-command",
		AggregateID:   fmt.Sprint(telegramID),
		Topic:         commonkafka.TopicBillingCommands,
		MessageKey:    fmt.Sprint(telegramID),
		EventType:     "billing.command",
		Payload:       cmd,
	})
}
