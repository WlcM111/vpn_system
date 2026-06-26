package tg_bot_gateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
	"github.com/skip2/go-qrcode"
)

func (a *App) handleUpdate(ctx context.Context, update tgbotapi.Update) {
	msg := update.Message
	if msg == nil {
		return
	}

	chatID := msg.Chat.ID
	text := strings.TrimSpace(msg.Text)

	unlock := a.lockChat(chatID)
	defer unlock()

	state, err := a.stateStore.Get(ctx, chatID)
	if err != nil || state == nil {
		log.Printf("[tg-bot] failed to get state chat=%d err=%v", chatID, err)
		state = &ChatState{Step: StepMainMenu}
	}

	username := ""
	if msg.From != nil {
		username = msg.From.UserName
	}

	log.Printf("[tg-bot] update chat=%d user=%s step=%s text=%q msg_id=%d",
		chatID, username, state.Step, text, msg.MessageID)

	if text == "/start" {
		a.handleStart(ctx, chatID, state)
		return
	}

	if text == btnMainMenu {
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendMainMenu(chatID, true)
		return
	}

	switch state.Step {
	case StepBuyMenu:
		a.handleBuyMenuChoice(ctx, chatID, state, text)
	case StepServicesMenu:
		a.handleServicesMenuChoice(ctx, chatID, state, text)
	default:
		a.handleMainMenuChoice(ctx, chatID, state, text)
	}
}

func (a *App) handleStart(ctx context.Context, chatID int64, state *ChatState) {
	if !state.WelcomeShown {
		msg := tgbotapi.NewMessage(chatID, welcomeText())
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = mainMenuKeyboard()
		if _, err := a.bot.Send(msg); err != nil {
			log.Printf("[tg-bot] failed to send welcome: %v", err)
		}

		state.WelcomeShown = true
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		return
	}

	state.Step = StepMainMenu
	_ = a.stateStore.Set(ctx, chatID, state)
	a.sendMainMenu(chatID, true)
}

func (a *App) sendMainMenu(chatID int64, withText bool) {
	text := ""
	if withText {
		text = "Главное меню:"
	}
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ReplyMarkup = mainMenuKeyboard()
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send main menu: %v", err)
	}
}

func (a *App) handleMainMenuChoice(ctx context.Context, chatID int64, state *ChatState, text string) {
	switch text {
	case btnBuySubscription:
		state.Step = StepBuyMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendText(chatID, "Выбери действие в разделе оплаты 👇", buyMenuKeyboard())

	case btnMySubscription:
		a.handleMySubscription(ctx, chatID, state)

	case btnTrial:
		a.handleStartTrial(ctx, chatID, state)

	case btnCancelSub:
		a.handleCancelSubscription(ctx, chatID, state)

	case btnDownloadVPN:
		a.handleDownloadVPN(ctx, chatID, state)

	case btnSupport:
		a.handleSupport(ctx, chatID, state)

	case btnGetConfig:
		a.handleGetConfig(ctx, chatID, state)

	case btnServicesInfo:
		a.handleServicesInfo(ctx, chatID, state)

	default:
		// Любое непонятное сообщение — возвращаем пользователя в главное меню.
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendMainMenu(chatID, true)
	}
}

func (a *App) handleBuyMenuChoice(ctx context.Context, chatID int64, state *ChatState, text string) {
	switch text {
	// Тарифы оплачиваются картой. Подписи кнопок содержат цену (labelMonthly и т.п.),
	// поэтому сравниваем именно с ними. Крипта и привязка карты временно скрыты.
	case labelMonthly():
		a.handleCardCheckoutFallback(ctx, chatID, state, "Подписка 30 дней", 30, priceMonthly())
	case labelQuarterly():
		a.handleCardCheckoutFallback(ctx, chatID, state, "Подписка 90 дней", 90, priceQuarterly())
	case labelSemiannual():
		a.handleCardCheckoutFallback(ctx, chatID, state, "Подписка 180 дней", 180, priceSemiannual())
	case labelAnnual():
		a.handleCardCheckoutFallback(ctx, chatID, state, "Подписка 360 дней", 360, priceAnnual())
	case btnBack:
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendMainMenu(chatID, true)
	default:
		a.sendText(chatID, "Выбери тариф 👇", buyMenuKeyboard())
	}
}

// handleCardCheckoutFallback показывает пользователю детали заказа и пример-ссылку
// на оплату картой, пока YooKassa недоступна. Команда в billing НЕ публикуется
// (чтобы не было «висящих» ожиданий) — это временная заглушка. Функционал оплаты
// картой (handleCreateCheckout) сохранён и не удалён; вернуть его — заменив тело
// этого метода обратно на вызов handleCreateCheckout.
func (a *App) handleCardCheckoutFallback(ctx context.Context, chatID int64, state *ChatState, title string, days int, priceRUB string) {
	state.Step = StepBuyMenu
	_ = a.stateStore.Set(ctx, chatID, state)

	msg := tgbotapi.NewMessage(chatID, cardPaymentFallbackText(title, days, priceRUB))
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = buyMenuKeyboard()
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send card fallback: %v", err)
	}
}

// handleCreateCryptoCheckout публикует команду в crypto.commands и сообщает пользователю,
// что готовится ссылка оплаты. Дальше бот ничего не делает — ссылку пользователю пришлёт
// crypto-billing-service через outbox → tg.notifications.
//
// В отличие от handleCreateCheckout для YooKassa, тут нет mock-режима: симуляция
// крипто-платежа сложна (нет фиатного "симулировать списание"), и в dev-режиме
// без Kafka криптокнопки просто отдают сообщение об этом.
func (a *App) handleCreateCryptoCheckout(
	ctx context.Context,
	chatID int64,
	state *ChatState,
	planCode kafkacontracts.PlanCode,
) {
	slog.Info("tg-bot action",
		"action", "create_crypto_checkout",
		"chat_id", chatID,
		"plan", planCode,
	)

	state.Step = StepBuyMenu
	_ = a.stateStore.Set(ctx, chatID, state)

	if !a.kafkaEnabled() {
		a.sendText(chatID,
			"Криптоплатежи доступны только при включённой Kafka. В mock-режиме не поддерживаются.",
			buyMenuKeyboard())
		return
	}

	cmd := &kafkacontracts.CreateCryptoCheckoutCommand{
		Type:       kafkacontracts.CryptoCommandCreateCheckout,
		CommandID:  uuid.NewString(),
		TelegramID: chatID,
		PlanCode:   planCode,
		// Asset пустой — сервис подставит CRYPTOBOT_DEFAULT_ASSET (USDT по умолчанию).
		// В будущем тут можно добавить под-меню выбора актива.
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := a.kafkaProducer.PublishJSON(opCtx, commonkafka.TopicCryptoCommands, fmt.Sprint(chatID), cmd); err != nil {
		slog.Error("tg-bot publish create crypto checkout failed", "err", err)
		a.sendText(chatID,
			"Не удалось начать оплату криптой. Попробуй чуть позже или выбери оплату картой.",
			buyMenuKeyboard())
		return
	}

	a.sendText(chatID,
		"Готовлю счёт на оплату криптой. Сейчас пришлю ссылку отдельным сообщением 👌",
		buyMenuKeyboard())
}

func (a *App) publishSubscriptionCommand(ctx context.Context, telegramID int64, cmd any) error {
	if !a.kafkaEnabled() {
		return nil
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return a.kafkaProducer.PublishJSON(opCtx, commonkafka.TopicSubscriptionCommands, fmt.Sprint(telegramID), cmd)
}

func (a *App) sendText(chatID int64, text string, keyboard tgbotapi.ReplyKeyboardMarkup) {
	msg := tgbotapi.NewMessage(chatID, text)
	if len(keyboard.Keyboard) > 0 {
		msg.ReplyMarkup = keyboard
	}
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send message: %v", err)
	}
}

func (a *App) handleCreateCheckout(
	ctx context.Context,
	chatID int64,
	state *ChatState,
	planCode kafkacontracts.PlanCode,
	durationDays int,
) {
	log.Printf("[tg-bot] action=create_checkout chat=%d plan=%s", chatID, planCode)

	state.Step = StepBuyMenu
	_ = a.stateStore.Set(ctx, chatID, state)

	if !a.kafkaEnabled() {
		_, err := a.backend.ApplyPaidSubscription(ctx, chatID, durationDays)
		if err != nil {
			log.Printf("[tg-bot] mock ApplyPaidSubscription error: %v", err)
			a.sendText(chatID, "Не удалось симулировать оплату в mock-режиме.", buyMenuKeyboard())
			return
		}

		a.sendText(
			chatID,
			"✅ MOCK-режим: оплата симулирована, подписка активирована.\nСейчас пришлю ключи.",
			mainMenuWithBackKeyboard(),
		)
		if err := a.sendSubscriptionLinksForUser(ctx, chatID); err != nil {
			log.Printf("[tg-bot] send links after mock payment error: %v", err)
		}
		return
	}

	cmd := &kafkacontracts.CreateSubscriptionCheckoutCommand{
		Type:       kafkacontracts.BillingCommandCreateSubscriptionCheckout,
		CommandID:  uuid.NewString(),
		TelegramID: chatID,
		PlanCode:   planCode,
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := a.kafkaProducer.PublishJSON(opCtx, commonkafka.TopicBillingCommands, fmt.Sprint(chatID), cmd); err != nil {
		log.Printf("[tg-bot] publish create checkout command error: %v", err)
		a.sendText(chatID, "Не удалось начать оплату. Попробуй чуть позже.", buyMenuKeyboard())
		return
	}

	a.sendText(chatID, "Готовлю ссылку на оплату. Сейчас пришлю её отдельным сообщением 👌", buyMenuKeyboard())
}

func (a *App) handleBindCard(ctx context.Context, chatID int64, state *ChatState) {
	log.Printf("[tg-bot] action=bind_card chat=%d", chatID)

	state.Step = StepBuyMenu
	_ = a.stateStore.Set(ctx, chatID, state)

	if !a.kafkaEnabled() {
		url, err := a.backend.BindCard(ctx, chatID)
		if err != nil {
			log.Printf("[tg-bot] mock BindCard error: %v", err)
			a.sendText(chatID, "Не удалось начать привязку карты.", buyMenuKeyboard())
			return
		}

		a.sendText(
			chatID,
			"MOCK-режим: карта считается привязанной.\n\nСсылка:\n"+url,
			buyMenuKeyboard(),
		)
		return
	}

	cmd := &kafkacontracts.BindCardCommand{
		Type:       kafkacontracts.BillingCommandBindCard,
		CommandID:  uuid.NewString(),
		TelegramID: chatID,
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := a.kafkaProducer.PublishJSON(opCtx, commonkafka.TopicBillingCommands, fmt.Sprint(chatID), cmd); err != nil {
		log.Printf("[tg-bot] publish bind card command error: %v", err)
		a.sendText(chatID, "Не удалось начать привязку карты.", buyMenuKeyboard())
		return
	}

	a.sendText(chatID, "Готовлю ссылку для привязки карты. Сейчас пришлю её отдельным сообщением 👌", buyMenuKeyboard())
}

func (a *App) handleUnbindCard(ctx context.Context, chatID int64, state *ChatState) {
	log.Printf("[tg-bot] action=unbind_card chat=%d", chatID)

	state.Step = StepBuyMenu
	_ = a.stateStore.Set(ctx, chatID, state)

	if !a.kafkaEnabled() {
		if err := a.backend.UnbindCard(ctx, chatID); err != nil {
			log.Printf("[tg-bot] mock UnbindCard error: %v", err)
			a.sendText(chatID, "Не удалось отвязать карту.", buyMenuKeyboard())
			return
		}

		a.sendText(chatID, "✅ MOCK-режим: карта отвязана.", buyMenuKeyboard())
		return
	}

	cmd := &kafkacontracts.UnbindCardCommand{
		Type:       kafkacontracts.BillingCommandUnbindCard,
		CommandID:  uuid.NewString(),
		TelegramID: chatID,
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := a.kafkaProducer.PublishJSON(opCtx, commonkafka.TopicBillingCommands, fmt.Sprint(chatID), cmd); err != nil {
		log.Printf("[tg-bot] publish unbind card command error: %v", err)
		a.sendText(chatID, "Не удалось отправить запрос на отвязку карты.", buyMenuKeyboard())
		return
	}

	a.sendText(chatID, "Запрос на отвязку карты отправлен. Подтверждение придет отдельным сообщением.", buyMenuKeyboard())
}

func (a *App) handleMySubscription(ctx context.Context, chatID int64, state *ChatState) {
	state.Step = StepMainMenu
	if err := a.stateStore.Set(ctx, chatID, state); err != nil {
		log.Printf("[tg-bot] failed to save state: %v", err)
	}

	if a.kafkaEnabled() {
		cmd := &kafkacontracts.GetStatusCommand{
			Type:       kafkacontracts.SubscriptionCommandGetStatus,
			CommandID:  uuid.NewString(),
			TelegramID: chatID,
		}

		if err := a.publishSubscriptionCommand(ctx, chatID, cmd); err != nil {
			log.Printf("[tg-bot] publish get_status error: %v", err)
			a.sendText(chatID, "Не удалось запросить статус подписки 😔\nПопробуй позже.", mainMenuKeyboard())
			return
		}

		a.sendText(chatID, "Запрашиваю статус подписки, подожди немного 👌", mainMenuWithBackKeyboard())
		return
	}

	info, err := a.backend.GetSubscriptionStatus(ctx, chatID)
	if err != nil {
		log.Printf("[tg-bot] GetSubscriptionStatus error: %v", err)
		a.sendText(chatID, "😔 Сейчас не получается получить статус подписки.\nПопробуй позже.", mainMenuKeyboard())
		return
	}

	switch info.Status {
	case StatusNone:
		a.sendText(chatID, "Пока у тебя нет активной подписки.", trialOrBuyKeyboard())
	case StatusTrial, StatusActive:
		a.sendText(chatID, "Подписка активна. Нажми «🔑 Получить ключи».", mySubKeyboardWithConfig())
	default:
		a.sendText(chatID, "Подписка истекла.", trialOrBuyKeyboard())
	}
}

func (a *App) handleStartTrial(ctx context.Context, chatID int64, state *ChatState) {
	state.Step = StepMainMenu
	if err := a.stateStore.Set(ctx, chatID, state); err != nil {
		log.Printf("[tg-bot] failed to save state: %v", err)
	}

	if a.kafkaEnabled() {
		cmd := &kafkacontracts.StartTrialCommand{
			Type:       kafkacontracts.SubscriptionCommandStartTrial,
			CommandID:  uuid.NewString(),
			TelegramID: chatID,
		}

		if err := a.publishSubscriptionCommand(ctx, chatID, cmd); err != nil {
			log.Printf("[tg-bot] publish start_trial error: %v", err)
			a.sendText(chatID, "Сейчас не получается оформить пробный период 😔", mainMenuKeyboard())
			return
		}

		a.sendText(chatID, "Запускаю пробный период, подожди немного 👌", mainMenuWithBackKeyboard())
		return
	}

	info, err := a.backend.StartTrial(ctx, chatID)
	if err != nil {
		log.Printf("[tg-bot] StartTrial error: %v", err)
		a.sendText(chatID, "Сейчас не получается оформить пробный период 😔", mainMenuKeyboard())
		return
	}

	if info.TrialUsed && info.Status != StatusTrial {
		a.sendText(chatID, "Похоже, триал уже использован.", trialOrBuyKeyboard())
		return
	}

	if err := a.sendSubscriptionLinksForUser(ctx, chatID); err != nil {
		log.Printf("[tg-bot] failed to send subscription link after trial: %v", err)
	}
}

func (a *App) handleCancelSubscription(ctx context.Context, chatID int64, state *ChatState) {
	state.Step = StepMainMenu
	if err := a.stateStore.Set(ctx, chatID, state); err != nil {
		log.Printf("[tg-bot] failed to save state: %v", err)
	}

	if a.kafkaEnabled() {
		cmd := &kafkacontracts.CancelSubscriptionCommand{
			Type:       kafkacontracts.SubscriptionCommandCancel,
			CommandID:  uuid.NewString(),
			TelegramID: chatID,
		}

		if err := a.publishSubscriptionCommand(ctx, chatID, cmd); err != nil {
			log.Printf("[tg-bot] publish cancel_subscription error: %v", err)
			a.sendText(chatID, "Сейчас не получается отменить подписку 😔", mainMenuKeyboard())
			return
		}

		a.sendText(chatID, "Отправил запрос на отмену подписки, подожди немного 👌", mainMenuWithBackKeyboard())
		return
	}

	info, err := a.backend.CancelSubscription(ctx, chatID)
	if err != nil {
		log.Printf("[tg-bot] CancelSubscription error: %v", err)
		a.sendText(chatID, "Сейчас не получается отменить подписку 😔", mainMenuKeyboard())
		return
	}

	if info.Status == StatusNone || info.Status == StatusExpired {
		a.sendText(chatID, "Активной подписки нет.", mainMenuWithBackKeyboard())
		return
	}

	a.sendText(chatID, "✅ Подписка отменена.", mainMenuKeyboard())
}

func (a *App) handleDownloadVPN(_ context.Context, chatID int64, _ *ChatState) {
	msg := tgbotapi.NewMessage(chatID, downloadVPNText())
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = mainMenuWithBackKeyboard()
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send download text: %v", err)
	}
}

func (a *App) handleSupport(_ context.Context, chatID int64, _ *ChatState) {
	a.sendText(chatID, supportText(), mainMenuWithBackKeyboard())
}

func (a *App) handleGetConfig(ctx context.Context, chatID int64, state *ChatState) {
	state.Step = StepMainMenu
	if err := a.stateStore.Set(ctx, chatID, state); err != nil {
		log.Printf("[tg-bot] failed to save state: %v", err)
	}

	if a.kafkaEnabled() {
		cmd := &kafkacontracts.GetLinksCommand{
			Type:       kafkacontracts.SubscriptionCommandGetLinks,
			CommandID:  uuid.NewString(),
			TelegramID: chatID,
		}

		if err := a.publishSubscriptionCommand(ctx, chatID, cmd); err != nil {
			log.Printf("[tg-bot] publish get_links error: %v", err)
			a.sendText(chatID, "Сейчас не получается получить ключи 😔", mainMenuWithBackKeyboard())
			return
		}

		a.sendText(chatID, "Готовлю два варианта ключей, подожди немного 👌", mainMenuWithBackKeyboard())
		return
	}

	if err := a.sendSubscriptionLinksForUser(ctx, chatID); err != nil {
		log.Printf("[tg-bot] sendSubscriptionLinksForUser error: %v", err)
	}
}

func (a *App) sendSubscriptionLinksForUser(ctx context.Context, chatID int64) error {
	links, err := a.backend.GetSubscriptionLinks(ctx, chatID)
	if err != nil {
		if errors.Is(err, ErrNoActiveSubscription) {
			a.sendText(
				chatID,
				"Похоже, у тебя нет активного триала или подписки.\nСначала активируй доступ, потом запроси ключи.",
				trialOrBuyKeyboard(),
			)
			return nil
		}
		a.sendText(chatID, "Сейчас не получается получить ключи.", mainMenuWithBackKeyboard())
		return err
	}

	var sb strings.Builder
	sb.WriteString("🎉 Вот твои *VLESS-ключи* для v2RayTun:\n\n")
	for _, link := range links {
		exp := "не ограничен"
		if link.ExpiresAt != nil {
			exp = formatDate(link.ExpiresAt)
		}

		sb.WriteString("*" + link.Title + "*\n")
		sb.WriteString("`" + link.URL + "`\n")
		sb.WriteString("Срок действия: *" + exp + "*\n\n")
	}
	sb.WriteString("1️⃣ Скопируй нужную ссылку.\n")
	sb.WriteString("2️⃣ Открой *v2RayTun* → *Connect*.\n")
	sb.WriteString("3️⃣ Нажми ➕ → *Enter link* или *Import from clipboard*.\n")
	sb.WriteString("4️⃣ Вставь ссылку и подтверди.\n\n")
	sb.WriteString("⚠️ Не делись этими ссылками с другими людьми.")

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = mainMenuWithBackKeyboard()
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send subscription links: %v", err)
	}

	if len(links) > 0 {
		if png, err := qrcode.Encode(links[0].URL, qrcode.Medium, 256); err == nil {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileBytes{
				Name:  "subscription-qr.png",
				Bytes: png,
			})
			photo.Caption = "QR для основного маршрута."
			if _, err := a.bot.Send(photo); err != nil {
				log.Printf("[tg-bot] failed to send qr: %v", err)
			}
		} else {
			log.Printf("[tg-bot] failed to generate qr: %v", err)
		}
	}

	a.sendText(chatID, "Если что-то не получается — напиши в поддержку 👇", mainMenuWithBackKeyboard())
	return nil
}
