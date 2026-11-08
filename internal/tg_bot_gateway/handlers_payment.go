package tg_bot_gateway

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
)

// ============================================================================
// Оплата: тариф → способ оплаты → счёт.
//
// После выбора тарифа бот показывает ровно два способа оплаты:
//   «Способ 1 - рекомендуемый» — Platega;
//   «Способ 2»                 — YooKassa.
// Внешний счёт создаётся только после явного выбора способа (billing получает
// команду с payment_provider). Цена в команде не передаётся: billing берёт её
// из своего тарифа (доверенный сервер). Бот секретов провайдеров не хранит.
// ============================================================================

const (
	// pendingPlanTTL — сколько живёт выбор тарифа перед выбором способа
	// оплаты. Нажатие на способ оплаты позже просит выбрать тариф заново.
	pendingPlanTTL = 30 * time.Minute
	// checkoutDebounce — повторное нажатие того же способа на тот же тариф в
	// течение этого времени не отправляет новую команду.
	checkoutDebounce = 10 * time.Second
	// paymentReturnURL — куда вернуть покупателя после оплаты (в бота).
	// Возврат оплату не подтверждает: подтверждение — только от провайдера.
	paymentReturnURL = "https://t.me/vpn_house_bot"
)

type planOption struct {
	Code  kafkacontracts.PlanCode
	Title string
	Days  int
	Price func() string
	Label func() string
}

func planOptions() []planOption {
	return []planOption{
		{kafkacontracts.PlanCodeMonthly, "Подписка 30 дней", 30, priceMonthly, labelMonthly},
		{kafkacontracts.PlanCodeQuarterly, "Подписка 90 дней", 90, priceQuarterly, labelQuarterly},
		{kafkacontracts.PlanCodeSemiannual, "Подписка 180 дней", 180, priceSemiannual, labelSemiannual},
		{kafkacontracts.PlanCodeAnnual, "Подписка 360 дней", 360, priceAnnual, labelAnnual},
	}
}

func findPlan(code string) (planOption, bool) {
	for _, p := range planOptions() {
		if string(p.Code) == code {
			return p, true
		}
	}
	return planOption{}, false
}

// planByButton — тариф по подписи кнопки (подпись содержит текущую цену:
// кнопка со старой ценой из старой клавиатуры тариф не выберет).
func planByButton(text string) (planOption, bool) {
	for _, p := range planOptions() {
		if text == p.Label() {
			return p, true
		}
	}
	return planOption{}, false
}

func paymentMethodPrompt(p planOption) string {
	return fmt.Sprintf("Тариф: %s — %s ₽.\n\nВыберите способ оплаты 👇", p.Title, p.Price())
}

// isPlategaConfigured проверяет, задан ли PLATEGA_MERCHANT_ID в формате
// валидного UUID и не выключен ли способ явно через PLATEGA_ENABLED=false.
// Если переменная не вставлена или пуста, при выборе первого способа бот
// сразу сообщает о временной недоступности и рекомендует «Способ 2», а после
// задания переменной и пересоздания контейнера бота сообщение исчезает.
//
// API-ключ (PLATEGA_SECRET) боту не передаётся: счета создаёт billing, и если
// ключ не задан или ключи неверны, billing сам отвечает пользователю, что
// «Способ 1» недоступен, и предлагает «Способ 2».
func isPlategaConfigured() bool {
	if enabled := strings.ToLower(strings.TrimSpace(os.Getenv("PLATEGA_ENABLED"))); enabled == "false" || enabled == "0" || enabled == "no" {
		return false
	}
	merchant := strings.TrimSpace(os.Getenv("PLATEGA_MERCHANT_ID"))
	if merchant == "" {
		return false
	}
	parsed, err := uuid.Parse(merchant)
	if err != nil || parsed == uuid.Nil {
		return false
	}
	return true
}

// handlePlanSelected — пользователь выбрал тариф: запоминаем его и предлагаем
// два способа оплаты. Счёт ещё не создаётся.
func (a *App) handlePlanSelected(ctx context.Context, chatID int64, state *ChatState, p planOption) {
	now := time.Now().UTC()
	state.Step = StepPaymentMethod
	state.PendingPlan = string(p.Code)
	state.PendingPlanAt = &now
	if err := a.stateStore.Set(ctx, chatID, state); err != nil {
		log.Printf("[tg-bot] failed to save state: %v", err)
	}
	a.sendText(chatID, paymentMethodPrompt(p), paymentMethodKeyboard())
}

// handlePaymentMethodMenu — сообщения на шаге выбора способа оплаты, кроме
// самих способов (их обрабатывает handlePaymentMethodChoice).
func (a *App) handlePaymentMethodMenu(ctx context.Context, chatID int64, state *ChatState, text string) {
	if text == btnBack {
		state.Step = StepBuyMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendText(chatID, "Выбери тариф 👇", buyMenuKeyboard())
		return
	}
	if p, ok := planByButton(text); ok {
		a.handlePlanSelected(ctx, chatID, state, p)
		return
	}
	if p, ok := a.freshPendingPlan(state); ok {
		a.sendText(chatID, paymentMethodPrompt(p), paymentMethodKeyboard())
		return
	}
	a.resetToTariffs(ctx, chatID, state, "Выбери тариф 👇")
}

// freshPendingPlan — выбранный тариф, если выбор ещё не устарел.
func (a *App) freshPendingPlan(state *ChatState) (planOption, bool) {
	if state.PendingPlan == "" || state.PendingPlanAt == nil {
		return planOption{}, false
	}
	if time.Since(*state.PendingPlanAt) > pendingPlanTTL {
		return planOption{}, false
	}
	return findPlan(state.PendingPlan)
}

func (a *App) resetToTariffs(ctx context.Context, chatID int64, state *ChatState, text string) {
	state.Step = StepBuyMenu
	state.PendingPlan = ""
	state.PendingPlanAt = nil
	_ = a.stateStore.Set(ctx, chatID, state)
	a.sendText(chatID, text, buyMenuKeyboard())
}

// handlePaymentMethodChoice — нажат «Способ 1 - рекомендуемый» или «Способ 2».
// Кнопка могла прийти и со старой клавиатуры: тогда без выбранного (или со
// слишком старым) тарифом счёт не создаётся — бот просит выбрать тариф.
func (a *App) handlePaymentMethodChoice(ctx context.Context, chatID int64, state *ChatState, provider kafkacontracts.BillingPaymentProvider) {
	p, ok := a.freshPendingPlan(state)
	if !ok {
		a.resetToTariffs(ctx, chatID, state, "Выбор тарифа устарел — выберите тариф заново 👇")
		return
	}
	if provider == kafkacontracts.BillingPaymentProviderPlatega && !isPlategaConfigured() {
		slog.Info("tg-bot platega method unavailable: env vars not configured", "chat_id", chatID, "plan", p.Code)
		a.sendText(chatID, "Способ оплаты «Способ 1 - рекомендуемый» сейчас недоступен.\nРекомендуем выбрать «Способ 2» 👇", paymentMethodKeyboard())
		return
	}

	label := btnPayMethodPlatega
	if provider == kafkacontracts.BillingPaymentProviderYooKassa {
		label = btnPayMethodYooKassa
	}

	now := time.Now().UTC()
	key := string(p.Code) + ":" + string(provider)
	if state.LastCheckoutKey == key && state.LastCheckoutAt != nil && now.Sub(*state.LastCheckoutAt) < checkoutDebounce {
		a.sendText(chatID, "Запрос уже отправлен — ссылка на оплату придёт отдельным сообщением 👌", paymentMethodKeyboard())
		return
	}

	slog.Info("tg-bot action", "action", "create_checkout", "chat_id", chatID, "plan", p.Code, "provider", provider)

	if !a.kafkaEnabled() {
		// Локальный mock-режим (в production запрещён в NewApp).
		if _, err := a.backend.ApplyPaidSubscription(ctx, chatID, p.Days); err != nil {
			log.Printf("[tg-bot] mock ApplyPaidSubscription error: %v", err)
			a.sendText(chatID, "Не удалось симулировать оплату в mock-режиме.", buyMenuKeyboard())
			return
		}
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendText(chatID, "✅ Подписка активирована.\nСейчас пришлю ссылку доступа.", mainMenuWithBackKeyboard())
		if err := a.sendSubscriptionLinksForUser(ctx, chatID); err != nil {
			log.Printf("[tg-bot] send links after mock payment error: %v", err)
		}
		return
	}

	cmd := &kafkacontracts.CreateSubscriptionCheckoutCommand{
		Type:       kafkacontracts.BillingCommandCreateSubscriptionCheckout,
		CommandID:  uuid.NewString(),
		TelegramID: chatID,
		PlanCode:   p.Code,
		// Автосписаний нет: способ оплаты не сохраняется.
		SavePaymentMethod: false,
		ReturnURL:         paymentReturnURL,
		PaymentProvider:   provider,
	}
	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := a.publishBilling(opCtx, chatID, cmd); err != nil {
		log.Printf("[tg-bot] publish create checkout command error: %v", err)
		a.sendText(chatID, "Не удалось начать оплату. Попробуйте ещё раз или выберите другой способ 👇", paymentMethodKeyboard())
		return
	}

	state.Step = StepPaymentMethod
	state.LastCheckoutKey = key
	state.LastCheckoutAt = &now
	if err := a.stateStore.Set(ctx, chatID, state); err != nil {
		log.Printf("[tg-bot] failed to save state: %v", err)
	}
	a.sendText(chatID, "Готовлю ссылку на оплату — "+label+". Пришлю её отдельным сообщением 👌", paymentMethodKeyboard())
}

// publishBilling отправляет команду в billing.commands (ключ — Telegram ID,
// чтобы команды одного пользователя обрабатывались по порядку).
func (a *App) publishBilling(ctx context.Context, telegramID int64, cmd any) error {
	if a.billingPublisher != nil {
		return a.billingPublisher.PublishJSON(ctx, commonkafka.TopicBillingCommands, fmt.Sprint(telegramID), cmd)
	}
	return a.kafkaProducer.PublishJSON(ctx, commonkafka.TopicBillingCommands, fmt.Sprint(telegramID), cmd)
}

// handleRemovedButton — нажатие на кнопку, которой больше нет в меню
// (сообщение со старой клавиатурой). Никаких действий не выполняется —
// только пояснение и актуальное меню.
func (a *App) handleRemovedButton(ctx context.Context, chatID int64, state *ChatState, text string) bool {
	var reply string
	switch text {
	case btnCancelSub:
		reply = "Автоматических списаний нет — подписка оплачивается разово и просто закончится в дату окончания. Отменять ничего не нужно.\n\nСрок подписки — в разделе «📦 Моя подписка»."
	case btnBindCard, btnUnbindCard:
		reply = "Привязка карты не используется: каждая оплата разовая, данные карты не сохраняются, автосписаний нет."
	default:
		return false
	}
	state.Step = StepMainMenu
	_ = a.stateStore.Set(ctx, chatID, state)
	a.sendText(chatID, reply, mainMenuKeyboard())
	return true
}

// staleCallbackText — ответ на нажатие inline-кнопки со старого сообщения.
// Бот не использует callback-кнопки (только reply-клавиатуры и URL-кнопки),
// но такие кнопки могли остаться в старых сообщениях. Без ответа на
// callback_query клиент Telegram показывает «загрузку» на кнопке.
const staleCallbackText = "Эта кнопка устарела — воспользуйтесь меню внизу экрана."

func (a *App) handleCallbackQuery(cq *tgbotapi.CallbackQuery) {
	if cq == nil || cq.ID == "" {
		return
	}
	if _, err := a.bot.Request(tgbotapi.NewCallback(cq.ID, staleCallbackText)); err != nil {
		log.Printf("[tg-bot] answer callback query failed: %v", err)
	}
}
