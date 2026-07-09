package tg_bot_gateway

import (
	"context"
	"fmt"
	"log"
	"strings"

	kafkacontracts "vpn-platform/internal/contracts/kafka"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
)

const referralDeepLinkPrefix = "ref_"

// handleReferralStartParam обрабатывает /start ref_<code>: публикует команду
// атрибуции. Не блокирует показ меню. Использует существующий publishSubscriptionCommand.
func (a *App) handleReferralStartParam(ctx context.Context, chatID int64, arg string) {
	if !strings.HasPrefix(arg, referralDeepLinkPrefix) {
		return
	}
	code := strings.TrimSpace(strings.TrimPrefix(arg, referralDeepLinkPrefix))
	if code == "" {
		return
	}
	cmd := &kafkacontracts.ReferralAttributeCommand{
		Type:              kafkacontracts.SubscriptionCommandReferralAttribute,
		CommandID:         uuid.NewString(),
		RefereeTelegramID: chatID,
		ReferrerCode:      code,
	}
	if err := a.publishSubscriptionCommand(ctx, chatID, cmd); err != nil {
		log.Printf("[tg-bot] referral attribute publish failed chat=%d: %v", chatID, err)
	}
}

// handleReferralMenu показывает экран реферальной программы со статистикой.
func (a *App) handleReferralMenu(ctx context.Context, chatID int64, state *ChatState) {
	state.Step = StepReferralMenu
	_ = a.stateStore.Set(ctx, chatID, state)

	stats, err := a.loadReferralStats(ctx, chatID)
	if err != nil {
		log.Printf("[tg-bot] load referral stats failed chat=%d: %v", chatID, err)
		a.sendText(chatID, "Не удалось загрузить реферальную программу. Попробуйте позже.", referralMenuKeyboard())
		return
	}

	msg := tgbotapi.NewMessage(chatID, referralScreenText(stats, a.botUsername()))
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = referralMenuKeyboard()
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] send referral screen failed: %v", err)
	}
}

// handleReferralMenuChoice роутит кнопки внутри реферального меню.
func (a *App) handleReferralMenuChoice(ctx context.Context, chatID int64, state *ChatState, text string) {
	switch text {
	case btnReferralCopy:
		a.handleReferralCopyLink(ctx, chatID)
	case btnReferralRedeem:
		a.handleReferralRedeem(ctx, chatID)
	case btnBack:
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendMainMenu(chatID, true)
	default:
		a.handleReferralMenu(ctx, chatID, state)
	}
}

// handleReferralCopyLink отправляет ссылку моноширинным сообщением (tap-to-copy) +
// подтверждение. Telegram Bot API не умеет класть текст в буфер устройства —
// моноширинный текст с копированием по тапу это стандартное решение.
func (a *App) handleReferralCopyLink(ctx context.Context, chatID int64) {
	stats, err := a.loadReferralStats(ctx, chatID)
	if err != nil {
		a.sendText(chatID, "Не удалось получить ссылку. Попробуйте позже.", referralMenuKeyboard())
		return
	}
	link := referralLink(a.botUsername(), stats.Code)

	linkMsg := tgbotapi.NewMessage(chatID, "`"+link+"`")
	linkMsg.ParseMode = "Markdown"
	if _, err := a.bot.Send(linkMsg); err != nil {
		log.Printf("[tg-bot] send referral link failed: %v", err)
	}

	a.sendText(chatID,
		"✅ Ваша ссылка отправлена выше. Нажмите на неё, чтобы скопировать, и делитесь с друзьями!",
		referralMenuKeyboard(),
	)
}

// handleReferralRedeem публикует команду начисления. Результат придёт уведомлением
// из user-subscription (как во всей системе оповещений).
func (a *App) handleReferralRedeem(ctx context.Context, chatID int64) {
	cmd := &kafkacontracts.ReferralRedeemCommand{
		Type:       kafkacontracts.SubscriptionCommandReferralRedeem,
		CommandID:  uuid.NewString(),
		TelegramID: chatID,
	}
	if err := a.publishSubscriptionCommand(ctx, chatID, cmd); err != nil {
		log.Printf("[tg-bot] referral redeem publish failed chat=%d: %v", chatID, err)
		a.sendText(chatID, "Не удалось обработать запрос. Попробуйте позже.", referralMenuKeyboard())
		return
	}
	a.sendText(chatID, "⏳ Проверяю доступные бесплатные месяцы…", referralMenuKeyboard())
}

// referralLink строит deep-link на бота с реферальным кодом.
func referralLink(botUsername, code string) string {
	return fmt.Sprintf("https://t.me/%s?start=%s%s", botUsername, referralDeepLinkPrefix, code)
}

// botUsername возвращает username бота (из Telegram API, без @).
func (a *App) botUsername() string {
	return a.bot.Self.UserName
}
