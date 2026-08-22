package tg_bot_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	commonkafka "vpn-platform/internal/common/kafka"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func (a *App) consumeNotifications(ctx context.Context) {
	reader := a.notificationsRead
	if reader == nil {
		return
	}

	log.Println("[tg-bot] starting kafka tg.notifications consumer...")

	err := commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"tg-bot-gateway",
		func(ctx context.Context, msg commonkafka.Message) error {
			var notif kafkacontracts.TgNotification
			if err := json.Unmarshal(msg.Value, &notif); err != nil {
				log.Printf("[tg-bot] invalid tg notification message: %v", err)
				return commonkafka.ErrSkip
			}
			return a.sendKafkaNotificationToUser(&notif)
		},
		a.kafkaProducer,
		commonkafka.TopicTgNotificationsDLT,
	)
	if err != nil {
		log.Printf("[tg-bot] kafka notifications consumer stopped: %v", err)
	}
}

func (a *App) sendKafkaNotificationToUser(n *kafkacontracts.TgNotification) error {
	if n == nil {
		return nil
	}

	msg := tgbotapi.NewMessage(n.TelegramID, n.Message)
	if n.ParseMode != "" {
		msg.ParseMode = n.ParseMode
	}

	// PayURL имеет приоритет: показываем inline-кнопку «Оплатить» (с reply-меню
	// в одном сообщении её совмещать нельзя — Telegram-ограничение).
	if strings.TrimSpace(n.PayURL) != "" {
		msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("💳 Оплатить", n.PayURL),
			),
		)
	} else {
		switch n.Keyboard {
		case kafkacontracts.TgKeyboardMainMenu:
			msg.ReplyMarkup = mainMenuKeyboard()
		case kafkacontracts.TgKeyboardBuyMenu:
			msg.ReplyMarkup = buyMenuKeyboard()
		case kafkacontracts.TgKeyboardTrialOrBuy:
			msg.ReplyMarkup = trialOrBuyKeyboard()
		case kafkacontracts.TgKeyboardMySubscriptionConfig:
			msg.ReplyMarkup = mySubKeyboardWithConfig()
		case kafkacontracts.TgKeyboardMainMenuWithBack:
			msg.ReplyMarkup = mainMenuWithBackKeyboard()
		}
	}

	if _, err := a.bot.Send(msg); err != nil {
		if undeliverableTelegramError(err) {
			// Ошибка невосстановимая: пользователь заблокировал бота, удалил
			// аккаунт или чата не существует. Возвращать её консьюмеру нельзя —
			// он уйдёт в бесконечный ретрай, не закоммитит offset и навсегда
			// оставит лаг на партиции, а в DLT сообщение при этом не попадёт.
			// Считаем обработанным и идём дальше.
			log.Printf("[tg-bot] notification to user %d dropped as undeliverable: %v", n.TelegramID, err)
			return nil
		}
		log.Printf("[tg-bot] failed to send kafka notification to user %d: %v", n.TelegramID, err)
		return fmt.Errorf("send telegram notification: %w", err)
	}
	return nil
}

// undeliverableTelegramError отличает временный сбой (сеть, 5xx, флуд-лимит)
// от постоянного, который не исправится сам никогда. Повторять доставку во
// втором случае бессмысленно: адресат недоступен навсегда.
func undeliverableTelegramError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"bot was blocked by the user",
		"user is deactivated",
		"chat not found",
		"peer_id_invalid",
		"user_is_blocked",
		"bot can't initiate conversation",
		"chat_write_forbidden",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
