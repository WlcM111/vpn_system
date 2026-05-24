package tg_bot_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send kafka notification to user %d: %v", n.TelegramID, err)
		return fmt.Errorf("send telegram notification: %w", err)
	}
	return nil
}
