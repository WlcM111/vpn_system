package tg_bot_gateway

import tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

const (
	btnBuySubscription = "💳 Купить подписку"
	btnMySubscription  = "📦 Моя подписка"
	btnTrial           = "✨ Пробный период 3 дня"
	btnCancelSub       = "⛔ Отменить подписку"
	btnDownloadVPN     = "⬇️ Скачать приложение"
	btnSupport         = "🆘 Поддержка"
	btnMainMenu        = "🏠 Главное меню"

	btnPlanMonthlyCrypto   = "🪙 Крипта 30 дней"
	btnPlanQuarterlyCrypto = "🪙 Крипта 90 дней"

	btnPlanMonthly   = "💳 Подписка 30 дней"
	btnPlanQuarterly = "💳 Подписка 90 дней"

	btnBindCard   = "🔗 Привязать карту"
	btnUnbindCard = "🔓 Отвязать карту"
	btnBack       = "⬅️ Назад"

	btnGetConfig = "🔑 Получить ключи"
)

func mainMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnBuySubscription),
			tgbotapi.NewKeyboardButton(btnMySubscription),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnTrial),
			tgbotapi.NewKeyboardButton(btnCancelSub),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnDownloadVPN),
			tgbotapi.NewKeyboardButton(btnSupport),
		),
	)
}

func buyMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnPlanMonthly),
			tgbotapi.NewKeyboardButton(btnPlanQuarterly),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnPlanMonthlyCrypto),
			tgbotapi.NewKeyboardButton(btnPlanQuarterlyCrypto),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnBindCard),
			tgbotapi.NewKeyboardButton(btnUnbindCard),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnBack),
			tgbotapi.NewKeyboardButton(btnMainMenu),
		),
	)
}

func trialOrBuyKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnTrial),
			tgbotapi.NewKeyboardButton(btnBuySubscription),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnMainMenu),
		),
	)
}

func mySubKeyboardWithConfig() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnGetConfig),
			tgbotapi.NewKeyboardButton(btnCancelSub),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnMainMenu),
		),
	)
}

func mainMenuWithBackKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnMainMenu),
		),
	)
}
