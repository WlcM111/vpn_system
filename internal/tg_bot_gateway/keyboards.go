package tg_bot_gateway

import (
	"os"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	btnBuySubscription = "💳 Купить подписку"
	btnMySubscription  = "📦 Моя подписка"
	btnTrial           = "✨ Пробный период"
	btnCancelSub       = "⛔ Отменить подписку"
	btnDownloadVPN     = "⬇️ Скачать приложение"
	btnSupport         = "🆘 Поддержка"
	btnMainMenu        = "🏠 Главное меню"

	// Новый раздел «Описание услуг» и кнопки документов.
	btnServicesInfo = "📄 Описание услуг"
	btnDocUser      = "📘 Пользовательское соглашение"
	btnDocOffer     = "📗 Публичная оферта"
	btnDocPrivacy   = "📕 Политика конфиденциальности"
	btnDocRefund    = "📙 Политика возврата"

	// Крипто-кнопки временно скрыты из меню. Константы оставлены — обработчики
	// в handlers.go продолжают существовать, чтобы при возврате крипты вернуть
	// кнопки в buyMenuKeyboard без восстановления логики.
	btnPlanMonthlyCrypto   = "🪙 Крипта 30 дней"
	btnPlanQuarterlyCrypto = "🪙 Крипта 90 дней"

	// Кнопки тарифов (оплата картой). Цена подставляется динамически — см.
	// planButtonLabel ниже. Базовые подписи без цены оставлены как fallback и
	// для обратной совместимости, но в меню используются версии с ценой.
	btnPlanMonthly    = "💳 Подписка 30 дней"
	btnPlanQuarterly  = "💳 Подписка 90 дней"
	btnPlanSemiannual = "💳 Подписка 180 дней"
	btnPlanAnnual     = "💳 Подписка 360 дней"

	// Привязка/отвязка карты временно скрыты из меню (константы и обработчики
	// сохранены для будущего возврата).
	btnBindCard   = "🔗 Привязать карту"
	btnUnbindCard = "🔓 Отвязать карту"

	btnBack = "⬅️ Назад"

	btnGetConfig = "🔗 Получить ссылку доступа"

	// Выбор клиента при выдаче ключа. Happ/Incy первыми — они дают
	// авто-настройку маршрутизации (российские сайты идут мимо VPN).
	btnClientHapp = "⭐ Happ / Incy — рекомендуем"
	btnClientXray = "v2RayTun / Streisand"

	// Реферальная программа.
	btnReferral       = "🎁 Реферальная программа"
	btnReferralCopy   = "🔗 Скопировать мою ссылку"
	btnReferralRedeem = "🎁 Получить бесплатные месяцы"
)

// ---------------------------------------------------------------------------
// Цены тарифов (в рублях). Берутся из .env, чтобы менять без пересборки.
// Значения по умолчанию соответствуют согласованным ценам.
// ---------------------------------------------------------------------------

// planPriceRUB возвращает цену тарифа в рублях (строкой) по env-переменной.
func planPriceRUB(envKey, fallback string) string {
	v := strings.TrimSpace(os.Getenv(envKey))
	if v == "" {
		return fallback
	}
	return v
}

// Цены по тарифам (читаются при каждом построении клавиатуры — это дёшево).
func priceMonthly() string    { return planPriceRUB("PLAN_MONTHLY_PRICE_RUB", "89") }
func priceQuarterly() string  { return planPriceRUB("PLAN_QUARTERLY_PRICE_RUB", "249") }
func priceSemiannual() string { return planPriceRUB("PLAN_SEMIANNUAL_PRICE_RUB", "439") }
func priceAnnual() string     { return planPriceRUB("PLAN_ANNUAL_PRICE_RUB", "799") }

// planButtonLabel формирует подпись кнопки тарифа с ценой:
//
//	«💳 Подписка 30 дней — 89 ₽»
func planButtonLabel(base, priceRUB string) string {
	return base + " — " + priceRUB + " ₽"
}

// Готовые подписи кнопок тарифов с ценой (используются в меню и для роутинга).
func labelMonthly() string    { return planButtonLabel(btnPlanMonthly, priceMonthly()) }
func labelQuarterly() string  { return planButtonLabel(btnPlanQuarterly, priceQuarterly()) }
func labelSemiannual() string { return planButtonLabel(btnPlanSemiannual, priceSemiannual()) }
func labelAnnual() string     { return planButtonLabel(btnPlanAnnual, priceAnnual()) }

// ---------------------------------------------------------------------------
// Клавиатуры.
// ---------------------------------------------------------------------------

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
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnServicesInfo),
			tgbotapi.NewKeyboardButton(btnReferral),
		),
	)
}

// referralMenuKeyboard — меню реферальной программы.
func referralMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnReferralCopy),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnReferralRedeem),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnBack),
			tgbotapi.NewKeyboardButton(btnMainMenu),
		),
	)
}

// buyMenuKeyboard — меню тарифов. Крипта и привязка карты скрыты; четыре тарифа
// картой с ценами, по две кнопки в ряд.
func buyMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(labelMonthly()),
			tgbotapi.NewKeyboardButton(labelQuarterly()),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(labelSemiannual()),
			tgbotapi.NewKeyboardButton(labelAnnual()),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnBack),
			tgbotapi.NewKeyboardButton(btnMainMenu),
		),
	)
}

// servicesMenuKeyboard — подменю «Описание услуг» с тремя документами.
func servicesMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnDocUser),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnDocOffer),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnDocPrivacy),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnDocRefund),
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

// clientChoiceKeyboard — меню выбора приложения при выдаче ключа.
// Happ/Incy стоит первым и помечен как рекомендуемый: только эта группа
// получает авто-активацию правил маршрутизации из подписки.
func clientChoiceKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnClientHapp),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnClientXray),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnBack),
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
