package tg_bot_gateway

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// welcomeText — приветственное сообщение бота (точный текст по требованию).
func welcomeText() string {
	return "👋 *Добро пожаловать в House VPN!*\n" +
		"\n" +
		"🔐 Мы помогаем настроить защищённое сетевое соединение для безопасной передачи данных через приложения *v2RayTun*, *Happ* или *Streisand*.\n" +
		"\n" +
		"✨ *Что можно сделать в боте:*\n" +
		"💳 Выбрать подходящий тариф\n" +
		"🎁 Активировать бесплатный пробный период\n" +
		"📲 Получить инструкцию по подключению\n" +
		"💬 Обратиться в поддержку\n" +
		"\n" +
		"👇 Выберите действие в меню ниже."
}

// downloadVPNText — гайд «как скачать клиенты». Текст и ссылки на приложения
// пользователь заполняет сам. Ниже — каркас с плейсхолдерами для трёх клиентов.
func downloadVPNText() string {
	return "📥 *Как скачать и установить клиент*\n\n" +
		"Для подключения подойдёт любое из совместимых приложений: *v2RayTun*, *Happ* или *Streisand*.\n\n" +
		"Ссылки для скачивания:\n" +
		"• v2RayTun — " + appLinkOrPlaceholder("APP_LINK_V2RAYTUN") + "\n" +
		"• Happ — " + appLinkOrPlaceholder("APP_LINK_HAPP") + "\n" +
		"• Streisand — " + appLinkOrPlaceholder("APP_LINK_STREISAND") + "\n\n" +
		"После установки:\n" +
		"1️⃣ Открой приложение.\n" +
		"2️⃣ Нажми ➕ и выбери импорт по ссылке (Enter link / Import from clipboard).\n" +
		"3️⃣ Вставь ссылку-ключ, которую пришлёт бот после активации подписки.\n" +
		"4️⃣ Подключайся 🚀"
}

// appLinkOrPlaceholder возвращает ссылку на приложение из env или плейсхолдер.
// Так ты можешь задать ссылки через .env, не пересобирая бот; либо вписать прямо
// в этот файл вместо вызова.
func appLinkOrPlaceholder(envKey string) string {
	v := strings.TrimSpace(os.Getenv(envKey))
	if v == "" {
		return "(ссылка скоро будет добавлена)"
	}
	return v
}

// supportText — контакты поддержки.
func supportText() string {
	email := strings.TrimSpace(os.Getenv("SUPPORT_EMAIL"))
	if email == "" {
		email = "support@example.com"
	}
	tg := strings.TrimSpace(os.Getenv("SUPPORT_TELEGRAM"))
	if tg == "" {
		tg = "@support"
	}
	return "Если что-то не работает или есть вопросы — напиши нам:\n\n" +
		"✉️ Email: " + email + "\n" +
		"💬 Telegram: " + tg
}

// ----------------------------------------------------------------------------
// Тексты, связанные с оплатой и описанием услуг.
// ----------------------------------------------------------------------------

// servicesDescriptionText — текст над кнопками раздела «Описание услуг».
func servicesDescriptionText() string {
	return "📄 Описание услуг и документы.\n\nВыбери документ, чтобы открыть его 👇"
}

// documentsUnavailableText — если Telegraph-страницы ещё не опубликованы.
func documentsUnavailableText() string {
	return "Документы сейчас готовятся, попробуй открыть их чуть позже 🙏"
}

// cardPaymentFallbackText формирует сообщение после выбора тарифа картой, пока
// YooKassa недоступна: что покупает пользователь, срок, сумма и пример-ссылка.
//
//	title    — название тарифа (напр. «Подписка 30 дней»).
//	days     — срок в днях.
//	priceRUB — цена в рублях (строка, напр. «89»).
func cardPaymentFallbackText(title string, days int, priceRUB string) string {
	return fmt.Sprintf(
		"🧾 *Оформление подписки*\n\n"+
			"Тариф: *%s*\n"+
			"Срок: *%d дней*\n"+
			"Сумма к оплате: *%s ₽*\n\n"+
			"Нажмите кнопку ниже, чтобы перейти к оплате 👇",
		title, days, priceRUB,
	)
}

// cardPaymentURL возвращает ссылку для кнопки «Оплатить».
// Пока — заглушка из env (CARD_PAYMENT_FALLBACK_URL). Когда заработает реальная
// оплата — ссылка будет формироваться billing'ом; эту функцию можно убрать.
func cardPaymentURL() string {
	link := strings.TrimSpace(os.Getenv("CARD_PAYMENT_FALLBACK_URL"))
	if link == "" {
		link = "https://example.com/pay/sample-link"
	}
	return link
}

func formatDate(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Format("02.01.2006")
}

// yooKassaConfigured сообщает, настроена ли оплата картой (YooKassa).
// Бот читает те же переменные, что и billing-сервис: если заданы и SHOP_ID,
// и SECRET_KEY — оплата картой доступна, и бот запускает реальный платёж.
// Проверка выполняется при каждом запросе, поэтому после заполнения переменных
// и перезапуска бота режим переключается автоматически (без правок кода).
func yooKassaConfigured() bool {
	shopID := strings.TrimSpace(os.Getenv("YOOKASSA_SHOP_ID"))
	secretKey := strings.TrimSpace(os.Getenv("YOOKASSA_SECRET_KEY"))
	return shopID != "" && secretKey != ""
}
