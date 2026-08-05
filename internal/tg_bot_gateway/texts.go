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
		"🔐 Мы помогаем настроить защищённое сетевое соединение для безопасной передачи данных через приложения *Happ*, *v2RayTun*, *Incy* или *Streisand*.\n" +
		"\n" +
		"✨ *Что можно сделать в боте:*\n" +
		"💳 Выбрать подходящий тариф\n" +
		"🎁 Активировать бесплатный пробный период\n" +
		"📲 Получить инструкцию по подключению\n" +
		"💬 Обратиться в поддержку\n" +
		"\n" +
		"👇 Выберите действие в меню ниже."
}

// ----------------------------------------------------------------------------
// Ссылки на клиентские приложения.
//
// Ссылки магазинов (App Store / Google Play) — стабильные идентификаторы
// приложений, поэтому заданы константами: конфигурацией они не являются.
// Ссылки на сайты (нужны для ПК) можно переопределить через .env (APP_LINK_*);
// если переменная пустая, используется рабочее значение по умолчанию.
//
// Важно: Streisand существует только в экосистеме Apple (iPhone/iPad/Mac),
// версии для Android не существует — в Android-блоке его нет намеренно.
// У Happ два разных приложения в App Store: глобальное и российское.
// ----------------------------------------------------------------------------
const (
	// App Store — iPhone, iPad, Mac.
	appStoreHappRus    = "https://apps.apple.com/ru/app/happ-proxy-utility/id6783623643"
	appStoreHappGlobal = "https://apps.apple.com/us/app/happ-proxy-utility/id6504287215"
	appStoreIncy       = "https://apps.apple.com/us/app/incy/id6756943388"
	appStoreV2RayTun   = "https://apps.apple.com/us/app/v2raytun/id6476628951"
	appStoreStreisand  = "https://apps.apple.com/us/app/streisand/id6450534064"

	// Google Play — Android.
	googlePlayHapp     = "https://play.google.com/store/apps/details?id=com.happproxy"
	googlePlayV2RayTun = "https://play.google.com/store/apps/details?id=com.v2raytun.android"
	googlePlayIncy     = "https://play.google.com/store/apps/details?id=llc.itdev.incy"

	// Официальные сайты — для ПК (Windows / macOS / Linux).
	siteHappDefault      = "https://www.happ.su/main"
	siteIncyDefault      = "https://incy.cc/"
	siteV2RayTunDefault  = "https://v2raytun.com/"
	siteStreisandDefault = "https://streisandapp.com/"
)

// downloadVPNText — инструкция «как скачать клиент», разделённая на три блока:
// Android, iPhone/iPad и компьютер. В каждом блоке — только те приложения,
// которые реально существуют на этой платформе.
func downloadVPNText() string {
	return "📥 *Скачать приложение*\n\n" +
		"Чтобы подключиться, нужен клиент.\n\n" +
		"⭐ *Настоятельно рекомендуем Happ или Incy* — с ними подключение и маршрутизация настраиваются полностью автоматически: российские сайты (банки, госуслуги, маркетплейсы) открываются напрямую, а заблокированные — через VPN. Ничего включать вручную не нужно, работает максимально стабильно.\n\n" +
		"Выберите блок под своё устройство 👇\n\n" +
		"━━━━━━━━━━━━━━━━━━\n\n" +
		"🤖 *ANDROID*\n\n" +
		"Установите любое приложение из Google Play — они взаимозаменяемы:\n\n" +
		"• [Happ](" + googlePlayHapp + ") — рекомендуем\n" +
		"• [v2RayTun](" + googlePlayV2RayTun + ")\n" +
		"• [Incy](" + googlePlayIncy + ")\n\n" +
		"━━━━━━━━━━━━━━━━━━\n\n" +
		"🍎 *IPHONE / IPAD*\n\n" +
		"⚠️ Некоторые приложения доступны не во всех регионах App Store. " +
		"Открывайте ссылки по очереди и установите то, которое откроется в вашем магазине — " +
		"все они работают одинаково:\n\n" +
		"• [Happ — для России](" + appStoreHappRus + ")\n" +
		"• [Happ — Global](" + appStoreHappGlobal + ")\n" +
		"• [Incy](" + appStoreIncy + ")\n" +
		"• [v2RayTun](" + appStoreV2RayTun + ")\n" +
		"• [Streisand](" + appStoreStreisand + ")\n\n" +
		"━━━━━━━━━━━━━━━━━━\n\n" +
		"💻 *КОМПЬЮТЕР* (Windows, macOS, Linux)\n\n" +
		"Откройте сайт приложения и скачайте версию для своей системы:\n\n" +
		"• [Happ](" + appLink("APP_LINK_HAPP", siteHappDefault) + ") — Windows, macOS, Linux\n" +
		"• [Incy](" + appLink("APP_LINK_INCY", siteIncyDefault) + ") — Windows, macOS, Linux\n" +
		"• [v2RayTun](" + appLink("APP_LINK_V2RAYTUN", siteV2RayTunDefault) + ") — Windows, macOS\n" +
		"• [Streisand](" + appLink("APP_LINK_STREISAND", siteStreisandDefault) + ") — только macOS\n\n" +
		"━━━━━━━━━━━━━━━━━━\n\n" +
		"📲 *Как подключиться*\n\n" +
		"1️⃣ Установите приложение\n" +
		"2️⃣ В меню бота нажмите «🔗 Получить ссылку доступа» и скопируйте её\n" +
		"3️⃣ В приложении нажмите ➕ и выберите «Добавить из буфера обмена»\n" +
		"4️⃣ Выберите сервер и подключайтесь 🚀\n\n" +
		"💬 Что-то не получается? Напишите в поддержку — поможем."
}

// appLink возвращает ссылку из переменной окружения, а если она не задана —
// рабочее значение по умолчанию. Так пользователь никогда не увидит заглушку.
func appLink(envKey, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
		return v
	}
	return fallback
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
