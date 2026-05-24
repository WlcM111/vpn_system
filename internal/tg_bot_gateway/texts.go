package tg_bot_gateway

import (
	"os"
	"strings"
	"time"
)

func welcomeText() string {
	return "Привет! 👋\n\n" +
		"Я помогу подключиться к нашему VPN через *v2RayTun*.\n\n" +
		"Что уже умеет бот:\n" +
		"• выдать пробный период на *3 дня*;\n" +
		"• принять оплату подписки;\n" +
		"• после активации выдать *два варианта VLESS-ссылок* для разных зарубежных маршрутов.\n\n" +
		"Выбери действие в меню ниже 👇"
}

func downloadVPNText() string {
	return "Чтобы пользоваться нашим VPN, установи клиент *v2RayTun*.\n\n" +
		"В приложении тебе понадобится:\n" +
		"• открыть вкладку *Connect*;\n" +
		"• нажать ➕;\n" +
		"• выбрать *Enter link* или *Import from clipboard*;\n" +
		"• вставить одну из ссылок, которые пришлет бот.\n\n" +
		"После этого появится список доступных серверов."
}

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

func formatDate(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Format("02.01.2006")
}
