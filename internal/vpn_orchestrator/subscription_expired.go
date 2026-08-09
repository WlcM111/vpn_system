package vpn_orchestrator

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// ============================================================================
// Ответ для истёкшей подписки.
//
// Раньше отдавали 403 «subscription is not active»: клиент показывал ошибку
// обновления, а нерабочие конфиги оставались в списке — для пользователя это
// выглядело как поломка сервиса.
//
// Теперь отдаём 200 с ПУСТЫМ списком серверов и объяснением. Клиент очищает
// список и показывает сообщение со ссылкой на бота.
//
// Параметры взяты из документации Happ (dev-docs/app-management):
//   announce             — текст объявления, до 200 символов, plain или base64
//   support-url          — кнопка поддержки (для Telegram-ссылки своя иконка)
//   profile-title        — название подписки, до 25 символов
//   subscription-userinfo — строка статуса с датой истечения
//
// sub-expire / sub-expire-button-link относятся к расширенным параметрам и
// требуют Provider ID. Отдаём их на будущее: без Provider ID клиент их просто
// игнорирует, а при его появлении заработает системный баннер с кнопкой
// «Продлить» без единой правки кода.
// ============================================================================

const (
	expiredProfileTitle = "House VPN — подписка истекла"

	// Лимит Happ на длину announce.
	announceMaxLen = 200
)

// expiredAnnounceText — сообщение, которое увидит пользователь.
// Держим в пределах 200 символов и без разметки: часть клиентов показывает
// текст как есть.
func expiredAnnounceText() string {
	return "Ваша подписка завершилась 💚 Чтобы снова пользоваться VPN, откройте " +
		"бота и продлите подписку — это займёт пару касаний. Будем рады видеть вас снова!"
}

// botLink возвращает ссылку на бота для кнопки поддержки.
func botLink() string {
	if v := strings.TrimSpace(os.Getenv("SUBSCRIPTION_RENEW_URL")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("SUPPORT_TELEGRAM")); v != "" {
		return "https://t.me/" + strings.TrimPrefix(v, "@")
	}
	return "https://t.me/vpn_house_bot"
}

// encodeAnnounce кодирует текст в формат, который Happ принимает для announce.
// Base64 используется, чтобы кириллица в HTTP-заголовке не ломалась: заголовки
// обязаны быть ASCII.
func encodeAnnounce(text string) string {
	r := []rune(text)
	if len(r) > announceMaxLen {
		r = r[:announceMaxLen]
	}
	return "base64:" + base64.StdEncoding.EncodeToString([]byte(string(r)))
}

// writeExpiredSubscription отдаёт ответ для истёкшей подписки: пустой список
// серверов плюс объяснение. Статус 200 — намеренно: при 4xx клиент считает
// обновление неуспешным и оставляет старые конфиги.
func writeExpiredSubscription(w http.ResponseWriter, feedFormat string) {
	announce := encodeAnnounce(expiredAnnounceText())
	renew := botLink()
	// Срок в прошлом: строка статуса в приложении покажет, что подписка истекла.
	expired := time.Now().Add(-time.Hour).Unix()

	// Тело подписки: те же параметры строками с '#'. Документация Happ
	// допускает оба способа доставки, а часть клиентов надёжнее читает тело.
	// Ссылок на серверы нет — список у пользователя очистится.
	// Клиенты, получившие подписку БЕЗ единого валидного конфига, считают её
	// невалидной и прекращают разбор — до заголовка announce дело не доходит
	// (в Streisand это прямо видно по ошибке "Subscription does not contain
	// valid configuration"). Поэтому кладём записи-заглушки: подключиться по
	// ним нельзя (адрес 127.0.0.1), но их НАЗВАНИЯ показывают все клиенты без
	// исключения — так сообщение видит и Happ, и Incy, и v2RayTun, и Streisand,
	// и Hiddify.
	placeholder := func(name string) string {
		return "vless://00000000-0000-0000-0000-000000000000@127.0.0.1:1" +
			"?type=tcp&security=none&encryption=none#" + escapeFragment(name)
	}

	body := strings.Join([]string{
		"#profile-title: " + expiredProfileTitle,
		"#announce: " + announce,
		"#support-url: " + renew,
		"#profile-web-page-url: " + renew,
		fmt.Sprintf("#subscription-userinfo: upload=0; download=0; total=0; expire=%d", expired),
		"#sub-expire: 1",
		"#sub-expire-button-link: " + renew,
		placeholder("⛔ Ваша Подписка закончилась. Рады, что вы были с нами. Всегда можно продлить. самые низкие цены!"),
		placeholder("👉 Продлить: t.me/vpn_house_bot"),
		"",
	}, "\n")

	if feedFormat == "base64" {
		body = base64.StdEncoding.EncodeToString([]byte(body))
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// HTTP-заголовки обязаны быть ASCII: кириллица в них превращается в
	// мусор при чтении клиентом. Формат base64 для profile-title описан
	// в документации Happ и используется панелями (3x-ui и другими).
	w.Header().Set("Profile-Title", "base64:"+
		base64.StdEncoding.EncodeToString([]byte(expiredProfileTitle)))
	w.Header().Set("Profile-Update-Interval", "6")
	w.Header().Set("Announce", announce)
	w.Header().Set("Support-Url", renew)
	w.Header().Set("Profile-Web-Page-Url", renew)
	w.Header().Set("Subscription-Userinfo",
		fmt.Sprintf("upload=0; download=0; total=0; expire=%d", expired))
	w.Header().Set("Sub-Expire", "1")
	w.Header().Set("Sub-Expire-Button-Link", renew)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}
