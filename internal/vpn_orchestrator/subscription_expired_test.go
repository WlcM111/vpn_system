package vpn_orchestrator

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ============================================================================
// Ответ для истёкшей подписки.
//
// Ключевое требование — статус 200. При 4xx клиент считает обновление
// неуспешным и ОСТАВЛЯЕТ у пользователя старые нерабочие конфиги: человек
// видит список серверов, которые не подключаются, и это выглядит как поломка
// сервиса, а не как окончание подписки.
// ============================================================================

func TestWriteExpiredSubscriptionStatusIsOK(t *testing.T) {
	for _, format := range []string{"base64", "plain"} {
		t.Run("формат="+format, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeExpiredSubscription(rec, format)

			if rec.Code != http.StatusOK {
				t.Errorf("статус = %d, ожидался 200: при 4xx клиент оставит старые конфиги", rec.Code)
			}
		})
	}
}

func TestWriteExpiredSubscriptionHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	writeExpiredSubscription(rec, "base64")

	required := []string{
		"Announce",
		"Support-Url",
		"Profile-Title",
		"Subscription-Userinfo",
		"Profile-Web-Page-Url",
	}
	for _, h := range required {
		if rec.Header().Get(h) == "" {
			t.Errorf("заголовок %s пуст", h)
		}
	}

	// Заголовки обязаны быть ASCII: кириллица в них превращается в мусор
	// при чтении клиентом, поэтому текст кодируется в base64.
	if got := rec.Header().Get("Announce"); !strings.HasPrefix(got, "base64:") {
		t.Errorf("Announce не в base64: %q", got)
	}
	if got := rec.Header().Get("Profile-Title"); !strings.HasPrefix(got, "base64:") {
		t.Errorf("Profile-Title не в base64 — кириллица приедет искажённой: %q", got)
	}

	// Кэширование ответа означало бы, что оплативший пользователь продолжит
	// видеть сообщение об истечении.
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Errorf("Cache-Control = %q, ожидался no-store", cc)
	}
}

func TestWriteExpiredSubscriptionBodyHasNoConfigs(t *testing.T) {
	rec := httptest.NewRecorder()
	writeExpiredSubscription(rec, "base64")

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rec.Body.String()))
	if err != nil {
		t.Fatalf("тело не декодируется из base64: %v", err)
	}
	body := string(decoded)

	// Рабочих конфигов быть не должно.
	//
	// Записи-заглушки при этом допустимы и необходимы: клиент, получивший
	// подписку без единого конфига, считает её невалидной и прекращает разбор,
	// так и не дойдя до заголовка announce. Заглушки ведут на 127.0.0.1:1 —
	// подключиться по ним нельзя, но их названия показывают все клиенты, и это
	// единственный способ донести сообщение до пользователя.
	//
	// Признак заглушки — нулевой UUID и локальный адрес. Всё остальное с
	// префиксом vless:// или hysteria2:// означало бы, что истёкшая подписка
	// всё ещё выдаёт доступ.
	const (
		placeholderUUID = "00000000-0000-0000-0000-000000000000"
		placeholderHost = "@127.0.0.1:1?"
	)

	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		isConfig := false
		for _, scheme := range []string{"vless://", "hysteria2://", "trojan://", "ss://"} {
			if strings.HasPrefix(line, scheme) {
				isConfig = true
				break
			}
		}
		if !isConfig {
			continue
		}

		if strings.Contains(line, placeholderUUID) && strings.Contains(line, placeholderHost) {
			continue // заглушка-сообщение, так и задумано
		}
		t.Errorf("в теле остался рабочий конфиг: %q", line)
	}

	// Параметры дублируются в теле: часть клиентов читает их надёжнее, чем
	// HTTP-заголовки.
	for _, marker := range []string{"#profile-title:", "#announce:", "#support-url:"} {
		if !strings.Contains(body, marker) {
			t.Errorf("в теле нет %q:\n%s", marker, body)
		}
	}
}

// Заглушки обязаны присутствовать: без них клиент считает подписку невалидной,
// прекращает разбор и не показывает пользователю ни сообщения, ни ссылки.
func TestWriteExpiredSubscriptionKeepsPlaceholders(t *testing.T) {
	rec := httptest.NewRecorder()
	writeExpiredSubscription(rec, "base64")

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rec.Body.String()))
	if err != nil {
		t.Fatalf("тело не декодируется из base64: %v", err)
	}

	var placeholders int
	for _, line := range strings.Split(string(decoded), "\n") {
		if strings.Contains(line, "00000000-0000-0000-0000-000000000000") {
			placeholders++
		}
	}
	if placeholders == 0 {
		t.Error("в теле нет записей-заглушек — пользователь не увидит сообщение об истечении")
	}
}

func TestWriteExpiredSubscriptionPlainFormat(t *testing.T) {
	rec := httptest.NewRecorder()
	writeExpiredSubscription(rec, "plain")

	body := rec.Body.String()
	if !strings.Contains(body, "#profile-title:") {
		t.Errorf("в plain-формате тело должно быть незакодированным:\n%s", body)
	}
}

func TestEncodeAnnounce(t *testing.T) {
	t.Run("кодирует в base64 с префиксом", func(t *testing.T) {
		got := encodeAnnounce("hello")
		const want = "base64:aGVsbG8="
		if got != want {
			t.Errorf("encodeAnnounce() = %q, ожидалось %q", got, want)
		}
	})

	t.Run("кириллица декодируется обратно без потерь", func(t *testing.T) {
		const text = "Ваша подписка завершилась"
		encoded := strings.TrimPrefix(encodeAnnounce(text), "base64:")

		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("не декодируется: %v", err)
		}
		if string(decoded) != text {
			t.Errorf("после round-trip = %q, ожидалось %q", decoded, text)
		}
	})

	t.Run("длинный текст обрезается по границе рун", func(t *testing.T) {
		// Обрезка по байтам разорвала бы многобайтовый символ и дала
		// невалидный UTF-8.
		long := strings.Repeat("я", announceMaxLen+50)
		encoded := strings.TrimPrefix(encodeAnnounce(long), "base64:")

		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("не декодируется: %v", err)
		}
		runes := []rune(string(decoded))
		if len(runes) != announceMaxLen {
			t.Errorf("длина = %d рун, ожидалось %d", len(runes), announceMaxLen)
		}
		if !strings.HasPrefix(string(decoded), "я") {
			t.Error("обрезка повредила многобайтовый символ")
		}
	})
}

func TestBotLink(t *testing.T) {
	t.Run("берёт SUBSCRIPTION_RENEW_URL", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_RENEW_URL", "https://t.me/custom_bot")
		if got := botLink(); got != "https://t.me/custom_bot" {
			t.Errorf("botLink() = %q", got)
		}
	})

	t.Run("собирает ссылку из SUPPORT_TELEGRAM", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_RENEW_URL", "")
		t.Setenv("SUPPORT_TELEGRAM", "@support_user")
		if got := botLink(); got != "https://t.me/support_user" {
			t.Errorf("botLink() = %q, ожидалось https://t.me/support_user", got)
		}
	})

	t.Run("всегда возвращает непустую ссылку", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_RENEW_URL", "")
		t.Setenv("SUPPORT_TELEGRAM", "")
		if got := botLink(); got == "" {
			t.Error("botLink() вернул пустую строку — кнопка в клиенте будет нерабочей")
		}
	})
}
