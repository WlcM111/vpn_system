package billing

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"vpn-platform/internal/billing/platega"
)

// PlategaSettings — настройки Platega из окружения billing-service.
//
//	PLATEGA_MERCHANT_ID        UUID мерчанта (ЛК Platega → Настройки)
//	PLATEGA_SECRET             API-ключ
//	PLATEGA_API_BASE           адрес API, по умолчанию https://app.platega.io
//	PLATEGA_PAYMENT_METHOD     код способа оплаты (PaymentMethodInt), по умолчанию 2 (СБП QR)
//	PLATEGA_RETURN_URL         адрес возврата после оплаты (необязательно)
//	PLATEGA_FAILED_URL         адрес возврата при неуспехе (необязательно)
//	PLATEGA_RECONCILE_TICK     период воркера сверки, по умолчанию 30s
//	PLATEGA_RECONCILE_BATCH_SIZE  заказов за проход, по умолчанию 50
//
// Возврат на return-/failed-URL оплату НЕ подтверждает: это только переход
// пользователя в браузере.
type PlategaSettings struct {
	Enabled bool
	Client  platega.Config

	ReturnURL string
	FailedURL string

	ReconcileTick  time.Duration
	ReconcileBatch int
	// CheckLease — аренда заказа на время проверки: упавший обработчик не
	// блокирует заказ дольше.
	CheckLease time.Duration
	// StaleCreating — через сколько незавершённое создание счёта считается
	// оборвавшимся (HTTP-таймаут клиента 15 с плюс запись результата).
	StaleCreating time.Duration
	// ReuseMinRemaining — повторное нажатие отдаёт ту же ссылку, если ей
	// осталось жить не меньше этого времени.
	ReuseMinRemaining time.Duration
	// DefaultExpiresIn — срок ссылки, если Platega не прислала expiresIn
	// (в документации ссылка живёт 15 минут).
	DefaultExpiresIn time.Duration
}

// LoadPlategaSettings читает настройки. Platega выключена, если не задан ни
// ID мерчанта, ни ключ. Если задано только одно из двух или значение
// некорректно — возвращается ошибка, а Platega остаётся выключенной: счёт
// с заведомо неверными ключами не создаётся.
func LoadPlategaSettings() (PlategaSettings, error) {
	st := PlategaSettings{
		ReconcileTick:     parseDurationEnv("PLATEGA_RECONCILE_TICK", 30*time.Second),
		ReconcileBatch:    parseIntEnv("PLATEGA_RECONCILE_BATCH_SIZE", 50),
		CheckLease:        2 * time.Minute,
		StaleCreating:     2 * time.Minute,
		ReuseMinRemaining: 2 * time.Minute,
		DefaultExpiresIn:  15 * time.Minute,
	}
	merchant := strings.TrimSpace(os.Getenv("PLATEGA_MERCHANT_ID"))
	secret := strings.TrimSpace(os.Getenv("PLATEGA_SECRET"))
	if merchant == "" && secret == "" {
		return st, nil
	}
	if merchant == "" || secret == "" {
		return st, errors.New("platega: PLATEGA_MERCHANT_ID and PLATEGA_SECRET must be set together")
	}

	base := strings.TrimSpace(os.Getenv("PLATEGA_API_BASE"))
	if base == "" {
		base = platega.DefaultBaseURL
	}
	method := platega.MethodSBPQR
	if raw := strings.TrimSpace(os.Getenv("PLATEGA_PAYMENT_METHOD")); raw != "" {
		m, err := platega.ParsePaymentMethod(raw)
		if err != nil {
			return st, err
		}
		method = m
	}
	st.Client = platega.Config{BaseURL: base, MerchantID: merchant, Secret: secret, PaymentMethod: method}
	if err := st.Client.Validate(); err != nil {
		return st, err
	}

	for _, kv := range []struct {
		key string
		dst *string
	}{
		{"PLATEGA_RETURN_URL", &st.ReturnURL},
		{"PLATEGA_FAILED_URL", &st.FailedURL},
	} {
		v := strings.TrimSpace(os.Getenv(kv.key))
		if v == "" {
			continue
		}
		if !isHTTPURL(v) {
			return st, fmt.Errorf("platega: %s must be an absolute http(s) url", kv.key)
		}
		*kv.dst = v
	}
	st.Enabled = true
	return st, nil
}

func isHTTPURL(v string) bool {
	u, err := url.Parse(v)
	return err == nil && u.Host != "" && (u.Scheme == "https" || u.Scheme == "http")
}
