package platega

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Status — статус транзакции Platega в терминах API.
//
// Источник: https://docs.platega.io (схемы PaymentStatus и статус платежа в
// колбэке; проверено 2026-11-05, редакция документации от 2026-10-27).
// PENDING возвращает только GET /transaction/{id}: в колбэках он не приходит.
type Status string

const (
	StatusPending           Status = "PENDING"
	StatusConfirmed         Status = "CONFIRMED"
	StatusCanceled          Status = "CANCELED"
	StatusChargebacked      Status = "CHARGEBACKED"
	StatusPartiallyRefunded Status = "PARTIALLY_REFUNDED"
	StatusDisputed          Status = "DISPUTED"
)

// ErrUnknownStatus — статус, которого нет в документированном перечне.
var ErrUnknownStatus = errors.New("platega: unknown transaction status")

// ParseStatus приводит строку к известному статусу. Регистр не важен:
// в примерах документации встречается разное написание ключей и значений.
func ParseStatus(s string) (Status, error) {
	switch st := Status(strings.ToUpper(strings.TrimSpace(s))); st {
	case StatusPending, StatusConfirmed, StatusCanceled,
		StatusChargebacked, StatusPartiallyRefunded, StatusDisputed:
		return st, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownStatus, s)
	}
}

// PaymentMethod — код способа оплаты из схемы PaymentMethodInt.
type PaymentMethod int

const (
	MethodSBPQR         PaymentMethod = 2
	MethodERIP          PaymentMethod = 3
	MethodCardAcquiring PaymentMethod = 11
	MethodInternational PaymentMethod = 12
	MethodCrypto        PaymentMethod = 13
	MethodSberPay       PaymentMethod = 14
)

// KnownPaymentMethods — документированные значения PaymentMethodInt с
// названиями из документации.
var KnownPaymentMethods = map[PaymentMethod]string{
	MethodSBPQR:         "СБП (QR-код)",
	MethodERIP:          "ЕРИП",
	MethodCardAcquiring: "Карточный эквайринг",
	MethodInternational: "Международная оплата",
	MethodCrypto:        "Криптовалюта",
	MethodSberPay:       "Sberpay",
}

// ParsePaymentMethod разбирает и проверяет код способа оплаты.
func ParsePaymentMethod(s string) (PaymentMethod, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("platega: payment method must be an integer, got %q", s)
	}
	m := PaymentMethod(n)
	if _, ok := KnownPaymentMethods[m]; !ok {
		return 0, fmt.Errorf("platega: unsupported payment method %d", n)
	}
	return m, nil
}

// IsUUID проверяет каноническую запись UUID (8-4-4-4-12, hex). Platega
// выдаёт идентификаторы транзакций и мерчанта в этом формате; проверка
// отсекает мусор до обращения к БД и к API.
func IsUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHex(c) {
				return false
			}
		}
	}
	return true
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// SameID сравнивает идентификаторы Platega без учёта регистра (UUID).
func SameID(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

// ParseTimeSpan разбирает поле expiresIn ("00:15:00"). Формат — .NET
// TimeSpan: [d.]hh:mm:ss[.fffffff]. Неизвестный формат — ошибка: вызывающий
// код подставляет консервативное значение, а не угадывает.
func ParseTimeSpan(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("platega: empty time span")
	}
	var days int64
	if dot := strings.IndexByte(s, '.'); dot >= 0 && dot < strings.IndexByte(s, ':') {
		d, err := strconv.ParseInt(s[:dot], 10, 32)
		if err != nil || d < 0 {
			return 0, fmt.Errorf("platega: bad time span %q", s)
		}
		days = d
		s = s[dot+1:]
	}
	var frac string
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		frac = s[dot+1:]
		s = s[:dot]
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("platega: bad time span %q", s)
	}
	limits := []int64{23, 59, 59}
	var vals [3]int64
	for i, p := range parts {
		if len(p) == 0 || len(p) > 2 || !allDigits(p) {
			return 0, fmt.Errorf("platega: bad time span %q", s)
		}
		v, _ := strconv.ParseInt(p, 10, 64)
		if v > limits[i] {
			return 0, fmt.Errorf("platega: bad time span %q", s)
		}
		vals[i] = v
	}
	if frac != "" && (len(frac) > 7 || !allDigits(frac)) {
		return 0, fmt.Errorf("platega: bad time span fraction %q", frac)
	}
	d := time.Duration(days)*24*time.Hour +
		time.Duration(vals[0])*time.Hour +
		time.Duration(vals[1])*time.Minute +
		time.Duration(vals[2])*time.Second
	if d <= 0 {
		return 0, fmt.Errorf("platega: non-positive time span %q", s)
	}
	return d, nil
}

// UserEmailForTelegram — идентификатор покупателя для metadata.user_email.
// Документация Platega для Telegram-магазинов предписывает формат
// {telegram_id}@telegram.org с числовым ID (username недопустим) и одно и то
// же значение для всех платежей покупателя.
func UserEmailForTelegram(telegramID int64) string {
	return strconv.FormatInt(telegramID, 10) + "@telegram.org"
}
