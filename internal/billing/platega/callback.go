package platega

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// MaxCallbackBodyBytes — предел тела колбэка. Документированный колбэк — это
// десяток коротких полей (сотни байт); всё, что больше, — не колбэк Platega.
const MaxCallbackBodyBytes = 64 << 10

// ErrCallbackMalformed — тело колбэка нельзя использовать: нет корректного id
// транзакции или JSON повреждён.
var ErrCallbackMalformed = errors.New("platega: malformed callback")

// Callback — колбэк Platega о смене статуса транзакции.
//
// Источник: https://docs.platega.io, разделы «Колбэки — общая информация» и
// CONFIRMED/CANCELED/CHARGEBACKED/PARTIALLY_REFUNDED/DISPUTED (проверено
// 2026-11-05). Тело: id (UUID), amount (number), currency, status, payload
// (обязательные), paymentMethod (integer), paymentLinkId, subscriptionId,
// nextChargeAt (необязательные).
//
// Колбэк — только сигнал «проверь транзакцию». Статус и сумма из него НЕ
// используются для выдачи подписки: истина берётся из GET /transaction/{id}
// по ключам магазина (так рекомендует и сама документация). Поэтому строго
// обязательным здесь считается только id; остальные поля разбираются как
// подсказки для журнала и метрик.
type Callback struct {
	ID string
	// Status — пустой, если статус не входит в документированный перечень.
	Status Status
	// RawStatus — значение как пришло (обрезано до 32 символов).
	RawStatus string
	// AmountMinor — -1, если сумму не удалось разобрать точно.
	AmountMinor int64
	Currency    string
	Payload     string
	// HasPayload — поле payload присутствовало в теле.
	HasPayload    bool
	PaymentLinkID string
	// SubscriptionID — колбэк о списании по подписке Platega. Наш сервис
	// подписки Platega не создаёт, поэтому такой колбэк заведомо чужой.
	SubscriptionID string
}

type callbackBody struct {
	ID             string          `json:"id"`
	Amount         json.RawMessage `json:"amount"`
	Currency       string          `json:"currency"`
	Status         string          `json:"status"`
	PaymentMethod  json.RawMessage `json:"paymentMethod"`
	Payload        *string         `json:"payload"`
	PaymentLinkID  string          `json:"paymentLinkId"`
	SubscriptionID string          `json:"subscriptionId"`
}

// ParseCallback разбирает тело колбэка. Ошибка возвращается только когда
// колбэк нельзя связать с транзакцией (нет UUID в id) или тело не JSON-объект.
func ParseCallback(raw []byte) (*Callback, error) {
	if len(raw) == 0 || len(raw) > MaxCallbackBodyBytes {
		return nil, fmt.Errorf("%w: body size %d", ErrCallbackMalformed, len(raw))
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var b callbackBody
	if err := dec.Decode(&b); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCallbackMalformed, err)
	}
	// После объекта не должно быть второго JSON-значения.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing data", ErrCallbackMalformed)
	}

	id := strings.ToLower(strings.TrimSpace(b.ID))
	if !IsUUID(id) {
		return nil, fmt.Errorf("%w: id is not a UUID", ErrCallbackMalformed)
	}

	cb := &Callback{
		ID:             id,
		RawStatus:      truncate(b.Status, 32),
		AmountMinor:    -1,
		Currency:       strings.ToUpper(truncate(b.Currency, 8)),
		PaymentLinkID:  truncate(b.PaymentLinkID, 64),
		SubscriptionID: truncate(b.SubscriptionID, 64),
	}
	if st, err := ParseStatus(b.Status); err == nil {
		cb.Status = st
	}
	cb.AmountMinor = parseHintAmount(b.Amount)
	if b.Payload != nil {
		cb.HasPayload = true
		// order_id — UUID (36 символов); длинное значение заведомо не наше.
		cb.Payload = truncate(*b.Payload, 128)
	}
	return cb, nil
}

// parseHintAmount разбирает сумму-подсказку: JSON-число или строка с числом.
// -1 — сумму не удалось разобрать точно (решения по ней всё равно не
// принимаются).
func parseHintAmount(raw json.RawMessage) int64 {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return -1
	}
	if strings.HasPrefix(s, `"`) {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return -1
		}
		s = strings.TrimSpace(str)
	}
	v, err := ParseJSONAmountMinor(json.Number(s))
	if err != nil {
		return -1
	}
	return v
}

// AuthenticateCallback проверяет заголовки X-MerchantId и X-Secret колбэка.
// Platega присылает в них ID мерчанта и API-ключ магазина; сравнение — за
// постоянное время, ID мерчанта (UUID) — без учёта регистра.
func AuthenticateCallback(h http.Header, merchantID, secret string) bool {
	if strings.TrimSpace(merchantID) == "" || secret == "" {
		return false
	}
	gotMerchant := strings.ToLower(strings.TrimSpace(h.Get("X-MerchantId")))
	gotSecret := h.Get("X-Secret")
	okMerchant := subtle.ConstantTimeCompare([]byte(gotMerchant), []byte(strings.ToLower(strings.TrimSpace(merchantID)))) == 1
	okSecret := subtle.ConstantTimeCompare([]byte(gotSecret), []byte(secret)) == 1
	return okMerchant && okSecret
}

// SanitizedJSON — безопасная для хранения копия колбэка: только поля, нужные
// для аудита. payload сохраняется, только если колбэк сопоставлен с нашим
// заказом (includePayload): чужие данные магазина не храним.
func (c *Callback) SanitizedJSON(includePayload bool) json.RawMessage {
	out := map[string]any{
		"id":     c.ID,
		"status": c.RawStatus,
	}
	if c.AmountMinor >= 0 {
		out["amount"] = json.Number(FormatMinor(c.AmountMinor))
	}
	if c.Currency != "" {
		out["currency"] = c.Currency
	}
	if includePayload && c.HasPayload {
		out["payload"] = c.Payload
	}
	if c.PaymentLinkID != "" {
		out["paymentLinkId"] = c.PaymentLinkID
	}
	if c.SubscriptionID != "" {
		out["subscriptionId"] = c.SubscriptionID
	}
	b, err := json.Marshal(out)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return b
}
