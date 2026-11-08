package platega

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// Пример тела из документации (callback CONFIRMED, «Обычный платёж»).
const docConfirmedExample = `{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "amount": 1500.25,
  "currency": "RUB",
  "status": "CONFIRMED",
  "paymentMethod": 2,
  "payload": "order-8089"
}`

func TestParseCallbackDocExample(t *testing.T) {
	cb, err := ParseCallback([]byte(docConfirmedExample))
	if err != nil {
		t.Fatalf("ParseCallback: %v", err)
	}
	if cb.ID != "3fa85f64-5717-4562-b3fc-2c963f66afa6" || cb.Status != StatusConfirmed ||
		cb.AmountMinor != 150025 || cb.Currency != "RUB" || cb.Payload != "order-8089" || !cb.HasPayload {
		t.Fatalf("unexpected %+v", cb)
	}
}

func TestParseCallbackVariants(t *testing.T) {
	// Подписочный колбэк из документации: пустой payload и subscriptionId.
	sub := `{"id":"3fa85f64-5717-4562-b3fc-2c963f66afa6","amount":200,"currency":"RUB","status":"CONFIRMED","paymentMethod":6,"payload":"","subscriptionId":"7c9e6679-7425-40de-944b-e07fc1f90ae7","nextChargeAt":"2026-11-28T09:00:00Z"}`
	cb, err := ParseCallback([]byte(sub))
	if err != nil {
		t.Fatal(err)
	}
	if cb.SubscriptionID == "" || cb.Payload != "" || !cb.HasPayload {
		t.Fatalf("unexpected %+v", cb)
	}

	// Неизвестный статус и неразборчивая сумма не делают колбэк негодным:
	// истина всё равно берётся из GET /transaction/{id}.
	odd := `{"ID":"3FA85F64-5717-4562-B3FC-2C963F66AFA6","amount":"x","status":"SOMETHING_NEW"}`
	cb, err = ParseCallback([]byte(odd))
	if err != nil {
		t.Fatal(err)
	}
	if cb.ID != "3fa85f64-5717-4562-b3fc-2c963f66afa6" || cb.Status != "" || cb.AmountMinor != -1 || cb.HasPayload {
		t.Fatalf("unexpected %+v", cb)
	}
}

func TestParseCallbackRejects(t *testing.T) {
	bad := []string{
		``,
		`{}`,
		`[]`,
		`{"id":"order-8089","status":"CONFIRMED"}`,
		`{"id":"3fa85f64-5717-4562-b3fc-2c963f66afa6"} {"id":"x"}`,
		`{"id":`,
		`null`,
		`{"id":"3fa85f64-5717-4562-b3fc-2c963f66afa6","amount":` + strings.Repeat("9", MaxCallbackBodyBytes) + `}`,
	}
	for _, b := range bad {
		if _, err := ParseCallback([]byte(b)); err == nil || !errors.Is(err, ErrCallbackMalformed) {
			t.Errorf("ParseCallback(%.40q) = %v; want ErrCallbackMalformed", b, err)
		}
	}
}

func TestAuthenticateCallback(t *testing.T) {
	h := http.Header{}
	h.Set("X-MerchantId", strings.ToUpper(testMerchant))
	h.Set("X-Secret", testSecret)
	if !AuthenticateCallback(h, testMerchant, testSecret) {
		t.Fatal("valid headers rejected")
	}
	h.Set("X-Secret", testSecret+"x")
	if AuthenticateCallback(h, testMerchant, testSecret) {
		t.Fatal("wrong secret accepted")
	}
	h.Set("X-Secret", testSecret)
	h.Set("X-MerchantId", "00000000-0000-0000-0000-000000000000")
	if AuthenticateCallback(h, testMerchant, testSecret) {
		t.Fatal("wrong merchant accepted")
	}
	if AuthenticateCallback(http.Header{}, "", "") {
		t.Fatal("empty configuration must never authenticate")
	}
	if AuthenticateCallback(http.Header{}, testMerchant, testSecret) {
		t.Fatal("missing headers accepted")
	}
}

func TestSanitizedJSON(t *testing.T) {
	cb, _ := ParseCallback([]byte(docConfirmedExample))
	var m map[string]any
	if err := json.Unmarshal(cb.SanitizedJSON(false), &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["payload"]; ok {
		t.Fatal("payload stored for unmatched callback")
	}
	if err := json.Unmarshal(cb.SanitizedJSON(true), &m); err != nil {
		t.Fatal(err)
	}
	if m["payload"] != "order-8089" || m["amount"] != 1500.25 {
		t.Fatalf("unexpected %v", m)
	}
}

func FuzzParseCallback(f *testing.F) {
	f.Add([]byte(docConfirmedExample))
	f.Add([]byte(`{"id":"3fa85f64-5717-4562-b3fc-2c963f66afa6","status":"CANCELED","amount":1e2}`))
	f.Add([]byte(`{"id":1}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		cb, err := ParseCallback(b)
		if err != nil {
			return
		}
		if !IsUUID(cb.ID) || cb.ID != strings.ToLower(cb.ID) {
			t.Fatalf("accepted bad id %q", cb.ID)
		}
		if !json.Valid(cb.SanitizedJSON(true)) {
			t.Fatal("sanitized JSON invalid")
		}
	})
}
