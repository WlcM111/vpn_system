package billing

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vpn-platform/internal/billing/platega"
	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

func kafkaProvider(s string) kafkacontracts.BillingPaymentProvider {
	return kafkacontracts.BillingPaymentProvider(s)
}

const (
	unitMerchant = "1a021d91-9b26-4762-b303-5d4aac74e921"
	unitSecret   = "unit-test-secret"
)

func unitService(t *testing.T, enabled bool) *Service {
	t.Helper()
	cfg := platega.Config{BaseURL: "http://127.0.0.1:1", MerchantID: unitMerchant, Secret: unitSecret, PaymentMethod: platega.MethodSBPQR}
	svc := &Service{plategaCfg: PlategaSettings{Enabled: enabled, Client: cfg}}
	if enabled {
		c, err := platega.NewClient(cfg, &http.Client{Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		svc.platega = c
	}
	return svc
}

// Отказы веб-хука, которые не должны доходить до БД: метод, выключенная
// Platega, неверная подпись, повреждённое тело.
func TestPlategaWebhookRejectsBeforeDatabase(t *testing.T) {
	body := `{"id":"3fa85f64-5717-4562-b3fc-2c963f66afa6","amount":89,"currency":"RUB","status":"CONFIRMED","payload":"x"}`
	cases := []struct {
		name    string
		enabled bool
		method  string
		merch   string
		secret  string
		body    string
		want    int
	}{
		{"get", true, http.MethodGet, unitMerchant, unitSecret, body, http.StatusMethodNotAllowed},
		{"disabled", false, http.MethodPost, unitMerchant, unitSecret, body, http.StatusServiceUnavailable},
		{"no-headers", true, http.MethodPost, "", "", body, http.StatusUnauthorized},
		{"bad-secret", true, http.MethodPost, unitMerchant, unitSecret + "x", body, http.StatusUnauthorized},
		{"bad-merchant", true, http.MethodPost, "00000000-0000-0000-0000-000000000000", unitSecret, body, http.StatusUnauthorized},
		{"malformed", true, http.MethodPost, unitMerchant, unitSecret, `{"id":"not-uuid"}`, http.StatusBadRequest},
		{"too-large", true, http.MethodPost, unitMerchant, unitSecret, `{"id":"` + strings.Repeat("a", platega.MaxCallbackBodyBytes+10) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			NewHTTPHandlers(unitService(t, tc.enabled)).Register(mux)
			req := httptest.NewRequest(tc.method, "/webhooks/platega", strings.NewReader(tc.body))
			if tc.merch != "" {
				req.Header.Set("X-MerchantId", tc.merch)
			}
			if tc.secret != "" {
				req.Header.Set("X-Secret", tc.secret)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.want, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), unitSecret) {
				t.Fatal("response leaks secret")
			}
		})
	}
}

func TestNormalizeProvider(t *testing.T) {
	for in, want := range map[string]string{"": "yookassa", "yookassa": "yookassa", " YooKassa ": "yookassa", "platega": "platega", "PLATEGA": "platega"} {
		got, ok := normalizeProvider(kafkaProvider(in))
		if !ok || string(got) != want {
			t.Errorf("normalizeProvider(%q) = %q, %v", in, got, ok)
		}
	}
	for _, in := range []string{"cryptobot", "card", "sbp"} {
		if _, ok := normalizeProvider(kafkaProvider(in)); ok {
			t.Errorf("normalizeProvider(%q) accepted", in)
		}
	}
}

func TestHumanRub(t *testing.T) {
	for in, want := range map[string]string{"89.00": "89", "89": "89", "89.5": "89,50", "1500.25": "1500,25", "bad": "bad"} {
		if got := humanRub(in); got != want {
			t.Errorf("humanRub(%q) = %q, want %q", in, got, want)
		}
	}
	if got := planSummary(30, "89.00"); got != "30 дней — 89 ₽" {
		t.Errorf("planSummary = %q", got)
	}
}

func TestLoadPlategaSettings(t *testing.T) {
	t.Setenv("PLATEGA_MERCHANT_ID", "")
	t.Setenv("PLATEGA_SECRET", "")
	st, err := LoadPlategaSettings()
	if err != nil || st.Enabled {
		t.Fatalf("empty config must disable Platega without error: %+v %v", st, err)
	}
	t.Setenv("PLATEGA_MERCHANT_ID", unitMerchant)
	if _, err := LoadPlategaSettings(); err == nil {
		t.Fatal("merchant without secret must be an error")
	}
	t.Setenv("PLATEGA_SECRET", unitSecret)
	st, err = LoadPlategaSettings()
	if err != nil || !st.Enabled || st.Client.PaymentMethod != platega.MethodSBPQR || st.Client.BaseURL != platega.DefaultBaseURL {
		t.Fatalf("defaults: %+v %v", st, err)
	}
	t.Setenv("PLATEGA_PAYMENT_METHOD", "6")
	if _, err := LoadPlategaSettings(); err == nil {
		t.Fatal("undocumented payment method accepted")
	}
	t.Setenv("PLATEGA_PAYMENT_METHOD", "11")
	t.Setenv("PLATEGA_RETURN_URL", "t.me/bot")
	if _, err := LoadPlategaSettings(); err == nil {
		t.Fatal("relative return url accepted")
	}
	t.Setenv("PLATEGA_RETURN_URL", "https://t.me/vpn_house_bot")
	st, err = LoadPlategaSettings()
	if err != nil || st.Client.PaymentMethod != platega.MethodCardAcquiring || st.ReturnURL != "https://t.me/vpn_house_bot" {
		t.Fatalf("custom: %+v %v", st, err)
	}
	if strings.Contains(strings.ToLower(st.Client.BaseURL), "secret") {
		t.Fatal("unexpected")
	}
}
