package platega

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testMerchant = "1a021d91-9b26-4762-b303-5d4aac74e921"
	testSecret   = "test-secret-value-never-logged"
	testTxID     = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
	testOrderID  = "0b3c5f7e-1a2b-4c3d-8e9f-001122334455"
)

func newTestClient(t *testing.T, baseURL string, timeout time.Duration) *Client {
	t.Helper()
	c, err := NewClient(Config{
		BaseURL:       baseURL,
		MerchantID:    testMerchant,
		Secret:        testSecret,
		PaymentMethod: MethodSBPQR,
	}, &http.Client{Timeout: timeout})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func testCreateRequest() CreateRequest {
	return CreateRequest{
		AmountMinor: 8900,
		Currency:    "RUB",
		Description: "Подписка на сервис защищённого соединения (30 дней)",
		ReturnURL:   "https://t.me/vpn_house_bot",
		FailedURL:   "https://t.me/vpn_house_bot",
		Payload:     testOrderID,
		OrderID:     testOrderID,
		UserEmail:   UserEmailForTelegram(5512345678),
		UserID:      "5512345678",
	}
}

func TestConfigValidate(t *testing.T) {
	const secret = "SECRET-VALUE-123"
	good := Config{BaseURL: DefaultBaseURL, MerchantID: testMerchant, Secret: secret, PaymentMethod: MethodSBPQR}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := []Config{
		{BaseURL: DefaultBaseURL, MerchantID: "", Secret: secret, PaymentMethod: MethodSBPQR},
		{BaseURL: DefaultBaseURL, MerchantID: "not-a-uuid", Secret: secret, PaymentMethod: MethodSBPQR},
		{BaseURL: DefaultBaseURL, MerchantID: testMerchant, Secret: "", PaymentMethod: MethodSBPQR},
		{BaseURL: DefaultBaseURL, MerchantID: testMerchant, Secret: secret, PaymentMethod: 6},
		{BaseURL: "app.platega.io", MerchantID: testMerchant, Secret: secret, PaymentMethod: MethodSBPQR},
		{BaseURL: "ftp://app.platega.io", MerchantID: testMerchant, Secret: secret, PaymentMethod: MethodSBPQR},
	}
	for i, c := range bad {
		err := c.Validate()
		if err == nil {
			t.Errorf("bad config %d accepted", i)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("config error leaks secret: %v", err)
		}
	}
}

// Контракт POST /transaction/process: путь, заголовки, тело — как в
// документации (docs.platega.io, «Создание платежной ссылки с заданным
// методом»).
func TestCreateTransactionContract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/transaction/process" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-MerchantId") != testMerchant || r.Header.Get("X-Secret") != testSecret {
			t.Errorf("auth headers missing")
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type %q", ct)
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			t.Fatalf("bad body: %v", err)
		}
		if body["paymentMethod"] != json.Number("2") {
			t.Errorf("paymentMethod = %v", body["paymentMethod"])
		}
		pd := body["paymentDetails"].(map[string]any)
		if pd["amount"] != json.Number("89.00") || pd["currency"] != "RUB" {
			t.Errorf("paymentDetails = %v", pd)
		}
		for _, k := range []string{"description", "return", "failedUrl", "payload", "orderId", "metadata"} {
			if _, ok := body[k]; !ok {
				t.Errorf("field %s missing", k)
			}
		}
		if _, ok := body["id"]; ok {
			t.Errorf("id must not be sent")
		}
		md := body["metadata"].(map[string]any)
		if md["user_email"] != "5512345678@telegram.org" || md["userId"] != "5512345678" {
			t.Errorf("metadata = %v", md)
		}
		if _, ok := md["userName"]; ok {
			t.Errorf("userName must not be sent")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"paymentMethod":"SBPQR","transactionId":"`+strings.ToUpper(testTxID)+`","redirect":"https://pay.platega.io?qrsbp","return":"https://t.me/vpn_house_bot","paymentDetails":"89 RUB","status":"PENDING","expiresIn":"00:15:00","merchantId":"`+testMerchant+`","usdtRate":93.45}`)
	}))
	defer srv.Close()

	res, err := newTestClient(t, srv.URL, 5*time.Second).CreateTransaction(context.Background(), testCreateRequest())
	if err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	if res.TransactionID != testTxID {
		t.Errorf("transaction id must be normalised to lower case, got %q", res.TransactionID)
	}
	if res.RedirectURL != "https://pay.platega.io?qrsbp" || res.Status != StatusPending || res.ExpiresIn != 15*time.Minute {
		t.Errorf("unexpected result %+v", res)
	}
}

func TestCreateTransactionErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		ambiguous bool
		code      string
	}{
		{"validation", 400, `{"code":"VALIDATION","type":"x","message":"bad amount","traceId":"t-1"}`, false, "VALIDATION"},
		{"unauthorized", 401, `{"code":"UNAUTHORIZED","message":"auth"}`, false, "UNAUTHORIZED"},
		{"forbidden", 403, ``, false, ""},
		{"rate", 429, `{}`, false, ""},
		{"server", 500, `{"code":"INTERNAL"}`, true, "INTERNAL"},
		{"gateway", 502, `<html>`, true, ""},
		{"redirect", 302, ``, true, ""},
		{"ok-garbage", 200, `not json`, true, ""},
		{"ok-no-id", 200, `{"redirect":"https://pay.platega.io/x","status":"PENDING"}`, true, ""},
		{"ok-bad-redirect", 200, `{"transactionId":"` + testTxID + `","redirect":"javascript:alert(1)"}`, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == 302 {
					w.Header().Set("Location", "/elsewhere")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			_, err := newTestClient(t, srv.URL, 5*time.Second).CreateTransaction(context.Background(), testCreateRequest())
			if err == nil {
				t.Fatal("expected error")
			}
			var re *RequestError
			if !errors.As(err, &re) {
				t.Fatalf("not a RequestError: %T", err)
			}
			if re.Ambiguous != tc.ambiguous || IsAmbiguous(err) != tc.ambiguous {
				t.Errorf("ambiguous = %v, want %v (%v)", re.Ambiguous, tc.ambiguous, err)
			}
			if re.Code != tc.code {
				t.Errorf("code = %q, want %q", re.Code, tc.code)
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Errorf("error leaks secret: %v", err)
			}
		})
	}
}

// Таймаут после отправки запроса — исход неизвестен: Platega могла создать
// транзакцию, а ответ потерялся.
func TestCreateTransactionTimeoutIsAmbiguous(t *testing.T) {
	var got atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got.Add(1)
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	_, err := newTestClient(t, srv.URL, 100*time.Millisecond).CreateTransaction(context.Background(), testCreateRequest())
	if err == nil || !IsAmbiguous(err) {
		t.Fatalf("expected ambiguous error, got %v", err)
	}
	if got.Load() != 1 {
		t.Fatalf("request must be sent exactly once, got %d", got.Load())
	}
}

// Соединение не установлено — запрос точно не обработан.
func TestCreateTransactionConnectionRefusedIsDefinite(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, err = newTestClient(t, "http://"+addr, time.Second).CreateTransaction(context.Background(), testCreateRequest())
	if err == nil || IsAmbiguous(err) {
		t.Fatalf("expected definite error, got %v", err)
	}
}

func TestCreateTransactionResponseTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"transactionId":"`+testTxID+`","redirect":"https://pay.platega.io/x","pad":"`+strings.Repeat("a", maxResponseBytes)+`"}`)
	}))
	defer srv.Close()
	_, err := newTestClient(t, srv.URL, time.Second).CreateTransaction(context.Background(), testCreateRequest())
	if err == nil || !IsAmbiguous(err) {
		t.Fatalf("expected ambiguous error, got %v", err)
	}
}

func TestCreateTransactionRejectsBadInput(t *testing.T) {
	c := newTestClient(t, "http://127.0.0.1:1", time.Second)
	for _, mut := range []func(*CreateRequest){
		func(r *CreateRequest) { r.AmountMinor = 0 },
		func(r *CreateRequest) { r.AmountMinor = -5 },
		func(r *CreateRequest) { r.UserEmail = "" },
		func(r *CreateRequest) { r.ReturnURL = "" },
		func(r *CreateRequest) { r.FailedURL = " " },
		func(r *CreateRequest) { r.Description = "" },
		func(r *CreateRequest) { r.Currency = "" },
	} {
		req := testCreateRequest()
		mut(&req)
		if _, err := c.CreateTransaction(context.Background(), req); err == nil || IsAmbiguous(err) {
			t.Errorf("bad request accepted or ambiguous: %v", err)
		}
	}
}

// Контракт GET /transaction/{id} (docs.platega.io, «Проверка статуса оплаты
// платежа», схема TransactionStatusResponse).
func TestGetTransactionContract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/transaction/"+testTxID {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-MerchantId") != testMerchant || r.Header.Get("X-Secret") != testSecret {
			t.Errorf("auth headers missing")
		}
		_, _ = io.WriteString(w, `{"id":"`+testTxID+`","status":"CONFIRMED","paymentDetails":{"amount":1500.25,"currency":"rub"},"merchantName":"m","mechantId":"`+testMerchant+`","comission":0,"paymentMethod":"SBPQR","expiresIn":"00:15:00","return":"https://t.me/x","payload":"`+testOrderID+`","person":{"name":"PII must be ignored"},"contactPhoneNumber":"+70000000000"}`)
	}))
	defer srv.Close()
	tx, err := newTestClient(t, srv.URL, time.Second).GetTransaction(context.Background(), testTxID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if tx.Status != StatusConfirmed || tx.AmountMinor != 150025 || tx.Currency != "RUB" || tx.Payload != testOrderID || tx.ID != testTxID {
		t.Fatalf("unexpected %+v", tx)
	}
}

func TestGetTransactionErrors(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		notFound bool
	}{
		{"not-found", 404, `{"code":"NOT_FOUND"}`, true},
		{"server", 503, ``, false},
		{"unknown-status", 200, `{"id":"` + testTxID + `","status":"PAID","paymentDetails":{"amount":89,"currency":"RUB"}}`, false},
		{"no-details", 200, `{"id":"` + testTxID + `","status":"CONFIRMED"}`, false},
		{"sub-kopeck", 200, `{"id":"` + testTxID + `","status":"CONFIRMED","paymentDetails":{"amount":89.001,"currency":"RUB"}}`, false},
		{"negative", 200, `{"id":"` + testTxID + `","status":"CONFIRMED","paymentDetails":{"amount":-89,"currency":"RUB"}}`, false},
		{"garbage", 200, `{`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			_, err := newTestClient(t, srv.URL, time.Second).GetTransaction(context.Background(), testTxID)
			if err == nil {
				t.Fatal("expected error")
			}
			if errors.Is(err, ErrNotFound) != tc.notFound {
				t.Errorf("ErrNotFound = %v, want %v (%v)", errors.Is(err, ErrNotFound), tc.notFound, err)
			}
		})
	}
	c := newTestClient(t, "http://127.0.0.1:1", time.Second)
	if _, err := c.GetTransaction(context.Background(), "../../etc/passwd"); err == nil {
		t.Fatal("non-UUID id must be rejected before any request")
	}
}

// Редиректы не выполняются: POST создания не должен превратиться в GET на
// чужой хост.
func TestClientDoesNotFollowRedirects(t *testing.T) {
	var target atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { target.Add(1) }))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	_, _ = newTestClient(t, srv.URL, time.Second).GetTransaction(context.Background(), testTxID)
	if target.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}
