//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// Сквозной путь Platega: команда «Способ 1» → транзакция в стабе Platega →
// колбэк → проверка статуса по GET → подписка → доступ → фид.
//
// Значения совпадают с deploy/.env.e2e (тестовые, не секреты).
const (
	stubURL         = "http://127.0.0.1:58099"
	plategaMerchant = "00000000-0000-4000-8000-0000000e2e00"
	plategaSecret   = "e2e-platega-secret"
)

func TestE2EPlategaPaymentGrantsAccess(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к БД: %v", err)
	}
	defer pool.Close()

	waitForServices(t)
	registerNode(t)
	registerPoolItem(t)

	telegramID := time.Now().UnixNano()%1_000_000_000 + 910_000_000_000
	commandID := uuid.NewString()
	publishPlategaCheckout(t, telegramID, commandID)

	// billing создаёт транзакцию в стабе и отправляет ссылку (в outbox).
	var paymentID string
	waitUntil(t, "создание транзакции Platega", func() bool {
		err := pool.QueryRow(ctx, `
			SELECT payment_id FROM payments
			WHERE order_id = $1 AND provider = 'platega' AND status = 'pending' AND payment_id <> ''`, commandID).Scan(&paymentID)
		return err == nil
	})
	t.Logf("транзакция Platega: %s", paymentID)

	var links int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_outbox
		WHERE topic = 'tg.notifications' AND payload->>'telegram_id' = $1 AND payload->>'pay_url' LIKE 'https://stub.local/platega/%'`,
		fmt.Sprint(telegramID)).Scan(&links); err != nil || links != 1 {
		t.Fatalf("ссылка на оплату: %d %v", links, err)
	}

	// Колбэк с неверным ключом отклоняется и ничего не меняет.
	if code := sendPlategaCallback(t, paymentID, commandID, "wrong-secret"); code != http.StatusUnauthorized {
		t.Fatalf("колбэк с неверным ключом: %d", code)
	}
	// Колбэк до подтверждения у провайдера подписку не выдаёт.
	if code := sendPlategaCallback(t, paymentID, commandID, plategaSecret); code != http.StatusOK {
		t.Fatalf("колбэк: %d", code)
	}
	time.Sleep(3 * time.Second)
	var status string
	_ = pool.QueryRow(ctx, `SELECT status FROM payments WHERE order_id = $1`, commandID).Scan(&status)
	if status != "pending" {
		t.Fatalf("статус до подтверждения: %s", status)
	}

	// Провайдер подтвердил оплату — колбэк (с дублем) выдаёт подписку один раз.
	setStubStatus(t, paymentID, "CONFIRMED")
	for i := 0; i < 2; i++ {
		if code := sendPlategaCallback(t, paymentID, commandID, plategaSecret); code != http.StatusOK {
			t.Fatalf("колбэк: %d", code)
		}
	}
	waitForActiveSubscription(t, pool, telegramID)
	waitForCredentials(t, pool, telegramID)
	body := fetchFeed(t, fetchToken(t, pool, telegramID))
	if !strings.Contains(body, "vless://") {
		t.Fatalf("фид не содержит конфигурации:\n%s", body)
	}

	var grants int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM subscription_grants WHERE business_key = $1`, paymentID).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if grants != 1 {
		t.Fatalf("начислений по платежу: %d, ожидалось 1", grants)
	}
	t.Log("Platega: оплата → подписка → доступ → фид работает, начисление ровно одно")
}

func publishPlategaCheckout(t *testing.T, telegramID int64, commandID string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"type":                "billing.create_subscription_checkout",
		"command_id":          commandID,
		"telegram_id":         telegramID,
		"plan_code":           "monthly_30d",
		"save_payment_method": false,
		"return_url":          "https://t.me/vpn_house_bot",
		"payment_provider":    "platega",
	})
	if err != nil {
		t.Fatal(err)
	}
	w := &kafka.Writer{Addr: kafka.TCP(kafkaBroker), Topic: "billing.commands", Balancer: &kafka.LeastBytes{}}
	defer func() { _ = w.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(fmt.Sprint(telegramID)), Value: raw}); err != nil {
		t.Fatalf("публикация команды: %v", err)
	}
}

func sendPlategaCallback(t *testing.T, paymentID, payload, secret string) int {
	t.Helper()
	body := fmt.Sprintf(`{"id":%q,"amount":89,"currency":"RUB","status":"CONFIRMED","paymentMethod":2,"payload":%q}`, paymentID, payload)
	req, err := http.NewRequest(http.MethodPost, billingURL+"/webhooks/platega", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-MerchantId", plategaMerchant)
	req.Header.Set("X-Secret", secret)
	req.Header.Set("User-Agent", "Platega Payment")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("колбэк: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func setStubStatus(t *testing.T, paymentID, status string) {
	t.Helper()
	resp, err := http.Post(stubURL+"/stub/platega/status/"+paymentID+"?status="+status, "text/plain", nil)
	if err != nil {
		t.Fatalf("стаб: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("стаб вернул %d", resp.StatusCode)
	}
}
