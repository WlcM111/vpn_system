//go:build e2e

// Сквозной тест: оплата → активация подписки → выдача доступа → фид отдаёт конфиг.
//
// Запуск:
//
//	make e2e
//
// Тест НЕ поднимает окружение сам — это делает make-цель. Так проще отлаживать:
// при падении контейнеры остаются живыми и можно посмотреть логи.
package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
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

const (
	// Порты сдвинуты в docker-compose.e2e.yml, чтобы окружение не
	// конфликтовало с другими проектами на машине разработчика.
	billingURL      = "http://127.0.0.1:58082"
	orchestratorURL = "http://127.0.0.1:58084"
	// EXTERNAL-listener Kafka: анонсирует себя как localhost, поэтому
	// клиент с машины разработчика корректно находит брокера.
	// Порт совпадает с анонсируемым Kafka адресом (EXTERNAL://localhost:19092):
	// клиент после подключения переходит именно на анонсированный адрес.
	kafkaBroker = "localhost:19092"
	dsn         = "postgres://vpn:vpn_e2e@127.0.0.1:55432/vpn_platform?sslmode=disable"

	adminUser    = "admin"
	adminPass    = "e2e-admin-pass"
	webhookToken = "e2e-webhook-token"

	// Общий дедлайн ожидания асинхронных эффектов: события идут через Kafka,
	// поэтому мгновенного результата не бывает.
	waitTimeout = 90 * time.Second
	pollEvery   = time.Second
)

func TestE2EPaymentGrantsAccess(t *testing.T) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к БД: %v", err)
	}
	defer pool.Close()

	waitForServices(t)

	// Уникальный пользователь на каждый прогон — тест можно гонять повторно.
	telegramID := time.Now().UnixNano()%1_000_000_000 + 900_000_000_000

	t.Logf("тестовый пользователь: %d", telegramID)

	// Шаг 1. Регистрируем ноду и пул-айтем через admin API.
	// Без них аллокатору нечего выдавать.
	registerNode(t)
	registerPoolItem(t)

	// Шаг 2. Публикуем команду создания платежа — так же, как это делает бот.
	commandID := uuid.NewString()
	publishCheckoutCommand(t, telegramID, commandID)

	// Шаг 3. Ждём, пока billing создаст платёж через стаб YooKassa.
	paymentID := waitForPayment(t, pool, telegramID)
	t.Logf("платёж создан: %s", paymentID)

	// Шаг 4. Имитируем вебхук об успешной оплате.
	sendWebhook(t, paymentID)

	// Шаг 5. Подписка должна активироваться (billing → Kafka → user-subscription).
	waitForActiveSubscription(t, pool, telegramID)
	t.Log("подписка активирована")

	// Шаг 6. Оркестратор должен выдать креды (subscription.events → orchestrator).
	waitForCredentials(t, pool, telegramID)
	t.Log("доступы выданы")

	// Шаг 7. Фид подписки отдаёт рабочий конфиг.
	token := fetchToken(t, pool, telegramID)
	body := fetchFeed(t, token)

	if !strings.Contains(body, "vless://") {
		t.Fatalf("фид не содержит конфигурации:\n%s", body)
	}
	t.Log("фид отдал конфигурацию — сквозной путь работает")
}

// --------------------------------------------------------------------------
// шаги
// --------------------------------------------------------------------------

func waitForServices(t *testing.T) {
	t.Helper()
	targets := map[string]string{
		"billing":      billingURL + "/readyz",
		"orchestrator": orchestratorURL + "/readyz",
	}
	deadline := time.Now().Add(waitTimeout)
	for name, url := range targets {
		for {
			if time.Now().After(deadline) {
				t.Fatalf("сервис %s не поднялся за %v", name, waitTimeout)
			}
			resp, err := http.Get(url)
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			time.Sleep(pollEvery)
		}
	}
}

func registerNode(t *testing.T) {
	t.Helper()
	body := map[string]any{
		"server_key":          "e2e-node-1",
		"node_id":             "e2e-node-1",
		"country_code":        "LT",
		"title":               "E2E Node",
		"public_host":         "e2e.example.com",
		"port":                443,
		"transport":           "ws",
		"security":            "tls",
		"default_inbound_tag": "vless-ws-in",
		"ws_path":             "/ws",
		"max_users":           1000,
		"weight":              100,
		"enabled":             true,
	}
	adminPost(t, "/admin/nodes", body)
}

func registerPoolItem(t *testing.T) {
	t.Helper()
	body := map[string]any{
		"item_key":     "e2e-node-1-ws",
		"server_key":   "e2e-node-1",
		"country_code": "LT",
		"title":        "E2E Lithuania",
		"enabled":      true,
		"sort_order":   10,
	}
	adminPost(t, "/admin/pool-items", body)
}

func adminPost(t *testing.T, path string, payload any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, orchestratorURL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(adminUser, adminPass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("admin POST %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin POST %s вернул %d: %s", path, resp.StatusCode, msg)
	}
}

func publishCheckoutCommand(t *testing.T, telegramID int64, commandID string) {
	t.Helper()
	cmd := map[string]any{
		"type":                "billing.create_subscription_checkout",
		"command_id":          commandID,
		"telegram_id":         telegramID,
		"plan_code":           "monthly_30d",
		"save_payment_method": false,
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  "billing.commands",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", telegramID)),
		Value: raw,
	}); err != nil {
		t.Fatalf("публикация команды в Kafka: %v", err)
	}
}

func waitForPayment(t *testing.T, pool *pgxpool.Pool, telegramID int64) string {
	t.Helper()
	var paymentID string
	waitUntil(t, "создание платежа", func() bool {
		err := pool.QueryRow(context.Background(),
			`SELECT payment_id FROM payments
			 WHERE telegram_id = $1 AND payment_id IS NOT NULL AND payment_id <> ''
			 ORDER BY created_at DESC LIMIT 1`, telegramID).Scan(&paymentID)
		return err == nil && paymentID != ""
	})
	return paymentID
}

func sendWebhook(t *testing.T, paymentID string) {
	t.Helper()
	payload := map[string]any{
		"type":  "notification",
		"event": "payment.succeeded",
		"object": map[string]any{
			"id":     paymentID,
			"status": "succeeded",
			"paid":   true,
			"payment_method": map[string]any{
				"id":    uuid.NewString(),
				"type":  "bank_card",
				"saved": false,
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(
		billingURL+"/webhooks/yookassa/"+webhookToken,
		"application/json",
		bytes.NewReader(raw),
	)
	if err != nil {
		t.Fatalf("отправка вебхука: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("вебхук вернул %d: %s", resp.StatusCode, body)
	}
}

func waitForActiveSubscription(t *testing.T, pool *pgxpool.Pool, telegramID int64) {
	t.Helper()
	waitUntil(t, "активацию подписки", func() bool {
		var status string
		err := pool.QueryRow(context.Background(),
			`SELECT status FROM user_subscriptions WHERE telegram_id = $1`, telegramID).Scan(&status)
		return err == nil && (status == "active" || status == "grace")
	})
}

func waitForCredentials(t *testing.T, pool *pgxpool.Pool, telegramID int64) {
	t.Helper()
	waitUntil(t, "выдачу доступов", func() bool {
		var n int
		err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM vpn_user_node_credentials
			 WHERE telegram_id = $1 AND enabled = true`, telegramID).Scan(&n)
		return err == nil && n > 0
	})
}

func fetchToken(t *testing.T, pool *pgxpool.Pool, telegramID int64) string {
	t.Helper()
	var token string
	if err := pool.QueryRow(context.Background(),
		`SELECT token FROM subscription_tokens WHERE telegram_id = $1`, telegramID).Scan(&token); err != nil {
		t.Fatalf("токен подписки не найден: %v", err)
	}
	return token
}

func fetchFeed(t *testing.T, token string) string {
	t.Helper()
	resp, err := http.Get(orchestratorURL + "/sub/" + token)
	if err != nil {
		t.Fatalf("запрос фида: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("фид вернул %d: %s", resp.StatusCode, raw)
	}

	// Формат задан SUBSCRIPTION_FEED_FORMAT=base64.
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		// Если фид отдан как plain — используем как есть.
		return string(raw)
	}
	return string(decoded)
}

// waitUntil опрашивает условие до истечения дедлайна.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(pollEvery)
	}
	t.Fatalf("не дождались: %s (таймаут %v)", what, waitTimeout)
}
