//go:build integration

package billing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vpn-platform/internal/billing/platega"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ============================================================================
// Интеграционные тесты Platega на настоящем PostgreSQL (testcontainers).
//
//   go test -tags=integration ./internal/billing/...
//
// Каждый тест получает отдельную базу с накатанными миграциями platform и
// billing-service. Platega и YooKassa заменены httptest-стабами, которые
// повторяют документированный контракт; Kafka не нужна — проверяется то,
// что billing кладёт в outbox (события и уведомления).
// ============================================================================

const (
	itMerchant = "1a021d91-9b26-4762-b303-5d4aac74e921"
	itSecret   = "integration-secret"
	itTelegram = int64(5512345678)
)

var (
	pgOnce      sync.Once
	pgBaseDSN   string
	pgSetupErr  error
	dbCounter   atomic.Int64
	pgContainer *tcpostgres.PostgresContainer
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pgContainer != nil {
		_ = pgContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

func basePostgres(t *testing.T) string {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("vpn_test"),
			tcpostgres.WithUsername("vpn"),
			tcpostgres.WithPassword("vpn"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(90*time.Second)),
		)
		if err != nil {
			pgSetupErr = err
			return
		}
		pgContainer = c
		pgBaseDSN, pgSetupErr = c.ConnectionString(ctx, "sslmode=disable")
	})
	if pgSetupErr != nil {
		t.Fatalf("postgres: %v", pgSetupErr)
	}
	return pgBaseDSN
}

// newDatabase создаёт отдельную базу и накатывает миграции из перечисленных
// каталогов (stopBefore — имя файла billing-service, до которого остановиться).
func newDatabase(t *testing.T, stopBefore string) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	base := basePostgres(t)
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("it_%d_%d", time.Now().UnixNano()%1_000_000, dbCounter.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	dsn := strings.Replace(base, "/vpn_test?", "/"+name+"?", 1)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyDir(t, pool, filepath.Join("..", "..", "migrations", "platform"), "")
	applyDir(t, pool, filepath.Join("..", "..", "migrations", "billing-service"), stopBefore)
	return pool, dsn
}

func applyDir(t *testing.T, pool *pgxpool.Pool, dir, stopBefore string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		if stopBefore != "" && n >= stopBefore {
			break
		}
		applyFile(t, pool, filepath.Join(dir, n))
	}
}

func applyFile(t *testing.T, pool *pgxpool.Pool, path string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), string(body)); err != nil {
		t.Fatalf("migration %s: %v", path, err)
	}
}

// ---------------------------------------------------------------------------
// Стаб Platega по документированному контракту.
// ---------------------------------------------------------------------------

type stubTx struct {
	status   string
	amount   string
	currency string
	payload  string
}

type plategaStub struct {
	t        *testing.T
	mu       sync.Mutex
	srv      *httptest.Server
	creates  []map[string]any
	txs      map[string]*stubTx
	mode     string // ok | 400 | 500 | timeout
	gets     atomic.Int32
	getError int
}

func newPlategaStub(t *testing.T) *plategaStub {
	st := &plategaStub{t: t, txs: map[string]*stubTx{}, mode: "ok"}
	st.srv = httptest.NewServer(http.HandlerFunc(st.serve))
	t.Cleanup(st.srv.Close)
	return st
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func (st *plategaStub) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-MerchantId") != itMerchant || r.Header.Get("X-Secret") != itSecret {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"code":"UNAUTHORIZED","message":"auth"}`)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/transaction/process":
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.UseNumber()
		_ = dec.Decode(&body)
		st.mu.Lock()
		st.creates = append(st.creates, body)
		mode := st.mode
		st.mu.Unlock()
		switch mode {
		case "400":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"code":"VALIDATION","message":"bad","traceId":"t-400"}`)
			return
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
			return
		case "timeout":
			// Транзакция «создана», но ответ не успевает дойти.
			id := newUUID()
			st.store(id, body)
			select {
			case <-r.Context().Done():
			case <-time.After(20 * time.Second):
			}
			return
		}
		id := newUUID()
		st.store(id, body)
		_, _ = fmt.Fprintf(w, `{"paymentMethod":"SBPQR","transactionId":%q,"redirect":"https://pay.platega.test/%s","return":"https://t.me/x","paymentDetails":"89 RUB","status":"PENDING","expiresIn":"00:15:00","merchantId":%q,"usdtRate":93.45}`, id, id, itMerchant)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/transaction/"):
		st.gets.Add(1)
		id := strings.TrimPrefix(r.URL.Path, "/transaction/")
		st.mu.Lock()
		tx, ok := st.txs[id]
		getErr := st.getError
		var cp stubTx
		if ok {
			cp = *tx
		}
		st.mu.Unlock()
		if getErr != 0 {
			w.WriteHeader(getErr)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"code":"NOT_FOUND"}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"id":%q,"status":%q,"paymentDetails":{"amount":%s,"currency":%q},"payload":%q,"person":{"name":"PII"}}`, id, cp.status, cp.amount, cp.currency, cp.payload)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (st *plategaStub) store(id string, body map[string]any) {
	pd, _ := body["paymentDetails"].(map[string]any)
	amount := fmt.Sprint(pd["amount"])
	currency, _ := pd["currency"].(string)
	payload, _ := body["payload"].(string)
	st.mu.Lock()
	st.txs[id] = &stubTx{status: "PENDING", amount: amount, currency: currency, payload: payload}
	st.mu.Unlock()
}

func (st *plategaStub) set(id, status string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if tx, ok := st.txs[id]; ok {
		tx.status = status
	}
}

func (st *plategaStub) addForeign(id, status, amount, payload string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.txs[id] = &stubTx{status: status, amount: amount, currency: "RUB", payload: payload}
}

func (st *plategaStub) createCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.creates)
}

func (st *plategaStub) setMode(m string) {
	st.mu.Lock()
	st.mode = m
	st.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Стаб YooKassa (минимальный: создание и проверка платежа).
// ---------------------------------------------------------------------------

type yooStub struct {
	srv     *httptest.Server
	creates atomic.Int32
	fail    atomic.Int32 // HTTP-код ошибки создания; 0 — успех
}

func newYooStub(t *testing.T) *yooStub {
	ys := &yooStub{}
	ys.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/payments":
			ys.creates.Add(1)
			if code := ys.fail.Load(); code != 0 {
				w.WriteHeader(int(code))
				_, _ = io.WriteString(w, `{"type":"error","code":"internal_server_error"}`)
				return
			}
			id := newUUID()
			_, _ = fmt.Fprintf(w, `{"id":%q,"status":"pending","paid":false,"confirmation":{"type":"redirect","confirmation_url":"https://yoomoney.test/%s"},"payment_method":{"id":%q,"type":"bank_card","saved":false}}`, id, id, id)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/payments/"):
			id := strings.TrimPrefix(r.URL.Path, "/payments/")
			_, _ = fmt.Fprintf(w, `{"id":%q,"status":"succeeded","paid":true}`, id)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ys.srv.Close)
	return ys
}

// ---------------------------------------------------------------------------
// Окружение теста.
// ---------------------------------------------------------------------------

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	svc  *Service
	pg   *plategaStub
	yoo  *yooStub
	mux  *http.ServeMux
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool, _ := newDatabase(t, "")
	return newEnvWithPool(t, pool)
}

func newEnvWithPool(t *testing.T, pool *pgxpool.Pool) *env {
	t.Helper()
	pg := newPlategaStub(t)
	yoo := newYooStub(t)
	t.Setenv("PLATEGA_MERCHANT_ID", itMerchant)
	t.Setenv("PLATEGA_SECRET", itSecret)
	t.Setenv("PLATEGA_API_BASE", pg.srv.URL)
	t.Setenv("PLATEGA_PAYMENT_METHOD", "2")
	t.Setenv("PLATEGA_RETURN_URL", "")
	t.Setenv("PLATEGA_FAILED_URL", "")
	t.Setenv("YOOKASSA_API_BASE", yoo.srv.URL)
	t.Setenv("YOOKASSA_SHOP_ID", "shop")
	t.Setenv("YOOKASSA_SECRET_KEY", "yoo-secret")
	t.Setenv("YOOKASSA_RETURN_URL", "https://t.me/vpn_house_bot")
	t.Setenv("PLAN_MONTHLY_PRICE_RUB", "89")
	svc := NewService(NewRepository(pool), nil)
	if svc.platega == nil {
		t.Fatal("platega must be enabled in the test environment")
	}
	// Короткий таймаут клиента для сценариев «ответ потерялся».
	c, err := platega.NewClient(svc.plategaCfg.Client, &http.Client{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	svc.platega = c
	mux := http.NewServeMux()
	NewHTTPHandlers(svc).Register(mux)
	return &env{t: t, pool: pool, svc: svc, pg: pg, yoo: yoo, mux: mux}
}

func checkoutCmd(provider kafkacontracts.BillingPaymentProvider) *kafkacontracts.CreateSubscriptionCheckoutCommand {
	return &kafkacontracts.CreateSubscriptionCheckoutCommand{
		Type:            kafkacontracts.BillingCommandCreateSubscriptionCheckout,
		CommandID:       newUUID(),
		TelegramID:      itTelegram,
		PlanCode:        kafkacontracts.PlanCodeMonthly,
		ReturnURL:       "https://t.me/vpn_house_bot",
		PaymentProvider: provider,
	}
}

type orderRow struct {
	Status, PaymentID, ProviderStatus, ConfirmationURL, ReviewReason, Amount string
	PaidAt                                                                   *time.Time
	NextCheck                                                                *time.Time
}

func (e *env) order(orderID string) orderRow {
	e.t.Helper()
	var r orderRow
	err := e.pool.QueryRow(context.Background(), `
		SELECT status, payment_id, provider_status, confirmation_url, review_reason, amount_value::text, paid_at, next_check_at
		FROM payments WHERE order_id = $1`, orderID).
		Scan(&r.Status, &r.PaymentID, &r.ProviderStatus, &r.ConfirmationURL, &r.ReviewReason, &r.Amount, &r.PaidAt, &r.NextCheck)
	if err != nil {
		e.t.Fatalf("order %s: %v", orderID, err)
	}
	return r
}

func (e *env) events(eventType string) []map[string]any {
	e.t.Helper()
	rows, err := e.pool.Query(context.Background(), `
		SELECT payload FROM event_outbox WHERE topic = 'billing.events' AND event_type = $1 ORDER BY id`, eventType)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			e.t.Fatal(err)
		}
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		out = append(out, m)
	}
	return out
}

func (e *env) notifications() []kafkacontracts.TgNotification {
	e.t.Helper()
	rows, err := e.pool.Query(context.Background(), `
		SELECT payload FROM event_outbox WHERE topic = 'tg.notifications' ORDER BY id`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []kafkacontracts.TgNotification
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			e.t.Fatal(err)
		}
		var n kafkacontracts.TgNotification
		_ = json.Unmarshal(raw, &n)
		out = append(out, n)
	}
	return out
}

func (e *env) callback(id, status, amount, payload string, secret string) int {
	e.t.Helper()
	body := fmt.Sprintf(`{"id":%q,"amount":%s,"currency":"RUB","status":%q,"paymentMethod":2,"payload":%q}`, id, amount, status, payload)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/platega", strings.NewReader(body))
	req.Header.Set("X-MerchantId", itMerchant)
	req.Header.Set("X-Secret", secret)
	req.Header.Set("User-Agent", "Platega Payment")
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec.Code
}

func (e *env) eventually(what string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("timeout waiting for %s", what)
}

func (e *env) makeDue(orderID string) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), `UPDATE payments SET next_check_at = now() - interval '1 second' WHERE order_id = $1`, orderID); err != nil {
		e.t.Fatal(err)
	}
}

func lastNotification(t *testing.T, e *env) kafkacontracts.TgNotification {
	t.Helper()
	ns := e.notifications()
	if len(ns) == 0 {
		t.Fatal("no notifications")
	}
	return ns[len(ns)-1]
}

// ---------------------------------------------------------------------------
// Сценарии.
// ---------------------------------------------------------------------------

func TestPlategaHappyPathAndIdempotentGrant(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	if o.Status != "pending" || o.PaymentID == "" || o.ConfirmationURL == "" || o.Amount != "89.00" || o.NextCheck == nil {
		t.Fatalf("order after create: %+v", o)
	}
	if e.pg.createCount() != 1 {
		t.Fatalf("creates = %d", e.pg.createCount())
	}
	body := e.pg.creates[0]
	if body["payload"] != cmd.CommandID || body["orderId"] != cmd.CommandID {
		t.Fatalf("payload/orderId: %v", body)
	}
	md := body["metadata"].(map[string]any)
	if md["user_email"] != "5512345678@telegram.org" || md["userId"] != "5512345678" {
		t.Fatalf("metadata: %v", md)
	}
	n := lastNotification(t, e)
	if n.PayURL != o.ConfirmationURL || !strings.Contains(n.Message, "Способ 1 - рекомендуемый") || !strings.Contains(n.Message, "89 ₽") {
		t.Fatalf("link notification: %+v", n)
	}

	// Колбэк с неверным ключом — отказ, состояние не меняется.
	if code := e.callback(o.PaymentID, "CONFIRMED", "89", cmd.CommandID, "wrong"); code != http.StatusUnauthorized {
		t.Fatalf("bad secret code %d", code)
	}
	// Колбэк CONFIRMED, но Platega по GET ещё PENDING: подписка не выдаётся.
	if code := e.callback(o.PaymentID, "CONFIRMED", "89", cmd.CommandID, itSecret); code != http.StatusOK {
		t.Fatalf("callback code %d", code)
	}
	e.eventually("check after callback", func() bool { return e.pg.gets.Load() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if len(e.events("billing.payment_succeeded")) != 0 {
		t.Fatal("granted on callback hint without authoritative CONFIRMED")
	}

	e.pg.set(o.PaymentID, "CONFIRMED")
	if code := e.callback(o.PaymentID, "CONFIRMED", "89", cmd.CommandID, itSecret); code != http.StatusOK {
		t.Fatalf("callback code %d", code)
	}
	e.eventually("grant", func() bool { return e.order(cmd.CommandID).Status == "succeeded" })
	// Дубль колбэка, проход воркера и прямые повторные проверки — без второй выдачи.
	_ = e.callback(o.PaymentID, "CONFIRMED", "89", cmd.CommandID, itSecret)
	e.makeDue(cmd.CommandID)
	if err := e.svc.ReconcilePlatega(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_ = e.svc.CheckPlategaPayment(ctx, cmd.CommandID, o.PaymentID, 1)
	}
	time.Sleep(300 * time.Millisecond)
	evs := e.events("billing.payment_succeeded")
	if len(evs) != 1 {
		t.Fatalf("payment_succeeded events = %d", len(evs))
	}
	ev := evs[0]
	if ev["payment_provider"] != "platega" || ev["payment_id"] != o.PaymentID || ev["amount_value"] != "89.00" ||
		ev["order_id"] != cmd.CommandID || ev["auto_renew_enabled"] != false || ev["plan_code"] != "monthly_30d" {
		t.Fatalf("event: %v", ev)
	}
	if got := e.order(cmd.CommandID); got.PaidAt == nil || got.NextCheck != nil {
		t.Fatalf("final order: %+v", got)
	}
}

func TestPlategaRepeatedAndConcurrentTapsCreateOneInvoice(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- e.svc.HandleCreateSubscriptionCheckout(ctx, checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if e.pg.createCount() != 1 {
		t.Fatalf("concurrent taps created %d invoices", e.pg.createCount())
	}
	// Повторное нажатие позже — та же ссылка, новый счёт не создаётся.
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)); err != nil {
		t.Fatal(err)
	}
	if e.pg.createCount() != 1 {
		t.Fatalf("repeated tap created a new invoice")
	}
	if n := lastNotification(t, e); !strings.Contains(n.Message, "новый не создавался") || n.PayURL == "" {
		t.Fatalf("reuse notification: %+v", n)
	}
	var cnt int
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM payments WHERE provider='platega'`).Scan(&cnt)
	if cnt != 1 {
		t.Fatalf("orders = %d", cnt)
	}
}

func TestPlategaRedeliveryAfterProviderCreatingDoesNotRecreate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	// Имитация падения процесса сразу после отметки provider_creating.
	rec := &PaymentRecord{OrderID: cmd.CommandID, CommandID: cmd.CommandID, TelegramID: cmd.TelegramID,
		CheckoutType: string(kafkacontracts.BillingCheckoutTypeSubscription), PlanCode: "monthly_30d", DurationDays: 30,
		AmountValue: "89.00", Currency: "RUB", Description: "x", IdempotenceKey: "platega:" + cmd.CommandID, Metadata: map[string]string{}}
	if _, err := e.svc.repo.ReservePlategaCheckout(ctx, rec, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.svc.repo.MarkPlategaProviderCreating(ctx, cmd.CommandID); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Повторная доставка той же команды.
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if e.pg.createCount() != 0 {
		t.Fatal("redelivered command re-created the invoice")
	}
	// Другие нажатия пользователя видят «счёт уже создаётся».
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)); err != nil {
		t.Fatal(err)
	}
	if n := lastNotification(t, e); !strings.Contains(n.Message, "уже создаётся") {
		t.Fatalf("inflight notification: %+v", n)
	}
	// Воркер закрывает зависшее создание как «исход неизвестен» и сообщает.
	if _, err := e.pool.Exec(ctx, `UPDATE payments SET updated_at = now() - interval '5 minutes' WHERE order_id = $1`, cmd.CommandID); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.ReconcilePlatega(ctx); err != nil {
		t.Fatal(err)
	}
	if st := e.order(cmd.CommandID).Status; st != "create_unknown" {
		t.Fatalf("status %s", st)
	}
	if n := lastNotification(t, e); n.Keyboard != kafkacontracts.TgKeyboardPaymentMethod || !strings.Contains(n.Message, "Способ 2") {
		t.Fatalf("stale notification: %+v", n)
	}
	if e.pg.createCount() != 0 {
		t.Fatal("sweep must not create invoices")
	}
}

func TestPlategaLostCreateResponseRecoveredByCallback(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.pg.setMode("timeout")
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	if o.Status != "create_unknown" || o.PaymentID != "" {
		t.Fatalf("after timeout: %+v", o)
	}
	if n := lastNotification(t, e); n.Keyboard != kafkacontracts.TgKeyboardPaymentMethod || n.PayURL != "" {
		t.Fatalf("timeout notification: %+v", n)
	}
	// Повтор той же команды не создаёт второй счёт вслепую.
	e.pg.setMode("ok")
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if e.pg.createCount() != 1 {
		t.Fatalf("blind re-create after timeout: %d", e.pg.createCount())
	}
	// Транзакция всё же создана у Platega; колбэк находит заказ по payload.
	var txID string
	e.pg.mu.Lock()
	for id, tx := range e.pg.txs {
		if tx.payload == cmd.CommandID {
			txID = id
		}
	}
	e.pg.mu.Unlock()
	if txID == "" {
		t.Fatal("stub has no transaction")
	}
	e.pg.set(txID, "CONFIRMED")
	if code := e.callback(txID, "CONFIRMED", "89.00", cmd.CommandID, itSecret); code != http.StatusOK {
		t.Fatalf("callback %d", code)
	}
	e.eventually("grant via payload match", func() bool { return e.order(cmd.CommandID).Status == "succeeded" })
	if evs := e.events("billing.payment_succeeded"); len(evs) != 1 || evs[0]["payment_id"] != txID {
		t.Fatalf("events: %v", evs)
	}
}

func TestPlategaDefiniteRejection(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.pg.setMode("400")
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if st := e.order(cmd.CommandID).Status; st != "create_failed" {
		t.Fatalf("status %s", st)
	}
	n := lastNotification(t, e)
	if n.Keyboard != kafkacontracts.TgKeyboardPaymentMethod || !strings.Contains(n.Message, "Деньги не списывались") {
		t.Fatalf("notification %+v", n)
	}
	// После отказа следующее нажатие создаёт новый счёт (это явное действие).
	e.pg.setMode("ok")
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)); err != nil {
		t.Fatal(err)
	}
	if e.pg.createCount() != 2 {
		t.Fatalf("creates %d", e.pg.createCount())
	}
}

func TestPlategaVerificationMismatchGoesToReview(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	e.pg.mu.Lock()
	e.pg.txs[o.PaymentID].status = "CONFIRMED"
	e.pg.txs[o.PaymentID].amount = "88.99"
	e.pg.mu.Unlock()
	if err := e.svc.CheckPlategaPayment(ctx, cmd.CommandID, o.PaymentID, 1); err != nil {
		t.Fatal(err)
	}
	got := e.order(cmd.CommandID)
	if got.Status != "review_required" || got.ReviewReason != "amount_mismatch" || got.PaidAt != nil {
		t.Fatalf("order %+v", got)
	}
	if len(e.events("billing.payment_succeeded")) != 0 {
		t.Fatal("granted despite amount mismatch")
	}
	if n := lastNotification(t, e); !strings.Contains(n.Message, "ручной проверки") {
		t.Fatalf("review notification %+v", n)
	}
}

func TestPlategaCanceledThenLateConfirmation(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	e.pg.set(o.PaymentID, "CANCELED")
	if err := e.svc.CheckPlategaPayment(ctx, cmd.CommandID, o.PaymentID, 1); err != nil {
		t.Fatal(err)
	}
	got := e.order(cmd.CommandID)
	if got.Status != "canceled" || got.NextCheck == nil {
		t.Fatalf("canceled order must stay scheduled for late confirmation: %+v", got)
	}
	if n := lastNotification(t, e); !strings.Contains(n.Message, "отменён или истёк") || n.Keyboard != kafkacontracts.TgKeyboardBuyMenu {
		t.Fatalf("cancel notification %+v", n)
	}
	// Повторное наблюдение CANCELED — без второго уведомления.
	before := len(e.notifications())
	_ = e.svc.CheckPlategaPayment(ctx, cmd.CommandID, o.PaymentID, 1)
	if len(e.notifications()) != before {
		t.Fatal("duplicate cancel notification")
	}
	// CANCELED → CONFIRMED (документировано) — подписка выдаётся.
	e.pg.set(o.PaymentID, "CONFIRMED")
	e.makeDue(cmd.CommandID)
	if err := e.svc.ReconcilePlatega(ctx); err != nil {
		t.Fatal(err)
	}
	if st := e.order(cmd.CommandID).Status; st != "succeeded" {
		t.Fatalf("late confirmation: %s", st)
	}
	if len(e.events("billing.payment_succeeded")) != 1 {
		t.Fatal("late confirmation not granted exactly once")
	}
}

func TestPlategaDisputeCycleGrantsOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	for _, st := range []string{"CONFIRMED", "DISPUTED", "CONFIRMED", "DISPUTED", "CHARGEBACKED", "CONFIRMED"} {
		e.pg.set(o.PaymentID, st)
		if err := e.svc.CheckPlategaPayment(ctx, cmd.CommandID, o.PaymentID, 1); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.events("billing.payment_succeeded")) != 1 {
		t.Fatal("dispute cycle granted more than once")
	}
	if got := e.order(cmd.CommandID); got.Status != "review_required" || got.ReviewReason != "transition_after_chargeback" {
		t.Fatalf("after chargeback: %+v", got)
	}
}

func TestPlategaReconcilerRecoversLostCallbacks(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	e.pg.set(o.PaymentID, "CONFIRMED")
	// API временно недоступно: проверка переносится, ничего не теряется.
	e.pg.mu.Lock()
	e.pg.getError = http.StatusBadGateway
	e.pg.mu.Unlock()
	e.makeDue(cmd.CommandID)
	_ = e.svc.ReconcilePlatega(ctx)
	if got := e.order(cmd.CommandID); got.Status != "pending" || got.NextCheck == nil {
		t.Fatalf("after API error: %+v", got)
	}
	e.pg.mu.Lock()
	e.pg.getError = 0
	e.pg.mu.Unlock()
	e.makeDue(cmd.CommandID)
	if err := e.svc.ReconcilePlatega(ctx); err != nil {
		t.Fatal(err)
	}
	if st := e.order(cmd.CommandID).Status; st != "succeeded" {
		t.Fatalf("reconciler did not find payment: %s", st)
	}
}

func TestPlategaSecondTransactionForOrderIsSeparatePurchase(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	second := newUUID()
	e.pg.addForeign(second, "CONFIRMED", "89.00", cmd.CommandID)
	if code := e.callback(second, "CONFIRMED", "89.00", cmd.CommandID, itSecret); code != http.StatusOK {
		t.Fatalf("callback %d", code)
	}
	dupOrder := cmd.CommandID + ":tx:" + second
	e.eventually("duplicate purchase granted", func() bool {
		var st string
		_ = e.pool.QueryRow(ctx, `SELECT status FROM payments WHERE order_id=$1`, dupOrder).Scan(&st)
		return st == "succeeded"
	})
	evs := e.events("billing.payment_succeeded")
	if len(evs) != 1 || evs[0]["payment_id"] != second || evs[0]["order_id"] != dupOrder {
		t.Fatalf("events %v", evs)
	}
	// Исходный заказ не тронут.
	if st := e.order(cmd.CommandID).Status; st != "pending" {
		t.Fatalf("original order %s", st)
	}
}

func TestPlategaForeignCallbackIgnored(t *testing.T) {
	e := newEnv(t)
	foreign := newUUID()
	if code := e.callback(foreign, "CONFIRMED", "500", "order-8089", itSecret); code != http.StatusOK {
		t.Fatalf("foreign callback %d", code)
	}
	var cnt int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM payments`).Scan(&cnt)
	if cnt != 0 || e.pg.gets.Load() != 0 {
		t.Fatalf("foreign callback changed state: payments=%d gets=%d", cnt, e.pg.gets.Load())
	}
	var stored string
	_ = e.pool.QueryRow(context.Background(), `SELECT raw_payload::text FROM billing_webhook_events WHERE provider='platega'`).Scan(&stored)
	if strings.Contains(stored, "order-8089") {
		t.Fatal("payload of a foreign transaction must not be stored")
	}
}

func TestPlategaDisabledReportsUnavailable(t *testing.T) {
	e := newEnv(t)
	e.svc.platega = nil
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	n := lastNotification(t, e)
	if n.Keyboard != kafkacontracts.TgKeyboardPaymentMethod || !strings.Contains(n.Message, "недоступен") || !strings.Contains(n.Message, "Способ 2") {
		t.Fatalf("notification %+v", n)
	}
	if e.yoo.creates.Load() != 0 {
		t.Fatal("hidden switch to YooKassa")
	}
}

func TestYooKassaPathPreservedWithReuseAndFailureNotice(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	// Команда старого бота: провайдер не указан → YooKassa, как раньше.
	old := checkoutCmd("")
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, old); err != nil {
		t.Fatal(err)
	}
	var provider, status, url string
	if err := e.pool.QueryRow(ctx, `SELECT provider, status, confirmation_url FROM payments WHERE order_id=$1`, old.CommandID).Scan(&provider, &status, &url); err != nil {
		t.Fatal(err)
	}
	if provider != "yookassa" || status != "pending" || url == "" || e.pg.createCount() != 0 {
		t.Fatalf("yookassa order: %s %s %q platega=%d", provider, status, url, e.pg.createCount())
	}
	// Повторное нажатие «Способ 2» — та же ссылка.
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, checkoutCmd(kafkacontracts.BillingPaymentProviderYooKassa)); err != nil {
		t.Fatal(err)
	}
	if e.yoo.creates.Load() != 1 {
		t.Fatalf("yookassa creates %d", e.yoo.creates.Load())
	}
	if n := lastNotification(t, e); n.PayURL != url {
		t.Fatalf("reuse %+v", n)
	}
	// Вебхук YooKassa для этого платежа по-прежнему выдаёт подписку.
	var paymentID string
	_ = e.pool.QueryRow(ctx, `SELECT payment_id FROM payments WHERE order_id=$1`, old.CommandID).Scan(&paymentID)
	n := &yooKassaWebhookNotification{Type: "notification", Event: "payment.succeeded", Object: yooKassaWebhookObject{ID: paymentID, Status: "succeeded", Paid: true}}
	if err := e.svc.ProcessWebhook(ctx, n, json.RawMessage(`{}`), "fp-"+paymentID); err != nil {
		t.Fatal(err)
	}
	evs := e.events("billing.payment_succeeded")
	if len(evs) != 1 || evs[0]["payment_provider"] != "yookassa" {
		t.Fatalf("yookassa events %v", evs)
	}
	// Ошибка YooKassa: пользователь получает сообщение и явный выбор способа.
	e.yoo.fail.Store(500)
	if _, err := e.pool.Exec(ctx, `UPDATE payments SET created_at = now() - interval '1 hour' WHERE provider='yookassa'`); err != nil {
		t.Fatal(err)
	}
	failed := checkoutCmd(kafkacontracts.BillingPaymentProviderYooKassa)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, failed); err != nil {
		t.Fatalf("yookassa failure must be handled, got %v", err)
	}
	_ = e.pool.QueryRow(ctx, `SELECT status FROM payments WHERE order_id=$1`, failed.CommandID).Scan(&status)
	if status != "create_unknown" {
		t.Fatalf("yookassa failure status %s", status)
	}
	if n := lastNotification(t, e); n.Keyboard != kafkacontracts.TgKeyboardPaymentMethod || !strings.Contains(n.Message, "Способ 1 - рекомендуемый") {
		t.Fatalf("failure notification %+v", n)
	}
}

func TestPlategaWebhookCannotTouchPlategaOrdersViaYooKassaPath(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
	if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	o := e.order(cmd.CommandID)
	n := &yooKassaWebhookNotification{Type: "notification", Event: "payment.succeeded", Object: yooKassaWebhookObject{ID: o.PaymentID, Status: "succeeded", Paid: true}}
	if err := e.svc.ProcessWebhook(ctx, n, json.RawMessage(`{}`), "fp-x"); err == nil {
		t.Fatal("yookassa webhook must not resolve a platega order")
	}
	if st := e.order(cmd.CommandID).Status; st != "pending" {
		t.Fatalf("platega order modified by yookassa path: %s", st)
	}
}

func TestUnknownProviderRejected(t *testing.T) {
	e := newEnv(t)
	err := e.svc.HandleCreateSubscriptionCheckout(context.Background(), checkoutCmd("card"))
	if err == nil || !strings.Contains(err.Error(), "unsupported payment provider") {
		t.Fatalf("err = %v", err)
	}
	if e.pg.createCount() != 0 || e.yoo.creates.Load() != 0 {
		t.Fatal("unknown provider created a payment")
	}
}

// Ограничения схемы отсекают недопустимые состояния даже при ошибке в коде.
func TestPlategaSchemaConstraints(t *testing.T) {
	pool, _ := newDatabase(t, "")
	ctx := context.Background()
	ins := func(orderID, provider, status, paymentID, amount string, paid bool) error {
		var paidAt any
		if paid {
			paidAt = time.Now()
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO payments (order_id, telegram_id, checkout_type, status, amount_value, currency, idempotence_key, provider, payment_id, paid_at)
			VALUES ($1, 1, 'subscription_purchase', $2, $3::numeric, 'RUB', $1, $4, $5, $6)`,
			orderID, status, amount, provider, paymentID, paidAt)
		return err
	}
	bad := []struct {
		name                                string
		provider, status, paymentID, amount string
		paid                                bool
	}{
		{"unknown provider", "stripe", "pending", newUUID(), "89", false},
		{"unknown platega status", "platega", "paid", newUUID(), "89", false},
		{"zero amount", "platega", "pending", newUUID(), "0", false},
		{"succeeded without paid_at", "platega", "succeeded", newUUID(), "89", false},
		{"pending without transaction", "platega", "pending", "", "89", false},
	}
	for _, b := range bad {
		if err := ins(newUUID(), b.provider, b.status, b.paymentID, b.amount, b.paid); err == nil {
			t.Errorf("%s: accepted", b.name)
		}
	}
	if err := ins(newUUID(), "platega", "local_creating", "", "89", false); err != nil {
		t.Fatal(err)
	}
	if err := ins(newUUID(), "platega", "provider_creating", "", "89", false); err == nil {
		t.Fatal("second in-flight creation for one user accepted")
	}
	// YooKassa-строки не ограничены перечнем статусов Platega.
	if err := ins(newUUID(), "yookassa", "waiting_for_capture", newUUID(), "89", false); err != nil {
		t.Fatal(err)
	}
	// Миграция идемпотентна.
	applyFile(t, pool, filepath.Join("..", "..", "migrations", "billing-service", "0006_platega_payments.sql"))
}

// Старые платежи YooKassa переживают миграцию и обрабатываются как раньше.
func TestMigrationKeepsOldYooKassaPayments(t *testing.T) {
	pool, _ := newDatabase(t, "0006")
	ctx := context.Background()
	paymentID := newUUID()
	orderID := newUUID()
	if _, err := pool.Exec(ctx, `
		INSERT INTO payments (order_id, command_id, telegram_id, checkout_type, plan_code, duration_days, payment_id, status,
		                      amount_value, currency, description, confirmation_url, idempotence_key, metadata)
		VALUES ($1, $1, $2, 'subscription_purchase', 'monthly_30d', 30, $3, 'pending', 89.00, 'RUB', 'old', 'https://yoomoney.test/old', 'checkout:'||$1,
		        '{"charge_source":"initial","attempt_no":"0"}'::jsonb)`, orderID, itTelegram, paymentID); err != nil {
		t.Fatal(err)
	}
	applyFile(t, pool, filepath.Join("..", "..", "migrations", "billing-service", "0006_platega_payments.sql"))
	var provider string
	if err := pool.QueryRow(ctx, `SELECT provider FROM payments WHERE order_id=$1`, orderID).Scan(&provider); err != nil || provider != "yookassa" {
		t.Fatalf("provider after migration: %q %v", provider, err)
	}
	e := newEnvWithPool(t, pool)
	n := &yooKassaWebhookNotification{Type: "notification", Event: "payment.succeeded", Object: yooKassaWebhookObject{ID: paymentID, Status: "succeeded", Paid: true}}
	if err := e.svc.ProcessWebhook(ctx, n, json.RawMessage(`{}`), "fp-old"); err != nil {
		t.Fatalf("late confirmation of an old payment: %v", err)
	}
	if evs := e.events("billing.payment_succeeded"); len(evs) != 1 || evs[0]["order_id"] != orderID {
		t.Fatalf("events %v", evs)
	}
}

// БД недоступна в момент колбэка: ответ 500, чтобы Platega повторила доставку
// (колбэк не подтверждается, пока не зафиксирован).
func TestPlategaCallbackWithDatabaseDownAsksForRetry(t *testing.T) {
	e := newEnv(t)
	e.pool.Close()
	if code := e.callback(newUUID(), "CONFIRMED", "89", newUUID(), itSecret); code != http.StatusInternalServerError {
		t.Fatalf("callback with DB down: %d, want 500", code)
	}
}

// Ограниченный нагрузочный smoke: 40 покупателей, по 5 параллельных дублей
// колбэка на каждую оплату. Ровно одна выдача на оплату, фоновые проверки
// ограничены семафором, горутины не утекают.
func TestPlategaLoadSmoke(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const users = 40
	before := runtime.NumGoroutine()
	start := time.Now()
	type order struct{ id, tx string }
	orders := make([]order, users)
	var wg sync.WaitGroup
	for i := 0; i < users; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := checkoutCmd(kafkacontracts.BillingPaymentProviderPlatega)
			cmd.TelegramID = itTelegram + int64(i) + 1
			if err := e.svc.HandleCreateSubscriptionCheckout(ctx, cmd); err != nil {
				t.Error(err)
				return
			}
			orders[i] = order{id: cmd.CommandID, tx: e.order(cmd.CommandID).PaymentID}
		}(i)
	}
	wg.Wait()
	for _, o := range orders {
		e.pg.set(o.tx, "CONFIRMED")
	}
	var codes sync.Map
	for _, o := range orders {
		for k := 0; k < 5; k++ {
			wg.Add(1)
			go func(o order) {
				defer wg.Done()
				code := e.callback(o.tx, "CONFIRMED", "89", o.id, itSecret)
				v, _ := codes.LoadOrStore(code, new(atomic.Int32))
				v.(*atomic.Int32).Add(1)
			}(o)
		}
	}
	wg.Wait()
	codes.Range(func(k, v any) bool {
		if k.(int) != http.StatusOK {
			t.Errorf("callback status %v x%d", k, v.(*atomic.Int32).Load())
		}
		return true
	})
	// Часть проверок могла уйти воркеру (семафор занят) — догоняем им.
	e.eventually("all orders granted", func() bool {
		var n int
		_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM payments WHERE provider='platega' AND status='succeeded'`).Scan(&n)
		if n == users {
			return true
		}
		_, _ = e.pool.Exec(ctx, `UPDATE payments SET next_check_at = now() WHERE provider='platega' AND status <> 'succeeded'`)
		_ = e.svc.ReconcilePlatega(ctx)
		return false
	})
	if n := len(e.events("billing.payment_succeeded")); n != users {
		t.Fatalf("grants = %d, want %d", n, users)
	}
	elapsed := time.Since(start)
	time.Sleep(500 * time.Millisecond)
	after := runtime.NumGoroutine()
	t.Logf("smoke: %d заказов, %d колбэков, %v; горутин до/после: %d/%d; GET: %d", users, users*5, elapsed.Round(time.Millisecond), before, after, e.pg.gets.Load())
	if after > before+10 {
		t.Fatalf("goroutine leak: %d -> %d", before, after)
	}
}
