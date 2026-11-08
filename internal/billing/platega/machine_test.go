package platega

import (
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)

func snap(status LocalStatus) Snapshot {
	return Snapshot{
		OrderID:     testOrderID,
		PaymentID:   testTxID,
		Status:      status,
		AmountMinor: 8900,
		Currency:    "RUB",
		LinkSent:    true,
		CreatedAt:   t0,
	}
}

func txOf(st Status) Transaction {
	return Transaction{ID: testTxID, Status: st, AmountMinor: 8900, Currency: "RUB", Payload: testOrderID}
}

func TestDecideTransitions(t *testing.T) {
	now := t0.Add(5 * time.Minute)
	cases := []struct {
		name     string
		s        Snapshot
		tx       Transaction
		status   LocalStatus
		grant    bool
		canceled bool
		review   bool
		next     bool
	}{
		{"pending stays pending", snap(LocalPending), txOf(StatusPending), LocalPending, false, false, false, true},
		{"confirmed grants once", snap(LocalPending), txOf(StatusConfirmed), LocalSucceeded, true, false, false, false},
		{"confirmed after unknown create grants", snap(LocalCreateUnknown), txOf(StatusConfirmed), LocalSucceeded, true, false, false, false},
		{"confirmed after create_failed grants", snap(LocalCreateFailed), txOf(StatusConfirmed), LocalSucceeded, true, false, false, false},
		{"late confirmation after cancel grants", snap(LocalCanceled), txOf(StatusConfirmed), LocalSucceeded, true, false, false, false},
		{"canceled from pending notifies", snap(LocalPending), txOf(StatusCanceled), LocalCanceled, false, true, false, true},
		{"chargeback without grant", snap(LocalPending), txOf(StatusChargebacked), LocalChargebacked, false, false, false, false},
		{"dispute holds without grant", snap(LocalPending), txOf(StatusDisputed), LocalDisputed, false, false, false, true},
		{"partial refund without grant needs review", snap(LocalPending), txOf(StatusPartiallyRefunded), LocalReviewRequired, false, false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := Decide(tc.s, tc.tx, now)
			if d.Status != tc.status || d.Grant != tc.grant || d.NotifyCanceled != tc.canceled || d.NotifyReview != tc.review || (d.NextCheckIn > 0) != tc.next {
				t.Fatalf("Decide = %+v", d)
			}
			if !d.Changed {
				t.Fatal("transition must be marked as changed")
			}
		})
	}
}

func TestDecideAfterGrant(t *testing.T) {
	now := t0.Add(time.Hour)
	granted := snap(LocalSucceeded)
	granted.Granted = true
	granted.ProviderStatus = StatusConfirmed

	// Повторный CONFIRMED — без повторной выдачи и без изменений.
	if d := Decide(granted, txOf(StatusConfirmed), now); d.Grant || d.Changed || d.Status != LocalSucceeded {
		t.Fatalf("duplicate CONFIRMED: %+v", d)
	}
	// Спор и его снятие: DISPUTED → CONFIRMED не выдаёт подписку второй раз.
	d := Decide(granted, txOf(StatusDisputed), now)
	if d.Status != LocalDisputed || d.Grant {
		t.Fatalf("dispute: %+v", d)
	}
	disputed := granted
	disputed.Status = LocalDisputed
	disputed.ProviderStatus = StatusDisputed
	if d := Decide(disputed, txOf(StatusConfirmed), now); d.Status != LocalSucceeded || d.Grant || !d.Changed {
		t.Fatalf("dispute lifted: %+v", d)
	}
	if d := Decide(disputed, txOf(StatusChargebacked), now); d.Status != LocalChargebacked || d.Grant {
		t.Fatalf("chargeback: %+v", d)
	}
	if d := Decide(granted, txOf(StatusPartiallyRefunded), now); d.Status != LocalPartiallyRefunded || d.Grant || d.NotifyReview {
		t.Fatalf("partial refund: %+v", d)
	}
	if d := Decide(granted, txOf(StatusCanceled), now); d.Status != LocalReviewRequired || d.Grant || d.NotifyCanceled {
		t.Fatalf("canceled after grant: %+v", d)
	}
	if d := Decide(granted, txOf(StatusPending), now); d.Status != LocalSucceeded || d.Changed || d.Grant {
		t.Fatalf("pending after grant: %+v", d)
	}
}

func TestDecideVerification(t *testing.T) {
	now := t0.Add(time.Minute)
	mut := map[string]func(*Transaction){
		"amount_mismatch":   func(tx *Transaction) { tx.AmountMinor = 8800 },
		"currency_mismatch": func(tx *Transaction) { tx.Currency = "USD" },
		"payload_mismatch":  func(tx *Transaction) { tx.Payload = "someone-else" },
		"id_mismatch":       func(tx *Transaction) { tx.ID = "00000000-0000-0000-0000-000000000000" },
	}
	for reason, m := range mut {
		tx := txOf(StatusConfirmed)
		m(&tx)
		d := Decide(snap(LocalPending), tx, now)
		if d.Status != LocalReviewRequired || d.Grant || d.Reason != reason || !d.NotifyReview {
			t.Errorf("%s: %+v", reason, d)
		}
	}
	// Пустой payload и регистр валюты не мешают.
	tx := txOf(StatusConfirmed)
	tx.Payload = ""
	tx.Currency = "rub"
	if d := Decide(snap(LocalPending), tx, now); !d.Grant {
		t.Fatalf("empty payload must not block: %+v", d)
	}
	// Отдельная строка для второй транзакции того же заказа сверяется с
	// payload исходного заказа.
	dup := snap(LocalPending)
	dup.OrderID = testOrderID + ":tx:" + testTxID
	dup.ExpectedPayload = testOrderID
	if d := Decide(dup, txOf(StatusConfirmed), now); !d.Grant {
		t.Fatalf("duplicate-row payload: %+v", d)
	}
}

func TestDecideReviewIsSticky(t *testing.T) {
	s := snap(LocalReviewRequired)
	s.ProviderStatus = StatusPending
	for _, st := range []Status{StatusConfirmed, StatusCanceled, StatusPending, StatusDisputed} {
		d := Decide(s, txOf(st), t0)
		if d.Status != LocalReviewRequired || d.Grant || d.NotifyCanceled || d.NotifyReview || d.NextCheckIn != 0 {
			t.Fatalf("review must be sticky, %s: %+v", st, d)
		}
	}
}

func TestDecideCancelNotification(t *testing.T) {
	// Ссылка не отправлялась (create_unknown → pending по колбэку) — об
	// отмене не сообщаем: пользователь этот счёт не видел.
	s := snap(LocalPending)
	s.LinkSent = false
	if d := Decide(s, txOf(StatusCanceled), t0); d.NotifyCanceled {
		t.Fatal("cancel notification without a sent link")
	}
	// Повторный CANCELED — без повторного уведомления.
	c := snap(LocalCanceled)
	c.ProviderStatus = StatusCanceled
	if d := Decide(c, txOf(StatusCanceled), t0); d.NotifyCanceled || d.Changed {
		t.Fatalf("repeated cancel: %+v", d)
	}
}

func TestDecideAfterChargeback(t *testing.T) {
	s := snap(LocalChargebacked)
	s.ProviderStatus = StatusChargebacked
	if d := Decide(s, txOf(StatusChargebacked), t0); d.Changed {
		t.Fatalf("repeated chargeback changed: %+v", d)
	}
	if d := Decide(s, txOf(StatusConfirmed), t0); d.Status != LocalReviewRequired || d.Grant {
		t.Fatalf("exit from chargeback: %+v", d)
	}
}

func TestSchedules(t *testing.T) {
	prev := time.Duration(0)
	for _, age := range []time.Duration{0, 29 * time.Minute, time.Hour, 3 * time.Hour, 2 * 24 * time.Hour, 8 * 24 * time.Hour} {
		d := pendingDelay(age)
		if d < prev || d <= 0 {
			t.Fatalf("pendingDelay(%v) = %v not monotonic", age, d)
		}
		prev = d
	}
	if pendingDelay(31*24*time.Hour) != 0 || canceledDelay(8*24*time.Hour) != 0 || disputedDelay(121*24*time.Hour) != 0 {
		t.Fatal("schedules must end")
	}
	if RetryDelay(0) != 30*time.Second || RetryDelay(1) != 30*time.Second || RetryDelay(2) != time.Minute || RetryDelay(100) != time.Hour {
		t.Fatalf("RetryDelay: %v %v %v", RetryDelay(1), RetryDelay(2), RetryDelay(100))
	}
}

// Свойство: при любом порядке, дублях и опозданиях наблюдений подписка
// выдаётся не более одного раза и только после наблюдения CONFIRMED с
// совпадающими данными.
func TestDecideRandomSequences(t *testing.T) {
	rng := rand.New(rand.NewSource(20261105))
	statuses := []Status{StatusPending, StatusConfirmed, StatusCanceled, StatusDisputed, StatusChargebacked, StatusPartiallyRefunded}
	starts := []LocalStatus{LocalPending, LocalCreateUnknown, LocalCreateFailed, LocalProviderCreating}
	for iter := 0; iter < 20000; iter++ {
		s := snap(starts[rng.Intn(len(starts))])
		s.LinkSent = rng.Intn(2) == 0
		grants := 0
		sawConfirmed := false
		now := t0
		for step := 0; step < 12; step++ {
			now = now.Add(time.Duration(rng.Intn(120)) * time.Minute)
			tx := txOf(statuses[rng.Intn(len(statuses))])
			if rng.Intn(15) == 0 {
				tx.AmountMinor++ // изредка — несовпадение суммы
			}
			d := Decide(s, tx, now)
			if d.Grant {
				grants++
				if tx.Status != StatusConfirmed || tx.AmountMinor != s.AmountMinor {
					t.Fatalf("grant on %s amount=%d", tx.Status, tx.AmountMinor)
				}
				sawConfirmed = true
			}
			if d.Grant && s.Granted {
				t.Fatal("grant for already granted order")
			}
			// Применяем решение, как это делает сервис.
			s.Status = d.Status
			s.ProviderStatus = d.ProviderStatus
			if d.Grant {
				s.Granted = true
			}
		}
		if grants > 1 {
			t.Fatalf("granted %d times", grants)
		}
		if grants == 1 && !sawConfirmed {
			t.Fatal("grant without confirmation")
		}
	}
}

// Перечень статусов в коде совпадает с CHECK-ограничением миграции.
func TestStatusesMatchMigration(t *testing.T) {
	raw, err := os.ReadFile("../../../migrations/billing-service/0006_platega_payments.sql")
	if err != nil {
		t.Skipf("migration not found: %v", err)
	}
	m := regexp.MustCompile(`(?s)chk_payments_platega_status.*?status IN \(([^)]*)\)`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("status CHECK not found in migration")
	}
	var fromSQL []string
	for _, part := range strings.Split(string(m[1]), ",") {
		fromSQL = append(fromSQL, strings.Trim(strings.TrimSpace(part), "'"))
	}
	var fromCode []string
	for _, s := range AllLocalStatuses {
		fromCode = append(fromCode, string(s))
	}
	sort.Strings(fromSQL)
	sort.Strings(fromCode)
	if strings.Join(fromSQL, ",") != strings.Join(fromCode, ",") {
		t.Fatalf("migration %v != code %v", fromSQL, fromCode)
	}
}
