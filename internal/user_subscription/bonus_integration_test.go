//go:build integration

package user_subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// Бонусная кампания (subscription.grant_bonus): ровно одно начисление и одно
// сообщение на пару (пользователь, кампания), срок «N дней с текущей даты»
// для ушедших, без уменьшения имеющегося срока, без вмешательства в grace.
//
//   go test -tags=integration ./internal/user_subscription/ -run Bonus
// ============================================================================

const bonusCampaign = "apology-test"

func seedSubscription(t *testing.T, pool *pgxpool.Pool, svc *Service, telegramID int64, status, plan, lastPayment string, expiresIn, graceIn time.Duration) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := svc.repo.ensureUserAndStateTx(ctx, tx, telegramID, "LT"); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().UTC().Add(expiresIn)
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2, current_plan_code = $3, last_payment_id = NULLIF($4, ''),
		    trial_used = true, started_at = now() - interval '40 days', expires_at = $5,
		    grace_until = CASE WHEN $2 = 'grace' THEN $6::timestamptz END
		WHERE telegram_id = $1`,
		telegramID, status, plan, lastPayment, expires, time.Now().UTC().Add(graceIn)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.repo.ensureTokenTx(ctx, tx, telegramID, &expires); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

type bonusSnapshot struct {
	status, plan, lastPayment string
	expires, tokenExpires     time.Time
	accessRev                 int64
	ledger, events, notifs    int
}

func takeBonusSnapshot(t *testing.T, pool *pgxpool.Pool, telegramID int64) bonusSnapshot {
	t.Helper()
	ctx := context.Background()
	var s bonusSnapshot
	if err := pool.QueryRow(ctx, `
		SELECT us.status, COALESCE(us.current_plan_code, ''), COALESCE(us.last_payment_id, ''),
		       us.expires_at, st.expires_at, us.access_rev
		FROM user_subscriptions us JOIN subscription_tokens st USING (telegram_id)
		WHERE us.telegram_id = $1`, telegramID).
		Scan(&s.status, &s.plan, &s.lastPayment, &s.expires, &s.tokenExpires, &s.accessRev); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprint(telegramID)
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM subscription_grants WHERE telegram_id = $1 AND source = 'admin' AND business_key = $3),
		  (SELECT count(*) FROM event_outbox WHERE topic = 'subscription.events' AND message_key = $2 AND payload->>'source' = 'bonus'),
		  (SELECT count(*) FROM event_outbox WHERE topic = 'tg.notifications' AND message_key = $2)`,
		telegramID, key, bonusCampaign).Scan(&s.ledger, &s.events, &s.notifs); err != nil {
		t.Fatal(err)
	}
	return s
}

func bonusCommand(telegramID int64, commandID string) *kafkacontracts.GrantBonusCommand {
	return &kafkacontracts.GrantBonusCommand{
		Type:       kafkacontracts.SubscriptionCommandGrantBonus,
		CommandID:  commandID,
		TelegramID: telegramID,
		Days:       30,
		Campaign:   bonusCampaign,
		Message:    "🙏 Приносим извинения. 🎁 Дарим {days} — доступ до {until}.",
	}
}

func near(a, b time.Time) bool {
	d := a.Sub(b)
	return d > -2*time.Minute && d < 2*time.Minute
}

func TestBonusGrantChurnedPaidUserOnceWithMessage(t *testing.T) {
	pool := setupSubscriptionDB(t)
	ctx := context.Background()
	svc := NewService(NewRepository(pool), "http://localhost/sub/", "LT")
	const tg = 201
	seedSubscription(t, pool, svc, tg, "expired", "monthly_30d", "pay-201", -5*24*time.Hour, 0)
	before := takeBonusSnapshot(t, pool, tg)

	if err := svc.HandleGrantBonus(ctx, bonusCommand(tg, "bonus:"+bonusCampaign+":201")); err != nil {
		t.Fatal(err)
	}
	after := takeBonusSnapshot(t, pool, tg)
	want := time.Now().UTC().Add(30 * 24 * time.Hour)
	if after.status != "active" || !near(after.expires, want) || !after.tokenExpires.Equal(after.expires) {
		t.Fatalf("after grant: %+v (want active until ~%s)", after, want)
	}
	if after.plan != "monthly_30d" || after.lastPayment != "pay-201" {
		t.Fatalf("payment contour changed: %+v", after)
	}
	if after.accessRev <= before.accessRev || after.ledger != 1 || after.events != 1 || after.notifs != 1 {
		t.Fatalf("grant effects: %+v", after)
	}

	// Событие для оркестратора и сообщение — с фактическими значениями.
	var evRaw, msgRaw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM event_outbox WHERE topic='subscription.events' AND message_key='201'`).Scan(&evRaw); err != nil {
		t.Fatal(err)
	}
	var ev kafkacontracts.SubscriptionActivatedEvent
	if err := json.Unmarshal(evRaw, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != kafkacontracts.SubscriptionEventActivated || ev.Source != "bonus" || ev.AccessRev != after.accessRev ||
		!ev.ActiveUntil.Equal(after.expires) || ev.PaymentID != "" || ev.AutoRenewEnabled {
		t.Fatalf("activation event: %+v", ev)
	}
	if err := pool.QueryRow(ctx, `SELECT payload FROM event_outbox WHERE topic='tg.notifications' AND message_key='201'`).Scan(&msgRaw); err != nil {
		t.Fatal(err)
	}
	var n kafkacontracts.TgNotification
	if err := json.Unmarshal(msgRaw, &n); err != nil {
		t.Fatal(err)
	}
	wantText := "🙏 Приносим извинения. 🎁 Дарим 30 дней — доступ до " + after.expires.UTC().Format("02.01.2006") + "."
	if n.Message != wantText || n.Keyboard != kafkacontracts.TgKeyboardMySubscriptionConfig || n.ParseMode != "" {
		t.Fatalf("notification: %+v", n)
	}

	// Повтор той же команды и другой command_id той же кампании — без эффектов.
	for _, id := range []string{"bonus:" + bonusCampaign + ":201", "other-command-id"} {
		if err := svc.HandleGrantBonus(ctx, bonusCommand(tg, id)); err != nil {
			t.Fatal(err)
		}
	}
	again := takeBonusSnapshot(t, pool, tg)
	if !again.expires.Equal(after.expires) || again.accessRev != after.accessRev || again.ledger != 1 || again.events != 1 || again.notifs != 1 {
		t.Fatalf("replay changed state: before %+v after %+v", after, again)
	}
}

func TestBonusGrantConcurrentDeliveriesGrantOnce(t *testing.T) {
	pool := setupSubscriptionDB(t)
	ctx := context.Background()
	svc := NewService(NewRepository(pool), "http://localhost/sub/", "LT")
	const tg = 202
	seedSubscription(t, pool, svc, tg, "trial", "trial_3d", "", -3*24*time.Hour, 0)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- svc.HandleGrantBonus(ctx, bonusCommand(tg, fmt.Sprintf("dup-%d", i)))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	s := takeBonusSnapshot(t, pool, tg)
	if s.ledger != 1 || s.events != 1 || s.notifs != 1 || s.status != "active" ||
		!near(s.expires, time.Now().UTC().Add(30*24*time.Hour)) || s.plan != "trial_3d" {
		t.Fatalf("concurrent grant: %+v", s)
	}
}

func TestBonusGrantNeverReducesAndSkipsGrace(t *testing.T) {
	pool := setupSubscriptionDB(t)
	ctx := context.Background()
	svc := NewService(NewRepository(pool), "http://localhost/sub/", "LT")

	// Вернулся и оплатил между выборкой и начислением: дни добавляются сверху.
	seedSubscription(t, pool, svc, 203, "active", "monthly_30d", "pay-203", 10*24*time.Hour, 0)
	before := takeBonusSnapshot(t, pool, 203)
	if err := svc.HandleGrantBonus(ctx, bonusCommand(203, "c-203")); err != nil {
		t.Fatal(err)
	}
	after := takeBonusSnapshot(t, pool, 203)
	if after.status != "active" || !near(after.expires, before.expires.Add(30*24*time.Hour)) {
		t.Fatalf("active user: before %+v after %+v", before, after)
	}

	// Льготный период — платёжный контур: бонус не начисляется, сообщения нет.
	seedSubscription(t, pool, svc, 204, "grace", "monthly_30d", "pay-204", -24*time.Hour, 2*24*time.Hour)
	before = takeBonusSnapshot(t, pool, 204)
	if err := svc.HandleGrantBonus(ctx, bonusCommand(204, "c-204")); err != nil {
		t.Fatal(err)
	}
	after = takeBonusSnapshot(t, pool, 204)
	if after.status != "grace" || !after.expires.Equal(before.expires) || after.ledger != 0 || after.events != 0 || after.notifs != 0 {
		t.Fatalf("grace user changed: before %+v after %+v", before, after)
	}

	// Неверная команда отклоняется до любых изменений.
	bad := bonusCommand(205, "c-205")
	bad.Days = 0
	if err := svc.HandleGrantBonus(ctx, bad); err == nil || !strings.Contains(err.Error(), "invalid grant bonus command") {
		t.Fatalf("invalid command: %v", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_subscriptions WHERE telegram_id = 205`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("invalid command touched DB: rows=%d err=%v", rows, err)
	}
}

// Пока биллинг ещё может изменить подписку (повторы автосписания, его
// льготный период, списание по расписанию), бонус не начисляется
// (deferred_billing): ни журнала, ни продления, ни сообщения — иначе
// неуспешное списание позже обрезало бы бонус. Завершённый профиль
// (expired/disabled, автопродление без карты) не мешает; когда профиль
// завершился, следующая команда той же кампании начисляет.
func TestBonusGrantDefersWhileBillingRenewalLive(t *testing.T) {
	pool := setupSubscriptionDB(t, "billing-service")
	ctx := context.Background()
	svc := NewService(NewRepository(pool), "http://localhost/sub/", "LT")
	profile := func(tg int64, status string, autoRenew bool, pm string, nextCharge any) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO billing_recurring_profiles (telegram_id, plan_code, duration_days, amount_value, payment_method_id,
			                                        auto_renew_enabled, status, next_charge_at, grace_until, last_payment_id)
			VALUES ($1, 'monthly_30d', 30, 199.00, $2, $3, $4::text, $5,
			        CASE WHEN $4::text IN ('grace', 'expiring') THEN now() + interval '2 days' END, $6)`,
			tg, pm, autoRenew, status, nextCharge, fmt.Sprintf("pay-%d", tg)); err != nil {
			t.Fatalf("profile %d: %v", tg, err)
		}
	}
	future := time.Now().Add(time.Hour)
	cases := []struct {
		tg        int64
		subStatus string
		expires   time.Duration
		status    string
		autoRenew bool
		pm        string
		next      any
		granted   bool
	}{
		{221, "expired", -2 * 24 * time.Hour, "retry", true, "pm-221", future, false},
		{222, "active", -24 * time.Hour, "retry", true, "pm-222", future, false}, // повтор списания после окончания
		{223, "expired", -3 * 24 * time.Hour, "grace", true, "pm-223", nil, false},
		{224, "expired", -3 * 24 * time.Hour, "expiring", true, "pm-224", nil, false},
		{225, "expired", -3 * 24 * time.Hour, "processing", true, "pm-225", future, false},
		{226, "expired", -4 * 24 * time.Hour, "active", true, "pm-226", future, false}, // списание по расписанию
		{227, "expired", -4 * 24 * time.Hour, "disabled", false, "", nil, true},
		{228, "expired", -4 * 24 * time.Hour, "expired", false, "pm-228", nil, true},
		{229, "expired", -4 * 24 * time.Hour, "active", false, "", nil, true},   // автопродление выключено
		{230, "expired", -4 * 24 * time.Hour, "active", true, "", future, true}, // нет карты — списывать нечем
	}
	for _, c := range cases {
		seedSubscription(t, pool, svc, c.tg, c.subStatus, "monthly_30d", fmt.Sprintf("pay-%d", c.tg), c.expires, 0)
		profile(c.tg, c.status, c.autoRenew, c.pm, c.next)
		before := takeBonusSnapshot(t, pool, c.tg)
		if err := svc.HandleGrantBonus(ctx, bonusCommand(c.tg, fmt.Sprintf("c-%d", c.tg))); err != nil {
			t.Fatalf("%d: %v", c.tg, err)
		}
		after := takeBonusSnapshot(t, pool, c.tg)
		if c.granted {
			if after.status != "active" || !near(after.expires, time.Now().UTC().Add(30*24*time.Hour)) ||
				after.ledger != 1 || after.events != 1 || after.notifs != 1 {
				t.Fatalf("%d (%s): want granted, got %+v", c.tg, c.status, after)
			}
			continue
		}
		if after.ledger != 0 || after.events != 0 || after.notifs != 0 || after.status == "active" ||
			(after.expires.After(time.Now()) && !after.expires.Equal(before.expires)) {
			t.Fatalf("%d (%s): want deferred, before %+v after %+v", c.tg, c.status, before, after)
		}
	}

	// Биллинг завершил продление неуспешно (профиль expired) — следующая
	// команда той же кампании начисляет.
	if _, err := pool.Exec(ctx, `UPDATE billing_recurring_profiles SET status = 'expired', next_charge_at = NULL WHERE telegram_id = 221`); err != nil {
		t.Fatal(err)
	}
	if err := svc.HandleGrantBonus(ctx, bonusCommand(221, "c-221-next-run")); err != nil {
		t.Fatal(err)
	}
	if s := takeBonusSnapshot(t, pool, 221); s.status != "active" || s.ledger != 1 || s.events != 1 || s.notifs != 1 ||
		!near(s.expires, time.Now().UTC().Add(30*24*time.Hour)) {
		t.Fatalf("221 after billing finished: %+v", s)
	}
}
