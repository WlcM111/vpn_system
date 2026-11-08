//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// Бонусная кампания через scripts/bonus_campaign.sh на живом стеке:
// выборка ушедших за 21 день (платные и пробные), подтверждение, пробная
// команда, остальные; начисление 30 дней в user-subscription, активация у
// оркестратора, сообщение в tg.notifications — только получателям.
// Повторный запуск с тем же ключом ничего не меняет. Пользователь с
// действующим автопродлением в биллинге откладывается (и скриптом, и
// обработчиком команды) и получает бонус повторным запуском, когда биллинг
// закончил, — несмотря на то что его команда прошлого запуска уже обработана.

type bonusSeed struct {
	name     string
	status   string
	expires  time.Duration // относительно now
	grace    time.Duration // для grace
	trial    bool
	lastPay  string
	ledger   string // 'paid' или ключ кампании для 'admin'
	billing  string // статус профиля автопродления (billing_recurring_profiles), если нужен
	expected bool
}

func TestE2EBonusCampaignGrantsOnlyChurnedUsers(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	waitForServices(t)
	// Нода и пул-айтем нужны оркестратору, чтобы выдать доступ (как в
	// TestE2EPaymentGrantsAccess); вызовы идемпотентны.
	registerNode(t)
	registerPoolItem(t)

	campaign := fmt.Sprintf("e2e-apology-%d", time.Now().UnixNano()%1_000_000_000)
	base := time.Now().UnixNano()%1_000_000_000 + 700_000_000_000
	day := 24 * time.Hour
	seeds := []bonusSeed{
		{name: "paid-churned-5d", status: "expired", expires: -5 * day, trial: true, lastPay: "e2e-pay", expected: true},
		{name: "trial-churned-10d", status: "trial", expires: -10 * day, trial: true, expected: true},
		{name: "paid-ledger-churned-2d", status: "expired", expires: -2 * day, trial: true, ledger: "paid", expected: true},
		{name: "paid-churned-30d", status: "expired", expires: -30 * day, trial: true, lastPay: "e2e-pay-old"},
		{name: "active-paid", status: "active", expires: 10 * day, trial: true, lastPay: "e2e-pay-active"},
		{name: "never-started", status: "none"},
		{name: "grace-in-progress", status: "grace", expires: -day, grace: 2 * day, trial: true, lastPay: "e2e-pay-grace"},
		{name: "already-gifted", status: "expired", expires: -3 * day, trial: true, ledger: campaign},
		{name: "no-trial-no-pay", status: "expired", expires: -4 * day},
		{name: "paid-autorenew-retry", status: "expired", expires: -2 * day, trial: true, lastPay: "e2e-pay-retry", billing: "retry"},
		{name: "paid-autorenew-finished", status: "expired", expires: -6 * day, trial: true, lastPay: "e2e-pay-fin", billing: "expired", expected: true},
	}
	ids := make(map[string]int64, len(seeds))
	for i, s := range seeds {
		tg := base + int64(i)
		ids[s.name] = tg
		seedBonusUser(t, pool, tg, s)
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	run := func(stdin string, args ...string) (string, error) {
		cmd := exec.Command("bash", append([]string{filepath.Join(root, "scripts", "bonus_campaign.sh"), "--campaign", campaign}, args...)...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "HOME="+home)
		cmd.Stdin = strings.NewReader(stdin)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		return out.String(), err
	}

	// 1. Предпросмотр: ничего не меняет, список — ровно ожидаемые из наших.
	out, err := run("", "--dry-run")
	if err != nil {
		t.Fatalf("dry-run: %v\n%s", err, out)
	}
	targets := readTargets(t, out)
	for _, s := range seeds {
		if _, ok := targets[ids[s.name]]; ok != s.expected {
			t.Fatalf("%s: in targets = %v, want %v\n%s", s.name, ok, s.expected, out)
		}
	}
	if targets[ids["paid-churned-5d"]] != "paid" || targets[ids["trial-churned-10d"]] != "trial" || targets[ids["paid-ledger-churned-2d"]] != "paid" {
		t.Fatalf("categories: %v", targets)
	}
	assertNoBonus(t, pool, campaign, ids["paid-churned-5d"])
	if !strings.Contains(out, "Отложено (в биллинге ещё действует автопродление):") {
		t.Fatalf("dry-run does not report deferred billing users:\n%s", out)
	}

	// 2. Неверное подтверждение — ничего не делается.
	if out, err := run("0\n"); err == nil || !strings.Contains(out, "ничего не сделано") {
		t.Fatalf("wrong confirmation accepted: %v\n%s", err, out)
	}
	assertNoBonus(t, pool, campaign, ids["paid-churned-5d"])

	// 3. Выполнение с подтверждением точным числом получателей.
	n := len(readTargets(t, mustRun(t, run, "", "--dry-run")))
	started := time.Now().UTC()
	out, err = run(strconv.Itoa(n) + "\n")
	if err != nil {
		t.Fatalf("campaign run: %v\n%s", err, out)
	}
	t.Logf("вывод скрипта:\n%s", out)
	if !strings.Contains(out, "Пробная команда обработана") {
		t.Fatalf("no canary confirmation:\n%s", out)
	}

	want := time.Now().UTC().Add(30 * day)
	for _, s := range seeds {
		tg := ids[s.name]
		var status string
		var expires *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, expires_at FROM user_subscriptions WHERE telegram_id=$1`, tg).Scan(&status, &expires); err != nil {
			t.Fatal(err)
		}
		var ledger, notifs int
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM subscription_grants WHERE telegram_id=$1 AND source='admin' AND business_key=$2),
			       (SELECT count(*) FROM event_outbox WHERE topic='tg.notifications' AND message_key=$3 AND created_at >= $4)`,
			tg, campaign, strconv.FormatInt(tg, 10), started).Scan(&ledger, &notifs); err != nil {
			t.Fatal(err)
		}
		switch {
		case s.expected:
			if status != "active" || expires == nil || expires.Sub(want).Abs() > 3*time.Minute || ledger != 1 || notifs != 1 {
				t.Fatalf("%s: status=%s expires=%v ledger=%d notifs=%d", s.name, status, expires, ledger, notifs)
			}
		case s.name == "already-gifted":
			if ledger != 1 || notifs != 0 || status != "expired" {
				t.Fatalf("%s gifted again: status=%s ledger=%d notifs=%d", s.name, status, ledger, notifs)
			}
		default:
			if ledger != 0 || notifs != 0 {
				t.Fatalf("%s must not be touched: status=%s ledger=%d notifs=%d", s.name, status, ledger, notifs)
			}
		}
	}

	// 4. Оркестратор получил активацию: проекция доступа продлена.
	deadline := time.Now().Add(waitTimeout)
	for _, name := range []string{"paid-churned-5d", "trial-churned-10d", "paid-ledger-churned-2d", "paid-autorenew-finished"} {
		for {
			var status string
			var until *time.Time
			err := pool.QueryRow(ctx, `SELECT status, access_until FROM vpn_user_access WHERE telegram_id=$1`, ids[name]).Scan(&status, &until)
			if err == nil && status == "active" && until != nil && until.Sub(want).Abs() < 3*time.Minute {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: orchestrator projection not updated (status=%s until=%v err=%v)", name, status, until, err)
			}
			time.Sleep(pollEvery)
		}
	}

	// 5. Сообщения ушли в Kafka (tg.notifications) — ровно получателям, с датой.
	got := readNotifications(t, ids, 4)
	for _, name := range []string{"paid-churned-5d", "trial-churned-10d", "paid-ledger-churned-2d", "paid-autorenew-finished"} {
		msg, ok := got[ids[name]]
		if !ok {
			t.Fatalf("%s: no notification in tg.notifications", name)
		}
		if !strings.Contains(msg.Message, "Приносим искренние извинения") || !strings.Contains(msg.Message, want.Format("02.01.2006")) ||
			strings.Contains(msg.Message, "{until}") || strings.Contains(msg.Message, "{days}") || msg.Keyboard != "my_subscription_with_config" {
			t.Fatalf("%s: notification %+v", name, msg)
		}
	}
	for _, s := range seeds {
		if _, ok := got[ids[s.name]]; ok && !s.expected {
			t.Fatalf("%s received a notification", s.name)
		}
	}

	// 6. Отложенный: команда прошлого запуска обработчиком уже обработана
	// (как если бы автопродление началось между выборкой и обработкой).
	retryTg := ids["paid-autorenew-retry"]
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_outbox (aggregate_type, aggregate_id, topic, message_key, event_type, payload)
		VALUES ('subscription', $1::text, 'subscription.commands', $1::text, 'subscription.grant_bonus',
		        jsonb_build_object('type', 'subscription.grant_bonus', 'command_id', 'bonus:' || $2::text || ':' || $1::text,
		                           'telegram_id', $1::bigint, 'days', 30, 'campaign', $2::text, 'message', 'e2e {until}'))`,
		strconv.FormatInt(retryTg, 10), campaign); err != nil {
		t.Fatal(err)
	}
	waitProcessed := time.Now().Add(waitTimeout)
	for {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_messages WHERE message_id = $1`,
			fmt.Sprintf("user-subscription-service:bonus:%s:%d", campaign, retryTg)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(waitProcessed) {
			t.Fatal("deferred command not processed")
		}
		time.Sleep(pollEvery)
	}
	assertNoBonus(t, pool, campaign, retryTg)

	// 7. Повторный запуск с тем же ключом: наших получателей в списке нет,
	// повторных начислений и сообщений нет.
	out = mustRun(t, run, "", "--dry-run")
	again := readTargets(t, out)
	for _, s := range seeds {
		if _, ok := again[ids[s.name]]; ok {
			t.Fatalf("%s is a target again after the campaign:\n%s", s.name, out)
		}
	}
	if len(again) > 0 {
		if out, err := run(strconv.Itoa(len(again)) + "\n"); err != nil {
			t.Fatalf("second run: %v\n%s", err, out)
		}
	} else if out, err := run(""); err != nil || !strings.Contains(out, "Получателей нет") {
		t.Fatalf("second run: %v\n%s", err, out)
	}
	var ledgers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM subscription_grants WHERE source='admin' AND business_key=$1 AND telegram_id = ANY($2)`,
		campaign, []int64{ids["paid-churned-5d"], ids["trial-churned-10d"], ids["paid-ledger-churned-2d"], ids["paid-autorenew-finished"]}).Scan(&ledgers); err != nil || ledgers != 4 {
		t.Fatalf("ledger after rerun: %d %v", ledgers, err)
	}
	assertNoBonus(t, pool, campaign, retryTg)

	// 8. Биллинг закончил (неуспешно): профиль expired. Следующий запуск с тем
	// же ключом включает отложенного и начисляет ему — у команды нового
	// запуска свой command_id, обработанная команда прошлого не мешает.
	if _, err := pool.Exec(ctx, `UPDATE billing_recurring_profiles SET status = 'expired', next_charge_at = NULL WHERE telegram_id = $1`, retryTg); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // новое время запуска → новый command_id
	out = mustRun(t, run, "", "--dry-run")
	third := readTargets(t, out)
	if _, ok := third[retryTg]; !ok {
		t.Fatalf("deferred user is not a target after billing finished:\n%s", out)
	}
	for _, s := range seeds {
		if _, ok := third[ids[s.name]]; ok && s.name != "paid-autorenew-retry" {
			t.Fatalf("%s is a target again:\n%s", s.name, out)
		}
	}
	if out, err := run(strconv.Itoa(len(third)) + "\n"); err != nil {
		t.Fatalf("third run: %v\n%s", err, out)
	}
	var st string
	var exp *time.Time
	var granted, notifs int
	if err := pool.QueryRow(ctx, `
		SELECT us.status, us.expires_at,
		       (SELECT count(*) FROM subscription_grants WHERE telegram_id=$1 AND source='admin' AND business_key=$2),
		       (SELECT count(*) FROM event_outbox WHERE topic='tg.notifications' AND message_key=$1::text AND payload->>'message' LIKE '%Приносим искренние извинения%')
		FROM user_subscriptions us WHERE us.telegram_id=$1`, retryTg, campaign).Scan(&st, &exp, &granted, &notifs); err != nil {
		t.Fatal(err)
	}
	if st != "active" || exp == nil || exp.Sub(time.Now().UTC().Add(30*day)).Abs() > 3*time.Minute || granted != 1 || notifs != 1 {
		t.Fatalf("deferred user after billing finished: status=%s expires=%v ledger=%d notifs=%d", st, exp, granted, notifs)
	}
	t.Log("бонусная кампания: начислено и отправлено ровно ушедшим за 21 день, автопродление отложено и дождалось, повтор безопасен")
}

func mustRun(t *testing.T, run func(string, ...string) (string, error), stdin string, args ...string) string {
	t.Helper()
	out, err := run(stdin, args...)
	if err != nil {
		t.Fatalf("script: %v\n%s", err, out)
	}
	return out
}

var targetsPathRe = regexp.MustCompile(`Список \(telegram_id, категория, окончание доступа UTC\): (\S+)`)

func readTargets(t *testing.T, out string) map[int64]string {
	t.Helper()
	m := targetsPathRe.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no targets file in output:\n%s", out)
	}
	raw, err := os.ReadFile(m[1])
	if err != nil {
		t.Fatal(err)
	}
	res := map[int64]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		f := strings.Split(line, "\t")
		id, err := strconv.ParseInt(f[0], 10, 64)
		if err != nil || len(f) != 3 {
			t.Fatalf("bad targets line %q", line)
		}
		res[id] = f[1]
	}
	return res
}

func seedBonusUser(t *testing.T, pool *pgxpool.Pool, tg int64, s bonusSeed) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	var expires, grace any
	if s.status != "none" {
		expires = now.Add(s.expires)
	}
	if s.status == "grace" {
		grace = now.Add(s.grace)
	}
	plan := any(nil)
	if s.status != "none" {
		plan = "monthly_30d"
		if s.lastPay == "" && s.ledger != "paid" {
			plan = "trial_3d"
		}
	}
	// last_payment_id уникален (idx_user_subscriptions_last_payment_id_non_empty).
	lastPay := ""
	if s.lastPay != "" {
		lastPay = fmt.Sprintf("%s-%d", s.lastPay, tg)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO telegram_users (telegram_id) VALUES ($1)`, []any{tg}},
		{`INSERT INTO user_subscriptions (telegram_id, status, current_plan_code, trial_used, started_at, expires_at, grace_until,
		     last_payment_id, country_code, auto_renew_enabled, cancel_at_period_end, access_rev)
		  VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), 'LT', false, false, 1)`,
			[]any{tg, s.status, plan, s.trial, now.Add(-60 * 24 * time.Hour), expires, grace, lastPay}},
		{`INSERT INTO subscription_tokens (telegram_id, token, expires_at) VALUES ($1, gen_random_uuid()::text, $2)`, []any{tg, expires}},
	}
	switch {
	case s.ledger == "paid":
		stmts = append(stmts, struct {
			sql  string
			args []any
		}{`INSERT INTO subscription_grants (telegram_id, source, business_key, duration_days, effective_from, effective_until)
		    VALUES ($1::bigint, 'paid', 'e2e-ledger-' || $1::bigint::text, 30, now() - interval '32 days', now() - interval '2 days')`, []any{tg}})
	case s.ledger != "":
		stmts = append(stmts, struct {
			sql  string
			args []any
		}{`INSERT INTO subscription_grants (telegram_id, source, business_key, duration_days, effective_from, effective_until)
		    VALUES ($1, 'admin', $2, 30, now() - interval '40 days', now() - interval '10 days')`, []any{tg, s.ledger}})
	}
	if s.billing != "" {
		stmts = append(stmts, struct {
			sql  string
			args []any
		}{`INSERT INTO billing_recurring_profiles (telegram_id, plan_code, duration_days, amount_value, payment_method_id,
		        auto_renew_enabled, status, next_charge_at, last_payment_id)
		    VALUES ($1, 'monthly_30d', 30, 199.00, 'e2e-pm', $2::text <> 'expired', $2::text,
		            CASE WHEN $2::text = 'retry' THEN now() + interval '6 hours' END, NULLIF($3, ''))`, []any{tg, s.billing, lastPay}})
	}
	for _, st := range stmts {
		if _, err := tx.Exec(ctx, st.sql, st.args...); err != nil {
			t.Fatalf("seed %s: %v", s.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertNoBonus(t *testing.T, pool *pgxpool.Pool, campaign string, tg int64) {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM subscription_grants WHERE telegram_id=$1 AND source='admin' AND business_key=$2`, tg, campaign).Scan(&n); err != nil || n != 0 {
		t.Fatalf("bonus granted unexpectedly: %d %v", n, err)
	}
}

type bonusNotification struct {
	TelegramID int64  `json:"telegram_id"`
	Message    string `json:"message"`
	Keyboard   string `json:"keyboard"`
}

// readNotifications читает tg.notifications с начала и собирает сообщения
// для наших пользователей; ждёт, пока придут хотя бы want штук.
func readNotifications(t *testing.T, ids map[string]int64, want int) map[int64]bonusNotification {
	t.Helper()
	ours := map[string]int64{}
	for _, id := range ids {
		ours[strconv.FormatInt(id, 10)] = id
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		GroupID:     fmt.Sprintf("e2e-bonus-reader-%d", time.Now().UnixNano()),
		Topic:       "tg.notifications",
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	got := map[int64]bonusNotification{}
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if len(got) >= want {
				return got
			}
			t.Fatalf("read tg.notifications: %v (got %d of %d)", err, len(got), want)
		}
		if id, ok := ours[string(msg.Key)]; ok {
			var n bonusNotification
			if err := json.Unmarshal(msg.Value, &n); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(n.Message, "Приносим искренние извинения") {
				got[id] = n
			}
		}
		if len(got) >= want {
			// Дочитываем хвост, чтобы поймать лишние сообщения не нашим получателям.
			tail, cancelTail := context.WithTimeout(context.Background(), 3*time.Second)
			for {
				msg, err := r.ReadMessage(tail)
				if err != nil {
					break
				}
				if id, ok := ours[string(msg.Key)]; ok {
					var n bonusNotification
					if json.Unmarshal(msg.Value, &n) == nil && strings.Contains(n.Message, "Приносим искренние извинения") {
						got[id] = n
					}
				}
			}
			cancelTail()
			return got
		}
	}
}
