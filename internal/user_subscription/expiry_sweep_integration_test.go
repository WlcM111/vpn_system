//go:build integration

package user_subscription

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ============================================================================
// Дефект D5: sweep-воркер не должен обрезать подписку, оплаченную между
// выборкой кандидатов и их отзывом.
//
//   go test -tags=integration ./internal/user_subscription/...
// ============================================================================

// setupSubscriptionDB поднимает PostgreSQL с миграциями platform и
// user-subscription-service; extra — миграции других сервисов той же БД
// (например, billing-service для проверок профилей автопродления).
func setupSubscriptionDB(t *testing.T, extra ...string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("vpn_test"), tcpostgres.WithUsername("vpn"), tcpostgres.WithPassword("vpn"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, dir := range append([]string{"platform", "user-subscription-service"}, extra...) {
		root := filepath.Join("..", "..", "migrations", dir)
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".sql") {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, n := range names {
			body, err := os.ReadFile(filepath.Join(root, n))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, string(body)); err != nil {
				t.Fatalf("%s/%s: %v", dir, n, err)
			}
		}
	}
	return pool
}

func seedExpired(t *testing.T, pool *pgxpool.Pool, svc *Service, telegramID int64, status string) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.repo.ensureUserAndStateTx(ctx, tx, telegramID, "LT"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = $2, expires_at = now() - interval '2 hours', grace_until = CASE WHEN $2 = 'grace' THEN now() - interval '2 hours' END
		WHERE telegram_id = $1`, telegramID, status); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func subscriptionState(t *testing.T, pool *pgxpool.Pool, telegramID int64) (string, time.Time) {
	t.Helper()
	var status string
	var expires time.Time
	if err := pool.QueryRow(context.Background(), `SELECT status, expires_at FROM user_subscriptions WHERE telegram_id=$1`, telegramID).Scan(&status, &expires); err != nil {
		t.Fatal(err)
	}
	return status, expires
}

func TestSweepDoesNotCutSubscriptionPaidAfterSelection(t *testing.T) {
	pool := setupSubscriptionDB(t)
	ctx := context.Background()
	svc := NewService(NewRepository(pool), "http://localhost/sub/", "LT")
	const plain, renew = 15 * time.Minute, 168 * time.Hour

	seedExpired(t, pool, svc, 101, "active") // оплатит между выборкой и отзывом
	seedExpired(t, pool, svc, 102, "active") // контроль: истёк и не оплатил
	seedExpired(t, pool, svc, 103, "grace")  // контроль: grace истёк

	candidates, err := svc.repo.FindExpiredForSweep(ctx, plain, renew, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 3 {
		t.Fatalf("candidates = %v", candidates)
	}
	// Оплата пришла после выборки кандидатов (как делает ActivatePaidTx).
	if _, err := pool.Exec(ctx, `UPDATE user_subscriptions SET status='active', expires_at = now() + interval '30 days' WHERE telegram_id = 101`); err != nil {
		t.Fatal(err)
	}
	for _, c := range candidates {
		if _, err := svc.suspendExpiredOne(ctx, c, plain, renew); err != nil {
			t.Fatalf("suspend %d: %v", c.TelegramID, err)
		}
	}
	if st, exp := subscriptionState(t, pool, 101); st != "active" || time.Until(exp) < 29*24*time.Hour {
		t.Fatalf("paid subscription was cut: %s until %v", st, exp)
	}
	for _, id := range []int64{102, 103} {
		if st, _ := subscriptionState(t, pool, id); st != "expired" {
			t.Fatalf("user %d: %s, want expired", id, st)
		}
	}
	var events int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE event_type = 'subscription.suspended'`).Scan(&events)
	if events != 2 {
		t.Fatalf("suspended events = %d, want 2", events)
	}
	// Повторный проход ничего не трогает.
	if err := svc.SweepExpiredSubscriptions(ctx, plain, renew, 10); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE event_type = 'subscription.suspended'`).Scan(&events)
	if events != 2 {
		t.Fatalf("second pass produced events: %d", events)
	}
}
