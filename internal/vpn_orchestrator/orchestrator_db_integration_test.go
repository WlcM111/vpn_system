//go:build integration

package vpn_orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startOrchestratorDB поднимает PostgreSQL 16 и накатывает миграции
// vpn-orchestrator-service в штатном порядке.
func startOrchestratorDB(t *testing.T) *pgxpool.Pool {
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
	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "vpn-orchestrator-service", "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("migrations: %v (%d files)", err, len(files))
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(f), err)
		}
	}
	return pool
}

// Дефект D10: при CDN_QUOTA_TELEMETRY_STALE_AFTER=0 («проверка выключена» —
// значение, которое допускает LoadCDNQuotaPolicyFromEnv) SummarizeQuotas
// передавал в запрос аргумент без плейсхолдера, pgx отвечал «expected 0
// arguments, got 1», и метрики квоты CDN не выставлялись ни разу.
func TestSummarizeQuotasWithAndWithoutStaleThreshold(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	repo := NewRepository(pool)
	now := time.Now().UTC()

	observe := func(tg int64, node string, observed int64, at time.Time) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := repo.ApplyObservationTx(ctx, tx, tg, node, observed, 0, 100, calendarPeriodKey(at), at); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	observe(1, "node-a", 10, now)
	observe(1, "node-a", 150, now) // 140 > 100 → exhausted
	observe(2, "node-a", 5, now)
	observe(3, "node-b", 7, now.Add(-2*time.Hour)) // давно не было отчёта

	for _, tc := range []struct {
		stale     time.Duration
		wantStale map[string]int
	}{
		{stale: 0, wantStale: map[string]int{"node-a": 0, "node-b": 0}},
		{stale: time.Hour, wantStale: map[string]int{"node-a": 0, "node-b": 1}},
	} {
		sums, err := repo.SummarizeQuotas(ctx, tc.stale)
		if err != nil {
			t.Fatalf("SummarizeQuotas(stale=%v): %v", tc.stale, err)
		}
		got := map[string]CDNQuotaSummary{}
		for _, s := range sums {
			got[s.NodeID] = s
		}
		a, b := got["node-a"], got["node-b"]
		if a.Rows != 2 || a.ExhaustedRows != 1 || a.UsedBytesTotal != 140 || b.Rows != 1 {
			t.Fatalf("stale=%v: summaries %+v", tc.stale, got)
		}
		for node, want := range tc.wantStale {
			if got[node].StaleRows != want {
				t.Fatalf("stale=%v: %s stale rows = %d, want %d", tc.stale, node, got[node].StaleRows, want)
			}
		}
	}
}

// Дефект D11 на реальной БД: строка Hysteria выключена оператором, но при
// заданном HYSTERIA_ADDRESS того же сервера подписка не должна получать
// эндпоинт из окружения. Пустая таблица по-прежнему использует окружение.
func TestDisabledHysteriaRowIsNotReplacedByEnv(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	repo := NewRepository(pool)
	svc := &Service{repo: repo}
	t.Setenv("HYSTERIA_CONFIG_ENABLED", "true")
	t.Setenv("HYSTERIA_ADDRESS", "env.example")
	t.Setenv("HYSTERIA_SERVER_KEY", "lt-1")

	if got := svc.loadHysteriaEndpoints(ctx); len(got) != 1 || got[0].HysteriaKey != "env" {
		t.Fatalf("пустая таблица: %+v, ожидался env-эндпоинт", got)
	}
	if err := repo.UpsertAdminNode(ctx, AdminNodeRequest{ServerKey: "lt-1", NodeID: "node-lt", PublicHost: "lt.example"}); err != nil {
		t.Fatal(err)
	}
	off := false
	if err := repo.UpsertAdminHysteriaEndpoint(ctx, AdminHysteriaEndpointRequest{
		HysteriaKey: "lt-hy", ServerKey: "lt-1", Enabled: &off, Address: "lt.example",
	}); err != nil {
		t.Fatal(err)
	}
	if got := svc.loadHysteriaEndpoints(ctx); len(got) != 0 {
		t.Fatalf("строка выключена, но к выдаче: %+v", got)
	}
}
