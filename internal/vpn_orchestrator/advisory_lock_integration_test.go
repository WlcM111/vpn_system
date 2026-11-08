//go:build integration

package vpn_orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Дефект D7: advisory-lock проходов reconcile/квоты CDN берётся и снимается
// на одном соединении; после прохода лок свободен для любой сессии.
//
//	go test -tags=integration ./internal/vpn_orchestrator/...
func TestWithAdvisoryLockReleasesOnSameConnection(t *testing.T) {
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
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MinConns = 4
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	repo := NewRepository(pool)
	const key int64 = 776644221100

	// Пока проход идёт, вторая «реплика» лок не получает.
	inside := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ran, err := repo.WithAdvisoryLock(ctx, key, func(context.Context) {
			close(inside)
			<-release
		})
		if err != nil || !ran {
			t.Errorf("first pass: ran=%v err=%v", ran, err)
		}
	}()
	<-inside
	ran, err := repo.WithAdvisoryLock(ctx, key, func(context.Context) { t.Error("second pass must be skipped") })
	if err != nil || ran {
		t.Fatalf("concurrent pass: ran=%v err=%v", ran, err)
	}
	close(release)
	wg.Wait()

	// После прохода лок свободен на ЛЮБОМ соединении пула — много раз подряд.
	for i := 0; i < 20; i++ {
		ran, err := repo.WithAdvisoryLock(ctx, key, func(context.Context) {})
		if err != nil || !ran {
			t.Fatalf("pass %d skipped: lock leaked (ran=%v err=%v)", i, ran, err)
		}
	}
	var held int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Fatalf("advisory locks still held: %d", held)
	}
}
