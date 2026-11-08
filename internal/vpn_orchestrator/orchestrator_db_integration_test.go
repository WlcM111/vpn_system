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

// Hysteria на реальной БД: эндпоинты берутся только из таблицы. Окружение
// (HYSTERIA_ADDRESS) их не добавляет ни к пустой таблице, ни на месте
// выключенной строки.
func TestHysteriaEndpointsComeOnlyFromTable(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	repo := NewRepository(pool)
	svc := &Service{repo: repo}
	t.Setenv("HYSTERIA_CONFIG_ENABLED", "true")
	t.Setenv("HYSTERIA_ADDRESS", "env.example")
	t.Setenv("HYSTERIA_SERVER_KEY", "lt-1")

	if got := svc.loadHysteriaEndpoints(ctx); len(got) != 0 {
		t.Fatalf("пустая таблица: %+v, ожидалось пусто", got)
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
	on := true
	if err := repo.UpsertAdminHysteriaEndpoint(ctx, AdminHysteriaEndpointRequest{
		HysteriaKey: "lt-hy", ServerKey: "lt-1", Enabled: &on, Address: "lt.example",
	}); err != nil {
		t.Fatal(err)
	}
	if got := svc.loadHysteriaEndpoints(ctx); len(got) != 1 || got[0].HysteriaKey != "lt-hy" {
		t.Fatalf("строка включена: %+v, ожидался lt-hy", got)
	}
}

// Миграция 0022 и XHTTP-эндпоинты на реальной БД: запись, чтение только
// включённых, удаление; ограничения таблицы не пускают строку без инбаунда и
// с путём не от корня.
func TestXHTTPEndpointsRepository(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	repo := NewRepository(pool)
	svc := &Service{repo: repo}

	if err := repo.UpsertAdminNode(ctx, AdminNodeRequest{ServerKey: "ro-1", NodeID: "node-ro", PublicHost: "ro.example"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertAdminXHTTPEndpoint(ctx, AdminXHTTPEndpointRequest{
		XHTTPKey: "ro-1-xhttp", ServerKey: "ro-1", InboundTag: "vless-xhttp-in",
		Address: "ro.example", Path: "/p1", Remarks: "RO XHTTP",
	}); err != nil {
		t.Fatal(err)
	}
	off := false
	if err := repo.UpsertAdminXHTTPEndpoint(ctx, AdminXHTTPEndpointRequest{
		XHTTPKey: "ro-1-xhttp-off", ServerKey: "ro-1", Enabled: &off, InboundTag: "vless-xhttp-in",
		Address: "ro.example", Path: "/p2",
	}); err != nil {
		t.Fatal(err)
	}

	got := svc.loadXHTTPEndpoints(ctx)
	if len(got) != 1 {
		t.Fatalf("включённых эндпоинтов %d, ожидался 1: %+v", len(got), got)
	}
	e := got[0]
	if e.XHTTPKey != "ro-1-xhttp" || e.ServerKey != "ro-1" || e.InboundTag != "vless-xhttp-in" ||
		e.Port != 443 || e.Path != "/p1" || e.Mode != "auto" || e.Fingerprint != "chrome" || e.ALPN != "h2,http/1.1" {
		t.Fatalf("прочитано %+v", e)
	}

	for name, sql := range map[string]string{
		"без инбаунда": `INSERT INTO vpn_xhttp_endpoints (xhttp_key, server_key, inbound_tag, address, path) VALUES ('k1', 'ro-1', ' ', 'ro.example', '/p')`,
		"путь не от /": `INSERT INTO vpn_xhttp_endpoints (xhttp_key, server_key, inbound_tag, address, path) VALUES ('k2', 'ro-1', 'vless-xhttp-in', 'ro.example', 'p')`,
		"порт 0":       `INSERT INTO vpn_xhttp_endpoints (xhttp_key, server_key, inbound_tag, address, path, port) VALUES ('k3', 'ro-1', 'vless-xhttp-in', 'ro.example', '/p', 0)`,
	} {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Errorf("%s: строка принята, ожидалась ошибка ограничения", name)
		}
	}

	if err := repo.DeleteXHTTPEndpoint(ctx, "ro-1-xhttp"); err != nil {
		t.Fatal(err)
	}
	if got := svc.loadXHTTPEndpoints(ctx); len(got) != 0 {
		t.Fatalf("после удаления: %+v", got)
	}
}

// Инбаунд основного профиля берётся из настроек: пул-айтем → сервер →
// Reality-эндпоинт сервера → XHTTP-эндпоинт. Если нигде не задан — пул-айтем
// не выдаётся (встроенного тега по умолчанию нет).
func TestPoolItemInboundComesFromSettings(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	repo := NewRepository(pool)

	for _, sk := range []string{"a-1", "b-1", "c-1", "d-1", "e-1"} {
		if err := repo.UpsertAdminNode(ctx, AdminNodeRequest{
			ServerKey: sk, NodeID: "node-" + sk, CountryCode: "XX", Title: sk, PublicHost: sk + ".example", Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := repo.UpsertAdminPoolItem(ctx, AdminPoolItemRequest{
			ItemKey: "item-" + sk, ServerKey: sk, CountryCode: "XX", Title: sk, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	mustExec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mustExec(`UPDATE vpn_pool_items SET inbound_tag = 'from-item' WHERE server_key = 'a-1'`)
	mustExec(`UPDATE vpn_servers SET default_inbound_tag = 'from-server' WHERE server_key IN ('a-1', 'b-1')`)
	if err := repo.UpsertAdminRealityEndpoint(ctx, AdminRealityEndpointRequest{
		RealityKey: "c-reality", ServerKey: "c-1", InboundTag: "from-reality", Address: "1.2.3.4",
		ServerName: "c-1.example", PublicKey: "pbk",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertAdminXHTTPEndpoint(ctx, AdminXHTTPEndpointRequest{
		XHTTPKey: "d-xhttp", ServerKey: "d-1", InboundTag: "from-xhttp", Address: "d-1.example", Path: "/p",
	}); err != nil {
		t.Fatal(err)
	}

	items, err := repo.ListEnabledPoolItems(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, it := range items {
		got[it.ServerKey] = it.InboundTag
	}
	want := map[string]string{"a-1": "from-item", "b-1": "from-server", "c-1": "from-reality", "d-1": "from-xhttp"}
	if len(got) != len(want) {
		t.Fatalf("пул-айтемы: %v, ожидалось %v (e-1 без инбаунда не выдаётся)", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: инбаунд %q, ожидался %q", k, got[k], v)
		}
	}
}
