//go:build integration

package vpn_orchestrator

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	commonmetrics "vpn-platform/internal/common/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

func nodeMaxUsers(t *testing.T, pool *pgxpool.Pool, serverKey string) (int, time.Time) {
	t.Helper()
	var maxUsers int
	var updatedAt time.Time
	if err := pool.QueryRow(context.Background(),
		`SELECT max_users, updated_at FROM vpn_servers WHERE server_key = $1`, serverKey,
	).Scan(&maxUsers, &updatedAt); err != nil {
		t.Fatalf("read %s: %v", serverKey, err)
	}
	return maxUsers, updatedAt
}

// Миграция 0021: лимит 1..399 поднимается до 400, больший лимит и «без лимита»
// (0 и меньше) не меняются, повторный прогон файла ничего не трогает, строка
// без max_users получает 400 из DEFAULT столбца.
func TestMigration0021RaisesNodeLimitTo400(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t) // все миграции, включая 0021, на пустой схеме

	var def string
	if err := pool.QueryRow(ctx, `
		SELECT column_default FROM information_schema.columns
		WHERE table_name = 'vpn_servers' AND column_name = 'max_users'`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if def != "400" {
		t.Fatalf("DEFAULT max_users = %q, want 400", def)
	}

	// Состояние до 0021: ноды с разными лимитами, updated_at в прошлом.
	before := map[string]int{"n1": 1, "n200": 200, "n399": 399, "n400": 400, "n1000": 1000, "n0": 0, "nneg": -5}
	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Microsecond)
	for key, maxUsers := range before {
		if _, err := pool.Exec(ctx, `
			INSERT INTO vpn_servers (server_key, country_code, title, max_users, updated_at)
			VALUES ($1, 'LT', $1, $2, $3)`, key, maxUsers, old); err != nil {
			t.Fatal(err)
		}
	}

	sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "vpn-orchestrator-service", "0021_node_max_users_400.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("apply 0021: %v", err)
	}

	want := map[string]int{"n1": 400, "n200": 400, "n399": 400, "n400": 400, "n1000": 1000, "n0": 0, "nneg": -5}
	afterFirst := map[string]time.Time{}
	for key, w := range want {
		got, upd := nodeMaxUsers(t, pool, key)
		if got != w {
			t.Fatalf("%s: max_users = %d, want %d", key, got, w)
		}
		changed := before[key] != w
		if changed != upd.After(old) {
			t.Fatalf("%s: updated_at changed = %v, want %v", key, upd.After(old), changed)
		}
		afterFirst[key] = upd
	}

	// Повторный прогон (как если бы файл выполнили ещё раз) ничего не меняет.
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("re-apply 0021: %v", err)
	}
	for key, w := range want {
		got, upd := nodeMaxUsers(t, pool, key)
		if got != w || !upd.Equal(afterFirst[key]) {
			t.Fatalf("%s after re-apply: max_users = %d (want %d), updated_at %v (want %v)", key, got, w, upd, afterFirst[key])
		}
	}

	if _, err := pool.Exec(ctx, `INSERT INTO vpn_servers (server_key, country_code, title) VALUES ('ndef', 'LT', 'ndef')`); err != nil {
		t.Fatal(err)
	}
	if got, _ := nodeMaxUsers(t, pool, "ndef"); got != 400 {
		t.Fatalf("row without max_users: max_users = %d, want 400", got)
	}
}

// Регистрация ноды без max_users (и повторная регистрация без него) ставит
// DefaultNodeMaxUsers; явно переданный лимит сохраняется.
func TestUpsertAdminNodeDefaultMaxUsers(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	repo := NewRepository(pool)

	if DefaultNodeMaxUsers != 400 {
		t.Fatalf("DefaultNodeMaxUsers = %d, want 400", DefaultNodeMaxUsers)
	}
	req := AdminNodeRequest{
		ServerKey: "lt-1", NodeID: "lt-1", CountryCode: "LT", Title: "LT",
		PublicHost: "lt.example.invalid", Enabled: true,
	}
	for _, tc := range []struct {
		name     string
		maxUsers int
		want     int
	}{
		{"без max_users", 0, 400},
		{"явный лимит", 250, 250},
		{"повторная регистрация без max_users", 0, 400},
		{"отрицательный лимит", -1, 400},
	} {
		req.MaxUsers = tc.maxUsers
		if err := repo.UpsertAdminNode(ctx, req); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got, _ := nodeMaxUsers(t, pool, "lt-1"); got != tc.want {
			t.Fatalf("%s: max_users = %d, want %d", tc.name, got, tc.want)
		}
	}
}

type gatheredSeries struct {
	labels map[string]string
	value  float64
}

// gatherNodeMetrics собирает выбранные векторы через отдельный реестр и
// возвращает их серии по имени метрики.
func gatherNodeMetrics(t *testing.T, cs ...prometheus.Collector) map[string][]gatheredSeries {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(cs...)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]gatheredSeries{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			s := gatheredSeries{labels: map[string]string{}, value: m.GetGauge().GetValue()}
			for _, lp := range m.GetLabel() {
				s.labels[lp.GetName()] = lp.GetValue()
			}
			out[mf.GetName()] = append(out[mf.GetName()], s)
		}
	}
	return out
}

// Выключенная нода (здесь — страна AR целиком) не попадает ни в одну
// построчную метрику node_*, не считается ни живой, ни проблемной и не входит
// в вместимость пула; включённые ноды экспортируются как раньше.
func TestCollectNodesSkipsDisabledNodes(t *testing.T) {
	ctx := context.Background()
	pool := startOrchestratorDB(t)
	now := time.Now().UTC()
	stale := now.Add(-time.Hour)

	for _, n := range []struct {
		key, country string
		enabled      bool
		hb           time.Time
		bandwidth    int
	}{
		{"ar-1", "AR", false, stale, 0},
		{"us-1", "US", true, now, 1000},
		{"lt-1", "LT", true, stale, 0},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO vpn_servers (server_key, node_id, country_code, title, public_host, enabled,
				max_users, last_heartbeat_at, bandwidth_mbps, uplink_bps, downlink_bps, online_users)
			VALUES ($1, $1, $2, $1, $1 || '.example.invalid', $3, 400, $4, $5, 1000000, 1000000, 2)`,
			n.key, n.country, n.enabled, n.hb, n.bandwidth); err != nil {
			t.Fatal(err)
		}
	}
	// Учётки: us-1 — 3 включённые и 1 выключенная, ar-1 — 2, lt-1 — 1.
	creds := []struct {
		tg      int64
		server  string
		enabled bool
	}{
		{1, "us-1", true}, {2, "us-1", true}, {3, "us-1", true}, {4, "us-1", false},
		{1, "ar-1", true}, {2, "ar-1", true},
		{1, "lt-1", true},
	}
	for _, c := range creds {
		item := c.server + "-item"
		if _, err := pool.Exec(ctx, `
			INSERT INTO vpn_user_node_credentials (telegram_id, item_key, server_key, node_id,
				inbound_tag, email, vless_uuid, enabled)
			VALUES ($1, $2, $3, $3, 'vless-ws-in', $4, $4, $5)`,
			c.tg, item, c.server, fmt.Sprintf("%d-%s", c.tg, item), c.enabled); err != nil {
			t.Fatal(err)
		}
	}

	collector := NewMetricsCollector(pool, 0, 0, 90*time.Second, nil)
	if err := collector.collectNodes(ctx); err != nil {
		t.Fatalf("collectNodes: %v", err)
	}

	perNode := []prometheus.Collector{
		commonmetrics.NodeActiveUsers, commonmetrics.NodeMaxUsers, commonmetrics.NodeLoadPercent,
		commonmetrics.NodeUp, commonmetrics.NodeHeartbeatAgeSeconds, commonmetrics.NodeTrafficBps,
		commonmetrics.NodeOnlineUsers, commonmetrics.NodeBandwidthPercent,
		commonmetrics.NodeBandwidthConfigured, commonmetrics.NodeBandwidthMbps,
	}
	got := gatherNodeMetrics(t, append(perNode, commonmetrics.NodesCount,
		commonmetrics.PoolCapacityTotal, commonmetrics.PoolActiveTotal)...)

	for _, name := range []string{
		"vpn_platform_node_active_users", "vpn_platform_node_max_users", "vpn_platform_node_load_percent",
		"vpn_platform_node_up", "vpn_platform_node_heartbeat_age_seconds", "vpn_platform_node_traffic_bps",
		"vpn_platform_node_online_users", "vpn_platform_node_bandwidth_percent",
		"vpn_platform_node_bandwidth_configured", "vpn_platform_node_bandwidth_mbps",
	} {
		keys := map[string]bool{}
		for _, s := range got[name] {
			if s.labels["country"] == "AR" || s.labels["server_key"] == "ar-1" {
				t.Fatalf("%s: выключенная нода в метриках: %v", name, s.labels)
			}
			keys[s.labels["server_key"]] = true
		}
		if !keys["us-1"] || !keys["lt-1"] {
			t.Fatalf("%s: нет включённых нод: %v", name, got[name])
		}
	}

	value := func(name, serverKey string) float64 {
		t.Helper()
		for _, s := range got[name] {
			if s.labels["server_key"] == serverKey {
				return s.value
			}
		}
		t.Fatalf("%s{server_key=%q} не найдена", name, serverKey)
		return 0
	}
	if v := value("vpn_platform_node_active_users", "us-1"); v != 3 {
		t.Fatalf("us-1 active_users = %v, want 3", v)
	}
	if v := value("vpn_platform_node_max_users", "us-1"); v != 400 {
		t.Fatalf("us-1 max_users = %v, want 400", v)
	}
	if v := value("vpn_platform_node_load_percent", "us-1"); math.Abs(v-0.75) > 1e-9 {
		t.Fatalf("us-1 load_percent = %v, want 0.75", v)
	}
	if v := value("vpn_platform_node_up", "us-1"); v != 1 {
		t.Fatalf("us-1 up = %v, want 1", v)
	}
	if v := value("vpn_platform_node_up", "lt-1"); v != 0 {
		t.Fatalf("lt-1 up = %v, want 0", v)
	}

	states := map[string]float64{}
	for _, s := range got["vpn_platform_nodes_count"] {
		states[s.labels["state"]] = s.value
	}
	for state, want := range map[string]float64{"enabled": 2, "disabled": 1, "alive": 1, "stale": 1} {
		if states[state] != want {
			t.Fatalf("nodes_count{state=%q} = %v, want %v (all: %v)", state, states[state], want, states)
		}
	}
	if v := got["vpn_platform_pool_capacity_total"][0].value; v != 800 {
		t.Fatalf("pool_capacity_total = %v, want 800", v)
	}
	if v := got["vpn_platform_pool_active_total"][0].value; v != 4 {
		t.Fatalf("pool_active_total = %v, want 4", v)
	}
}
