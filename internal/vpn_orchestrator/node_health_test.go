package vpn_orchestrator

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestClassifyNodeHealth(t *testing.T) {
	tests := []struct {
		name           string
		heartbeatAlive bool
		probeChecked   bool
		probeAlive     bool
		wantState      NodeHealthState
		wantUp         bool
		wantBlocked    bool
	}{
		{
			name:           "живой heartbeat и успешный TCP-пробник -> healthy",
			heartbeatAlive: true,
			probeChecked:   true,
			probeAlive:     true,
			wantState:      NodeStateHealthy,
			wantUp:         true,
			wantBlocked:    false,
		},
		{
			name:           "живой heartbeat, но TCP-порт заблокирован из РФ -> blocked_dataplane (нода НЕ упала)",
			heartbeatAlive: true,
			probeChecked:   true,
			probeAlive:     false,
			wantState:      NodeStateBlockedDataplane,
			wantUp:         true,
			wantBlocked:    true,
		},
		{
			name:           "heartbeat заблокирован, но VPN-порт отвечает по TCP -> blocked_controlplane (нода НЕ упала)",
			heartbeatAlive: false,
			probeChecked:   true,
			probeAlive:     true,
			wantState:      NodeStateBlockedControlplane,
			wantUp:         true,
			wantBlocked:    true,
		},
		{
			name:           "ни heartbeat, ни TCP-пробник не отвечают -> down (нода реально упала)",
			heartbeatAlive: false,
			probeChecked:   true,
			probeAlive:     false,
			wantState:      NodeStateDown,
			wantUp:         false,
			wantBlocked:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, up, blocked := ClassifyNodeHealth(tt.heartbeatAlive, tt.probeChecked, tt.probeAlive)
			if state != tt.wantState || up != tt.wantUp || blocked != tt.wantBlocked {
				t.Fatalf("ClassifyNodeHealth() = (%s, up=%v, blocked=%v), want (%s, up=%v, blocked=%v)",
					state, up, blocked, tt.wantState, tt.wantUp, tt.wantBlocked)
			}
		})
	}
}

func TestAllocateKeepsNodeWhenControlplaneBlockedButProbeAlive(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	items := []PoolItem{poolItem(1, "ro-main", "ro-main-1", "RO", 10)}
	stale := now.Add(-10 * time.Minute)

	servers := map[string][]ServerLoad{
		"RO": {{
			ServerKey:       "ro-main-1",
			CountryCode:     "RO",
			Enabled:         true,
			MaxUsers:        400,
			ActiveUsers:     25,
			Weight:          100,
			LastHeartbeatAt: &stale, // Kafka-heartbeat не доходит
			ProbeChecked:    true,
			ProbeAlive:      true, // но TCP-порт ноды живой
		}},
	}

	a := NewAllocator(90*time.Second, true)
	got := itemKeys(a.Allocate(items, servers, noSticky, now))
	if !equalStrings(got, []string{"ro-main"}) {
		t.Fatalf("Allocate() dropped working node with live TCP probe: got %v, want [ro-main]", got)
	}
}

func TestPreserveCountriesOnMassDrop(t *testing.T) {
	candidates := []PoolItem{
		poolItem(1, "ro-main", "ro-main-1", "RO", 10),
		poolItem(2, "de-main", "de-main-1", "DE", 20),
	}
	// Допустим, для RO аллокатор вернул пусто из-за обрыва связи центр<->нода, а DE жива.
	allocated := []PoolItem{candidates[1]}

	got := itemKeys(PreserveCountriesOnMassDrop(candidates, allocated, noSticky))
	want := []string{"ro-main", "de-main"}
	if !equalStrings(got, want) {
		t.Fatalf("PreserveCountriesOnMassDrop() = %v, want %v", got, want)
	}
}

func TestFilterPoolItemsForTrial(t *testing.T) {
	items := []PoolItem{
		poolItem(1, "lt-main", "lt-main-1", "LT", 10),
		poolItem(2, "de-main", "de-main-1", "DE", 20),
		poolItem(3, "ro-main", "ro-main-1", "RO", 30),
	}

	for _, target := range []string{"", "RO", "ro", "ro-main-1", "ro-main"} {
		got := itemKeys(FilterPoolItemsForTrial(items, target))
		if !equalStrings(got, []string{"ro-main"}) {
			t.Fatalf("FilterPoolItemsForTrial(%q) = %v, want [ro-main]", target, got)
		}
	}
}

// Двухбуквенная цель не должна совпадать с названиями чужих стран
// ("ro" есть в «Europe» и «Toronto»): пробная подписка — только Румыния.
func TestFilterPoolItemsForTrialPrefersCountryCodeOverTitle(t *testing.T) {
	eu := poolItem(1, "eu-main", "eu-main-1", "EU", 5)
	eu.Title = "🇪🇺 Europe"
	ca := poolItem(2, "ca-main", "ca-main-1", "CA", 6)
	ca.Title = "🇨🇦 Toronto"
	ro := poolItem(3, "ro-main", "ro-main-1", "RO", 30)
	ro.Title = "🇷🇴 Румыния"
	items := []PoolItem{eu, ca, ro}

	for _, target := range []string{"", "RO", "ro", "ro-main-1", "ro-main", "Румыния"} {
		got := itemKeys(FilterPoolItemsForTrial(items, target))
		if !equalStrings(got, []string{"ro-main"}) {
			t.Fatalf("FilterPoolItemsForTrial(%q) = %v, want [ro-main]", target, got)
		}
	}
}

// Страна, где живые ноды есть, но все переполнены, при softOverflow=false
// исключается решением оператора — защита от массового выпадения её не
// возвращает. Страна без единой живой ноды сохраняется одним пул-айтемом.
func TestAllocateWithMassDropProtection(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	items := []PoolItem{
		poolItem(1, "ro-main", "ro-main-1", "RO", 10),
		poolItem(2, "de-main", "de-main-1", "DE", 20),
		poolItem(3, "lt-main", "lt-main-1", "LT", 30),
	}
	servers := map[string][]ServerLoad{
		"RO": {serverLoad("ro-main-1", "RO", 10, 400, 100, 10*time.Second, now)},  // живая, есть места
		"DE": {serverLoad("de-main-1", "DE", 400, 400, 100, 10*time.Second, now)}, // живая, переполнена
		"LT": {serverLoad("lt-main-1", "LT", 10, 400, 100, 30*time.Minute, now)},  // мёртвая (heartbeat старый, пробы нет)
	}

	strict := NewAllocator(90*time.Second, false)
	if got := itemKeys(strict.AllocateWithMassDropProtection(items, servers, noSticky, now)); !equalStrings(got, []string{"ro-main", "lt-main"}) {
		t.Fatalf("softOverflow=false: got %v, want [ro-main lt-main]", got)
	}

	soft := NewAllocator(90*time.Second, true)
	if got := itemKeys(soft.AllocateWithMassDropProtection(items, servers, noSticky, now)); !equalStrings(got, []string{"ro-main", "de-main", "lt-main"}) {
		t.Fatalf("softOverflow=true: got %v, want [ro-main de-main lt-main]", got)
	}
}

func TestProbeTCPTargets(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := closed.Addr().String()
	_ = closed.Close()

	ctx := context.Background()
	if checked, alive := ProbeTCPTargets(ctx, []string{closedAddr, ln.Addr().String()}, time.Second); !checked || !alive {
		t.Fatalf("one reachable target: checked=%v alive=%v, want true true", checked, alive)
	}
	if checked, alive := ProbeTCPTargets(ctx, []string{closedAddr}, time.Second); !checked || alive {
		t.Fatalf("unreachable target: checked=%v alive=%v, want true false", checked, alive)
	}
	if checked, _ := ProbeTCPTargets(ctx, []string{"node.example.com:443", "x.invalid:443", " "}, time.Second); checked {
		t.Fatal("reserved/empty targets must not count as checked")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if checked, alive := ProbeTCPTargets(canceled, []string{closedAddr}, time.Second); checked || alive {
		t.Fatalf("canceled parent context: checked=%v alive=%v, want false false (no false 'blocked')", checked, alive)
	}
}
