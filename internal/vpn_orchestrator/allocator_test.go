package vpn_orchestrator

import (
	"testing"
	"time"
)

// ============================================================================
// Тесты аллокатора — сердце выдачи доступа.
//
// Аллокатор решает, какую ноду получит пользователь. Ошибка здесь означает
// либо выдачу мёртвой ноды (человек не подключится), либо перекос нагрузки,
// либо потерю sticky-привязки (пользователь скачет между нодами).
// ============================================================================

// helpers

func poolItem(id int64, itemKey, serverKey, country string, sortOrder int) PoolItem {
	return PoolItem{
		ID:          id,
		ItemKey:     itemKey,
		ServerKey:   serverKey,
		CountryCode: country,
		Enabled:     true,
		SortOrder:   sortOrder,
	}
}

func serverLoad(serverKey, country string, active, max, weight int, heartbeatAgo time.Duration, now time.Time) ServerLoad {
	hb := now.Add(-heartbeatAgo)
	return ServerLoad{
		ServerKey:       serverKey,
		CountryCode:     country,
		Enabled:         true,
		MaxUsers:        max,
		ActiveUsers:     active,
		Weight:          weight,
		LastHeartbeatAt: &hb,
	}
}

// noSticky — пользователь ни к чему не привязан.
func noSticky(string) string { return "" }

func itemKeys(items []PoolItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.ItemKey)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestAllocate(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	const ttl = 90 * time.Second

	tests := []struct {
		name         string
		items        []PoolItem
		servers      map[string][]ServerLoad
		sticky       stickyResolver
		softOverflow bool
		want         []string
	}{
		{
			name:  "одна живая нода — она и выдаётся",
			items: []PoolItem{poolItem(1, "lt-ws", "lt-1", "LT", 10)},
			servers: map[string][]ServerLoad{
				"LT": {serverLoad("lt-1", "LT", 5, 100, 100, 10*time.Second, now)},
			},
			sticky: noSticky,
			want:   []string{"lt-ws"},
		},
		{
			name:  "мёртвая нода исключается всегда",
			items: []PoolItem{poolItem(1, "lt-ws", "lt-1", "LT", 10)},
			servers: map[string][]ServerLoad{
				// heartbeat старше TTL — нода считается мёртвой
				"LT": {serverLoad("lt-1", "LT", 5, 100, 100, 5*time.Minute, now)},
			},
			sticky: noSticky,
			want:   []string{},
		},
		{
			name: "least-load: выбирается наименее загруженная",
			items: []PoolItem{
				poolItem(1, "lt-a", "lt-a", "LT", 10),
				poolItem(2, "lt-b", "lt-b", "LT", 20),
			},
			servers: map[string][]ServerLoad{
				"LT": {
					serverLoad("lt-a", "LT", 90, 100, 100, time.Second, now),
					serverLoad("lt-b", "LT", 10, 100, 100, time.Second, now), // свободнее
				},
			},
			sticky: noSticky,
			want:   []string{"lt-b"},
		},
		{
			name: "вес учитывается: мощная нода забирает больше",
			items: []PoolItem{
				poolItem(1, "lt-a", "lt-a", "LT", 10),
				poolItem(2, "lt-b", "lt-b", "LT", 20),
			},
			servers: map[string][]ServerLoad{
				"LT": {
					// score = (active+1)/weight: (50+1)/200 = 0.255
					serverLoad("lt-a", "LT", 50, 1000, 200, time.Second, now),
					// (20+1)/50 = 0.42 — хуже, несмотря на меньшее число юзеров
					serverLoad("lt-b", "LT", 20, 1000, 50, time.Second, now),
				},
			},
			sticky: noSticky,
			want:   []string{"lt-a"},
		},
		{
			name: "sticky побеждает least-load",
			items: []PoolItem{
				poolItem(1, "lt-a", "lt-a", "LT", 10),
				poolItem(2, "lt-b", "lt-b", "LT", 20),
			},
			servers: map[string][]ServerLoad{
				"LT": {
					serverLoad("lt-a", "LT", 90, 100, 100, time.Second, now), // загружена
					serverLoad("lt-b", "LT", 1, 100, 100, time.Second, now),  // свободна
				},
			},
			// пользователь уже привязан к загруженной lt-a — остаётся на ней
			sticky: func(itemKey string) string {
				if itemKey == "lt-a" {
					return "lt-a"
				}
				return ""
			},
			want: []string{"lt-a"},
		},
		{
			name: "sticky на мёртвую ноду игнорируется",
			items: []PoolItem{
				poolItem(1, "lt-a", "lt-a", "LT", 10),
				poolItem(2, "lt-b", "lt-b", "LT", 20),
			},
			servers: map[string][]ServerLoad{
				"LT": {
					serverLoad("lt-a", "LT", 1, 100, 100, 10*time.Minute, now), // мертва
					serverLoad("lt-b", "LT", 50, 100, 100, time.Second, now),
				},
			},
			sticky: func(itemKey string) string {
				if itemKey == "lt-a" {
					return "lt-a"
				}
				return ""
			},
			want: []string{"lt-b"},
		},
		{
			name:  "переполнение + softOverflow=true: выдаём деградировавшую",
			items: []PoolItem{poolItem(1, "lt-ws", "lt-1", "LT", 10)},
			servers: map[string][]ServerLoad{
				"LT": {serverLoad("lt-1", "LT", 100, 100, 100, time.Second, now)},
			},
			sticky:       noSticky,
			softOverflow: true,
			want:         []string{"lt-ws"},
		},
		{
			name:  "переполнение + softOverflow=false: страна исключается",
			items: []PoolItem{poolItem(1, "lt-ws", "lt-1", "LT", 10)},
			servers: map[string][]ServerLoad{
				"LT": {serverLoad("lt-1", "LT", 100, 100, 100, time.Second, now)},
			},
			sticky:       noSticky,
			softOverflow: false,
			want:         []string{},
		},
		{
			name: "по одному пункту на страну",
			items: []PoolItem{
				poolItem(1, "lt-a", "lt-a", "LT", 10),
				poolItem(2, "lt-b", "lt-b", "LT", 20),
				poolItem(3, "de-a", "de-a", "DE", 30),
			},
			servers: map[string][]ServerLoad{
				"LT": {
					serverLoad("lt-a", "LT", 1, 100, 100, time.Second, now),
					serverLoad("lt-b", "LT", 50, 100, 100, time.Second, now),
				},
				"DE": {serverLoad("de-a", "DE", 5, 100, 100, time.Second, now)},
			},
			sticky: noSticky,
			want:   []string{"lt-a", "de-a"},
		},
		{
			name:    "пустой пул — пустой результат",
			items:   []PoolItem{},
			servers: map[string][]ServerLoad{},
			sticky:  noSticky,
			want:    []string{},
		},
		{
			name:  "выключенный пункт не выдаётся",
			items: []PoolItem{{ID: 1, ItemKey: "lt-ws", ServerKey: "lt-1", CountryCode: "LT", Enabled: false}},
			servers: map[string][]ServerLoad{
				"LT": {serverLoad("lt-1", "LT", 1, 100, 100, time.Second, now)},
			},
			sticky: noSticky,
			want:   []string{},
		},
		{
			name:    "нет метрик ноды — пункт пропускается",
			items:   []PoolItem{poolItem(1, "lt-ws", "unknown-node", "LT", 10)},
			servers: map[string][]ServerLoad{},
			sticky:  noSticky,
			want:    []string{},
		},
		{
			name: "равные веса и загрузка: стабильный выбор по sort_order",
			items: []PoolItem{
				poolItem(2, "lt-b", "lt-b", "LT", 20),
				poolItem(1, "lt-a", "lt-a", "LT", 10),
			},
			servers: map[string][]ServerLoad{
				"LT": {
					serverLoad("lt-a", "LT", 10, 100, 100, time.Second, now),
					serverLoad("lt-b", "LT", 10, 100, 100, time.Second, now),
				},
			},
			sticky: noSticky,
			want:   []string{"lt-a"}, // меньший sort_order
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAllocator(ttl, tt.softOverflow)
			got := itemKeys(a.Allocate(tt.items, tt.servers, tt.sticky, now))
			if !equalStrings(got, tt.want) {
				t.Errorf("Allocate() = %v, ожидалось %v", got, tt.want)
			}
		})
	}
}

// Детерминированность критична: при одинаковом входе пользователь должен
// получать одну и ту же ноду, иначе он будет скакать между серверами.
func TestAllocateIsDeterministic(t *testing.T) {
	now := time.Now().UTC()
	items := []PoolItem{
		poolItem(1, "lt-a", "lt-a", "LT", 10),
		poolItem(2, "lt-b", "lt-b", "LT", 10),
		poolItem(3, "lt-c", "lt-c", "LT", 10),
	}
	servers := map[string][]ServerLoad{
		"LT": {
			serverLoad("lt-a", "LT", 10, 100, 100, time.Second, now),
			serverLoad("lt-b", "LT", 10, 100, 100, time.Second, now),
			serverLoad("lt-c", "LT", 10, 100, 100, time.Second, now),
		},
	}

	a := NewAllocator(90*time.Second, true)
	first := itemKeys(a.Allocate(items, servers, noSticky, now))
	for i := 0; i < 50; i++ {
		got := itemKeys(a.Allocate(items, servers, noSticky, now))
		if !equalStrings(got, first) {
			t.Fatalf("прогон %d дал %v, первый прогон %v — выбор недетерминирован", i, got, first)
		}
	}
}

func TestNewAllocatorDefaultsTTL(t *testing.T) {
	a := NewAllocator(0, false)
	if a.heartbeatTTL != 90*time.Second {
		t.Errorf("при нулевом TTL ожидалось 90s, получено %v", a.heartbeatTTL)
	}
	a = NewAllocator(-5*time.Second, false)
	if a.heartbeatTTL != 90*time.Second {
		t.Errorf("при отрицательном TTL ожидалось 90s, получено %v", a.heartbeatTTL)
	}
}

func TestSortPoolItems(t *testing.T) {
	in := []PoolItem{
		poolItem(3, "c", "s3", "LT", 30),
		poolItem(1, "a", "s1", "LT", 10),
		poolItem(2, "b", "s2", "LT", 20),
	}
	got := itemKeys((&Allocator{}).SortPoolItems(in))
	want := []string{"a", "b", "c"}
	if !equalStrings(got, want) {
		t.Errorf("SortPoolItems() = %v, ожидалось %v", got, want)
	}
	// исходный срез не должен мутировать
	if in[0].ItemKey != "c" {
		t.Error("SortPoolItems изменил исходный срез")
	}
}
