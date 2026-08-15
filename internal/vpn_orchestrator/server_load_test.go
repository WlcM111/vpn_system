package vpn_orchestrator

import (
	"testing"
	"time"
)

func TestServerLoadAliveByHeartbeat(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	const ttl = 90 * time.Second
	fresh := now.Add(-10 * time.Second)
	stale := now.Add(-5 * time.Minute)
	exactly := now.Add(-ttl)

	tests := []struct {
		name string
		load ServerLoad
		want bool
	}{
		{"свежий heartbeat", ServerLoad{Enabled: true, LastHeartbeatAt: &fresh}, true},
		{"протухший heartbeat", ServerLoad{Enabled: true, LastHeartbeatAt: &stale}, false},
		{"ровно на границе TTL — ещё жива", ServerLoad{Enabled: true, LastHeartbeatAt: &exactly}, true},
		{"выключена — мертва независимо от heartbeat", ServerLoad{Enabled: false, LastHeartbeatAt: &fresh}, false},
		// Нода заведена, но ещё не рапортовала: считаем живой, чтобы её можно
		// было ввести в работу до первого heartbeat.
		{"heartbeat ещё не приходил", ServerLoad{Enabled: true, LastHeartbeatAt: nil}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.load.AliveByHeartbeat(now, ttl); got != tt.want {
				t.Errorf("AliveByHeartbeat() = %v, ожидалось %v", got, tt.want)
			}
		})
	}
}

func TestServerLoadHasCapacity(t *testing.T) {
	tests := []struct {
		name string
		load ServerLoad
		want bool
	}{
		{"есть место", ServerLoad{MaxUsers: 100, ActiveUsers: 50}, true},
		{"ровно заполнена", ServerLoad{MaxUsers: 100, ActiveUsers: 100}, false},
		{"переполнена", ServerLoad{MaxUsers: 100, ActiveUsers: 150}, false},
		{"лимит 0 — безлимит", ServerLoad{MaxUsers: 0, ActiveUsers: 9999}, true},
		{"отрицательный лимит — безлимит", ServerLoad{MaxUsers: -1, ActiveUsers: 9999}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.load.HasCapacity(); got != tt.want {
				t.Errorf("HasCapacity() = %v, ожидалось %v", got, tt.want)
			}
		})
	}
}

func TestServerLoadScore(t *testing.T) {
	tests := []struct {
		name string
		load ServerLoad
		want float64
	}{
		{"обычный расчёт", ServerLoad{ActiveUsers: 9, Weight: 10}, 1.0},
		{"пустая нода", ServerLoad{ActiveUsers: 0, Weight: 100}, 0.01},
		// Деление на ноль недопустимо: вес 0 подменяется единицей.
		{"нулевой вес не делит на ноль", ServerLoad{ActiveUsers: 4, Weight: 0}, 5.0},
		{"отрицательный вес не делит на ноль", ServerLoad{ActiveUsers: 4, Weight: -10}, 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.load.LoadScore()
			if got != tt.want {
				t.Errorf("LoadScore() = %v, ожидалось %v", got, tt.want)
			}
		})
	}
}
