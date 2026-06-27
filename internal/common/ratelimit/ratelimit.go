package ratelimit

import (
	"sync"
	"time"
)

// ============================================================================
// Простой in-memory rate limiter со скользящим окном фиксированной длины.
// Потокобезопасен. Используется как fallback, когда распределённый лимитер
// (Redis) недоступен — чтобы не оставаться полностью без защиты (fail-open).
//
// Без внешних зависимостей. Память самоочищается: устаревшие ключи удаляются
// фоновой уборкой.
// ============================================================================

type counter struct {
	count       int
	windowStart time.Time
}

// Limiter — потокобезопасный счётчик запросов по ключу за окно.
type Limiter struct {
	mu     sync.Mutex
	counts map[string]*counter
	window time.Duration
}

// New создаёт лимитер с заданным окном и запускает фоновую уборку.
func New(window time.Duration) *Limiter {
	if window <= 0 {
		window = time.Minute
	}
	l := &Limiter{
		counts: make(map[string]*counter),
		window: window,
	}
	go l.cleanupLoop()
	return l
}

// Allow увеличивает счётчик ключа и сообщает, не превышен ли лимит за окно.
func (l *Limiter) Allow(key string, limit int) bool {
	if limit <= 0 {
		return true
	}
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	c, ok := l.counts[key]
	if !ok || now.Sub(c.windowStart) >= l.window {
		l.counts[key] = &counter{count: 1, windowStart: now}
		return true
	}
	c.count++
	return c.count <= limit
}

// cleanupLoop периодически удаляет устаревшие ключи, чтобы map не рос бесконечно.
func (l *Limiter) cleanupLoop() {
	ticker := time.NewTicker(l.window)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		l.mu.Lock()
		for k, c := range l.counts {
			if now.Sub(c.windowStart) >= l.window {
				delete(l.counts, k)
			}
		}
		l.mu.Unlock()
	}
}
