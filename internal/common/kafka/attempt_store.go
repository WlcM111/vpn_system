package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AttemptStore — хранилище счётчиков попыток обработки сообщений. Inc увеличивает
// счётчик по ключу и возвращает новое значение; Reset удаляет ключ (сообщение
// обработано/ушло в DLT). Реализации: in-memory (по умолчанию) и БД (переживает
// рестарты — нужно для гарантированного попадания отравленных сообщений в DLT).
type AttemptStore interface {
	Inc(ctx context.Context, key string) int
	Reset(ctx context.Context, key string)
}

// attemptStore — текущая реализация. По умолчанию in-memory; main каждого сервиса
// заменяет её на БД через SetAttemptStore.
var attemptStore AttemptStore = newMemoryAttemptStore()

// SetAttemptStore задаёт хранилище попыток (вызывается из main после инициализации пула).
func SetAttemptStore(s AttemptStore) {
	if s != nil {
		attemptStore = s
	}
}

// --- in-memory реализация (fallback) ---

type memoryAttemptStore struct {
	mu sync.Mutex
	m  map[string]int
}

func newMemoryAttemptStore() *memoryAttemptStore {
	return &memoryAttemptStore{m: make(map[string]int)}
}

func (s *memoryAttemptStore) Inc(_ context.Context, key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key]++
	return s.m[key]
}

func (s *memoryAttemptStore) Reset(_ context.Context, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

// --- БД-реализация (персистентная) ---

type dbAttemptStore struct {
	pool *pgxpool.Pool
}

// NewDBAttemptStore создаёт персистентное хранилище попыток поверх Postgres.
func NewDBAttemptStore(pool *pgxpool.Pool) AttemptStore {
	return &dbAttemptStore{pool: pool}
}

func (s *dbAttemptStore) Inc(ctx context.Context, key string) int {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var n int
	err := s.pool.QueryRow(cctx, `
		INSERT INTO consumer_attempts (attempt_key, attempts)
		VALUES ($1, 1)
		ON CONFLICT (attempt_key)
		DO UPDATE SET attempts = consumer_attempts.attempts + 1, updated_at = now()
		RETURNING attempts
	`, key).Scan(&n)
	if err != nil {
		// При сбое БД не блокируем обработку: возвращаем 1, как будто первая попытка.
		// Это деградирует до поведения "не копить", но не роняет консьюмер.
		return 1
	}
	return n
}

func (s *dbAttemptStore) Reset(ctx context.Context, key string) {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _ = s.pool.Exec(cctx, `DELETE FROM consumer_attempts WHERE attempt_key = $1`, key)
}
