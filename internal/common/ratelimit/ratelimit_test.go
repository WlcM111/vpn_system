package ratelimit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// Лимитер запросов.
//
// Используется как запасной механизм защиты подписки, когда распределённый
// лимитер на Redis недоступен. Ошибка здесь означает либо открытую дверь для
// перебора токенов, либо отказ легитимным пользователям.
// ============================================================================

func TestAllowRespectsLimit(t *testing.T) {
	l := New(time.Minute)
	defer l.Close()

	const limit = 3
	for i := 1; i <= limit; i++ {
		if !l.Allow("key", limit) {
			t.Errorf("запрос %d из %d отклонён, хотя лимит не исчерпан", i, limit)
		}
	}
	if l.Allow("key", limit) {
		t.Error("запрос сверх лимита должен быть отклонён")
	}
}

func TestAllowIsolatesKeys(t *testing.T) {
	l := New(time.Minute)
	defer l.Close()

	// Исчерпываем лимит одного ключа.
	l.Allow("first", 1)
	if l.Allow("first", 1) {
		t.Error("первый ключ должен быть исчерпан")
	}

	// Второй ключ это затрагивать не должно.
	if !l.Allow("second", 1) {
		t.Error("лимит одного ключа не должен влиять на другой")
	}
}

func TestAllowResetsAfterWindow(t *testing.T) {
	l := New(50 * time.Millisecond)
	defer l.Close()

	if !l.Allow("key", 1) {
		t.Fatal("первый запрос должен пройти")
	}
	if l.Allow("key", 1) {
		t.Fatal("второй запрос в том же окне должен быть отклонён")
	}

	time.Sleep(80 * time.Millisecond)

	if !l.Allow("key", 1) {
		t.Error("после смены окна счётчик должен обнулиться")
	}
}

func TestAllowWithNonPositiveLimit(t *testing.T) {
	l := New(time.Minute)
	defer l.Close()

	// Нулевой лимит трактуется как «без ограничения»: иначе неверно
	// заданная конфигурация закрыла бы сервис полностью.
	for i := 0; i < 100; i++ {
		if !l.Allow("key", 0) {
			t.Fatal("при лимите 0 ограничения быть не должно")
		}
	}
	if !l.Allow("key", -1) {
		t.Error("отрицательный лимит должен трактоваться как отсутствие лимита")
	}
}

// Запускать с -race: лимитер вызывается из обработчиков HTTP параллельно.
func TestAllowIsConcurrencySafe(t *testing.T) {
	l := New(time.Minute)
	defer l.Close()

	const (
		goroutines = 20
		perRoutine = 50
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			for i := 0; i < perRoutine; i++ {
				l.Allow(key, 10)
			}
		}(g)
	}
	wg.Wait()
}

// Общий ключ под нагрузкой: суммарное число разрешений не должно превышать
// лимит, иначе защита дырявая.
func TestAllowSharedKeyUnderConcurrency(t *testing.T) {
	l := New(time.Minute)
	defer l.Close()

	const (
		goroutines = 50
		limit      = 10
	)

	var (
		mu      sync.Mutex
		allowed int
		wg      sync.WaitGroup
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("shared", limit) {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed > limit {
		t.Errorf("разрешено %d запросов при лимите %d — защита не держит гонку", allowed, limit)
	}
	if allowed == 0 {
		t.Error("не разрешено ни одного запроса — лимитер блокирует всё")
	}
}

func TestNewNormalizesWindow(t *testing.T) {
	for _, window := range []time.Duration{0, -time.Second} {
		l := New(window)
		if l.window != time.Minute {
			t.Errorf("при окне %v ожидалась минута, получено %v", window, l.window)
		}
		l.Close()
	}
}

// Close должен быть безопасен при повторном вызове: иначе двойное закрытие
// канала уронит процесс.
func TestCloseIsIdempotent(t *testing.T) {
	l := New(time.Minute)
	l.Close()
	l.Close()
	l.Close()
}
