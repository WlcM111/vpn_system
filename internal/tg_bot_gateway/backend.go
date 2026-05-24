package tg_bot_gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
)

type SubscriptionStatus string

const (
	StatusNone    SubscriptionStatus = "none"
	StatusTrial   SubscriptionStatus = "trial"
	StatusActive  SubscriptionStatus = "active"
	StatusExpired SubscriptionStatus = "expired"
)

type SubscriptionInfo struct {
	Status      SubscriptionStatus
	CountryCode string
	TrialUntil  *time.Time
	ActiveUntil *time.Time
	TrialUsed   bool
	DaysLeft    int
	CardBound   bool
}

type SubscriptionLink struct {
	Kind      string
	Title     string
	URL       string
	ExpiresAt *time.Time
}

var ErrNoActiveSubscription = errors.New("no active subscription or trial")

type Backend interface {
	BindCard(ctx context.Context, telegramID int64) (string, error)
	UnbindCard(ctx context.Context, telegramID int64) error

	GetSubscriptionStatus(ctx context.Context, telegramID int64) (*SubscriptionInfo, error)
	StartTrial(ctx context.Context, telegramID int64) (*SubscriptionInfo, error)
	CancelSubscription(ctx context.Context, telegramID int64) (*SubscriptionInfo, error)

	GetSubscriptionLinks(ctx context.Context, telegramID int64) ([]SubscriptionLink, error)

	ApplyPaidSubscription(ctx context.Context, telegramID int64, durationDays int) (*SubscriptionInfo, error)
	MarkCardBound(ctx context.Context, telegramID int64) error
	MarkCardUnbound(ctx context.Context, telegramID int64) error
}

type localBackend struct {
	mu       sync.Mutex
	users    map[int64]*localUser
	config   localConfig
	kafka    *commonkafka.Producer
	trialDur time.Duration
}

type localUser struct {
	TelegramID  int64
	CardBound   bool
	TrialUsed   bool
	Status      SubscriptionStatus
	TrialUntil  *time.Time
	ActiveUntil *time.Time
	CountryCode string

	SubToken          string
	SubTokenExpiresAt *time.Time
}

type localConfig struct {
	DirectSubscriptionBaseURL string
	AltSubscriptionBaseURL    string
	DefaultCountry            string
}

func NewMockBackend(kafkaProd *commonkafka.Producer, directBaseURL string) Backend {
	directBaseURL = normalizeBaseURL(directBaseURL, "https://race-src.com/sub/")
	altBaseURL := normalizeBaseURL(
		strings.TrimSpace(strings.ReplaceAll(directBaseURL, "/sub/", "/sub-alt/")),
		"",
	)

	return &localBackend{
		users: make(map[int64]*localUser),
		config: localConfig{
			DirectSubscriptionBaseURL: directBaseURL,
			AltSubscriptionBaseURL:    altBaseURL,
			DefaultCountry:            "LT",
		},
		kafka:    kafkaProd,
		trialDur: 3 * 24 * time.Hour,
	}
}

func normalizeBaseURL(v, fallback string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		s = fallback
	}
	if s == "" {
		return ""
	}
	if !strings.HasSuffix(s, "/") {
		s += "/"
	}
	return s
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}

func (b *localBackend) ensureUserLocked(id int64) *localUser {
	u, ok := b.users[id]
	if !ok {
		u = &localUser{
			TelegramID:  id,
			Status:      StatusNone,
			CountryCode: b.config.DefaultCountry,
		}
		b.users[id] = u
	}
	return u
}

func (b *localBackend) snapshotInfoLocked(u *localUser) *SubscriptionInfo {
	now := time.Now()
	status := u.Status
	daysLeft := 0

	switch u.Status {
	case StatusTrial:
		if u.TrialUntil != nil {
			if now.After(*u.TrialUntil) {
				status = StatusExpired
			} else {
				daysLeft = int(u.TrialUntil.Sub(now).Hours()/24) + 1
			}
		}
	case StatusActive:
		if u.ActiveUntil != nil {
			if now.After(*u.ActiveUntil) {
				status = StatusExpired
			} else {
				daysLeft = int(u.ActiveUntil.Sub(now).Hours()/24) + 1
			}
		}
	}

	return &SubscriptionInfo{
		Status:      status,
		CountryCode: u.CountryCode,
		TrialUntil:  cloneTime(u.TrialUntil),
		ActiveUntil: cloneTime(u.ActiveUntil),
		TrialUsed:   u.TrialUsed,
		DaysLeft:    daysLeft,
		CardBound:   u.CardBound,
	}
}

func (b *localBackend) BindCard(_ context.Context, telegramID int64) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	u.CardBound = true

	return fmt.Sprintf("https://pay.example.com/mock-bind?user=%d", telegramID), nil
}

func (b *localBackend) UnbindCard(_ context.Context, telegramID int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	u.CardBound = false
	return nil
}

func (b *localBackend) GetSubscriptionStatus(_ context.Context, telegramID int64) (*SubscriptionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	return b.snapshotInfoLocked(u), nil
}

func (b *localBackend) StartTrial(_ context.Context, telegramID int64) (*SubscriptionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	if u.TrialUsed {
		return b.snapshotInfoLocked(u), nil
	}

	now := time.Now()
	trialUntil := now.Add(b.trialDur)

	u.TrialUsed = true
	u.Status = StatusTrial
	u.TrialUntil = &trialUntil
	u.ActiveUntil = nil

	if u.SubToken == "" {
		u.SubToken = fmt.Sprintf("trial-%d-%d", telegramID, now.Unix())
	}
	u.SubTokenExpiresAt = &trialUntil

	return b.snapshotInfoLocked(u), nil
}

func (b *localBackend) CancelSubscription(_ context.Context, telegramID int64) (*SubscriptionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	if u.Status == StatusNone || u.Status == StatusExpired {
		return b.snapshotInfoLocked(u), nil
	}

	now := time.Now()
	u.Status = StatusExpired
	u.ActiveUntil = &now
	u.TrialUntil = nil
	u.SubTokenExpiresAt = &now

	return b.snapshotInfoLocked(u), nil
}

func (b *localBackend) ApplyPaidSubscription(_ context.Context, telegramID int64, durationDays int) (*SubscriptionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)

	start := time.Now()
	if u.ActiveUntil != nil && u.ActiveUntil.After(start) {
		start = *u.ActiveUntil
	}

	activeUntil := start.Add(time.Duration(durationDays) * 24 * time.Hour)
	u.Status = StatusActive
	u.ActiveUntil = &activeUntil
	u.TrialUntil = nil

	if u.SubToken == "" {
		u.SubToken = fmt.Sprintf("sub-%d-%d", telegramID, time.Now().Unix())
	}
	u.SubTokenExpiresAt = &activeUntil

	return b.snapshotInfoLocked(u), nil
}

func (b *localBackend) MarkCardBound(_ context.Context, telegramID int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	u.CardBound = true
	return nil
}

func (b *localBackend) MarkCardUnbound(_ context.Context, telegramID int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	u.CardBound = false
	return nil
}

func (b *localBackend) GetSubscriptionLinks(_ context.Context, telegramID int64) ([]SubscriptionLink, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u := b.ensureUserLocked(telegramID)
	now := time.Now()

	if u.Status != StatusTrial && u.Status != StatusActive {
		return nil, ErrNoActiveSubscription
	}

	if u.SubToken == "" {
		u.SubToken = fmt.Sprintf("sub-%d-%d", telegramID, now.Unix())
	}

	var exp *time.Time
	if u.Status == StatusTrial && u.TrialUntil != nil {
		exp = cloneTime(u.TrialUntil)
	} else if u.Status == StatusActive && u.ActiveUntil != nil {
		exp = cloneTime(u.ActiveUntil)
	} else {
		t := now.Add(30 * 24 * time.Hour)
		exp = &t
	}
	u.SubTokenExpiresAt = cloneTime(exp)

	links := make([]SubscriptionLink, 0, 2)

	if b.config.DirectSubscriptionBaseURL != "" {
		links = append(links, SubscriptionLink{
			Kind:      "direct",
			Title:     "Основной зарубежный маршрут",
			URL:       b.config.DirectSubscriptionBaseURL + u.SubToken,
			ExpiresAt: cloneTime(exp),
		})
	}

	if b.config.AltSubscriptionBaseURL != "" && b.config.AltSubscriptionBaseURL != b.config.DirectSubscriptionBaseURL {
		links = append(links, SubscriptionLink{
			Kind:      "alt",
			Title:     "Альтернативный зарубежный маршрут",
			URL:       b.config.AltSubscriptionBaseURL + u.SubToken,
			ExpiresAt: cloneTime(exp),
		})
	}

	if len(links) == 0 {
		return nil, fmt.Errorf("no subscription base urls configured")
	}

	return links, nil
}
