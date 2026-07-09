package tg_bot_gateway

import (
	"context"
	"sync"
)

type Step string

const (
	StepMainMenu                  Step = "main_menu"
	StepBuyMenu                   Step = "buy_menu"
	StepBindingCardPending        Step = "binding_card_pending"
	StepWaitingSubscription       Step = "waiting_subscription_status"
	StepTrialRequestPending       Step = "trial_request_pending"
	StepCancelSubscriptionPending Step = "cancel_subscription_pending"
	StepConfigRequestPending      Step = "config_request_pending"
	StepServicesMenu              Step = "services_menu"
	StepReferralMenu              Step = "referral_menu"
)

type ChatState struct {
	Step         Step `json:"step"`
	WelcomeShown bool `json:"welcome_shown"`
}

type StateStore interface {
	Get(ctx context.Context, telegramID int64) (*ChatState, error)
	Set(ctx context.Context, telegramID int64, state *ChatState) error
	Clear(ctx context.Context, telegramID int64) error
}

type memoryStateStore struct {
	mu    sync.RWMutex
	store map[int64]*ChatState
}

func NewMemoryStateStore() StateStore {
	return &memoryStateStore{
		store: make(map[int64]*ChatState),
	}
}

func (m *memoryStateStore) Get(_ context.Context, telegramID int64) (*ChatState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if s, ok := m.store[telegramID]; ok {
		cp := *s
		return &cp, nil
	}

	return &ChatState{
		Step:         StepMainMenu,
		WelcomeShown: false,
	}, nil
}

func (m *memoryStateStore) Set(_ context.Context, telegramID int64, state *ChatState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *state
	m.store[telegramID] = &cp
	return nil
}

func (m *memoryStateStore) Clear(_ context.Context, telegramID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, telegramID)
	return nil
}
