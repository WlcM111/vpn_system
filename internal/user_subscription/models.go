package user_subscription

import (
	"errors"
	"time"
)

type SubscriptionStatus string

const (
	StatusNone    SubscriptionStatus = "none"
	StatusTrial   SubscriptionStatus = "trial"
	StatusActive  SubscriptionStatus = "active"
	StatusGrace   SubscriptionStatus = "grace"
	StatusExpired SubscriptionStatus = "expired"
)

const TrialPlanCode = "trial_3d"

var (
	ErrNoActiveSubscription = errors.New("no active subscription")
)

type SubscriptionState struct {
	TelegramID        int64
	Status            SubscriptionStatus
	CurrentPlanCode   string
	TrialUsed         bool
	StartedAt         *time.Time
	ExpiresAt         *time.Time
	GraceUntil        *time.Time
	CanceledAt        *time.Time
	LastPaymentID     string
	CountryCode       string
	AutoRenewEnabled  bool
	CancelAtPeriodEnd bool
	AccessRev         int64
	DaysLeft          int
}

type SubscriptionLink struct {
	Kind      string
	Title     string
	URL       string
	ExpiresAt *time.Time
}
