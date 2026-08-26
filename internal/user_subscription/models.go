package user_subscription

import (
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

// TrialGrantOutcome — почему выдача триала завершилась именно так.
// Одно подтверждённое значение доменной операции, из которого строятся ВСЕ
// ветки сообщений: повторное чтение состояния между начислением и ответом
// запрещено, потому что оно может увидеть уже другое состояние.
type TrialGrantOutcome string

const (
	// Триал выдан впервые, платной подписки не было.
	TrialGranted TrialGrantOutcome = "granted"
	// Триал выдан впервые и добавлен К ДЕЙСТВУЮЩЕЙ платной подписке.
	TrialGrantedOnTopOfPaid TrialGrantOutcome = "granted_on_paid"
	// Триал уже выдавался когда-либо: повторно не выдаётся никогда.
	TrialAlreadyUsed TrialGrantOutcome = "already_used"
	// Подписка в льготном периоде: состояние принадлежит платёжному контуру,
	// начисление отложено до его разрешения.
	TrialDeferredGrace TrialGrantOutcome = "deferred_grace"
)

// TrialGrantResult — единственный результат доменной операции «выдать триал».
type TrialGrantResult struct {
	State     *SubscriptionState
	Outcome   TrialGrantOutcome
	TrialDays int
	// PaidActiveBefore — была ли на момент выдачи действующая платная подписка.
	// Нужен для текста: «добавлены к подписке» против «активирован».
	PaidActiveBefore bool
}

// Granted сообщает, было ли начисление в этом вызове.
func (r TrialGrantResult) Granted() bool {
	return r.Outcome == TrialGranted || r.Outcome == TrialGrantedOnTopOfPaid
}

type SubscriptionLink struct {
	Kind      string
	Title     string
	URL       string
	ExpiresAt *time.Time
}
