package kafka

type SubscriptionCommandType string

const (
	SubscriptionCommandStartTrial        SubscriptionCommandType = "subscription.start_trial"
	SubscriptionCommandGetStatus         SubscriptionCommandType = "subscription.get_status"
	SubscriptionCommandGetLinks          SubscriptionCommandType = "subscription.get_links"
	SubscriptionCommandCancel            SubscriptionCommandType = "subscription.cancel"
	SubscriptionCommandReferralAttribute SubscriptionCommandType = "subscription.referral_attribute"
	SubscriptionCommandReferralRedeem    SubscriptionCommandType = "subscription.referral_redeem"
	SubscriptionCommandRotateToken       SubscriptionCommandType = "subscription.rotate_token"
)

// RotateTokenCommand — запрос на перевыпуск ссылки подписки.
// Старая ссылка после обработки перестаёт работать.
type RotateTokenCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
	Reason     string                  `json:"reason,omitempty"`
}

type StartTrialCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}

type GetStatusCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}

type GetLinksCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
	// ClientGroup — какой клиент выбрал пользователь в боте: "happ" (Happ/Incy)
	// или "xray" (v2RayTun/Streisand). Влияет на текст инструкции и на подсказку
	// формата роутинга в ссылке (?c=). Пустое значение = "happ" (рекомендуемый).
	ClientGroup string `json:"client_group,omitempty"`
}

type CancelSubscriptionCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}

// ReferralAttributeCommand — приглашённый (RefereeTelegramID) перешёл по коду
// реферера (ReferrerCode). Атрибуция фиксируется как pending; конверсия — при оплате.
type ReferralAttributeCommand struct {
	Type              SubscriptionCommandType `json:"type"`
	CommandID         string                  `json:"command_id"`
	RefereeTelegramID int64                   `json:"referee_telegram_id"`
	ReferrerCode      string                  `json:"referrer_code"`
}

// ReferralRedeemCommand — пользователь запросил начисление доступных бесплатных месяцев.
type ReferralRedeemCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}
