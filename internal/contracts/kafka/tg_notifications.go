package kafka

type TgKeyboard string

const (
	TgKeyboardMainMenu             TgKeyboard = "main_menu"
	TgKeyboardBuyMenu              TgKeyboard = "buy_menu"
	TgKeyboardTrialOrBuy           TgKeyboard = "trial_or_buy"
	TgKeyboardMySubscriptionConfig TgKeyboard = "my_subscription_with_config"
	TgKeyboardMainMenuWithBack     TgKeyboard = "main_menu_with_back"
	// TgKeyboardPaymentMethod — выбор способа оплаты для уже выбранного
	// тарифа («Способ 1 - рекомендуемый» / «Способ 2»). Billing просит
	// показать его, когда счёт выбранным способом создать не удалось.
	TgKeyboardPaymentMethod TgKeyboard = "payment_method"
)

type TgNotification struct {
	TelegramID int64      `json:"telegram_id"`
	Message    string     `json:"message"`
	ParseMode  string     `json:"parse_mode,omitempty"`
	Keyboard   TgKeyboard `json:"keyboard,omitempty"`
	// PayURL — если задан, под сообщением показывается inline-кнопка «Оплатить»
	// с этой ссылкой (ссылка на оплату Platega или YooKassa). Не сочетается
	// с Keyboard в одном сообщении (Telegram-ограничение), поэтому при PayURL
	// reply-клавиатура игнорируется.
	PayURL string `json:"pay_url,omitempty"`
}
