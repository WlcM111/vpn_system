package kafka

import (
	"encoding/json"
	"strings"
	"testing"
)

// Команда старого бота (без payment_provider) читается новым billing как
// YooKassa; новое поле не ломает старых читателей (неизвестное поле JSON
// игнорируется) и не попадает в JSON, когда пусто.
func TestCreateCheckoutCommandCompatibility(t *testing.T) {
	old := `{"type":"billing.create_subscription_checkout","command_id":"c1","telegram_id":1,"plan_code":"monthly_30d","save_payment_method":false,"return_url":"https://t.me/vpn_house_bot"}`
	var cmd CreateSubscriptionCheckoutCommand
	if err := json.Unmarshal([]byte(old), &cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.PaymentProvider != "" || cmd.PlanCode != PlanCodeMonthly {
		t.Fatalf("old command decoded as %+v", cmd)
	}
	cmd.PaymentProvider = BillingPaymentProviderPlatega
	raw, _ := json.Marshal(cmd)
	if !strings.Contains(string(raw), `"payment_provider":"platega"`) {
		t.Fatalf("encoded %s", raw)
	}
	var legacy struct {
		Type     string `json:"type"`
		PlanCode string `json:"plan_code"`
	}
	if err := json.Unmarshal(raw, &legacy); err != nil || legacy.PlanCode != "monthly_30d" {
		t.Fatalf("old reader: %+v %v", legacy, err)
	}
	cmd.PaymentProvider = ""
	raw, _ = json.Marshal(cmd)
	if strings.Contains(string(raw), "payment_provider") {
		t.Fatalf("empty provider must be omitted: %s", raw)
	}
}

func TestPaymentMethodKeyboardValue(t *testing.T) {
	raw, _ := json.Marshal(TgNotification{TelegramID: 1, Message: "x", Keyboard: TgKeyboardPaymentMethod})
	if !strings.Contains(string(raw), `"keyboard":"payment_method"`) {
		t.Fatalf("encoded %s", raw)
	}
}
