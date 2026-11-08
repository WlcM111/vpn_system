package user_subscription

import (
	"errors"
	"strings"
	"testing"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

func validBonusCommand() *kafkacontracts.GrantBonusCommand {
	return &kafkacontracts.GrantBonusCommand{
		Type:       kafkacontracts.SubscriptionCommandGrantBonus,
		CommandID:  "bonus:apology-2026-11:1",
		TelegramID: 1,
		Days:       30,
		Campaign:   "apology-2026-11",
		Message:    "🎁 Доступ до {until}",
	}
}

func TestValidateGrantBonusCommand(t *testing.T) {
	if err := ValidateGrantBonusCommand(validBonusCommand()); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}
	cases := map[string]func(c *kafkacontracts.GrantBonusCommand){
		"нулевой telegram_id":  func(c *kafkacontracts.GrantBonusCommand) { c.TelegramID = 0 },
		"0 дней":               func(c *kafkacontracts.GrantBonusCommand) { c.Days = 0 },
		"больше года":          func(c *kafkacontracts.GrantBonusCommand) { c.Days = 367 },
		"пустая кампания":      func(c *kafkacontracts.GrantBonusCommand) { c.Campaign = "" },
		"кампания с пробелом":  func(c *kafkacontracts.GrantBonusCommand) { c.Campaign = "apology 2026" },
		"кампания заглавными":  func(c *kafkacontracts.GrantBonusCommand) { c.Campaign = "Apology" },
		"слишком длинный ключ": func(c *kafkacontracts.GrantBonusCommand) { c.Campaign = strings.Repeat("a", 65) },
		"пустое сообщение":     func(c *kafkacontracts.GrantBonusCommand) { c.Message = "  \n " },
		"длинное сообщение":    func(c *kafkacontracts.GrantBonusCommand) { c.Message = strings.Repeat("я", 3501) },
		"битый UTF-8":          func(c *kafkacontracts.GrantBonusCommand) { c.Message = "\xff\xfe" },
		"неизвестная разметка": func(c *kafkacontracts.GrantBonusCommand) { c.ParseMode = "XML" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := validBonusCommand()
			mutate(c)
			if err := ValidateGrantBonusCommand(c); !errors.Is(err, ErrInvalidBonusCommand) {
				t.Fatalf("err = %v, want ErrInvalidBonusCommand", err)
			}
		})
	}
	if err := ValidateGrantBonusCommand(nil); !errors.Is(err, ErrInvalidBonusCommand) {
		t.Fatalf("nil command: %v", err)
	}
}

func TestRenderBonusMessage(t *testing.T) {
	until := time.Date(2026, 12, 5, 21, 30, 0, 0, time.UTC)
	got := renderBonusMessage("Дарим {days}. Доступ до {until}. Снова: {until}", until, 30)
	want := "Дарим 30 дней. Доступ до 05.12.2026. Снова: 05.12.2026"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if renderBonusMessage("без подстановок 🙏", until, 1) != "без подстановок 🙏" {
		t.Fatal("text without placeholders must stay unchanged")
	}
}
