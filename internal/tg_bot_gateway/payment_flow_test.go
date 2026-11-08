package tg_bot_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ============================================================================
// Сценарии оплаты в боте на настоящей библиотеке telegram-bot-api: запросы
// к Telegram Bot API перехватывает httptest-сервер, команды в billing —
// фейковый публикатор. Kafka и сеть не нужны.
// ============================================================================

type sentRequest struct {
	Method string
	Params map[string]string
}

type fakeTelegram struct {
	mu   sync.Mutex
	reqs []sentRequest
	srv  *httptest.Server
}

func newFakeTelegram(t *testing.T) *fakeTelegram {
	ft := &fakeTelegram{}
	ft.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		method := parts[len(parts)-1]
		_ = r.ParseMultipartForm(1 << 20)
		params := map[string]string{}
		for k, v := range r.Form {
			params[k] = v[0]
		}
		ft.mu.Lock()
		ft.reqs = append(ft.reqs, sentRequest{Method: method, Params: params})
		ft.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch method {
		case "getMe":
			_, _ = fmt.Fprint(w, `{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"test","username":"test_bot"}}`)
		case "answerCallbackQuery":
			_, _ = fmt.Fprint(w, `{"ok":true,"result":true}`)
		default:
			_, _ = fmt.Fprint(w, `{"ok":true,"result":{"message_id":1,"date":0,"chat":{"id":1,"type":"private"}}}`)
		}
	}))
	t.Cleanup(ft.srv.Close)
	return ft
}

func (ft *fakeTelegram) sent(method string) []sentRequest {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	var out []sentRequest
	for _, r := range ft.reqs {
		if r.Method == method {
			out = append(out, r)
		}
	}
	return out
}

func (ft *fakeTelegram) last(t *testing.T) sentRequest {
	t.Helper()
	msgs := ft.sent("sendMessage")
	if len(msgs) == 0 {
		t.Fatal("no messages sent")
	}
	return msgs[len(msgs)-1]
}

type published struct {
	Topic, Key string
	Cmd        kafkacontracts.CreateSubscriptionCheckoutCommand
}

type fakePublisher struct {
	mu   sync.Mutex
	msgs []published
	err  error
}

func (f *fakePublisher) PublishJSON(_ context.Context, topic, key string, v any) error {
	if f.err != nil {
		return f.err
	}
	raw, _ := json.Marshal(v)
	var cmd kafkacontracts.CreateSubscriptionCheckoutCommand
	_ = json.Unmarshal(raw, &cmd)
	f.mu.Lock()
	f.msgs = append(f.msgs, published{Topic: topic, Key: key, Cmd: cmd})
	f.mu.Unlock()
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

const testChat = int64(5512345678)

func newTestApp(t *testing.T) (*App, *fakeTelegram, *fakePublisher) {
	t.Helper()
	t.Setenv("PLAN_MONTHLY_PRICE_RUB", "89")
	t.Setenv("PLAN_QUARTERLY_PRICE_RUB", "249")
	t.Setenv("PLAN_SEMIANNUAL_PRICE_RUB", "439")
	t.Setenv("PLAN_ANNUAL_PRICE_RUB", "799")
	t.Setenv("PLATEGA_MERCHANT_ID", "1a021d91-9b26-4762-b303-5d4aac74e921")
	t.Setenv("PLATEGA_ENABLED", "")
	ft := newFakeTelegram(t)
	bot, err := tgbotapi.NewBotAPIWithAPIEndpoint("123:TEST", ft.srv.URL+"/bot%s/%s")
	if err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	app := &App{
		bot:        bot,
		stateStore: NewMemoryStateStore(),
		// Выключенный продюсер: kafkaEnabled() = true, а любая случайная
		// публикация мимо billingPublisher вернёт ошибку.
		kafkaProducer:    commonkafka.NewProducer(nil),
		billingPublisher: pub,
	}
	return app, ft, pub
}

func privateText(text string) tgbotapi.Update {
	return tgbotapi.Update{Message: &tgbotapi.Message{
		MessageID: 1,
		From:      &tgbotapi.User{ID: testChat},
		Chat:      &tgbotapi.Chat{ID: testChat, Type: "private"},
		Text:      text,
	}}
}

func keyboardButtons(t *testing.T, r sentRequest) [][]string {
	t.Helper()
	raw := r.Params["reply_markup"]
	if raw == "" {
		return nil
	}
	var kb struct {
		Keyboard [][]struct {
			Text string `json:"text"`
		} `json:"keyboard"`
	}
	if err := json.Unmarshal([]byte(raw), &kb); err != nil {
		t.Fatalf("reply_markup: %v", err)
	}
	var out [][]string
	for _, row := range kb.Keyboard {
		var r []string
		for _, b := range row {
			r = append(r, b.Text)
		}
		out = append(out, r)
	}
	return out
}

func flatten(rows [][]string) []string {
	var out []string
	for _, r := range rows {
		out = append(out, r...)
	}
	return out
}

func send(app *App, text string) {
	app.handleUpdate(context.Background(), privateText(text))
}

// После выбора тарифа — ровно два способа оплаты в заданном порядке, счёт
// ещё не создаётся.
func TestTariffShowsExactlyTwoMethodsInOrder(t *testing.T) {
	app, ft, pub := newTestApp(t)
	send(app, btnBuySubscription)
	send(app, labelMonthly())
	last := ft.last(t)
	rows := keyboardButtons(t, last)
	want := [][]string{{"Способ 1 - рекомендуемый"}, {"Способ 2"}, {btnBack, btnMainMenu}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("keyboard = %v, want %v", rows, want)
	}
	if !strings.Contains(last.Params["text"], "Подписка 30 дней — 89 ₽") {
		t.Fatalf("text = %q", last.Params["text"])
	}
	if pub.count() != 0 {
		t.Fatal("invoice requested before a payment method was chosen")
	}
}

func TestMethodChoicePublishesCommandWithProvider(t *testing.T) {
	for _, tc := range []struct {
		button   string
		provider kafkacontracts.BillingPaymentProvider
	}{
		{btnPayMethodPlatega, kafkacontracts.BillingPaymentProviderPlatega},
		{btnPayMethodYooKassa, kafkacontracts.BillingPaymentProviderYooKassa},
	} {
		t.Run(tc.button, func(t *testing.T) {
			app, ft, pub := newTestApp(t)
			send(app, btnBuySubscription)
			send(app, labelQuarterly())
			send(app, tc.button)
			if pub.count() != 1 {
				t.Fatalf("published %d commands", pub.count())
			}
			m := pub.msgs[0]
			if m.Topic != commonkafka.TopicBillingCommands || m.Key != fmt.Sprint(testChat) {
				t.Fatalf("topic/key %s/%s", m.Topic, m.Key)
			}
			c := m.Cmd
			if c.Type != kafkacontracts.BillingCommandCreateSubscriptionCheckout || c.PlanCode != kafkacontracts.PlanCodeQuarterly ||
				c.PaymentProvider != tc.provider || c.SavePaymentMethod || c.TelegramID != testChat || c.CommandID == "" {
				t.Fatalf("command %+v", c)
			}
			if !strings.Contains(ft.last(t).Params["text"], tc.button) {
				t.Fatalf("reply %q", ft.last(t).Params["text"])
			}
		})
	}
}

// Когда переменные окружения Platega не заданы (или пусты), при выборе
// «Способ 1 - рекомендуемый» бот сразу сообщает о недоступности и рекомендует
// «Способ 2», не отправляя команду в Kafka. Когда переменные заданы — сообщение
// исчезает и создаётся счёт.
func TestPlategaUnavailableWhenEnvVarsMissing(t *testing.T) {
	app, ft, pub := newTestApp(t)
	t.Setenv("PLATEGA_MERCHANT_ID", "")

	send(app, btnBuySubscription)
	send(app, labelMonthly())
	send(app, btnPayMethodPlatega)

	if pub.count() != 0 {
		t.Fatalf("expected 0 published commands when Platega env vars are missing, got %d", pub.count())
	}
	reply := ft.last(t).Params["text"]
	if !strings.Contains(reply, "сейчас недоступен") || !strings.Contains(reply, "Способ 2") {
		t.Fatalf("unexpected reply when Platega env missing: %q", reply)
	}

	// После установки переменных окружения сообщение о недоступности исчезает
	t.Setenv("PLATEGA_MERCHANT_ID", "1a021d91-9b26-4762-b303-5d4aac74e921")
	send(app, btnPayMethodPlatega)

	if pub.count() != 1 {
		t.Fatalf("expected 1 published command after setting Platega env vars, got %d", pub.count())
	}
	if !strings.Contains(ft.last(t).Params["text"], "Готовлю ссылку на оплату") {
		t.Fatalf("unexpected reply after setting Platega env vars: %q", ft.last(t).Params["text"])
	}
}

// PLATEGA_ENABLED=false выключает «Способ 1» в боте даже при заданном ID
// мерчанта; API-ключ боту для проверки не нужен.
func TestPlategaDisabledBySwitch(t *testing.T) {
	app, ft, pub := newTestApp(t)
	t.Setenv("PLATEGA_ENABLED", "false")

	send(app, btnBuySubscription)
	send(app, labelMonthly())
	send(app, btnPayMethodPlatega)
	if pub.count() != 0 {
		t.Fatalf("expected 0 published commands with PLATEGA_ENABLED=false, got %d", pub.count())
	}
	if reply := ft.last(t).Params["text"]; !strings.Contains(reply, "сейчас недоступен") {
		t.Fatalf("unexpected reply with PLATEGA_ENABLED=false: %q", reply)
	}

	t.Setenv("PLATEGA_ENABLED", "")
	send(app, btnPayMethodPlatega)
	if pub.count() != 1 {
		t.Fatalf("expected 1 published command after re-enabling, got %d", pub.count())
	}
}

// Повторные нажатия подряд не порождают новые команды.
func TestRepeatedMethodTapIsDebounced(t *testing.T) {
	app, ft, pub := newTestApp(t)
	send(app, btnBuySubscription)
	send(app, labelMonthly())
	for i := 0; i < 5; i++ {
		send(app, btnPayMethodPlatega)
	}
	if pub.count() != 1 {
		t.Fatalf("published %d commands", pub.count())
	}
	if !strings.Contains(ft.last(t).Params["text"], "Запрос уже отправлен") {
		t.Fatalf("reply %q", ft.last(t).Params["text"])
	}
	// Другой способ — это другое явное действие.
	send(app, btnPayMethodYooKassa)
	if pub.count() != 2 || pub.msgs[1].Cmd.PaymentProvider != kafkacontracts.BillingPaymentProviderYooKassa {
		t.Fatalf("switch to method 2: %d", pub.count())
	}
}

// Кнопка способа оплаты со старой клавиатуры без свежего тарифа счёт не создаёт.
func TestStaleMethodButtonDoesNotCreateInvoice(t *testing.T) {
	app, ft, pub := newTestApp(t)
	send(app, btnPayMethodPlatega)
	if pub.count() != 0 {
		t.Fatal("invoice created without a tariff")
	}
	if !strings.Contains(ft.last(t).Params["text"], "Выбор тарифа устарел") {
		t.Fatalf("reply %q", ft.last(t).Params["text"])
	}
	// Тариф выбран давно.
	old := time.Now().Add(-pendingPlanTTL - time.Minute)
	_ = app.stateStore.Set(context.Background(), testChat, &ChatState{Step: StepPaymentMethod, PendingPlan: string(kafkacontracts.PlanCodeMonthly), PendingPlanAt: &old})
	send(app, btnPayMethodYooKassa)
	if pub.count() != 0 {
		t.Fatal("invoice created for a stale tariff")
	}
	if rows := flatten(keyboardButtons(t, ft.last(t))); !contains(rows, labelMonthly()) {
		t.Fatalf("tariff menu expected, got %v", rows)
	}
}

// Тариф по кнопке со старой ценой (цена изменилась) не выбирается.
func TestTariffButtonWithOldPriceIsNotAccepted(t *testing.T) {
	app, _, pub := newTestApp(t)
	send(app, btnBuySubscription)
	send(app, planButtonLabel(btnPlanMonthly, "79"))
	st, _ := app.stateStore.Get(context.Background(), testChat)
	if st.Step == StepPaymentMethod || st.PendingPlan != "" || pub.count() != 0 {
		t.Fatalf("old-price button accepted: %+v", st)
	}
}

func TestPublishFailureKeepsMethodChoice(t *testing.T) {
	app, ft, pub := newTestApp(t)
	pub.err = fmt.Errorf("kafka down")
	send(app, btnBuySubscription)
	send(app, labelMonthly())
	send(app, btnPayMethodPlatega)
	last := ft.last(t)
	if !strings.Contains(last.Params["text"], "Не удалось начать оплату") {
		t.Fatalf("reply %q", last.Params["text"])
	}
	if rows := flatten(keyboardButtons(t, last)); !contains(rows, btnPayMethodYooKassa) {
		t.Fatalf("method keyboard expected, got %v", rows)
	}
}

// Убранные кнопки из старых клавиатур: только пояснение, без команд.
func TestRemovedButtonsAreExplainedWithoutAction(t *testing.T) {
	for _, b := range []string{btnCancelSub, btnBindCard, btnUnbindCard} {
		t.Run(b, func(t *testing.T) {
			app, ft, pub := newTestApp(t)
			send(app, b)
			last := ft.last(t)
			if pub.count() != 0 || strings.Contains(last.Params["text"], "не получается") {
				t.Fatalf("removed button triggered an action: %q", last.Params["text"])
			}
			rows := flatten(keyboardButtons(t, last))
			if contains(rows, btnCancelSub) || contains(rows, btnBindCard) || contains(rows, btnUnbindCard) {
				t.Fatalf("menu still offers removed buttons: %v", rows)
			}
		})
	}
}

func TestMenusDoNotOfferRemovedButtons(t *testing.T) {
	for name, kb := range map[string]tgbotapi.ReplyKeyboardMarkup{
		"main": mainMenuKeyboard(), "mysub": mySubKeyboardWithConfig(), "buy": buyMenuKeyboard(),
		"trial": trialOrBuyKeyboard(), "method": paymentMethodKeyboard(),
	} {
		for _, row := range kb.Keyboard {
			for _, b := range row {
				if b.Text == btnCancelSub || b.Text == btnBindCard || b.Text == btnUnbindCard {
					t.Errorf("%s keyboard offers %q", name, b.Text)
				}
			}
		}
	}
	// Остальные пункты главного меню на месте.
	var main []string
	for _, row := range mainMenuKeyboard().Keyboard {
		for _, b := range row {
			main = append(main, b.Text)
		}
	}
	for _, want := range []string{btnBuySubscription, btnMySubscription, btnTrial, btnDownloadVPN, btnSupport, btnServicesInfo, btnReferral} {
		if !contains(main, want) {
			t.Errorf("main menu lost %q", want)
		}
	}
}

func TestGroupChatIsIgnored(t *testing.T) {
	app, ft, pub := newTestApp(t)
	app.handleUpdate(context.Background(), tgbotapi.Update{Message: &tgbotapi.Message{
		From: &tgbotapi.User{ID: 42}, Chat: &tgbotapi.Chat{ID: -100500, Type: "supergroup"}, Text: btnPayMethodPlatega,
	}})
	if len(ft.sent("sendMessage")) != 0 || pub.count() != 0 {
		t.Fatal("group message handled")
	}
}

func TestCallbackQueryIsAnswered(t *testing.T) {
	app, ft, _ := newTestApp(t)
	app.handleCallbackQuery(&tgbotapi.CallbackQuery{ID: "cbq-1", From: &tgbotapi.User{ID: testChat}, Data: "old:button"})
	answers := ft.sent("answerCallbackQuery")
	if len(answers) != 1 || answers[0].Params["callback_query_id"] != "cbq-1" || answers[0].Params["text"] != staleCallbackText {
		t.Fatalf("answers %v", answers)
	}
}

func TestNotificationRendersPaymentMethodKeyboard(t *testing.T) {
	app, ft, _ := newTestApp(t)
	if err := app.sendKafkaNotificationToUser(&kafkacontracts.TgNotification{
		TelegramID: testChat, Message: "x", Keyboard: kafkacontracts.TgKeyboardPaymentMethod,
	}); err != nil {
		t.Fatal(err)
	}
	if rows := keyboardButtons(t, ft.last(t)); fmt.Sprint(rows) != fmt.Sprint([][]string{{btnPayMethodPlatega}, {btnPayMethodYooKassa}, {btnBack, btnMainMenu}}) {
		t.Fatalf("keyboard %v", rows)
	}
	// Ссылка на оплату — inline URL-кнопка, без callback_data.
	if err := app.sendKafkaNotificationToUser(&kafkacontracts.TgNotification{
		TelegramID: testChat, Message: "pay", PayURL: "https://pay.platega.io/x",
	}); err != nil {
		t.Fatal(err)
	}
	raw := ft.last(t).Params["reply_markup"]
	if !strings.Contains(raw, `"url":"https://pay.platega.io/x"`) || strings.Contains(raw, "callback_data") {
		t.Fatalf("pay button %s", raw)
	}
}

func TestBackFromMethodChoiceReturnsToTariffs(t *testing.T) {
	app, ft, pub := newTestApp(t)
	send(app, btnBuySubscription)
	send(app, labelAnnual())
	send(app, btnBack)
	st, _ := app.stateStore.Get(context.Background(), testChat)
	if st.Step != StepBuyMenu || pub.count() != 0 {
		t.Fatalf("state %+v", st)
	}
	if rows := flatten(keyboardButtons(t, ft.last(t))); !contains(rows, labelAnnual()) {
		t.Fatalf("tariffs expected, got %v", rows)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
