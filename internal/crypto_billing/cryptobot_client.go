package crypto_billing

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// CryptoBotClient — тонкая обёртка над CryptoBot Pay API.
// Документация: https://help.crypt.bot/crypto-pay-api
type CryptoBotClient struct {
	apiBase  string
	apiToken string
	http     *http.Client
}

// NewCryptoBotClient создаёт клиент. apiBase — это либо https://pay.crypt.bot/api для
// прода, либо https://testnet-pay.crypt.bot/api для тестов.
func NewCryptoBotClient(apiBase, apiToken string) *CryptoBotClient {
	return &CryptoBotClient{
		apiBase:  apiBase,
		apiToken: apiToken,
		http: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// createInvoiceRequest описывает тело запроса CryptoBot createInvoice.
// Не все поля API экспонированы — мы вытащили только те, что нужны для v1.
// Если в будущем понадобятся allow_anonymous, hidden_message и т.д. — добавить
// в эту структуру и пробросить через CreateInvoice.
type createInvoiceRequest struct {
	Asset         string `json:"asset"`
	Amount        string `json:"amount"`
	Description   string `json:"description,omitempty"`
	HiddenMessage string `json:"hidden_message,omitempty"`
	PaidBtnName   string `json:"paid_btn_name,omitempty"`
	PaidBtnURL    string `json:"paid_btn_url,omitempty"`
	Payload       string `json:"payload,omitempty"`
	ExpiresIn     int    `json:"expires_in,omitempty"`
	AllowComments bool   `json:"allow_comments"`
}

// CryptoBotInvoice — структура инвойса, как её отдаёт CryptoBot.
// CryptoBot эволюционирует API и добавляет новые поля; мы парсим только нужные.
// Поля для оплаты (Pay URL'ы) могут приходить под разными именами в зависимости от
// версии API и настроек приложения — берём первое непустое (см. PickPayURL).
type CryptoBotInvoice struct {
	InvoiceID int64  `json:"invoice_id"`
	Status    string `json:"status"`
	Hash      string `json:"hash"`
	Asset     string `json:"asset"`
	Amount    string `json:"amount"`
	// Актуальные поля ссылок оплаты (Crypto Pay API 1.4+).
	// bot_invoice_url — основная ссылка: открывает оплату в @CryptoBot внутри Telegram.
	// mini_app_invoice_url — версия для Telegram Mini App.
	// web_app_invoice_url — веб-версия Crypto Bot.
	BotPay     string `json:"bot_invoice_url,omitempty"`
	MiniAppPay string `json:"mini_app_invoice_url,omitempty"`
	WebAppPay  string `json:"web_app_invoice_url,omitempty"`
	// PayURL — устаревшее поле pay_url (deprecated с API 1.2). Оставлено только как
	// последний fallback на случай старого ответа; новый код полагается на bot_invoice_url.
	PayURL      string `json:"pay_url,omitempty"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	Payload     string `json:"payload"`
	// Поля фиат-инвойса (currency_type=fiat). До оплаты paid_* пустые.
	CurrencyType   string   `json:"currency_type,omitempty"`
	Fiat           string   `json:"fiat,omitempty"`
	AcceptedAssets []string `json:"accepted_assets,omitempty"`
}

// apiResponse — общая обёртка ответа CryptoBot API: {ok, result, error}.
type apiResponse struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code int    `json:"code"`
		Name string `json:"name"`
	} `json:"error,omitempty"`
}

// CreateInvoice вызывает CryptoBot createInvoice и возвращает разобранный инвойс,
// плюс сырой JSON-ответ — он сохраняется в БД как raw_create_response для аудита.
//
// Ошибка возвращается, если:
//   - сетевая ошибка / таймаут;
//   - HTTP не 2xx;
//   - в JSON-ответе ok=false;
//   - не удалось распарсить result.
//
// Идемпотентность: CryptoBot НЕ поддерживает идемпотентные ключи. Если мы делаем
// повторный вызов с тем же payload, получим новый invoice_id. Поэтому
// вызывающий код (service.go) НЕ должен вызывать CreateInvoice дважды для одной команды.
// Это гарантируется processed_messages dedup на уровне Kafka-команды.
func (c *CryptoBotClient) CreateInvoice(
	ctx context.Context,
	asset kafkacontracts.CryptoAsset,
	amount, description, payload, paidBtnName, paidBtnURL string,
	expiresIn time.Duration,
) (*CryptoBotInvoice, json.RawMessage, error) {
	body, err := json.Marshal(&createInvoiceRequest{
		Asset:         string(asset),
		Amount:        amount,
		Description:   description,
		PaidBtnName:   paidBtnName,
		PaidBtnURL:    paidBtnURL,
		Payload:       payload,
		ExpiresIn:     int(expiresIn.Seconds()),
		AllowComments: false,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal createInvoice: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+"/createInvoice", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Crypto-Pay-API-Token", c.apiToken)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("cryptobot createInvoice http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Лимит на размер ответа — защита от случайного 100 МБ ответа.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, raw, err
	}

	var parsed apiResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, raw, fmt.Errorf("cryptobot createInvoice decode: %w (body=%s)", err, string(raw))
	}
	if !parsed.OK {
		errName := ""
		errCode := 0
		if parsed.Error != nil {
			errName = parsed.Error.Name
			errCode = parsed.Error.Code
		}
		return nil, raw, fmt.Errorf("cryptobot createInvoice failed http=%d code=%d name=%s body=%s",
			resp.StatusCode, errCode, errName, string(raw))
	}

	var inv CryptoBotInvoice
	if err := json.Unmarshal(parsed.Result, &inv); err != nil {
		return nil, raw, fmt.Errorf("cryptobot decode invoice: %w", err)
	}
	return &inv, raw, nil
}

// VerifyWebhookSignature проверяет HMAC-SHA256 подпись webhook'а.
// Согласно CryptoBot:
//
//	key = sha256(api_token)
//	signature = HMAC-SHA256(raw_body, key)
//	передаётся в заголовке "crypto-pay-api-signature" в hex.
//
// Используем hmac.Equal для constant-time сравнения (защита от timing-атак).
func VerifyWebhookSignature(apiToken, signatureHex string, rawBody []byte) bool {
	keyHash := sha256.Sum256([]byte(apiToken))
	mac := hmac.New(sha256.New, keyHash[:])
	mac.Write(rawBody)
	expected := mac.Sum(nil)

	got, err := hex.DecodeString(signatureHex)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, got)
}

// PickPayURL выбирает лучший из доступных URL'ов оплаты.
// Приоритет для нашего сценария (Telegram-бот): bot_invoice_url — открывает оплату
// прямо в @CryptoBot внутри Telegram. Затем mini_app / web. pay_url (deprecated) —
// самый последний fallback на случай старого формата ответа.
func PickPayURL(inv *CryptoBotInvoice) string {
	if inv == nil {
		return ""
	}
	if inv.BotPay != "" {
		return inv.BotPay
	}
	if inv.MiniAppPay != "" {
		return inv.MiniAppPay
	}
	if inv.WebAppPay != "" {
		return inv.WebAppPay
	}
	return inv.PayURL
}

// ----------------------------------------------------------------------------
// 1. Курсы валют (getExchangeRates)
// ----------------------------------------------------------------------------

// ExchangeRate — один элемент ответа getExchangeRates.
// Пример: {is_valid:true, source:"TON", target:"RUB", rate:"135.50"}.
type ExchangeRate struct {
	IsValid bool   `json:"is_valid"`
	Source  string `json:"source"`
	Target  string `json:"target"`
	Rate    string `json:"rate"`
}

// GetExchangeRates вызывает CryptoBot getExchangeRates и возвращает список курсов.
// Метод не требует параметров. Используется RatesWorker'ом для получения курсов
// крипты к рублю (target=RUB).
func (c *CryptoBotClient) GetExchangeRates(ctx context.Context) ([]ExchangeRate, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiBase+"/getExchangeRates", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Crypto-Pay-API-Token", c.apiToken)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cryptobot getExchangeRates http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var parsed apiResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("cryptobot getExchangeRates decode: %w (body=%s)", err, string(raw))
	}
	if !parsed.OK {
		errName, errCode := "", 0
		if parsed.Error != nil {
			errName = parsed.Error.Name
			errCode = parsed.Error.Code
		}
		return nil, fmt.Errorf("cryptobot getExchangeRates failed code=%d name=%s", errCode, errName)
	}

	var rates []ExchangeRate
	if err := json.Unmarshal(parsed.Result, &rates); err != nil {
		return nil, fmt.Errorf("cryptobot decode exchange rates: %w", err)
	}
	return rates, nil
}

// ----------------------------------------------------------------------------
// 2. Фиат-инвойс (createInvoice с currency_type=fiat)
// ----------------------------------------------------------------------------

// createInvoiceFiatRequest — тело запроса для фиат-инвойса.
// CryptoBot сам конвертирует фиат в крипту по курсу на момент оплаты.
type createInvoiceFiatRequest struct {
	CurrencyType   string `json:"currency_type"` // всегда "fiat"
	Fiat           string `json:"fiat"`          // напр. "RUB"
	Amount         string `json:"amount"`        // сумма в фиате, напр. "200"
	AcceptedAssets string `json:"accepted_assets,omitempty"`
	Description    string `json:"description,omitempty"`
	PaidBtnName    string `json:"paid_btn_name,omitempty"`
	PaidBtnURL     string `json:"paid_btn_url,omitempty"`
	Payload        string `json:"payload,omitempty"`
	ExpiresIn      int    `json:"expires_in,omitempty"`
	AllowComments  bool   `json:"allow_comments"`
}

// CreateInvoiceFiat создаёт инвойс в фиатной валюте (например, рубли).
// CryptoBot принимает оплату в одной из accepted_assets, конвертируя по своему
// курсу на момент оплаты. Это максимально точный способ держать рублёвую цену.
//
//	fiat            — код фиата ("RUB").
//	amount          — сумма в фиате ("200").
//	acceptedAssets  — список крипто-активов через запятую ("USDT,TON"); пусто = все.
//
// Возвращает разобранный инвойс и сырой JSON (для аудита в БД).
func (c *CryptoBotClient) CreateInvoiceFiat(
	ctx context.Context,
	fiat, amount, acceptedAssets, description, payload, paidBtnName, paidBtnURL string,
	expiresIn time.Duration,
) (*CryptoBotInvoice, json.RawMessage, error) {
	body, err := json.Marshal(&createInvoiceFiatRequest{
		CurrencyType:   "fiat",
		Fiat:           fiat,
		Amount:         amount,
		AcceptedAssets: acceptedAssets,
		Description:    description,
		PaidBtnName:    paidBtnName,
		PaidBtnURL:     paidBtnURL,
		Payload:        payload,
		ExpiresIn:      int(expiresIn.Seconds()),
		AllowComments:  false,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal createInvoice(fiat): %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+"/createInvoice", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Crypto-Pay-API-Token", c.apiToken)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("cryptobot createInvoice(fiat) http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, raw, err
	}

	var parsed apiResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, raw, fmt.Errorf("cryptobot createInvoice(fiat) decode: %w (body=%s)", err, string(raw))
	}
	if !parsed.OK {
		errName, errCode := "", 0
		if parsed.Error != nil {
			errName = parsed.Error.Name
			errCode = parsed.Error.Code
		}
		return nil, raw, fmt.Errorf("cryptobot createInvoice(fiat) failed http=%d code=%d name=%s body=%s",
			resp.StatusCode, errCode, errName, string(raw))
	}

	var inv CryptoBotInvoice
	if err := json.Unmarshal(parsed.Result, &inv); err != nil {
		return nil, raw, fmt.Errorf("cryptobot decode invoice(fiat): %w", err)
	}
	return &inv, raw, nil
}
