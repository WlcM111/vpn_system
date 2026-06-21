package crypto_billing

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// PricingMode задаёт способ формирования крипто-цены инвойса.
type PricingMode string

const (
	// PricingModeCrypto — крипто-сумма вычисляется из рублёвой цены по курсу,
	// который периодически обновляет RatesWorker (CryptoBot getExchangeRates).
	// Сумма и актив фиксируются в момент СОЗДАНИЯ инвойса. Это режим по умолчанию:
	// он работает в рамках уже существующей и проверенной логики webhook-проверок
	// (asset/amount в БД совпадают с тем, что вернёт CryptoBot при оплате).
	PricingModeCrypto PricingMode = "crypto"

	// PricingModeFiat — инвойс создаётся прямо в рублях (currency_type=fiat),
	// CryptoBot сам конвертирует в крипту по курсу на МОМЕНТ ОПЛАТЫ. Максимально
	// точно держит рублёвую цену, но требует адаптированных webhook-проверок
	// (см. service.go: при fiat в БД сохраняется paid_asset/paid_amount из ответа).
	PricingModeFiat PricingMode = "fiat"
)

// Plan описывает один тарифный план. Базовая цена — в рублях (PriceRUB).
// StaticPrices — резервные фиксированные крипто-цены (fallback, если курс
// недоступен в crypto-режиме) — те же переменные, что были в проекте раньше.
type Plan struct {
	Code         kafkacontracts.PlanCode
	Title        string
	DurationDays int
	// PriceRUB — базовая цена плана в рублях. Главный источник истины для цены.
	PriceRUB float64
	// StaticPrices — fallback крипто-цены по активам (используются, только если
	// в crypto-режиме курс недоступен). Ключ — актив, значение — строковая сумма.
	StaticPrices map[kafkacontracts.CryptoAsset]string
}

// Config — все настройки crypto-billing-service.
type Config struct {
	APIBase        string
	APIToken       string
	WebhookToken   string
	DefaultAsset   kafkacontracts.CryptoAsset
	InvoiceExpires time.Duration
	PaidBtnName    string
	PaidBtnURL     string
	HTTPAddr       string

	// --- ценообразование ---
	PricingMode    PricingMode                  // crypto (default) | fiat
	FiatCurrency   string                       // код фиата для fiat-режима (RUB)
	AcceptedAssets []kafkacontracts.CryptoAsset // какие активы принимать
	RatesInterval  time.Duration                // период обновления курсов (crypto-режим)
	RatesMaxAge    time.Duration                // макс. возраст курса, после — fallback
	MarkupPercent  float64                      // наценка % (запас на волатильность)

	Plans map[kafkacontracts.PlanCode]Plan
}

// LoadConfigFromEnv читает все настройки и проверяет обязательные значения.
func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		APIBase:        envOr("CRYPTOBOT_API_BASE", "https://pay.crypt.bot/api"),
		APIToken:       strings.TrimSpace(os.Getenv("CRYPTOBOT_API_TOKEN")),
		WebhookToken:   strings.TrimSpace(os.Getenv("CRYPTOBOT_WEBHOOK_TOKEN")),
		DefaultAsset:   kafkacontracts.CryptoAsset(envOr("CRYPTOBOT_DEFAULT_ASSET", "USDT")),
		InvoiceExpires: parseDurationOr("CRYPTOBOT_INVOICE_EXPIRES_IN", 30*time.Minute),
		PaidBtnName:    envOr("CRYPTOBOT_PAID_BTN_NAME", "callback"),
		PaidBtnURL:     strings.TrimSpace(os.Getenv("CRYPTOBOT_PAID_BTN_URL")),
		HTTPAddr:       envOr("CRYPTO_BILLING_HTTP_ADDR", ":8086"),

		PricingMode:   PricingMode(strings.ToLower(envOr("CRYPTO_PRICING_MODE", "crypto"))),
		FiatCurrency:  strings.ToUpper(envOr("CRYPTO_FIAT_CURRENCY", "RUB")),
		RatesInterval: parseDurationOr("CRYPTO_RATES_REFRESH_INTERVAL", 5*time.Minute),
		RatesMaxAge:   parseDurationOr("CRYPTO_RATES_MAX_AGE", 30*time.Minute),
		MarkupPercent: parseFloatOr("CRYPTO_PRICE_MARKUP_PERCENT", 0),
	}

	if cfg.APIToken == "" {
		return cfg, fmt.Errorf("CRYPTOBOT_API_TOKEN is required")
	}
	if cfg.WebhookToken == "" {
		return cfg, fmt.Errorf("CRYPTOBOT_WEBHOOK_TOKEN is required (URL secret)")
	}
	if cfg.PricingMode != PricingModeCrypto && cfg.PricingMode != PricingModeFiat {
		return cfg, fmt.Errorf("CRYPTO_PRICING_MODE must be 'crypto' or 'fiat', got %q", cfg.PricingMode)
	}

	// Какие активы принимаем. Для fiat-режима это accepted_assets, для crypto-режима —
	// список активов, для которых считаем цену из рублей. По умолчанию USDT,TON.
	cfg.AcceptedAssets = parseAssets(envOr("CRYPTO_ACCEPTED_ASSETS", "USDT,TON"))
	if len(cfg.AcceptedAssets) == 0 {
		cfg.AcceptedAssets = []kafkacontracts.CryptoAsset{kafkacontracts.CryptoAssetUSDT, kafkacontracts.CryptoAssetTON}
	}

	// Базовые цены в рублях (главный источник истины).
	monthlyRUB := parseFloatOr("CRYPTO_PLAN_MONTHLY_RUB", 200)
	quarterlyRUB := parseFloatOr("CRYPTO_PLAN_QUARTERLY_RUB", 500)

	cfg.Plans = map[kafkacontracts.PlanCode]Plan{
		kafkacontracts.PlanCodeMonthly: {
			Code:         kafkacontracts.PlanCodeMonthly,
			Title:        "Подписка на сервис защищённого соединения (30 дней)",
			DurationDays: 30,
			PriceRUB:     monthlyRUB,
			StaticPrices: map[kafkacontracts.CryptoAsset]string{
				kafkacontracts.CryptoAssetUSDT: envOr("CRYPTO_PLAN_MONTHLY_USDT", "2.00"),
				kafkacontracts.CryptoAssetTON:  envOr("CRYPTO_PLAN_MONTHLY_TON", "1.3"),
				kafkacontracts.CryptoAssetBTC:  envOr("CRYPTO_PLAN_MONTHLY_BTC", "0.00008"),
				kafkacontracts.CryptoAssetETH:  envOr("CRYPTO_PLAN_MONTHLY_ETH", "0.0015"),
			},
		},
		kafkacontracts.PlanCodeQuarterly: {
			Code:         kafkacontracts.PlanCodeQuarterly,
			Title:        "Подписка на сервис защищённого соединения (90 дней)",
			DurationDays: 90,
			PriceRUB:     quarterlyRUB,
			StaticPrices: map[kafkacontracts.CryptoAsset]string{
				kafkacontracts.CryptoAssetUSDT: envOr("CRYPTO_PLAN_QUARTERLY_USDT", "5.00"),
				kafkacontracts.CryptoAssetTON:  envOr("CRYPTO_PLAN_QUARTERLY_TON", "3.1"),
				kafkacontracts.CryptoAssetBTC:  envOr("CRYPTO_PLAN_QUARTERLY_BTC", "0.00021"),
				kafkacontracts.CryptoAssetETH:  envOr("CRYPTO_PLAN_QUARTERLY_ETH", "0.004"),
			},
		},
	}

	return cfg, nil
}

// envOr возвращает значение переменной окружения или fallback, если пусто.
func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

// parseFloatOr парсит float из env или возвращает fallback.
func parseFloatOr(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	// поддержка запятой как десятичного разделителя ("200,5")
	v = strings.ReplaceAll(v, ",", ".")
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return fallback
	}
	return f
}

// parseAssets парсит "USDT,TON" в список активов.
func parseAssets(s string) []kafkacontracts.CryptoAsset {
	parts := strings.Split(s, ",")
	out := make([]kafkacontracts.CryptoAsset, 0, len(parts))
	seen := map[string]bool{}
	for _, p := range parts {
		a := strings.ToUpper(strings.TrimSpace(p))
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		out = append(out, kafkacontracts.CryptoAsset(a))
	}
	return out
}

// AcceptedAssetsCSV возвращает принимаемые активы строкой "USDT,TON"
// (для параметра accepted_assets фиат-инвойса).
func (c Config) AcceptedAssetsCSV() string {
	parts := make([]string, 0, len(c.AcceptedAssets))
	for _, a := range c.AcceptedAssets {
		parts = append(parts, string(a))
	}
	return strings.Join(parts, ",")
}

// parseDurationOr поддерживает два формата: time.Duration ("30m") и число секунд ("1800").
func parseDurationOr(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	if d, err := time.ParseDuration(v + "s"); err == nil && d > 0 {
		return d
	}
	return fallback
}
