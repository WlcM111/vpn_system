package crypto_billing

import (
	"fmt"
	"os"
	"strings"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// Plan описывает один тарифный план в крипте. У одного плана может быть несколько цен
// в разных активах (USDT, TON, BTC, ETH). Сумма — строка, чтобы не терять точность
// (особенно для BTC с 8 знаками после точки).
type Plan struct {
	Code         kafkacontracts.PlanCode
	Title        string
	DurationDays int
	// Prices: какая сумма в каком активе стоит этот план.
	// Ключ — CryptoAsset, значение — строковая сумма, которую принимает CryptoBot API.
	Prices map[kafkacontracts.CryptoAsset]string
}

// Config — все настройки crypto-billing-service, читаются один раз при старте из env.
type Config struct {
	APIBase        string // URL CryptoBot Pay API (prod или testnet)
	APIToken       string // секретный токен от приложения CryptoBot
	WebhookToken   string // секрет в URL вебхука (defense-in-depth поверх HMAC)
	DefaultAsset   kafkacontracts.CryptoAsset
	InvoiceExpires time.Duration // через сколько инвойс протухает (CryptoBot expires_in)
	PaidBtnName    string        // имя кнопки в CryptoBot UI после успешной оплаты
	PaidBtnURL     string        // URL этой кнопки (обычно — ссылка обратно в наш бот)
	HTTPAddr       string        // адрес HTTP-сервера (webhook + healthz)

	Plans map[kafkacontracts.PlanCode]Plan
}

// LoadConfigFromEnv читает все настройки и проверяет обязательные значения.
// Возвращает ошибку, если критичные секреты не заданы — лучше упасть на старте,
// чем падать на первом же запросе пользователя.
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
	}

	if cfg.APIToken == "" {
		return cfg, fmt.Errorf("CRYPTOBOT_API_TOKEN is required")
	}
	if cfg.WebhookToken == "" {
		return cfg, fmt.Errorf("CRYPTOBOT_WEBHOOK_TOKEN is required (URL secret)")
	}

	// Цены в крипте задаются через ENV в формате "5.00" (для USDT) или "0.00008" (для BTC).
	// Если переменная не задана, используем разумный fallback.
	cfg.Plans = map[kafkacontracts.PlanCode]Plan{
		kafkacontracts.PlanCodeMonthly: {
			Code:         kafkacontracts.PlanCodeMonthly,
			Title:        "VPN подписка 30 дней",
			DurationDays: 30,
			Prices: map[kafkacontracts.CryptoAsset]string{
				kafkacontracts.CryptoAssetUSDT: envOr("CRYPTO_PLAN_MONTHLY_USDT", "5.00"),
				kafkacontracts.CryptoAssetTON:  envOr("CRYPTO_PLAN_MONTHLY_TON", "1.5"),
				kafkacontracts.CryptoAssetBTC:  envOr("CRYPTO_PLAN_MONTHLY_BTC", "0.00008"),
				kafkacontracts.CryptoAssetETH:  envOr("CRYPTO_PLAN_MONTHLY_ETH", "0.0015"),
			},
		},
		kafkacontracts.PlanCodeQuarterly: {
			Code:         kafkacontracts.PlanCodeQuarterly,
			Title:        "VPN подписка 90 дней",
			DurationDays: 90,
			Prices: map[kafkacontracts.CryptoAsset]string{
				kafkacontracts.CryptoAssetUSDT: envOr("CRYPTO_PLAN_QUARTERLY_USDT", "13.00"),
				kafkacontracts.CryptoAssetTON:  envOr("CRYPTO_PLAN_QUARTERLY_TON", "4.0"),
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

// parseDurationOr поддерживает два формата: time.Duration (например, "30m") и
// просто число секунд ("1800"). CryptoBot документация даёт expires_in в секундах,
// поэтому второй формат удобен.
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
