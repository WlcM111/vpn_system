package crypto_billing

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Динамическое ценообразование: рубль — базовая валюта.
//
// Идея: цена плана задаётся в рублях (CRYPTO_PLAN_*_RUB). Курсы криптовалют
// волатильны, поэтому крипто-сумму инвойса вычисляем на лету из актуального
// курса, который периодически обновляет RatesWorker через CryptoBot
// getExchangeRates (тот же источник, по которому идёт реальная оплата —
// это даёт максимальную согласованность курса).
//
// Потокобезопасность: RatesCache читается из горутины обработки команд
// (HandleCreateCheckout) и пишется из горутины воркера. Защищено sync.RWMutex.
// ============================================================================

// RatesCache — потокобезопасный снимок курсов "1 единица актива = N рублей".
// Пример: rates["TON"] = 135.50 означает 1 TON = 135.50 RUB.
type RatesCache struct {
	mu        sync.RWMutex
	rubPerOne map[kafkacontracts.CryptoAsset]float64
	updatedAt time.Time
}

// NewRatesCache создаёт пустой кэш.
func NewRatesCache() *RatesCache {
	return &RatesCache{
		rubPerOne: make(map[kafkacontracts.CryptoAsset]float64),
	}
}

// Set атомарно заменяет все курсы и фиксирует время обновления.
func (c *RatesCache) Set(rates map[kafkacontracts.CryptoAsset]float64, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// копируем, чтобы вызывающий не мог потом мутировать внутреннюю map
	cp := make(map[kafkacontracts.CryptoAsset]float64, len(rates))
	for k, v := range rates {
		cp[k] = v
	}
	c.rubPerOne = cp
	c.updatedAt = at
}

// RubPerOne возвращает курс "сколько рублей за 1 единицу актива" и флаг наличия.
func (c *RatesCache) RubPerOne(asset kafkacontracts.CryptoAsset) (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.rubPerOne[asset]
	return v, ok && v > 0
}

// Age возвращает, сколько прошло с последнего успешного обновления.
// Если обновления ещё не было — возвращает очень большое значение.
func (c *RatesCache) Age() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.updatedAt.IsZero() {
		return time.Duration(1<<62 - 1)
	}
	return time.Since(c.updatedAt)
}

// IsFresh сообщает, актуальны ли курсы (обновлялись не позже maxAge назад).
func (c *RatesCache) IsFresh(maxAge time.Duration) bool {
	return c.Age() <= maxAge
}

// ConvertRubToAsset переводит рублёвую сумму в сумму актива по текущему курсу,
// применяя наценку markupPercent (например, 2.0 = +2% запас на волатильность),
// и форматирует результат с нужным числом знаков для CryptoBot.
//
// Возвращает ошибку, если курс актива неизвестен или некорректен — вызывающий
// код должен обработать это (fallback на статичную цену или сообщение об ошибке).
func (c *RatesCache) ConvertRubToAsset(rub float64, asset kafkacontracts.CryptoAsset, markupPercent float64) (string, error) {
	rubPerOne, ok := c.RubPerOne(asset)
	if !ok {
		return "", fmt.Errorf("no exchange rate for asset %s", asset)
	}
	if rub <= 0 {
		return "", fmt.Errorf("invalid rub amount: %v", rub)
	}

	// сумма в активе = рубли / (рублей за единицу), с наценкой
	amount := rub / rubPerOne
	if markupPercent != 0 {
		amount = amount * (1.0 + markupPercent/100.0)
	}

	return formatAssetAmount(amount, asset), nil
}

// formatAssetAmount форматирует сумму актива с разумным числом знаков.
// USDT — 2 знака; TON/прочие — 4 знака; BTC — 8 знаков; ETH — 6 знаков.
// CryptoBot принимает float-строку; лишние знаки безопасны, но мы держим
// аккуратный вид и достаточную точность, чтобы не терять рубли на округлении.
func formatAssetAmount(amount float64, asset kafkacontracts.CryptoAsset) string {
	decimals := 4
	switch asset {
	case kafkacontracts.CryptoAssetUSDT:
		decimals = 2
	case kafkacontracts.CryptoAssetBTC:
		decimals = 8
	case kafkacontracts.CryptoAssetETH:
		decimals = 6
	case kafkacontracts.CryptoAssetTON:
		decimals = 4
	}
	s := strconv.FormatFloat(amount, 'f', decimals, 64)
	// убираем хвостовые нули и точку, чтобы "1.5000" -> "1.5", "2.00" -> "2"
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	if s == "" || s == "0" {
		// защита от схлопывания в ноль при крайне малых суммах
		return strconv.FormatFloat(amount, 'f', decimals, 64)
	}
	return s
}

// ============================================================================
// RatesWorker — фоновое обновление курсов.
// ============================================================================

// RatesProvider — то, что умеет отдавать курсы (реализуется CryptoBotClient).
// Вынесено в интерфейс ради тестируемости и слабой связанности.
type RatesProvider interface {
	GetExchangeRates(ctx context.Context) ([]ExchangeRate, error)
}

// RunRatesWorker периодически опрашивает источник курсов и обновляет кэш.
// Блокирующий вызов — запускать в горутине из main.
//
//	interval — период обновления (CRYPTO_RATES_REFRESH_INTERVAL, по умолчанию 5m).
//
// Первое обновление делается сразу при старте, чтобы кэш не был пустым.
// Целевая фиат-валюта — рубль (target=RUB в getExchangeRates).
func RunRatesWorker(ctx context.Context, provider RatesProvider, cache *RatesCache, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	refresh := func() {
		rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		rates, err := provider.GetExchangeRates(rctx)
		if err != nil {
			slog.Error("crypto-billing rates refresh failed", "err", err)
			return
		}

		parsed := make(map[kafkacontracts.CryptoAsset]float64)
		for _, r := range rates {
			if !r.IsValid {
				continue
			}
			// нас интересуют только курсы крипты в рубли
			if !strings.EqualFold(r.Target, "RUB") {
				continue
			}
			rate, perr := strconv.ParseFloat(strings.TrimSpace(r.Rate), 64)
			if perr != nil || rate <= 0 {
				continue
			}
			parsed[kafkacontracts.CryptoAsset(strings.ToUpper(r.Source))] = rate
		}

		if len(parsed) == 0 {
			slog.Warn("crypto-billing rates refresh got no RUB rates, keeping previous cache")
			return
		}

		cache.Set(parsed, time.Now())
		slog.Info("crypto-billing rates refreshed", "assets", len(parsed))
	}

	// первый прогон сразу
	refresh()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("crypto-billing rates worker stopped")
			return
		case <-ticker.C:
			refresh()
		}
	}
}
