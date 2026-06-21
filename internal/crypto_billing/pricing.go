package crypto_billing

import (
	"log/slog"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Логика выбора цены инвойса. Вынесена отдельно от service.go, чтобы
// HandleCreateCheckout оставался читаемым, а ценообразование тестировалось
// изолированно.
//
// Два режима (cfg.PricingMode):
//   crypto — считаем крипто-сумму из рублёвой цены по курсу из RatesCache;
//            если курс недоступен — fallback на статичную цену (StaticPrices).
//   fiat   — отдаём рублёвую сумму, инвойс создаётся в фиате (конвертация на
//            стороне CryptoBot). См. invoicePricing.IsFiat.
// ============================================================================

// invoicePricing — результат расчёта цены для конкретного инвойса.
type invoicePricing struct {
	IsFiat bool // true → создавать фиат-инвойс (amount в рублях)

	// Для fiat-режима:
	FiatCurrency string // "RUB"
	FiatAmount   string // "200"

	// Для crypto-режима:
	Asset  kafkacontracts.CryptoAsset // выбранный актив
	Amount string                     // крипто-сумма (строка для CryptoBot)

	// Для записи в БД и сообщений пользователю (оба режима):
	// в crypto-режиме = Asset/Amount; в fiat-режиме до оплаты крипто-значения
	// неизвестны, поэтому в БД пишем рубли (FiatCurrency/FiatAmount), а webhook
	// затем сверяет paid_asset/paid_amount.
	DBAsset  string
	DBAmount string
}

// resolvePricing вычисляет цену инвойса для плана и (опционально) запрошенного актива.
//
//	plan          — тарифный план с рублёвой ценой.
//	requestedAsset— актив, который выбрал пользователь (может быть пустым).
//
// Возвращает invoicePricing либо ошибку (например, нет ни курса, ни статичной цены).
func (s *Service) resolvePricing(plan Plan, requestedAsset kafkacontracts.CryptoAsset) (invoicePricing, error) {
	// ----- FIAT-режим: просто отдаём рубли, конвертация на стороне CryptoBot -----
	if s.cfg.PricingMode == PricingModeFiat {
		amount := formatRub(plan.PriceRUB)
		return invoicePricing{
			IsFiat:       true,
			FiatCurrency: s.cfg.FiatCurrency,
			FiatAmount:   amount,
			// в БД до оплаты пишем фиат; webhook сверит paid_asset/paid_amount
			DBAsset:  s.cfg.FiatCurrency,
			DBAmount: amount,
		}, nil
	}

	// ----- CRYPTO-режим: считаем крипто-сумму из рублёвой цены по курсу -----

	// выбираем актив: запрошенный пользователем, иначе дефолтный.
	asset := requestedAsset
	if asset == "" {
		asset = s.cfg.DefaultAsset
	}

	// пытаемся посчитать из актуального курса (если кэш свежий)
	if s.rates != nil && s.rates.IsFresh(s.cfg.RatesMaxAge) {
		amount, err := s.rates.ConvertRubToAsset(plan.PriceRUB, asset, s.cfg.MarkupPercent)
		if err == nil && amount != "" {
			return invoicePricing{
				IsFiat:   false,
				Asset:    asset,
				Amount:   amount,
				DBAsset:  string(asset),
				DBAmount: amount,
			}, nil
		}
		slog.Warn("crypto-billing dynamic price unavailable, falling back to static",
			"plan", plan.Code, "asset", asset, "err", err)
	} else {
		slog.Warn("crypto-billing rates cache stale or empty, using static price",
			"plan", plan.Code, "asset", asset)
	}

	// fallback: статичная цена из конфига (как было в проекте раньше)
	if amount, ok := plan.StaticPrices[asset]; ok && amount != "" {
		return invoicePricing{
			IsFiat:   false,
			Asset:    asset,
			Amount:   amount,
			DBAsset:  string(asset),
			DBAmount: amount,
		}, nil
	}

	return invoicePricing{}, &pricingError{plan: string(plan.Code), asset: string(asset)}
}

// formatRub форматирует рублёвую сумму без лишних знаков ("200", "199.5").
func formatRub(rub float64) string {
	return formatAssetAmount(rub, "RUB")
}

// pricingError — ошибка, когда не удалось определить цену ни одним способом.
type pricingError struct {
	plan  string
	asset string
}

func (e *pricingError) Error() string {
	return "no price available for plan=" + e.plan + " asset=" + e.asset +
		" (no live rate and no static fallback)"
}
