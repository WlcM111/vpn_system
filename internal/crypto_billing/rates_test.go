package crypto_billing

import (
	"testing"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Конвертация рублей в криптовалюту — денежный путь.
// Ошибка означает неверную сумму счёта: либо клиент переплачивает,
// либо сервис недополучает.
// ============================================================================

func TestConvertRubToAsset(t *testing.T) {
	tests := []struct {
		name    string
		rates   map[kafkacontracts.CryptoAsset]float64
		rub     float64
		asset   kafkacontracts.CryptoAsset
		markup  float64
		want    string
		wantErr bool
	}{
		{
			name:  "USDT без наценки",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100},
			rub:   890, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			want: "8.9",
		},
		{
			name:  "USDT с наценкой 5%",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100},
			rub:   1000, asset: kafkacontracts.CryptoAssetUSDT, markup: 5,
			want: "10.5",
		},
		{
			name:  "BTC — 8 знаков",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetBTC: 10000000},
			rub:   890, asset: kafkacontracts.CryptoAssetBTC, markup: 0,
			want: "0.000089",
		},
		{
			name:  "хвостовые нули убираются",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100},
			rub:   1000, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			want: "10",
		},
		{
			name:  "нет курса — ошибка",
			rates: map[kafkacontracts.CryptoAsset]float64{},
			rub:   890, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			wantErr: true,
		},
		{
			name:  "нулевой курс не делит на ноль",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 0},
			rub:   890, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			wantErr: true,
		},
		{
			name:  "отрицательный курс отвергается",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: -5},
			rub:   890, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			wantErr: true,
		},
		{
			name:  "нулевая сумма отвергается",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100},
			rub:   0, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			wantErr: true,
		},
		{
			name:  "отрицательная сумма отвергается",
			rates: map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100},
			rub:   -100, asset: kafkacontracts.CryptoAssetUSDT, markup: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewRatesCache()
			c.Set(tt.rates, time.Now())

			got, err := c.ConvertRubToAsset(tt.rub, tt.asset, tt.markup)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ожидалась ошибка, получено %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if got != tt.want {
				t.Errorf("ConvertRubToAsset() = %q, ожидалось %q", got, tt.want)
			}
		})
	}
}

func TestRatesCacheFreshness(t *testing.T) {
	c := NewRatesCache()

	// Пустой кеш: возраст заведомо больше любого разумного maxAge.
	if c.IsFresh(time.Hour) {
		t.Error("пустой кеш не должен считаться свежим")
	}

	c.Set(map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100}, time.Now())
	if !c.IsFresh(time.Hour) {
		t.Error("только что обновлённый кеш должен быть свежим")
	}

	// Устаревший кеш.
	c.Set(map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100}, time.Now().Add(-2*time.Hour))
	if c.IsFresh(time.Hour) {
		t.Error("кеш возрастом 2ч не должен быть свежим при maxAge=1ч")
	}
}

// Set обязан копировать карту: иначе вызывающий может мутировать
// внутреннее состояние кеша уже после установки курсов.
func TestRatesCacheSetCopiesMap(t *testing.T) {
	c := NewRatesCache()
	rates := map[kafkacontracts.CryptoAsset]float64{kafkacontracts.CryptoAssetUSDT: 100}
	c.Set(rates, time.Now())

	rates[kafkacontracts.CryptoAssetUSDT] = 999 // мутируем снаружи

	got, ok := c.RubPerOne(kafkacontracts.CryptoAssetUSDT)
	if !ok || got != 100 {
		t.Errorf("курс = %v (ok=%v), ожидалось 100 — Set не скопировал карту", got, ok)
	}
}

func TestRubPerOneRejectsNonPositive(t *testing.T) {
	c := NewRatesCache()
	c.Set(map[kafkacontracts.CryptoAsset]float64{
		kafkacontracts.CryptoAssetUSDT: 0,
		kafkacontracts.CryptoAssetTON:  -1,
		kafkacontracts.CryptoAssetBTC:  100,
	}, time.Now())

	if _, ok := c.RubPerOne(kafkacontracts.CryptoAssetUSDT); ok {
		t.Error("нулевой курс не должен считаться валидным")
	}
	if _, ok := c.RubPerOne(kafkacontracts.CryptoAssetTON); ok {
		t.Error("отрицательный курс не должен считаться валидным")
	}
	if _, ok := c.RubPerOne(kafkacontracts.CryptoAssetBTC); !ok {
		t.Error("положительный курс должен быть валидным")
	}
}
