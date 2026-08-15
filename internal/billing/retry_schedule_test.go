package billing

import (
	"testing"
	"time"
)

// ============================================================================
// Расписание повторных списаний.
//
// Ошибка в парсинге означает либо шквал попыток списания (претензии от
// платёжного провайдера), либо отсутствие повторов (потерянные деньги).
// При любой некорректной строке обязателен откат к безопасному значению.
// ============================================================================

func TestParseRetryScheduleEnv(t *testing.T) {
	fallback := []time.Duration{15 * time.Minute, 6 * time.Hour, 24 * time.Hour}

	tests := []struct {
		name  string
		value string
		set   bool
		want  []time.Duration
	}{
		{"переменная не задана — fallback", "", false, fallback},
		{"пустая строка — fallback", "", true, fallback},
		{"пробелы — fallback", "   ", true, fallback},
		{"корректное расписание", "15m,6h,24h", true,
			[]time.Duration{15 * time.Minute, 6 * time.Hour, 24 * time.Hour}},
		{"пробелы вокруг значений", " 5m , 1h ", true,
			[]time.Duration{5 * time.Minute, time.Hour}},
		{"одно значение", "30m", true, []time.Duration{30 * time.Minute}},
		{"нераспознаваемая длительность — fallback", "15m,абв,24h", true, fallback},
		{"нулевая длительность — fallback", "15m,0s", true, fallback},
		{"отрицательная длительность — fallback", "-5m", true, fallback},
		{"мусор — fallback", "не-длительность", true, fallback},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const key = "TEST_BILLING_RETRY_SCHEDULE"
			if tt.set {
				t.Setenv(key, tt.value)
			}

			got := parseRetryScheduleEnv(key, fallback)
			if len(got) != len(tt.want) {
				t.Fatalf("длина = %d (%v), ожидалось %d (%v)", len(got), got, len(tt.want), tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("элемент %d = %v, ожидалось %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// Расписание должно быть неубывающим: иначе повторы пойдут чаще со временем,
// что противоречит смыслу экспоненциального отката.
func TestDefaultRetryScheduleIsIncreasing(t *testing.T) {
	got := parseRetryScheduleEnv("TEST_UNSET_RETRY_SCHEDULE",
		[]time.Duration{15 * time.Minute, 6 * time.Hour, 24 * time.Hour})

	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Errorf("интервал %d (%v) не больше предыдущего (%v)", i, got[i], got[i-1])
		}
	}
}
