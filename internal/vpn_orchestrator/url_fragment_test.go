package vpn_orchestrator

import "testing"

// ============================================================================
// Регрессия на реальный баг: названия профилей приезжали в клиент с плюсами
// вместо пробелов («Литва+-+Маленький+Пинг»), потому что использовался
// url.QueryEscape. В query-строке пробел кодируется как «+», но в
// URL-фрагменте (после #) «+» означает буквальный плюс.
// ============================================================================

func TestEscapeFragment(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"пробел кодируется как %20, а не +", "Литва - Маленький Пинг",
			"%D0%9B%D0%B8%D1%82%D0%B2%D0%B0%20-%20%D0%9C%D0%B0%D0%BB%D0%B5%D0%BD%D1%8C%D0%BA%D0%B8%D0%B9%20%D0%9F%D0%B8%D0%BD%D0%B3"},
		{"латиница с пробелом", "House VPN", "House%20VPN"},
		{"без пробелов не меняется", "Lithuania", "Lithuania"},
		{"пустая строка", "", ""},
		{"несколько пробелов подряд", "a  b", "a%20%20b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeFragment(tt.in)
			if got != tt.want {
				t.Errorf("escapeFragment(%q) = %q, ожидалось %q", tt.in, got, tt.want)
			}
		})
	}
}

// Главный инвариант: в результате не должно остаться ни одного «+».
func TestEscapeFragmentNeverProducesPlus(t *testing.T) {
	inputs := []string{
		"Литва - Маленький пинг (игры)",
		"Если обычные ссылки не работают",
		"🇱🇹 Литва — Hysteria",
		"a b c d e",
	}
	for _, in := range inputs {
		got := escapeFragment(in)
		for _, r := range got {
			if r == '+' {
				t.Errorf("escapeFragment(%q) = %q — содержит '+', клиент покажет его буквально", in, got)
				break
			}
		}
	}
}
