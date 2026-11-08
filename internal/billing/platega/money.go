package platega

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// ============================================================================
// Деньги.
//
// Внутри сервиса суммы хранятся и сравниваются только в копейках (int64).
// float не используется нигде: ни при разборе ответа Platega, ни при
// формировании запроса. Platega передаёт сумму JSON-числом в рублях с
// копейками (пример из документации: 1500.25), поэтому на входе число
// разбирается точной рациональной арифметикой, а на выходе пишется
// десятичной строкой с двумя знаками через json.Number.
// ============================================================================

// maxAmountMinor — верхняя граница суммы (10 млрд ₽). Защищает от
// переполнения int64 и от заведомо мусорных значений во входящих данных.
const maxAmountMinor int64 = 1_000_000_000_000

// ErrInvalidAmount — сумма не является корректной неотрицательной суммой с
// точностью до копейки.
var ErrInvalidAmount = errors.New("platega: invalid amount")

// ParseDecimalMinor разбирает человекочитаемую десятичную сумму в рублях
// ("89", "89.5", "89.00") в копейки. Экспонента, знак, пробелы внутри числа,
// запятая и более двух знаков после точки отвергаются: такие значения
// приходят из конфигурации или БД и должны быть однозначными.
func ParseDecimalMinor(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	intPart, frac, hasDot := strings.Cut(s, ".")
	if intPart == "" || !allDigits(intPart) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if hasDot && (frac == "" || len(frac) > 2 || !allDigits(frac)) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: leading zero in %q", ErrInvalidAmount, s)
	}
	for len(frac) < 2 {
		frac += "0"
	}
	return ratToMinor(intPart+"."+frac, s)
}

// ParseJSONAmountMinor разбирает сумму из JSON-ответа Platega. Принимается
// любая корректная запись JSON-числа (в том числе с экспонентой), но значение
// обязано быть неотрицательным и кратным копейке. Иначе — ErrInvalidAmount:
// такую сумму нельзя сопоставить с заказом без округления, а округлять деньги
// при сверке нельзя.
func ParseJSONAmountMinor(n json.Number) (int64, error) {
	s := strings.TrimSpace(string(n))
	if !isJSONNumber(s) || strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	return ratToMinor(s, s)
}

// FormatMinor форматирует копейки в рубли с двумя знаками: 8900 → "89.00".
func FormatMinor(minor int64) string {
	if minor < 0 {
		return "-" + FormatMinor(-minor)
	}
	return fmt.Sprintf("%d.%02d", minor/100, minor%100)
}

func ratToMinor(num, orig string) (int64, error) {
	r, ok := new(big.Rat).SetString(num)
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, orig)
	}
	if r.Sign() < 0 {
		return 0, fmt.Errorf("%w: negative %q", ErrInvalidAmount, orig)
	}
	r.Mul(r, big.NewRat(100, 1))
	if !r.IsInt() {
		return 0, fmt.Errorf("%w: more than two decimals in %q", ErrInvalidAmount, orig)
	}
	v := r.Num()
	if !v.IsInt64() || v.Int64() > maxAmountMinor {
		return 0, fmt.Errorf("%w: out of range %q", ErrInvalidAmount, orig)
	}
	return v.Int64(), nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isJSONNumber проверяет грамматику JSON-числа (RFC 8259, раздел 6) без
// преобразования во float.
func isJSONNumber(s string) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	if i >= len(s) {
		return false
	}
	switch {
	case s[i] == '0':
		i++
	case s[i] >= '1' && s[i] <= '9':
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	default:
		return false
	}
	if i < len(s) && s[i] == '.' {
		i++
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(s)
}
