package platega

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseDecimalMinor(t *testing.T) {
	ok := map[string]int64{
		"89":         8900,
		"89.00":      8900,
		"89.5":       8950,
		"0.01":       1,
		"0":          0,
		"249.00":     24900,
		" 439.00 ":   43900,
		"1500.25":    150025,
		"9999999999": 999999999900,
	}
	for in, want := range ok {
		got, err := ParseDecimalMinor(in)
		if err != nil || got != want {
			t.Errorf("ParseDecimalMinor(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	bad := []string{
		"", " ", "89,00", "89.001", "-1", "+1", "1e2", "0x10", "089", "89.",
		".5", "1 000", "NaN", "Inf", "10000000001", "89.0.0",
	}
	for _, in := range bad {
		if v, err := ParseDecimalMinor(in); err == nil || !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("ParseDecimalMinor(%q) = %d, %v; want ErrInvalidAmount", in, v, err)
		}
	}
}

func TestParseJSONAmountMinor(t *testing.T) {
	ok := map[string]int64{
		"1500.25": 150025,
		"89":      8900,
		"89.0":    8900,
		"89.000":  8900,
		"1.5e2":   15000,
		"1E2":     10000,
		"0.1e1":   100,
		"0":       0,
		"0.01":    1,
	}
	for in, want := range ok {
		got, err := ParseJSONAmountMinor(json.Number(in))
		if err != nil || got != want {
			t.Errorf("ParseJSONAmountMinor(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	bad := []string{"", "-1", "-0.5", "0.001", "89.005", "1e400", "1e-400", "abc", "01", "1.", ".1", "+1", "1e", "1e+", "9e18"}
	for _, in := range bad {
		if v, err := ParseJSONAmountMinor(json.Number(in)); err == nil {
			t.Errorf("ParseJSONAmountMinor(%q) = %d; want error", in, v)
		}
	}
}

func TestFormatMinor(t *testing.T) {
	cases := map[int64]string{0: "0.00", 1: "0.01", 8900: "89.00", 150025: "1500.25", 10: "0.10", -150: "-1.50"}
	for in, want := range cases {
		if got := FormatMinor(in); got != want {
			t.Errorf("FormatMinor(%d) = %q; want %q", in, got, want)
		}
	}
}

// Сумма в JSON-запросе к Platega — JSON-число, а не строка и не float.
func TestFormatMinorIsJSONNumber(t *testing.T) {
	b, err := json.Marshal(map[string]json.Number{"amount": json.Number(FormatMinor(8900))})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"amount":89.00}` {
		t.Fatalf("unexpected encoding %s", b)
	}
}

func FuzzParseJSONAmountMinor(f *testing.F) {
	for _, s := range []string{"1500.25", "89", "1e2", "-1", "0.001", "1e400", "", "x"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		v, err := ParseJSONAmountMinor(json.Number(s))
		if err != nil {
			return
		}
		if v < 0 || v > maxAmountMinor {
			t.Fatalf("out of range value %d for %q", v, s)
		}
		back, err := ParseDecimalMinor(FormatMinor(v))
		if err != nil || back != v {
			t.Fatalf("round trip %q -> %d -> %q -> %d, %v", s, v, FormatMinor(v), back, err)
		}
	})
}

func FuzzParseDecimalMinor(f *testing.F) {
	for _, s := range []string{"89.00", "0", "1.5", "089", "1,5"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		v, err := ParseDecimalMinor(s)
		if err != nil {
			return
		}
		if got, err := ParseDecimalMinor(FormatMinor(v)); err != nil || got != v {
			t.Fatalf("round trip failed for %q: %d -> %q", s, v, FormatMinor(v))
		}
	})
}
