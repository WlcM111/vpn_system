package platega

import (
	"testing"
	"time"
)

func TestParseStatus(t *testing.T) {
	for _, s := range []string{"PENDING", "CONFIRMED", "CANCELED", "CHARGEBACKED", "PARTIALLY_REFUNDED", "DISPUTED", " confirmed "} {
		if _, err := ParseStatus(s); err != nil {
			t.Errorf("ParseStatus(%q): %v", s, err)
		}
	}
	for _, s := range []string{"", "PAID", "CANCELLED", "REFUNDED", "SUCCESS"} {
		if _, err := ParseStatus(s); err == nil {
			t.Errorf("ParseStatus(%q) accepted unknown status", s)
		}
	}
}

func TestParsePaymentMethod(t *testing.T) {
	for _, s := range []string{"2", "3", "11", "12", "13", "14", " 2 "} {
		if _, err := ParsePaymentMethod(s); err != nil {
			t.Errorf("ParsePaymentMethod(%q): %v", s, err)
		}
	}
	for _, s := range []string{"", "1", "4", "6", "SBPQR", "2.0", "-2"} {
		if _, err := ParsePaymentMethod(s); err == nil {
			t.Errorf("ParsePaymentMethod(%q) accepted undocumented method", s)
		}
	}
}

func TestIsUUID(t *testing.T) {
	good := []string{"3fa85f64-5717-4562-b3fc-2c963f66afa6", "3FA85F64-5717-4562-B3FC-2C963F66AFA6"}
	bad := []string{"", "3fa85f64571745 62b3fc2c963f66afa6", "3fa85f64-5717-4562-b3fc-2c963f66afa", "3fa85f64-5717-4562-b3fc-2c963f66afa6 ", "zfa85f64-5717-4562-b3fc-2c963f66afa6", "3fa85f64_5717-4562-b3fc-2c963f66afa6"}
	for _, s := range good {
		if !IsUUID(s) {
			t.Errorf("IsUUID(%q) = false", s)
		}
	}
	for _, s := range bad {
		if IsUUID(s) {
			t.Errorf("IsUUID(%q) = true", s)
		}
	}
	if !SameID("3fa85f64-5717-4562-b3fc-2c963f66afa6", " 3FA85F64-5717-4562-B3FC-2C963F66AFA6") {
		t.Error("SameID must ignore case and spaces")
	}
}

func TestParseTimeSpan(t *testing.T) {
	ok := map[string]time.Duration{
		"00:15:00":           15 * time.Minute,
		"01:00:00":           time.Hour,
		"1.00:00:00":         24 * time.Hour,
		"00:00:30":           30 * time.Second,
		"00:15:00.0000000":   15 * time.Minute,
		"2.03:04:05.1234567": 51*time.Hour + 4*time.Minute + 5*time.Second,
	}
	for in, want := range ok {
		got, err := ParseTimeSpan(in)
		if err != nil || got != want {
			t.Errorf("ParseTimeSpan(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "00:00:00", "24:00:00", "00:60:00", "15m", "00:15", "a:b:c", "-00:15:00", "00:15:00.12345678"} {
		if d, err := ParseTimeSpan(in); err == nil {
			t.Errorf("ParseTimeSpan(%q) = %v; want error", in, d)
		}
	}
}

func FuzzParseTimeSpan(f *testing.F) {
	for _, s := range []string{"00:15:00", "1.00:00:00", "x", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if d, err := ParseTimeSpan(s); err == nil && d <= 0 {
			t.Fatalf("non-positive duration %v for %q", d, s)
		}
	})
}

func TestUserEmailForTelegram(t *testing.T) {
	if got := UserEmailForTelegram(5512345678); got != "5512345678@telegram.org" {
		t.Fatalf("got %q", got)
	}
}
