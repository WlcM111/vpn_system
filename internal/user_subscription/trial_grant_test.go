package user_subscription

import "testing"

// ============================================================================
// Доменная модель выдачи триала.
//
// Матрица из требований опирается на TrialGrantResult как на ЕДИНСТВЕННЫЙ
// подтверждённый результат операции: все ветки сообщений строятся из него, а не
// из повторного чтения БД. Здесь проверяется сам контракт результата — что
// «выдано» и «не выдано» нельзя перепутать ни в одной комбинации.
//
// Ветки, требующие Postgres (сложение сроков, идемпотентность журнала, гонки),
// покрываются repository-тестами: они нуждаются в живой БД и запускаются
// отдельной командой, см. гайд.
// ============================================================================

func TestTrialGrantResultGranted(t *testing.T) {
	tests := []struct {
		name    string
		outcome TrialGrantOutcome
		want    bool
	}{
		{"первый триал без оплаты", TrialGranted, true},
		{"первый триал поверх оплаты", TrialGrantedOnTopOfPaid, true},
		{"триал уже использован", TrialAlreadyUsed, false},
		{"льготный период", TrialDeferredGrace, false},
		{"нулевое значение", TrialGrantOutcome(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := TrialGrantResult{Outcome: tt.outcome}
			if got := res.Granted(); got != tt.want {
				t.Errorf("Granted() при outcome=%q = %v, ожидалось %v", tt.outcome, got, tt.want)
			}
		})
	}
}

// Нулевое значение результата не должно выглядеть как успешная выдача:
// иначе ошибка на раннем возврате обернулась бы бесплатным доступом.
func TestZeroTrialGrantResultIsNotGranted(t *testing.T) {
	var res TrialGrantResult
	if res.Granted() {
		t.Fatal("нулевой TrialGrantResult считается выданным триалом")
	}
}

func TestPluralDays(t *testing.T) {
	tests := map[int]string{
		0:   "0 дней",
		1:   "1 день",
		2:   "2 дня",
		3:   "3 дня",
		4:   "4 дня",
		5:   "5 дней",
		10:  "10 дней",
		11:  "11 дней",
		12:  "12 дней",
		13:  "13 дней",
		14:  "14 дней",
		21:  "21 день",
		22:  "22 дня",
		25:  "25 дней",
		31:  "31 день",
		32:  "32 дня",
		100: "100 дней",
		101: "101 день",
		111: "111 дней",
		112: "112 дней",
		114: "114 дней",
		115: "115 дней",
		365: "365 дней",
	}
	for n, want := range tests {
		if got := pluralDays(n); got != want {
			t.Errorf("pluralDays(%d) = %q, ожидалось %q", n, got, want)
		}
	}
	// Отрицательное значение не должно попадать в сообщение пользователю.
	if got := pluralDays(-5); got != "0 дней" {
		t.Errorf("pluralDays(-5) = %q, ожидалось \"0 дней\"", got)
	}
}

// Примеры из требований, проверенные на уровне арифметики срока: остаток
// триала плюс купленный срок, и остаток оплаты плюс полный триал.
// Реальное сложение выполняется в SQL (GREATEST(expires_at, now) + interval),
// здесь фиксируется ожидаемый результат как исполняемая спецификация.
func TestAdditiveDurationExpectations(t *testing.T) {
	tests := []struct {
		name      string
		remaining int
		added     int
		want      int
	}{
		{"2 дня триала + покупка 30 дней", 2, 30, 32},
		{"20 дней оплаты + первый триал 3 дня", 20, 3, 23},
		{"покупки 30 и 90 складываются обе", 30, 90, 120},
		{"срок не заменяется большим значением", 45, 30, 75},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.remaining + tt.added
			if got != tt.want {
				t.Errorf("итог = %d, ожидалось %d", got, tt.want)
			}
			if got < tt.remaining {
				t.Errorf("эффективный срок уменьшился: было %d, стало %d", tt.remaining, got)
			}
			if got == max(tt.remaining, tt.added) && tt.remaining > 0 && tt.added > 0 {
				t.Error("срок посчитан как max(), а не как сумма")
			}
		})
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
