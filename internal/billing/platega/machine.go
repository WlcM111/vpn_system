package platega

import (
	"strings"
	"time"
)

// ============================================================================
// Машина состояний заказа Platega.
//
// Локальный статус заказа (payments.status) меняется только здесь — по данным
// GET /transaction/{id}, а не по колбэку и не по возврату пользователя на
// return-URL. Колбэк лишь будит проверку. Поэтому порядок, дубли и опоздание
// колбэков не важны: каждое решение принимается по текущему авторитетному
// состоянию транзакции и текущему локальному состоянию заказа под блокировкой
// строки.
//
// Подписка выдаётся ровно один раз: Grant=true возможен только при первом
// подтверждении (paid_at IS NULL). Повторный CONFIRMED (например, после
// снятия спора DISPUTED → CONFIRMED) статус возвращает, но подписку повторно
// не выдаёт.
// ============================================================================

// LocalStatus — статус заказа Platega в таблице payments.
type LocalStatus string

const (
	// LocalCreating — строка заказа создана, к Platega ещё не обращались.
	LocalCreating LocalStatus = "local_creating"
	// LocalProviderCreating — запрос создания отправляется; отметка сделана
	// до HTTP-вызова. Повтор создания из этого статуса запрещён.
	LocalProviderCreating LocalStatus = "provider_creating"
	// LocalCreateFailed — Platega точно отказала (4xx) или запрос не ушёл.
	LocalCreateFailed LocalStatus = "create_failed"
	// LocalCreateUnknown — исход создания неизвестен (таймаут после
	// отправки, 5xx, неразборчивый ответ). Ссылку пользователь не получал.
	LocalCreateUnknown LocalStatus = "create_unknown"
	// LocalPending — транзакция создана, ждём оплату.
	LocalPending LocalStatus = "pending"
	// LocalSucceeded — оплата подтверждена, подписка выдана.
	LocalSucceeded LocalStatus = "succeeded"
	// LocalCanceled — транзакция отменена/истекла. Может стать CONFIRMED
	// (документация допускает CANCELED → CONFIRMED), поэтому проверки
	// продолжаются с затухающей частотой.
	LocalCanceled LocalStatus = "canceled"
	// LocalReviewRequired — данные транзакции не совпали с заказом или
	// переход недопустим. Автоматически из него не выходим.
	LocalReviewRequired LocalStatus = "review_required"
	// LocalDisputed — по платежу открыт спор.
	LocalDisputed LocalStatus = "disputed"
	// LocalChargebacked — чарджбэк, терминальный статус.
	LocalChargebacked LocalStatus = "chargebacked"
	// LocalPartiallyRefunded — частичный возврат после выдачи подписки.
	LocalPartiallyRefunded LocalStatus = "partially_refunded"
)

// AllLocalStatuses — полный перечень; совпадает с CHECK-ограничением
// миграции billing-service/0006 (сверяется тестом).
var AllLocalStatuses = []LocalStatus{
	LocalCreating, LocalProviderCreating, LocalCreateFailed, LocalCreateUnknown,
	LocalPending, LocalSucceeded, LocalCanceled, LocalReviewRequired,
	LocalDisputed, LocalChargebacked, LocalPartiallyRefunded,
}

// Snapshot — локальное состояние заказа, прочитанное под блокировкой строки.
type Snapshot struct {
	OrderID string
	// ExpectedPayload — payload, с которым создавалась транзакция (order_id
	// исходного заказа).
	ExpectedPayload string
	PaymentID       string
	Status          LocalStatus
	ProviderStatus  Status
	AmountMinor     int64
	Currency        string
	// Granted — paid_at IS NOT NULL: событие об оплате уже записано в outbox.
	Granted bool
	// LinkSent — пользователь получил ссылку на оплату (confirmation_url).
	LinkSent  bool
	CreatedAt time.Time
}

// Decision — что сделать с заказом.
type Decision struct {
	Status         LocalStatus
	ProviderStatus Status
	// Grant — записать BillingPaymentSucceededEvent и paid_at.
	Grant bool
	// NotifyCanceled — сообщить пользователю об отмене счёта.
	NotifyCanceled bool
	// NotifyReview — сообщить, что поступивший платёж проверяется вручную.
	NotifyReview bool
	// NextCheckIn — через сколько проверить транзакцию снова; 0 — не
	// проверять по расписанию (колбэк всё равно разбудит проверку).
	NextCheckIn time.Duration
	// Reason — машинный код причины для review_reason и журнала.
	Reason string
	// Changed — изменился статус, статус провайдера или нужна выдача.
	Changed bool
}

// Decide вычисляет решение по авторитетному состоянию транзакции.
func Decide(s Snapshot, tx Transaction, now time.Time) Decision {
	age := now.Sub(s.CreatedAt)
	if age < 0 {
		age = 0
	}
	d := Decision{Status: s.Status, ProviderStatus: tx.Status}

	finish := func() Decision {
		d.Changed = d.Status != s.Status || d.ProviderStatus != s.ProviderStatus || d.Grant
		return d
	}

	// Ручная очередь: решение принимает человек (см. docs/payments-platega.md).
	if s.Status == LocalReviewRequired {
		d.Reason = "review_sticky"
		return finish()
	}

	if reason := verify(s, tx); reason != "" {
		d.Status = LocalReviewRequired
		d.Reason = reason
		// Деньги, возможно, получены, а выдать подписку автоматически нельзя.
		d.NotifyReview = !s.Granted && tx.Status == StatusConfirmed
		return finish()
	}

	if s.Status == LocalChargebacked && tx.Status != StatusChargebacked {
		// CHARGEBACKED по документации терминален; любой выход из него — аномалия.
		d.Status = LocalReviewRequired
		d.Reason = "transition_after_chargeback"
		return finish()
	}

	switch tx.Status {
	case StatusConfirmed:
		d.Status = LocalSucceeded
		d.Grant = !s.Granted

	case StatusPending:
		if s.Granted {
			// Подтверждённая и выданная транзакция не может вернуться в PENDING.
			// Состояние не трогаем, расписание не продолжаем.
			d.Reason = "pending_after_grant_ignored"
			d.ProviderStatus = s.ProviderStatus
			break
		}
		d.Status = LocalPending
		d.NextCheckIn = pendingDelay(age)

	case StatusCanceled:
		if s.Granted {
			d.Status = LocalReviewRequired
			d.Reason = "canceled_after_grant"
			break
		}
		d.Status = LocalCanceled
		d.NotifyCanceled = s.Status == LocalPending && s.LinkSent
		d.NextCheckIn = canceledDelay(age)

	case StatusDisputed:
		d.Status = LocalDisputed
		d.NextCheckIn = disputedDelay(age)

	case StatusChargebacked:
		d.Status = LocalChargebacked

	case StatusPartiallyRefunded:
		if s.Granted {
			d.Status = LocalPartiallyRefunded
			break
		}
		d.Status = LocalReviewRequired
		d.Reason = "partially_refunded_without_grant"
		d.NotifyReview = true

	default:
		d.Status = LocalReviewRequired
		d.Reason = "unknown_provider_status"
	}
	return finish()
}

// verify сверяет транзакцию с заказом. Пустая строка — совпадает.
func verify(s Snapshot, tx Transaction) string {
	if tx.ID != "" && !SameID(tx.ID, s.PaymentID) {
		return "id_mismatch"
	}
	expected := s.ExpectedPayload
	if expected == "" {
		expected = s.OrderID
	}
	// Пустой payload в ответе допустим: связь заказа с транзакцией уже
	// доказана тем, что ID получен из нашего же запроса создания (или заказ
	// сопоставлен по payload до вызова Decide).
	if tx.Payload != "" && tx.Payload != expected {
		return "payload_mismatch"
	}
	if tx.AmountMinor != s.AmountMinor {
		return "amount_mismatch"
	}
	if !strings.EqualFold(strings.TrimSpace(tx.Currency), strings.TrimSpace(s.Currency)) {
		return "currency_mismatch"
	}
	return ""
}

// pendingDelay — частота опроса неоплаченной транзакции. Колбэк — основной
// сигнал; опрос страхует от потерянных колбэков (Platega повторяет их около
// трёх раз и перестаёт).
func pendingDelay(age time.Duration) time.Duration {
	switch {
	case age < 30*time.Minute:
		return time.Minute
	case age < 2*time.Hour:
		return 5 * time.Minute
	case age < 24*time.Hour:
		return 30 * time.Minute
	case age < 7*24*time.Hour:
		return 6 * time.Hour
	case age < 30*24*time.Hour:
		return 24 * time.Hour
	default:
		return 0
	}
}

// canceledDelay — проверка отменённой транзакции на позднее подтверждение.
func canceledDelay(age time.Duration) time.Duration {
	switch {
	case age < time.Hour:
		return 10 * time.Minute
	case age < 24*time.Hour:
		return 2 * time.Hour
	case age < 7*24*time.Hour:
		return 12 * time.Hour
	default:
		return 0
	}
}

// disputedDelay — спор разрешается долго; проверяем дважды в сутки.
func disputedDelay(age time.Duration) time.Duration {
	if age < 120*24*time.Hour {
		return 12 * time.Hour
	}
	return 0
}

// RetryDelay — пауза перед повтором проверки после ошибки обращения к API:
// 30 с, 1, 2, 4 … минут, не больше часа.
func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := 30 * time.Second
	for i := 1; i < attempt && d < time.Hour; i++ {
		d *= 2
	}
	if d > time.Hour {
		d = time.Hour
	}
	return d
}
