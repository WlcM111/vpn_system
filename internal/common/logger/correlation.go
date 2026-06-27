package logger

import (
	"context"

	"github.com/google/uuid"
)

// ============================================================================
// Correlation ID (trace_id) для сквозной трассировки запроса через сервисы.
// Прокидывается через context и Kafka-события, логируется в каждом сервисе.
// ============================================================================

type ctxKey string

const traceIDKey ctxKey = "trace_id"

// NewTraceID генерирует новый trace_id.
func NewTraceID() string {
	return uuid.NewString()
}

// WithTraceID кладёт trace_id в context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = NewTraceID()
	}
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFromContext достаёт trace_id из context (или "" если нет).
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}
