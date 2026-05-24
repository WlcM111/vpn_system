package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type correlationIDKey struct{}

func Init() {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))
}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	if strings.TrimSpace(id) == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationIDKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	v, _ := ctx.Value(correlationIDKey{}).(string)
	return v
}

func FromContext(ctx context.Context) *slog.Logger {
	id := CorrelationID(ctx)
	if id == "" {
		return slog.Default()
	}
	return slog.Default().With("correlation_id", id)
}
