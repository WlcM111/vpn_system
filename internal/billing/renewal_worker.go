package billing

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

func RunRenewalScheduler(ctx context.Context, svc *Service) {
	tick := parseDurationEnv("BILLING_RENEWAL_TICK", time.Minute)
	batchSize := parseIntEnv("BILLING_RENEWAL_BATCH_SIZE", 50)

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("billing renewal scheduler stopped")
			return
		case <-ticker.C:
			if err := svc.ProcessDueRenewals(ctx, batchSize); err != nil {
				slog.Error("billing ProcessDueRenewals failed", "err", err)
			}
		}
	}
}

func RunGraceExpirationWorker(ctx context.Context, svc *Service) {
	tick := parseDurationEnv("BILLING_GRACE_EXPIRATION_TICK", time.Minute)
	batchSize := parseIntEnv("BILLING_GRACE_EXPIRATION_BATCH_SIZE", 50)

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("billing grace expiration worker stopped")
			return
		case <-ticker.C:
			if err := svc.ProcessExpiredGrace(ctx, batchSize); err != nil {
				slog.Error("billing ProcessExpiredGrace failed", "err", err)
			}
		}
	}
}

func parseIntEnv(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func RunRefundRetryWorker(ctx context.Context, svc *Service) {
	tick := parseDurationEnv("BILLING_REFUND_RETRY_TICK", time.Minute)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("billing refund retry worker stopped")
			return
		case <-ticker.C:
			if err := svc.ProcessPendingRefunds(ctx, 50); err != nil {
				slog.Error("billing ProcessPendingRefunds failed", "err", err)
			}
		}
	}
}
