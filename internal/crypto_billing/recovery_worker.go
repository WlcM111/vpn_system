package crypto_billing

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"
)

// RunStuckInvoicesWorker — фоновый воркер для устранения "висящих" инвойсов в
// статусе 'creating'. Запускается из main.go одной горутиной.
//
// Сценарий, который он покрывает: транзакция 1 в HandleCreateCheckout создала
// запись со статусом 'creating', CryptoBot.CreateInvoice прошёл успешно
// (на их стороне инвойс реально создан), но процесс упал до того, как
// MarkInvoiceActiveTx обновил нашу запись. В этом случае инвойс висит в
// 'creating' навсегда, пользователь ничего не получает.
//
// Параметры через ENV:
//
//	CRYPTO_STUCK_INVOICES_TICK — как часто запускать (по умолчанию 1m).
//	CRYPTO_STUCK_INVOICES_THRESHOLD — после какого возраста считать застрявшим
//	  (по умолчанию 2m; HTTP-вызов CryptoBot обычно укладывается в секунду,
//	  2 минуты — заведомо безопасный порог).
func RunStuckInvoicesWorker(ctx context.Context, svc *Service) {
	tick := parseWorkerDuration("CRYPTO_STUCK_INVOICES_TICK", time.Minute)
	threshold := parseWorkerDuration("CRYPTO_STUCK_INVOICES_THRESHOLD", 2*time.Minute)
	batch := 50

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	slog.Info("crypto-billing stuck invoices worker started",
		"tick", tick.String(), "threshold", threshold.String(), "batch", batch)

	for {
		select {
		case <-ctx.Done():
			slog.Info("crypto-billing stuck invoices worker stopped")
			return
		case <-ticker.C:
			if err := svc.ProcessStuckCreatingInvoices(ctx, threshold, batch); err != nil {
				slog.Error("crypto-billing process stuck invoices failed", "err", err)
			}
		}
	}
}

// parseWorkerDuration — локальный helper для парсинга длительности из env.
// Поддерживает оба формата: time.Duration ("2m", "90s") и просто число секунд ("120").
// Имя умышленно отличается от parseDurationOr в config.go, чтобы не путать назначения.
func parseWorkerDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	if d, err := time.ParseDuration(v + "s"); err == nil && d > 0 {
		return d
	}
	return fallback
}
