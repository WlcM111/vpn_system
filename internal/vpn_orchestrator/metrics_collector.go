package vpn_orchestrator

import (
	"context"
	"log/slog"
	"time"

	commonmetrics "vpn-platform/internal/common/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MetricsCollector периодически читает агрегаты из БД и выставляет их
// как Prometheus gauge. Запускается фоном из main оркестратора.
//
// Почему здесь: оркестратор уже владеет данными о нодах (vpn_servers) и имеет
// доступ к общей БД vpn_platform, где лежат подписки и крипто-инвойсы. Отдельный
// сервис ради метрик не нужен — это лишняя инфраструктура.
type MetricsCollector struct {
	pool         *pgxpool.Pool
	interval     time.Duration
	heartbeatTTL time.Duration
}

// NewMetricsCollector создаёт сборщик.
//
//	interval     — как часто опрашивать БД (рекомендуется 15s).
//	heartbeatTTL — порог свежести heartbeat ноды (та же величина, что у балансировщика).
func NewMetricsCollector(pool *pgxpool.Pool, interval, heartbeatTTL time.Duration) *MetricsCollector {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if heartbeatTTL <= 0 {
		heartbeatTTL = 90 * time.Second
	}
	return &MetricsCollector{pool: pool, interval: interval, heartbeatTTL: heartbeatTTL}
}

// Run запускает цикл сбора до отмены контекста. Блокирующий — вызывать в горутине.
func (c *MetricsCollector) Run(ctx context.Context) {
	// первый сбор сразу, чтобы метрики появились без задержки
	c.collectOnce(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collectOnce(ctx)
		}
	}
}

func (c *MetricsCollector) collectOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.collectSubscriptions(cctx); err != nil {
		slog.Error("[metrics] collect subscriptions failed", "err", err)
	}
	if err := c.collectNodes(cctx); err != nil {
		slog.Error("[metrics] collect nodes failed", "err", err)
	}
	if err := c.collectPoolItems(cctx); err != nil {
		slog.Error("[metrics] collect pool items failed", "err", err)
	}
	if err := c.collectCryptoInvoices(cctx); err != nil {
		// крипто-инвойсы не критичны для общей картины — только лог
		slog.Warn("[metrics] collect crypto invoices failed", "err", err)
	}
	if err := c.collectCryptoRevenue(cctx); err != nil {
		slog.Warn("[metrics] collect crypto revenue failed", "err", err)
	}

	commonmetrics.MetricsCollectorLastRun.Set(float64(time.Now().Unix()))
}

// collectSubscriptions — пользователи по статусу подписки.
func (c *MetricsCollector) collectSubscriptions(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM user_subscriptions GROUP BY status
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// сбрасываем известные статусы в 0, чтобы исчезнувшие группы не "залипали"
	known := []string{"none", "trial", "active", "grace", "expired"}
	for _, s := range known {
		commonmetrics.SubscriptionsByStatus.WithLabelValues(s).Set(0)
	}

	for rows.Next() {
		var status string
		var cnt float64
		if err := rows.Scan(&status, &cnt); err != nil {
			return err
		}
		commonmetrics.SubscriptionsByStatus.WithLabelValues(status).Set(cnt)
	}
	return rows.Err()
}

// collectNodes — нагрузка, вместимость, живость нод + агрегаты пула.
func (c *MetricsCollector) collectNodes(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT server_key, country_code, title, enabled,
		       max_users, active_users, last_heartbeat_at
		FROM vpn_servers
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// перед перезаписью сбрасываем per-node gauge, чтобы удалённые ноды не залипали.
	// (Prometheus client не умеет "забывать" лейблы сам — Reset чистит весь вектор.)
	commonmetrics.NodeActiveUsers.Reset()
	commonmetrics.NodeMaxUsers.Reset()
	commonmetrics.NodeLoadPercent.Reset()
	commonmetrics.NodeUp.Reset()
	commonmetrics.NodeHeartbeatAgeSeconds.Reset()

	var (
		totalEnabled, totalDisabled, totalAlive, totalStale int
		capacityTotal, activeTotal                          float64
	)
	now := time.Now()

	for rows.Next() {
		var (
			serverKey, country, title string
			enabled                   bool
			maxUsers, activeUsers     int
			lastHB                    *time.Time
		)
		if err := rows.Scan(&serverKey, &country, &title, &enabled, &maxUsers, &activeUsers, &lastHB); err != nil {
			return err
		}

		lbl := []string{serverKey, country, title}

		commonmetrics.NodeActiveUsers.WithLabelValues(lbl...).Set(float64(activeUsers))
		commonmetrics.NodeMaxUsers.WithLabelValues(lbl...).Set(float64(maxUsers))

		loadPct := 0.0
		if maxUsers > 0 {
			loadPct = float64(activeUsers) / float64(maxUsers) * 100.0
		}
		commonmetrics.NodeLoadPercent.WithLabelValues(lbl...).Set(loadPct)

		// живость по heartbeat
		alive := false
		if lastHB != nil {
			age := now.Sub(*lastHB)
			commonmetrics.NodeHeartbeatAgeSeconds.WithLabelValues(lbl...).Set(age.Seconds())
			if age <= c.heartbeatTTL {
				alive = true
			}
		} else {
			// нет heartbeat вообще — считаем возраст очень большим
			commonmetrics.NodeHeartbeatAgeSeconds.WithLabelValues(lbl...).Set(-1)
		}
		if alive {
			commonmetrics.NodeUp.WithLabelValues(lbl...).Set(1)
		} else {
			commonmetrics.NodeUp.WithLabelValues(lbl...).Set(0)
		}

		// агрегаты
		if enabled {
			totalEnabled++
			capacityTotal += float64(maxUsers)
			activeTotal += float64(activeUsers)
		} else {
			totalDisabled++
		}
		if alive {
			totalAlive++
		} else {
			totalStale++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	commonmetrics.NodesCount.WithLabelValues("enabled").Set(float64(totalEnabled))
	commonmetrics.NodesCount.WithLabelValues("disabled").Set(float64(totalDisabled))
	commonmetrics.NodesCount.WithLabelValues("alive").Set(float64(totalAlive))
	commonmetrics.NodesCount.WithLabelValues("stale").Set(float64(totalStale))
	commonmetrics.PoolCapacityTotal.Set(capacityTotal)
	commonmetrics.PoolActiveTotal.Set(activeTotal)

	return nil
}

// collectPoolItems — профили в пуле по доступности.
func (c *MetricsCollector) collectPoolItems(ctx context.Context) error {
	var enabledCnt, disabledCnt float64
	err := c.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE enabled),
			COUNT(*) FILTER (WHERE NOT enabled)
		FROM vpn_pool_items
	`).Scan(&enabledCnt, &disabledCnt)
	if err != nil {
		return err
	}
	commonmetrics.PoolItemsCount.WithLabelValues("enabled").Set(enabledCnt)
	commonmetrics.PoolItemsCount.WithLabelValues("disabled").Set(disabledCnt)
	return nil
}

// collectCryptoInvoices — крипто-инвойсы по статусу.
func (c *MetricsCollector) collectCryptoInvoices(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM crypto_invoices GROUP BY status
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	commonmetrics.CryptoInvoicesByStatus.Reset()
	for rows.Next() {
		var status string
		var cnt float64
		if err := rows.Scan(&status, &cnt); err != nil {
			return err
		}
		commonmetrics.CryptoInvoicesByStatus.WithLabelValues(status).Set(cnt)
	}
	return rows.Err()
}

// collectCryptoRevenue — суммы оплаченных крипто-инвойсов по активу + общее число.
func (c *MetricsCollector) collectCryptoRevenue(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT asset, COALESCE(SUM(amount_value::numeric), 0), COUNT(*)
		FROM crypto_invoices
		WHERE status = 'paid'
		GROUP BY asset
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	commonmetrics.CryptoRevenueTotal.Reset()
	var paidTotal float64
	for rows.Next() {
		var asset string
		var sum, cnt float64
		if err := rows.Scan(&asset, &sum, &cnt); err != nil {
			return err
		}
		commonmetrics.CryptoRevenueTotal.WithLabelValues(asset).Set(sum)
		paidTotal += cnt
	}
	commonmetrics.CryptoPaidCount.Set(paidTotal)
	return rows.Err()
}
