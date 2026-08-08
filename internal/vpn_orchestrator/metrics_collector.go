package vpn_orchestrator

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	commonmetrics "vpn-platform/internal/common/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
)

// MetricsCollector периодически читает агрегаты из БД (и лаг из Kafka) и
// выставляет их как Prometheus gauge. Запускается фоном из main оркестратора.
//
// S4: метрики разделены на «лёгкие» (ноды, heartbeat, outbox depth, lock waits,
// connections — собираются часто, дёшевы) и «тяжёлые» (COUNT(*) по подпискам,
// крипто-инвойсам — собираются реже, т.к. на больших таблицах это дорого).
type MetricsCollector struct {
	pool          *pgxpool.Pool
	interval      time.Duration // лёгкие метрики
	heavyInterval time.Duration // тяжёлые агрегаты (COUNT(*))
	heartbeatTTL  time.Duration

	// S12: для сбора Kafka consumer lag. Если brokers пуст — лаг не собирается.
	kafkaBrokers []string
	lagTargets   []lagTarget
}

// lagTarget — пара (группа, топик), по которой считаем лаг.
type lagTarget struct {
	group string
	topic string
}

// NewMetricsCollector создаёт сборщик.
//
//	interval      — период лёгких метрик (рекомендуется 15s).
//	heavyInterval — период тяжёлых агрегатов (рекомендуется 60s).
//	heartbeatTTL  — порог свежести heartbeat ноды (та же величина, что у балансировщика).
//	kafkaBrokers  — брокеры для сбора consumer lag (nil/пусто — лаг не собирается).
func NewMetricsCollector(pool *pgxpool.Pool, interval, heavyInterval, heartbeatTTL time.Duration, kafkaBrokers []string) *MetricsCollector {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if heavyInterval <= 0 {
		heavyInterval = 60 * time.Second
	}
	if heartbeatTTL <= 0 {
		heartbeatTTL = 90 * time.Second
	}
	return &MetricsCollector{
		pool:          pool,
		interval:      interval,
		heavyInterval: heavyInterval,
		heartbeatTTL:  heartbeatTTL,
		kafkaBrokers:  kafkaBrokers,
		// Все consumer-группы и топики, которые они читают (см. main каждого сервиса).
		lagTargets: []lagTarget{
			{"vpn-orchestrator-service", commonkafka.TopicSubscriptionEvents},
			{"vpn-orchestrator-service", commonkafka.TopicVPNEvents},
			{"billing-service", commonkafka.TopicBillingCommands},
			{"billing-service", commonkafka.TopicSubscriptionEvents},
			{"user-subscription-service", commonkafka.TopicSubscriptionCommands},
			{"user-subscription-service", commonkafka.TopicBillingEvents},
			{"crypto-billing-service", commonkafka.TopicCryptoCommands},
		},
	}
}

// Run запускает циклы сбора до отмены контекста. Блокирующий — вызывать в горутине.
func (c *MetricsCollector) Run(ctx context.Context) {
	// первый сбор сразу, чтобы метрики появились без задержки
	c.collectLight(ctx)
	c.collectHeavy(ctx)

	lightTicker := time.NewTicker(c.interval)
	defer lightTicker.Stop()
	heavyTicker := time.NewTicker(c.heavyInterval)
	defer heavyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-lightTicker.C:
			c.collectLight(ctx)
		case <-heavyTicker.C:
			c.collectHeavy(ctx)
		}
	}
}

// collectLight — дешёвые метрики, собираются часто.
func (c *MetricsCollector) collectLight(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.collectNodes(cctx); err != nil {
		slog.Error("[metrics] collect nodes failed", "err", err)
	}
	if err := c.collectPoolItems(cctx); err != nil {
		slog.Error("[metrics] collect pool items failed", "err", err)
	}
	if err := c.collectOutboxDepth(cctx); err != nil {
		slog.Warn("[metrics] collect outbox depth failed", "err", err)
	}
	if err := c.collectPostgresHealth(cctx); err != nil {
		slog.Warn("[metrics] collect postgres health failed", "err", err)
	}
	c.collectKafkaLag(cctx)

	commonmetrics.MetricsCollectorLastRun.Set(float64(time.Now().Unix()))
}

// collectHeavy — дорогие агрегаты (COUNT(*)), собираются реже (S4).
func (c *MetricsCollector) collectHeavy(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := c.collectSubscriptions(cctx); err != nil {
		slog.Error("[metrics] collect subscriptions failed", "err", err)
	}
	if err := c.collectSubscriptionLifecycle(cctx); err != nil {
		slog.Error("[metrics] collect subscription lifecycle failed", "err", err)
	}
	if err := c.collectCryptoInvoices(cctx); err != nil {
		slog.Warn("[metrics] collect crypto invoices failed", "err", err)
	}
	if err := c.collectCryptoRevenue(cctx); err != nil {
		slog.Warn("[metrics] collect crypto revenue failed", "err", err)
	}
	if err := c.collectRenewedThisMonth(cctx); err != nil {
		slog.Warn("[metrics] collect renewed this month failed", "err", err)
	}
}

// collectRenewedThisMonth — сколько уникальных людей оплатили подписку в
// текущем календарном месяце. Считаем и карточные платежи, и крипто-инвойсы;
// один человек, оплативший обоими способами, учитывается один раз.
func (c *MetricsCollector) collectRenewedThisMonth(ctx context.Context) error {
	var renewed float64
	err := c.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT telegram_id
			FROM payments
			WHERE status = 'succeeded'
			  AND created_at >= date_trunc('month', now())
			UNION
			SELECT telegram_id
			FROM crypto_invoices
			WHERE status = 'paid'
			  AND COALESCE(paid_at, created_at) >= date_trunc('month', now())
		) AS renewed_users
	`).Scan(&renewed)
	if err != nil {
		return err
	}
	commonmetrics.SubscriptionsRenewedThisMonth.Set(renewed)
	return nil
}

// collectSubscriptions — пользователи по статусу подписки (тяжёлый COUNT).
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

// collectSubscriptionLifecycle считает подписки по ФАКТИЧЕСКОМУ сроку действия,
// а не по сырому status. Это устраняет завышение «Триалов»/«Активных» из-за
// ленивого истечения: протухшие строки остаются в своём status до обращения,
// а тут мы разделяем их по expires_at vs now().
//
//	kind=trial: status='trial'                    → active/expired по expires_at
//	kind=paid:  status IN ('active','grace')      → active/expired по expires_at
//
// total выставляется как active+expired (без отдельного запроса).
func (c *MetricsCollector) collectSubscriptionLifecycle(ctx context.Context) error {
	var (
		trialActive, trialExpired float64
		paidActive, paidExpired   float64
	)

	err := c.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE status = 'trial' AND expires_at IS NOT NULL AND expires_at > now()
			) AS trial_active,
			COUNT(*) FILTER (
				WHERE status = 'trial' AND (expires_at IS NULL OR expires_at <= now())
			) AS trial_expired,
			COUNT(*) FILTER (
				WHERE status IN ('active', 'grace') AND expires_at IS NOT NULL AND expires_at > now()
			) AS paid_active,
			COUNT(*) FILTER (
				WHERE status IN ('active', 'grace') AND (expires_at IS NULL OR expires_at <= now())
			) AS paid_expired
		FROM user_subscriptions
	`).Scan(&trialActive, &trialExpired, &paidActive, &paidExpired)
	if err != nil {
		return err
	}

	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "active").Set(trialActive)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "expired").Set(trialExpired)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "total").Set(trialActive + trialExpired)

	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "active").Set(paidActive)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "expired").Set(paidExpired)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "total").Set(paidActive + paidExpired)

	return nil
}

// collectNodes — нагрузка, вместимость, живость нод + агрегаты пула.
// S2: active_users считается агрегацией из vpn_user_node_credentials (LEFT JOIN),
// т.к. столбец vpn_servers.active_users больше не поддерживается.
func (c *MetricsCollector) collectNodes(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT s.server_key, s.country_code, s.title, s.enabled,
		       s.max_users,
		       COALESCE(a.active_users, 0) AS active_users,
		       s.last_heartbeat_at
		FROM vpn_servers s
		LEFT JOIN (
			SELECT server_key, COUNT(*) AS active_users
			FROM vpn_user_node_credentials
			WHERE enabled = true
			GROUP BY server_key
		) a ON a.server_key = s.server_key
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

// collectCryptoInvoices — крипто-инвойсы по статусу (тяжёлый COUNT).
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

// collectOutboxDepth — S12: глубина очереди outbox по статусам.
func (c *MetricsCollector) collectOutboxDepth(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM event_outbox GROUP BY status
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// сбрасываем известные статусы, чтобы исчезнувшие не залипали
	known := []string{"pending", "processing", "retry", "published", "failed"}
	for _, s := range known {
		commonmetrics.OutboxDepth.WithLabelValues(s).Set(0)
	}

	var failed float64
	for rows.Next() {
		var status string
		var cnt float64
		if err := rows.Scan(&status, &cnt); err != nil {
			return err
		}
		commonmetrics.OutboxDepth.WithLabelValues(status).Set(cnt)
		if status == "failed" {
			failed = cnt
		}
	}
	commonmetrics.OutboxFailedTotal.Set(failed)
	return rows.Err()
}

// collectPostgresHealth — S12: ожидания блокировок и занятость пула соединений.
func (c *MetricsCollector) collectPostgresHealth(ctx context.Context) error {
	var lockWaits float64
	if err := c.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'
	`).Scan(&lockWaits); err != nil {
		return err
	}
	commonmetrics.PostgresLockWaits.Set(lockWaits)

	stat := c.pool.Stat()
	commonmetrics.PostgresConnectionsInUse.Set(float64(stat.AcquiredConns()))
	return nil
}

// collectKafkaLag — S12: лаг consumer-групп по топикам. Если брокеры не заданы —
// тихо пропускаем (Kafka отключена или сбор лага не нужен).
func (c *MetricsCollector) collectKafkaLag(ctx context.Context) {
	if len(c.kafkaBrokers) == 0 {
		return
	}
	client := &kafkago.Client{Addr: kafkago.TCP(c.kafkaBrokers...)}

	for _, t := range c.lagTargets {
		lag, err := fetchConsumerLag(ctx, client, t.group, t.topic)
		if err != nil {
			slog.Debug("[metrics] kafka lag fetch failed", "group", t.group, "topic", t.topic, "err", err)
			continue
		}
		for partition, l := range lag {
			commonmetrics.KafkaConsumerLag.
				WithLabelValues(t.topic, t.group, strconv.Itoa(partition)).
				Set(float64(l))
		}
	}
}

// fetchConsumerLag возвращает лаг по каждой партиции топика для группы:
// lag = (последний offset партиции) − (закоммиченный группой offset).
func fetchConsumerLag(ctx context.Context, client *kafkago.Client, group, topic string) (map[int]int64, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 1) закоммиченные группой offset'ы по партициям
	offsetsResp, err := client.OffsetFetch(cctx, &kafkago.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: nil}, // nil → все партиции топика
	})
	if err != nil {
		return nil, err
	}
	committed := make(map[int]int64)
	for _, p := range offsetsResp.Topics[topic] {
		committed[p.Partition] = p.CommittedOffset
	}
	if len(committed) == 0 {
		return nil, nil
	}

	// 2) последние (log end) offset'ы тех же партиций
	reqOffsets := make(map[string][]kafkago.OffsetRequest)
	for partition := range committed {
		reqOffsets[topic] = append(reqOffsets[topic], kafkago.OffsetRequest{
			Partition: partition,
			Timestamp: kafkago.LastOffset,
		})
	}
	listResp, err := client.ListOffsets(cctx, &kafkago.ListOffsetsRequest{
		Topics: reqOffsets,
	})
	if err != nil {
		return nil, err
	}

	lag := make(map[int]int64)
	for _, po := range listResp.Topics[topic] {
		last := po.LastOffset
		comm, ok := committed[po.Partition]
		if !ok {
			continue
		}
		// если группа ещё ничего не коммитила (comm<0), лаг считаем от 0
		if comm < 0 {
			comm = 0
		}
		l := last - comm
		if l < 0 {
			l = 0
		}
		lag[po.Partition] = l
	}
	return lag, nil
}
