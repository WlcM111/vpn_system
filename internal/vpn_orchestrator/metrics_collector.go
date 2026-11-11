package vpn_orchestrator

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	commonmetrics "vpn-platform/internal/common/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
)

// MetricsCollector периодически читает агрегаты из БД (и лаг из Kafka) и
// выставляет их как Prometheus gauge. Запускается фоном из main оркестратора.
type MetricsCollector struct {
	pool          *pgxpool.Pool
	interval      time.Duration // лёгкие метрики
	heavyInterval time.Duration // тяжёлые агрегаты (COUNT(*))
	heartbeatTTL  time.Duration

	kafkaBrokers []string
	lagTargets   []lagTarget
}

// Параметры TCP-проверки VPN-портов нод в collectNodes: таймаут одной
// попытки и число нод, проверяемых одновременно.
const (
	nodeProbeTimeout     = 1500 * time.Millisecond
	nodeProbeParallelism = 16
)

type lagTarget struct {
	group string
	topic string
}

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

func (c *MetricsCollector) Run(ctx context.Context) {
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
	if err := c.collectSegments(cctx); err != nil {
		slog.Warn("[metrics] collect segments failed", "err", err)
	}
}

func (c *MetricsCollector) collectSegments(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT segment, count(*)
		FROM (
			SELECT
				CASE
					WHEN us.status = 'none' THEN 'never_started'
					WHEN us.status IN ('trial','active','grace')
					     AND (us.expires_at IS NULL OR us.expires_at <= now())
					     THEN 'pending_revoke'
					WHEN us.status = 'trial' AND EXISTS (
						SELECT 1 FROM payments p
						WHERE p.telegram_id = us.telegram_id AND p.status = 'succeeded'
					) THEN 'trial_after_paid'
					WHEN us.status = 'trial' THEN 'trial_new'
					WHEN us.status IN ('active','grace') THEN 'paid_active'
					WHEN us.status = 'expired' AND EXISTS (
						SELECT 1 FROM payments p
						WHERE p.telegram_id = us.telegram_id AND p.status = 'succeeded'
					) THEN 'churned_paid'
					ELSE 'churned_trial'
				END AS segment
			FROM user_subscriptions us
		) t
		GROUP BY segment
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	found := map[string]float64{
		"never_started":    0,
		"trial_new":        0,
		"trial_after_paid": 0,
		"paid_active":      0,
		"churned_trial":    0,
		"churned_paid":     0,
		"pending_revoke":   0,
	}
	for rows.Next() {
		var segment string
		var cnt float64
		if err := rows.Scan(&segment, &cnt); err != nil {
			return err
		}
		found[segment] = cnt
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for segment, cnt := range found {
		commonmetrics.SubscriptionsBySegment.WithLabelValues(segment).Set(cnt)
	}
	return nil
}

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

func (c *MetricsCollector) collectSubscriptions(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM user_subscriptions GROUP BY status
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

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

func (c *MetricsCollector) collectSubscriptionLifecycle(ctx context.Context) error {
	var (
		trialActive, trialExpired float64
		paidActive, paidExpired   float64
		trialChurned, paidChurned float64
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
			) AS paid_expired,
			COUNT(*) FILTER (
				WHERE status = 'expired' AND NOT EXISTS (
					SELECT 1 FROM payments p
					WHERE p.telegram_id = us.telegram_id AND p.status = 'succeeded'
				)
			) AS trial_churned,
			COUNT(*) FILTER (
				WHERE status = 'expired' AND EXISTS (
					SELECT 1 FROM payments p
					WHERE p.telegram_id = us.telegram_id AND p.status = 'succeeded'
				)
			) AS paid_churned
		FROM user_subscriptions us
	`).Scan(&trialActive, &trialExpired, &paidActive, &paidExpired, &trialChurned, &paidChurned)
	if err != nil {
		return err
	}

	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "active").Set(trialActive)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "expired").Set(trialExpired)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "total").Set(trialActive + trialExpired)

	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "active").Set(paidActive)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "expired").Set(paidExpired)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "total").Set(paidActive + paidExpired)

	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("trial", "churned").Set(trialChurned)
	commonmetrics.SubscriptionsByLifecycle.WithLabelValues("paid", "churned").Set(paidChurned)

	return nil
}

// collectNodes — нагрузка, вместимость, живость нод (с различением «заблокировано»
// и «упало») + агрегаты пула.
//
// Порядок: сначала строки читаются из БД целиком (соединение пула не держится
// на время сетевых проверок), затем TCP-пробы всех включённых нод идут
// параллельно, и только после этого gauge сбрасываются и выставляются заново.
// Последовательные пробы внутри открытого курсора укладывались бы в общий
// 10-секундный дедлайн сбора лишь при паре нод: на заблокированных нодах
// каждая попытка ждёт таймаут, дедлайн истекал, оставшиеся ноды получали
// ложное «недоступен», а следующие метрики (пул, outbox, Postgres, лаг Kafka)
// не собирались.
func (c *MetricsCollector) collectNodes(ctx context.Context) error {
	probeTargets := make(map[string][]string)
	if rRows, rErr := c.pool.Query(ctx, `
		SELECT COALESCE(server_key, ''), address, port
		FROM vpn_reality_endpoints
		WHERE enabled = true AND server_key IS NOT NULL AND server_key <> ''
	`); rErr == nil {
		for rRows.Next() {
			var sk, addr string
			var port int
			if scanErr := rRows.Scan(&sk, &addr, &port); scanErr == nil {
				if hp := formatHostPort(addr, port, 8443); hp != "" {
					probeTargets[sk] = append(probeTargets[sk], hp)
				}
			}
		}
		rRows.Close()
	} else {
		slog.Warn("[metrics] load reality endpoints for node probes failed", "err", rErr)
	}

	if xRows, xErr := c.pool.Query(ctx, `
		SELECT COALESCE(server_key, ''), address, port
		FROM vpn_xhttp_endpoints
		WHERE enabled = true AND server_key IS NOT NULL AND server_key <> ''
	`); xErr == nil {
		for xRows.Next() {
			var sk, addr string
			var port int
			if scanErr := xRows.Scan(&sk, &addr, &port); scanErr == nil {
				if hp := formatHostPort(addr, port, 443); hp != "" {
					probeTargets[sk] = append(probeTargets[sk], hp)
				}
			}
		}
		xRows.Close()
	} else {
		slog.Warn("[metrics] load xhttp endpoints for node probes failed", "err", xErr)
	}

	type nodeRow struct {
		serverKey, country, title  string
		enabled                    bool
		maxUsers, activeUsers      int
		lastHB                     *time.Time
		onlineUsers, bandwidthMbps int
		uplinkBps, downlinkBps     int64
		publicHost                 string
		port                       int

		probeChecked, probeAlive bool
	}

	rows, err := c.pool.Query(ctx, `
		SELECT s.server_key, s.country_code, s.title, s.enabled,
			s.max_users,
			COALESCE(a.active_users, 0) AS active_users,
			s.last_heartbeat_at,
			s.online_users, s.uplink_bps, s.downlink_bps, s.bandwidth_mbps,
			s.public_host, s.port
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
	var nodes []nodeRow
	for rows.Next() {
		var n nodeRow
		if err := rows.Scan(&n.serverKey, &n.country, &n.title, &n.enabled, &n.maxUsers, &n.activeUsers, &n.lastHB,
			&n.onlineUsers, &n.uplinkBps, &n.downlinkBps, &n.bandwidthMbps, &n.publicHost, &n.port); err != nil {
			rows.Close()
			return err
		}
		nodes = append(nodes, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// TCP-проверка VPN-портов (Reality, XHTTP и основной публичный адрес) всех
	// включённых нод — параллельно, не больше nodeProbeParallelism за раз.
	probeAt := time.Now().UTC()
	sem := make(chan struct{}, nodeProbeParallelism)
	var wg sync.WaitGroup
	for i := range nodes {
		n := &nodes[i]
		if !n.enabled {
			continue
		}
		targets := append([]string(nil), probeTargets[n.serverKey]...)
		if hp := formatHostPort(n.publicHost, n.port, 443); hp != "" {
			targets = append(targets, hp)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			n.probeChecked, n.probeAlive = ProbeTCPTargets(ctx, targets, nodeProbeTimeout)
		}()
	}
	wg.Wait()

	commonmetrics.NodeActiveUsers.Reset()
	commonmetrics.NodeMaxUsers.Reset()
	commonmetrics.NodeLoadPercent.Reset()
	commonmetrics.NodeUp.Reset()
	commonmetrics.NodeBlocked.Reset()
	commonmetrics.NodeProbeUp.Reset()
	commonmetrics.NodeHealthState.Reset()
	commonmetrics.NodeHeartbeatAgeSeconds.Reset()
	commonmetrics.NodeTrafficBps.Reset()
	commonmetrics.NodeOnlineUsers.Reset()
	commonmetrics.NodeBandwidthPercent.Reset()
	commonmetrics.NodeBandwidthConfigured.Reset()
	commonmetrics.NodeBandwidthMbps.Reset()

	var (
		totalEnabled, totalDisabled, totalAlive, totalStale, totalBlocked, totalDown int
		capacityTotal, activeTotal                                                   float64
	)
	now := time.Now()

	for _, n := range nodes {
		if !n.enabled {
			totalDisabled++
			continue
		}

		lbl := []string{n.serverKey, n.country, n.title}

		commonmetrics.NodeOnlineUsers.WithLabelValues(lbl...).Set(float64(n.onlineUsers))
		commonmetrics.NodeTrafficBps.WithLabelValues(n.serverKey, n.country, n.title, "uplink").Set(float64(n.uplinkBps))
		commonmetrics.NodeTrafficBps.WithLabelValues(n.serverKey, n.country, n.title, "downlink").Set(float64(n.downlinkBps))

		bwPct := 0.0
		configured := 0.0
		if n.bandwidthMbps > 0 {
			bwPct = float64(n.uplinkBps+n.downlinkBps) / (float64(n.bandwidthMbps) * 1_000_000) * 100.0
			configured = 1.0
		}
		commonmetrics.NodeBandwidthPercent.WithLabelValues(lbl...).Set(bwPct)
		commonmetrics.NodeBandwidthConfigured.WithLabelValues(lbl...).Set(configured)
		commonmetrics.NodeBandwidthMbps.WithLabelValues(lbl...).Set(float64(n.bandwidthMbps))

		commonmetrics.NodeActiveUsers.WithLabelValues(lbl...).Set(float64(n.activeUsers))
		commonmetrics.NodeMaxUsers.WithLabelValues(lbl...).Set(float64(n.maxUsers))

		loadPct := 0.0
		if n.maxUsers > 0 {
			loadPct = float64(n.activeUsers) / float64(n.maxUsers) * 100.0
		}
		commonmetrics.NodeLoadPercent.WithLabelValues(lbl...).Set(loadPct)

		// 1. Проверка свежести Kafka-heartbeat
		hbAlive := false
		if n.lastHB != nil {
			age := now.Sub(*n.lastHB)
			commonmetrics.NodeHeartbeatAgeSeconds.WithLabelValues(lbl...).Set(age.Seconds())
			if age <= c.heartbeatTTL {
				hbAlive = true
			}
		} else {
			commonmetrics.NodeHeartbeatAgeSeconds.WithLabelValues(lbl...).Set(-1)
		}

		// 2. Результат TCP-проверки VPN-портов ноды (см. выше)
		if n.probeChecked {
			RecordNodeProbe(n.serverKey, n.probeAlive, probeAt)
		}

		state, isUp, isBlocked := ClassifyNodeHealth(hbAlive, n.probeChecked, n.probeAlive)

		if isUp {
			commonmetrics.NodeUp.WithLabelValues(lbl...).Set(1)
		} else {
			commonmetrics.NodeUp.WithLabelValues(lbl...).Set(0)
		}
		if isBlocked {
			commonmetrics.NodeBlocked.WithLabelValues(lbl...).Set(1)
		} else {
			commonmetrics.NodeBlocked.WithLabelValues(lbl...).Set(0)
		}
		if n.probeAlive {
			commonmetrics.NodeProbeUp.WithLabelValues(lbl...).Set(1)
		} else {
			commonmetrics.NodeProbeUp.WithLabelValues(lbl...).Set(0)
		}
		for _, st := range allNodeHealthStates {
			val := 0.0
			if st == state {
				val = 1.0
			}
			commonmetrics.NodeHealthState.WithLabelValues(n.serverKey, n.country, n.title, string(st)).Set(val)
		}

		totalEnabled++
		capacityTotal += float64(n.maxUsers)
		activeTotal += float64(n.activeUsers)
		if isUp {
			totalAlive++
		} else {
			totalStale++
			totalDown++
		}
		if isBlocked {
			totalBlocked++
		}
	}

	commonmetrics.NodesCount.WithLabelValues("enabled").Set(float64(totalEnabled))
	commonmetrics.NodesCount.WithLabelValues("disabled").Set(float64(totalDisabled))
	commonmetrics.NodesCount.WithLabelValues("alive").Set(float64(totalAlive))
	commonmetrics.NodesCount.WithLabelValues("stale").Set(float64(totalStale))
	commonmetrics.NodesCount.WithLabelValues("blocked").Set(float64(totalBlocked))
	commonmetrics.NodesCount.WithLabelValues("down").Set(float64(totalDown))
	commonmetrics.PoolCapacityTotal.Set(capacityTotal)
	commonmetrics.PoolActiveTotal.Set(activeTotal)

	return nil
}

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

func (c *MetricsCollector) collectOutboxDepth(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM event_outbox GROUP BY status
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

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

func (c *MetricsCollector) collectKafkaLag(ctx context.Context) {
	if len(c.kafkaBrokers) == 0 {
		return
	}
	client := &kafkago.Client{
		Addr:      kafkago.TCP(c.kafkaBrokers...),
		Transport: commonkafka.TransportFromEnv(),
	}

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

func fetchConsumerLag(ctx context.Context, client *kafkago.Client, group, topic string) (map[int]int64, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	offsetsResp, err := client.OffsetFetch(cctx, &kafkago.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: nil},
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
