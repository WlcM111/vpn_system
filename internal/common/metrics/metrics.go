package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---------------------------------------------------------------------------
// Технические метрики (поток сообщений, HTTP, длительности обработчиков).
// Инкрементируются в коде сервисов по мере обработки.
// ---------------------------------------------------------------------------

var (
	KafkaConsumedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_kafka_consumed_total",
		Help: "Kafka consumed messages by topic and status.",
	}, []string{"topic", "status"})

	KafkaPublishedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_kafka_published_total",
		Help: "Kafka published messages by topic and status.",
	}, []string{"topic", "status"})

	HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_http_requests_total",
		Help: "HTTP requests by route, method and status.",
	}, []string{"route", "method", "status"})

	SubscriptionActivationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_subscription_activations_total",
		Help: "Subscription activations actually applied to the database.",
	}, []string{"source", "result"})

	SubscriptionBonusGrantsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_subscription_bonus_grants_total",
		Help: "Operator bonus grants by outcome.",
	}, []string{"outcome"})

	BillingPaymentsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_billing_payments_total",
		Help: "Billing payment outcomes as reported by the provider, before activation.",
	}, []string{"outcome"})

	BillingPlategaRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_billing_platega_requests_total",
		Help: "Platega API calls by operation and result.",
	}, []string{"op", "result"})

	BillingPlategaCallbacksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_billing_platega_callbacks_total",
		Help: "Platega callbacks by handling result.",
	}, []string{"result"})

	BillingPlategaPaymentsByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_billing_platega_payments",
		Help: "Platega orders by local status.",
	}, []string{"status"})

	BillingPlategaOverdueChecks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_billing_platega_overdue_checks",
		Help: "Platega orders whose status check is overdue by more than 10 minutes.",
	})

	BillingPlategaReconcileLastRun = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_billing_platega_reconcile_last_run_timestamp",
		Help: "Unix timestamp of the last Platega reconcile pass.",
	})

	KafkaHandlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vpn_platform_kafka_handler_duration_seconds",
		Help:    "Kafka handler duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})
)

// ---------------------------------------------------------------------------
// Бизнес-метрики (gauge). Выставляются фоновым сборщиком из БД
// (см. internal/vpn_orchestrator/metrics_collector.go).
// ---------------------------------------------------------------------------

var (
	SubscriptionsByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_total",
		Help: "Number of user subscriptions by status.",
	}, []string{"status"})

	SubscriptionsByLifecycle = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_lifecycle_total",
		Help: "User subscriptions by kind (trial/paid) and lifecycle (active/expired/total), based on expires_at vs now().",
	}, []string{"kind", "lifecycle"})

	SubscriptionsBySegment = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_segment",
		Help: "Users by mutually exclusive lifecycle segment.",
	}, []string{"segment"})

	SubscriptionsRenewedThisMonth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_renewed_month",
		Help: "Distinct users with a successful payment in the current calendar month.",
	})

	NodeActiveUsers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_active_users",
		Help: "Active users currently allocated to a node.",
	}, []string{"server_key", "country", "title"})

	NodeMaxUsers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_max_users",
		Help: "Maximum users capacity of a node.",
	}, []string{"server_key", "country", "title"})

	NodeLoadPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_load_percent",
		Help: "Node fill percentage (active/max * 100).",
	}, []string{"server_key", "country", "title"})

	// Живость ноды: 1 — нода работает (есть свежий heartbeat ИЛИ проходит TCP-проверка VPN-порта),
	// 0 — нода полностью упала (ни heartbeat, ни TCP-проверка не проходят).
	NodeUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_up",
		Help: "Node liveness (1 alive via heartbeat or probe, 0 completely down).",
	}, []string{"server_key", "country", "title"})

	// Признак блокировки маршрута между центром и нодой (1 — заблокировано, 0 — норма).
	// Позволяет отличить «нода заблокирована на стыке с центром» от «нода упала».
	NodeBlocked = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_blocked",
		Help: "1 if node is alive on one channel (heartbeat or TCP probe) but blocked on the other, 0 otherwise.",
	}, []string{"server_key", "country", "title"})

	// Результат активной TCP-проверки VPN-портов ноды из центра (1 — порт доступен, 0 — недоступен).
	NodeProbeUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_probe_up",
		Help: "1 if direct TCP probe from center to node VPN port succeeded, 0 otherwise.",
	}, []string{"server_key", "country", "title"})

	// Детальное состояние здоровья ноды: state = healthy | blocked_dataplane | blocked_controlplane | down.
	NodeHealthState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_health_state",
		Help: "Node health classification state (1 for active state, 0 for others).",
	}, []string{"server_key", "country", "title", "state"})

	// Секунды с момента последнего heartbeat ноды.
	NodeHeartbeatAgeSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_heartbeat_age_seconds",
		Help: "Seconds since last heartbeat of a node.",
	}, []string{"server_key", "country", "title"})

	NodesCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_nodes_count",
		Help: "Number of nodes by state: enabled and disabled count all nodes; alive, blocked and down count enabled nodes.",
	}, []string{"state"})

	PoolCapacityTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_pool_capacity_total",
		Help: "Total max_users capacity across enabled nodes.",
	})
	PoolActiveTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_pool_active_total",
		Help: "Total active_users across enabled nodes.",
	})

	PoolItemsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_pool_items_count",
		Help: "Number of pool items (profiles) by state.",
	}, []string{"state"})

	CryptoInvoicesByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_crypto_invoices_total",
		Help: "Crypto invoices by status.",
	}, []string{"status"})

	CryptoRevenueTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_crypto_revenue_total",
		Help: "Sum of paid crypto invoice amounts by asset.",
	}, []string{"asset"})

	CryptoPaidCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_crypto_paid_count",
		Help: "Number of paid crypto invoices.",
	})

	NodeTrafficBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_traffic_bps",
		Help: "Node traffic rate in bytes per second by direction.",
	}, []string{"server_key", "country", "title", "direction"})

	NodeOnlineUsers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_online_users",
		Help: "Users that actually transferred data during the last interval.",
	}, []string{"server_key", "country", "title"})

	NodeBandwidthPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_bandwidth_percent",
		Help: "Node bandwidth utilisation as a percentage of the configured capacity.",
	}, []string{"server_key", "country", "title"})

	NodeTrafficBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_traffic_bytes",
		Help: "Node traffic in bytes by direction (uplink/downlink). Populated by node-agent (future).",
	}, []string{"server_key", "country", "direction"})

	NodeBandwidthConfigured = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_bandwidth_configured",
		Help: "1 if bandwidth_mbps is set for the node, 0 otherwise.",
	}, []string{"server_key", "country", "title"})

	NodeBandwidthMbps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_bandwidth_mbps",
		Help: "Configured node uplink capacity in Mbps; 0 means not configured.",
	}, []string{"server_key", "country", "title"})

	MetricsCollectorLastRun = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_metrics_collector_last_run_timestamp",
		Help: "Unix timestamp of the last successful DB metrics collection.",
	})

	CDNQuotaUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_cdn_quota_used_bytes",
		Help: "Sum of CDN bytes consumed in the current quota period, by node.",
	}, []string{"node_id"})

	CDNQuotaRowsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_cdn_quota_rows",
		Help: "Number of tracked CDN quota rows (user-node pairs), by node.",
	}, []string{"node_id"})

	CDNQuotaExhaustedRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_cdn_quota_exhausted_rows",
		Help: "Number of CDN quota rows currently in the exhausted state, by node.",
	}, []string{"node_id"})

	CDNQuotaStaleRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_cdn_quota_stale_rows",
		Help: "CDN quota rows with telemetry older than the configured threshold, by node.",
	}, []string{"node_id"})

	CDNQuotaExhaustedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_exhausted_total",
		Help: "Transitions into the exhausted state, by node.",
	}, []string{"node_id"})

	CDNQuotaResetTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_reset_total",
		Help: "CDN quota period resets, by node and trigger.",
	}, []string{"node_id", "trigger"})

	CDNQuotaResetErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_reset_errors_total",
		Help: "Failed CDN quota reset attempts, by trigger.",
	}, []string{"trigger"})

	CDNQuotaOvershootBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vpn_platform_cdn_quota_overshoot_bytes",
		Help:    "Bytes consumed above the limit before enforcement kicked in.",
		Buckets: prometheus.ExponentialBuckets(1_000_000, 4, 8),
	}, []string{"node_id"})

	CDNQuotaCounterResetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_counter_resets_total",
		Help: "Observed rollbacks of node-side cumulative counters, by node.",
	}, []string{"node_id"})

	CDNQuotaUnclassifiedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_unclassified_total",
		Help: "Traffic report items without a usable inbound tag, by node.",
	}, []string{"node_id"})

	KafkaConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_kafka_consumer_lag",
		Help: "Consumer group lag (messages behind log end offset) by topic and group.",
	}, []string{"topic", "group", "partition"})

	OutboxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_outbox_depth",
		Help: "Number of outbox events awaiting publication by status.",
	}, []string{"status"})

	OutboxFailedTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_outbox_failed_total",
		Help: "Number of outbox events in failed state.",
	})

	PostgresLockWaits = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_postgres_lock_waits",
		Help: "Number of backends currently waiting on a lock.",
	})

	PostgresConnectionsInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_postgres_connections_in_use",
		Help: "Number of Postgres connections currently in use by this service pool.",
	})
)

func init() {
	prometheus.MustRegister(
		// технические
		KafkaConsumedTotal, KafkaPublishedTotal, HTTPRequestsTotal, BillingPaymentsTotal, KafkaHandlerDuration,
		SubscriptionActivationsTotal,
		SubscriptionBonusGrantsTotal,
		BillingPlategaRequestsTotal, BillingPlategaCallbacksTotal, BillingPlategaPaymentsByStatus,
		BillingPlategaOverdueChecks, BillingPlategaReconcileLastRun,
		// бизнес-метрики
		SubscriptionsByStatus,
		SubscriptionsByLifecycle,
		SubscriptionsRenewedThisMonth,
		SubscriptionsBySegment,
		NodeActiveUsers, NodeMaxUsers, NodeLoadPercent, NodeUp, NodeBlocked, NodeProbeUp, NodeHealthState, NodeHeartbeatAgeSeconds,
		NodesCount, PoolCapacityTotal, PoolActiveTotal, PoolItemsCount,
		CryptoInvoicesByStatus, CryptoRevenueTotal, CryptoPaidCount,
		NodeTrafficBytes,
		NodeBandwidthConfigured,
		NodeBandwidthMbps,
		NodeTrafficBps,
		NodeOnlineUsers,
		NodeBandwidthPercent, MetricsCollectorLastRun,
		// Квота CDN-трафика
		CDNQuotaUsedBytes, CDNQuotaRowsTotal, CDNQuotaExhaustedRows, CDNQuotaStaleRows,
		CDNQuotaExhaustedTotal, CDNQuotaResetTotal, CDNQuotaResetErrorsTotal,
		CDNQuotaOvershootBytes, CDNQuotaCounterResetsTotal, CDNQuotaUnclassifiedTotal,
		// S12: метрики масштабируемости
		KafkaConsumerLag, OutboxDepth, OutboxFailedTotal, PostgresLockWaits, PostgresConnectionsInUse,
	)
}

func Handler() http.Handler { return promhttp.Handler() }

func StartServer(ctx context.Context, defaultAddr string) {
	addr := strings.TrimSpace(os.Getenv("METRICS_ADDR"))
	if addr == "" {
		addr = defaultAddr
	}
	if addr == "" {
		addr = ":9090"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	go func() {
		slog.Info("metrics server started", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server stopped", "err", err)
		}
	}()
}

// statusRecorder перехватывает HTTP-статус ответа для метрики.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// HTTPMetricsMiddleware оборачивает handler и инкрементирует HTTPRequestsTotal
// по маршруту, методу и статусу. Служебные эндпоинты (/livez, /readyz, /metrics)
// не учитываются, чтобы не зашумлять метрику.
func HTTPMetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/livez", "/readyz", "/metrics":
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		HTTPRequestsTotal.WithLabelValues(
			routeLabel(r.URL.Path),
			r.Method,
			strconv.Itoa(rec.status),
		).Inc()
	})
}

// routeLabel огрубляет путь до стабильного маршрута, чтобы не плодить
// бесконечные значения лейбла из токенов/ID (например /sub/<token> → /sub).
func routeLabel(path string) string {
	for _, prefix := range []string{"/sub/", "/s/", "/cdn/", "/admin/", "/webhooks/", "/internal/"} {
		if strings.HasPrefix(path, prefix) {
			return strings.TrimRight(prefix, "/")
		}
	}
	if path == "" {
		return "/"
	}
	return path
}
