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

	// SubscriptionActivationsTotal считает ФАКТИЧЕСКИЕ активации подписки, а не
	// ответы платёжного провайдера. BillingPaymentsTotal увеличивается в
	// billing-service до публикации события и до изменения подписки, поэтому по
	// нему нельзя судить, получил ли пользователь доступ. Метки: source —
	// initial или recurring, result — applied или duplicate.
	SubscriptionActivationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_subscription_activations_total",
		Help: "Subscription activations actually applied to the database.",
	}, []string{"source", "result"})

	BillingPaymentsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_billing_payments_total",
		Help: "Billing payment outcomes as reported by the provider, before activation.",
	}, []string{"outcome"})

	KafkaHandlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vpn_platform_kafka_handler_duration_seconds",
		Help:    "Kafka handler duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})
)

// ---------------------------------------------------------------------------
// Бизнес-метрики (gauge). Выставляются фоновым сборщиком из БД
// (см. internal/vpn_orchestrator/metrics_collector.go).
// Это срез текущего состояния системы, который видно в Grafana.
// ---------------------------------------------------------------------------

var (
	// Кол-во пользователей по статусу подписки: none/trial/active/grace/expired.
	SubscriptionsByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_total",
		Help: "Number of user subscriptions by status.",
	}, []string{"status"})

	// Кол-во подписок по РЕАЛЬНОМУ сроку действия (а не по сырому status).
	// kind: "trial" | "paid". lifecycle: "active" | "expired" | "total".
	//   active  — срок ещё не истёк (expires_at > now());
	//   expired — статус/тип соответствует, но срок уже прошёл (expires_at <= now());
	//   total   — все строки этого kind (active + expired).
	// Нужна, потому что истечение в системе ленивое: протухшие триалы/подписки
	// остаются в своём status до следующего обращения к строке, из-за чего
	// SubscriptionsByStatus{status="trial"} завышается на «спящих» протухших.
	SubscriptionsByLifecycle = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_lifecycle_total",
		Help: "User subscriptions by kind (trial/paid) and lifecycle (active/expired/total), based on expires_at vs now().",
	}, []string{"kind", "lifecycle"})

	// Сегменты пользователей — взаимоисключающие и покрывающие всех.
	// Сумма по всем сегментам равна общему числу подписок, поэтому ни один
	// человек не теряется между категориями.
	//
	// never_started   — завёл бота, подписку не оформлял
	// trial_new       — первый триал, ни разу не платил
	// trial_after_paid— вернулся на триал после оплаты
	// paid_active     — активная платная подписка
	// churned_trial   — ушёл, ни разу не платил
	// churned_paid    — ушёл, платил
	// pending_revoke  — срок вышел, доступ ещё не отозван
	SubscriptionsBySegment = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_segment",
		Help: "Users by mutually exclusive lifecycle segment.",
	}, []string{"segment"})

	// Уникальные пользователи с успешной оплатой в ТЕКУЩЕМ календарном месяце
	// (карта + крипта). Обнуляется сама при смене месяца, потому что запрос
	// привязан к date_trunc('month', now()).
	SubscriptionsRenewedThisMonth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_subscriptions_renewed_month",
		Help: "Distinct users with a successful payment in the current calendar month.",
	})

	// Кол-во активных пользователей на ноде (active_users из vpn_servers).
	NodeActiveUsers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_active_users",
		Help: "Active users currently allocated to a node.",
	}, []string{"server_key", "country", "title"})

	// Лимит пользователей на ноде (max_users).
	NodeMaxUsers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_max_users",
		Help: "Maximum users capacity of a node.",
	}, []string{"server_key", "country", "title"})

	// Процент заполнения ноды (0..100).
	NodeLoadPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_load_percent",
		Help: "Node fill percentage (active/max * 100).",
	}, []string{"server_key", "country", "title"})

	// Живость ноды по heartbeat: 1 — heartbeat свежий, 0 — устарел/отсутствует.
	NodeUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_up",
		Help: "Node liveness by heartbeat freshness (1 alive, 0 stale/down).",
	}, []string{"server_key", "country", "title"})

	// Секунды с момента последнего heartbeat ноды.
	NodeHeartbeatAgeSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_heartbeat_age_seconds",
		Help: "Seconds since last heartbeat of a node.",
	}, []string{"server_key", "country", "title"})

	// Кол-во нод по состоянию: enabled/disabled/alive/stale.
	NodesCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_nodes_count",
		Help: "Number of nodes by state.",
	}, []string{"state"})

	// Суммарная вместимость и занятость пула (для общей картины).
	PoolCapacityTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_pool_capacity_total",
		Help: "Total max_users capacity across enabled nodes.",
	})
	PoolActiveTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_pool_active_total",
		Help: "Total active_users across enabled nodes.",
	})

	// Кол-во профилей в пуле (vpn_pool_items) по доступности.
	PoolItemsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_pool_items_count",
		Help: "Number of pool items (profiles) by state.",
	}, []string{"state"})

	// Крипто-инвойсы по статусу (creating/active/paid/expired и т.п.).
	CryptoInvoicesByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_crypto_invoices_total",
		Help: "Crypto invoices by status.",
	}, []string{"status"})

	// Выручка по оплаченным крипто-инвойсам, нарастающим итогом, по активу.
	CryptoRevenueTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_crypto_revenue_total",
		Help: "Sum of paid crypto invoice amounts by asset.",
	}, []string{"asset"})

	// Количество оплаченных инвойсов (нарастающим итогом).
	CryptoPaidCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_crypto_paid_count",
		Help: "Number of paid crypto invoices.",
	})

	// Трафик ноды в байтах (накопительный). ЗАГЛУШКА на этапе MVP:
	// источник — Xray Stats API на ноде, который node-agent должен публиковать.
	// Кумулятивный трафик по узлам (uplink/downlink), выставляется в ApplyNodeTraffic
	// из событий node-agent (P5). Используется панелью «Трафик по нодам».
	// Скорость трафика ноды, байт/с. Приходит в heartbeat от агента как
	// разница счётчиков инбаундов между двумя опросами.
	NodeTrafficBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_traffic_bps",
		Help: "Node traffic rate in bytes per second by direction.",
	}, []string{"server_key", "country", "title", "direction"})

	// Пользователи, реально передававшие данные за последний интервал.
	// Отличается от node_active_users, который считает выданные учётки.
	NodeOnlineUsers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_online_users",
		Help: "Users that actually transferred data during the last interval.",
	}, []string{"server_key", "country", "title"})

	// Загрузка канала в процентах от заявленной полосы.
	NodeBandwidthPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_bandwidth_percent",
		Help: "Node bandwidth utilisation as a percentage of the configured capacity.",
	}, []string{"server_key", "country", "title"})

	NodeTrafficBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_traffic_bytes",
		Help: "Node traffic in bytes by direction (uplink/downlink). Populated by node-agent (future).",
	}, []string{"server_key", "country", "direction"})

	// Признак того, что для узла задана полоса канала. Без него ноль в
	// vpn_platform_node_bandwidth_percent неотличим от «канал свободен»:
	// именно так узел с забитым каналом полгода выглядел незагруженным.
	NodeBandwidthConfigured = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_bandwidth_configured",
		Help: "1 if bandwidth_mbps is set for the node, 0 otherwise.",
	}, []string{"server_key", "country", "title"})

	// Заявленная полоса канала узла в Мбит/с. Ноль означает «не задана».
	NodeBandwidthMbps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_node_bandwidth_mbps",
		Help: "Configured node uplink capacity in Mbps; 0 means not configured.",
	}, []string{"server_key", "country", "title"})

	// Время последнего успешного сбора DB-метрик (для контроля живости сборщика).
	MetricsCollectorLastRun = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_metrics_collector_last_run_timestamp",
		Help: "Unix timestamp of the last successful DB metrics collection.",
	})

	// --- S12: метрики масштабируемости ---

	// Лаг консьюмера: отставание (в сообщениях) группы от конца партиции.
	// Выставляется сборщиком метрик через Kafka lag-запрос. Чем больше — тем
	// сильнее обработка не успевает за входящим потоком.
	// ── Квота CDN-трафика ────────────────────────────────────────────────
	// Метки только по узлу: telegram_id в лейблах Prometheus запрещён из-за
	// неконтролируемой кардинальности.
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

	// Строки, по которым давно не приходило отчётов: индикатор сбоя телеметрии.
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

	// Превышение лимита к моменту обнаружения. Строгий hard cap при
	// периодической телеметрии недостижим — величина измеряется, а не
	// декларируется нулём.
	CDNQuotaOvershootBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vpn_platform_cdn_quota_overshoot_bytes",
		Help:    "Bytes consumed above the limit before enforcement kicked in.",
		Buckets: prometheus.ExponentialBuckets(1_000_000, 4, 8),
	}, []string{"node_id"})

	// Счётчик Xray/агента откатился назад — строка перебазирована.
	CDNQuotaCounterResetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_counter_resets_total",
		Help: "Observed rollbacks of node-side cumulative counters, by node.",
	}, []string{"node_id"})

	// Трафик, который невозможно отнести ни к CDN, ни к не-CDN (агент старой
	// версии или учётка, общая для нескольких инбаундов). В квоту не попадает.
	CDNQuotaUnclassifiedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_cdn_quota_unclassified_total",
		Help: "Traffic report items without a usable inbound tag, by node.",
	}, []string{"node_id"})

	KafkaConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_kafka_consumer_lag",
		Help: "Consumer group lag (messages behind log end offset) by topic and group.",
	}, []string{"topic", "group", "partition"})

	// Глубина очереди outbox: сколько событий ждут публикации (pending+retry).
	// Растёт, если publisher не успевает или Kafka недоступна.
	OutboxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vpn_platform_outbox_depth",
		Help: "Number of outbox events awaiting publication by status.",
	}, []string{"status"})

	// Количество событий outbox в статусе failed (ушли в DLT после 20 попыток).
	OutboxFailedTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_outbox_failed_total",
		Help: "Number of outbox events in failed state.",
	})

	// Ожидания блокировок в Postgres (количество сессий, ждущих lock).
	// Индикатор contention (например, по горячим строкам). Растёт — есть споры за блокировки.
	PostgresLockWaits = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vpn_platform_postgres_lock_waits",
		Help: "Number of backends currently waiting on a lock.",
	})

	// Активные соединения пула Postgres (для контроля приближения к лимиту).
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
		// бизнес-метрики
		SubscriptionsByStatus,
		SubscriptionsByLifecycle,
		SubscriptionsRenewedThisMonth,
		SubscriptionsBySegment,
		NodeActiveUsers, NodeMaxUsers, NodeLoadPercent, NodeUp, NodeHeartbeatAgeSeconds,
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
