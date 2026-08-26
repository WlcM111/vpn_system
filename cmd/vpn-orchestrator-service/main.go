package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vpn-platform/internal/common/httpserver"
	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/logger"
	commonmetrics "vpn-platform/internal/common/metrics"
	"vpn-platform/internal/common/outbox"
	"vpn-platform/internal/common/postgres"
	"vpn-platform/internal/vpn_orchestrator"
)

func main() {
	logger.Init()

	ctx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	commonmetrics.StartServer(ctx, ":9090")

	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to init postgres: %v", err)
	}
	defer pool.Close()

	// P2: персистентный счётчик ретраев консьюмера (переживает рестарты).
	commonkafka.SetAttemptStore(commonkafka.NewDBAttemptStore(pool))

	var producer *commonkafka.Producer
	var brokers []string

	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersEnv != "" {
		brokers = strings.Split(brokersEnv, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}
		producer = commonkafka.NewProducer(brokers)
	} else {
		log.Println("[vpn-orchestrator] KAFKA_BROKERS not set, kafka disabled")
	}

	consumerWorkers := parseIntEnv("CONSUMER_WORKERS", 3)

	// Политика CDN-квоты валидируется на старте: неоднозначная конфигурация
	// должна давать понятную ошибку запуска, а не тихо работать не так.
	cdnQuota, err := vpn_orchestrator.LoadCDNQuotaPolicyFromEnv()
	if err != nil {
		log.Fatalf("invalid CDN quota configuration: %v", err)
	}
	log.Printf("[vpn-orchestrator] cdn quota: enabled=%v enforce=%v limit=%d bytes fail_mode=%s",
		cdnQuota.Enabled, cdnQuota.Enforce, cdnQuota.LimitBytes, cdnQuota.FailMode)

	repo := vpn_orchestrator.NewRepository(pool)
	svc := vpn_orchestrator.NewService(repo, producer, vpn_orchestrator.ServiceConfig{
		FeedFormat:       strings.TrimSpace(os.Getenv("SUBSCRIPTION_FEED_FORMAT")),
		NodeHeartbeatTTL: parseDurationEnv("NODE_HEARTBEAT_TTL", 90*time.Second),
		DefaultMaxUsers:  parseIntEnv("NODE_DEFAULT_MAX_USERS", 200),
		DefaultWeight:    parseIntEnv("NODE_DEFAULT_WEIGHT", 100),
		SoftOverflow:     parseBoolEnv("BALANCER_SOFT_OVERFLOW", true),
		CDNQuota:         cdnQuota,
	})

	httpAddr := strings.TrimSpace(os.Getenv("VPN_ORCHESTRATOR_HTTP_ADDR"))
	if httpAddr == "" {
		httpAddr = ":8084"
	}

	mux := http.NewServeMux()
	vpn_orchestrator.NewHTTPHandlers(svc).Register(mux)

	httpserver.RegisterLivez(mux)
	httpserver.RegisterReadyz(mux, func(ctx context.Context) error {
		return pool.Ping(ctx)
	})

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           commonmetrics.HTTPMetricsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// S1: пул воркеров на партиции. Каждый воркер — свой reader в той же
	// consumer group; Kafka распределяет партиции топика между воркерами.
	if len(brokers) > 0 {
		for i := 0; i < consumerWorkers; i++ {
			go func() {
				r := commonkafka.NewReader(brokers, commonkafka.TopicSubscriptionEvents, "vpn-orchestrator-service")
				if err := vpn_orchestrator.RunSubscriptionEventsConsumer(ctx, r, svc); err != nil {
					log.Printf("[vpn-orchestrator] subscription events consumer stopped: %v", err)
				}
			}()
			go func() {
				r := commonkafka.NewReader(brokers, commonkafka.TopicVPNEvents, "vpn-orchestrator-service")
				if err := vpn_orchestrator.RunVPNEventsConsumer(ctx, r, svc); err != nil {
					log.Printf("[vpn-orchestrator] vpn events consumer stopped: %v", err)
				}
			}()
		}
	}

	go func() {
		log.Printf("[vpn-orchestrator] http started on %s", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("vpn-orchestrator http server error: %v", err)
		}
	}()

	go outbox.RunPublisher(ctx, pool, producer, "vpn-orchestrator-service")

	// P1: периодическая пересинхронизация нод (самовосстановление после сбоев).
	go svc.RunReconcileWorker(
		ctx,
		parseDurationEnv("RECONCILE_INTERVAL", 10*time.Minute),
		parseIntEnv("RECONCILE_BATCH_SIZE", 200),
	)

	// Фоновый сборщик бизнес-метрик из БД для Prometheus/Grafana.
	// Интервал и TTL heartbeat настраиваются через env (значения по умолчанию ниже).
	metricsCollector := vpn_orchestrator.NewMetricsCollector(
		pool,
		parseDurationEnv("METRICS_COLLECT_INTERVAL", 15*time.Second),
		parseDurationEnv("METRICS_HEAVY_INTERVAL", 60*time.Second),
		parseDurationEnv("NODE_HEARTBEAT_TTL", 90*time.Second),
		brokers,
	)
	go metricsCollector.Run(ctx)

	// Квота CDN: плановый календарный сброс периода и экспорт метрик.
	// Проход сверяет ключ периода, поэтому пропуск полуночи из-за рестарта
	// или деплоя не теряет сброс.
	go svc.RunCDNQuotaWorker(
		ctx,
		parseDurationEnv("CDN_QUOTA_WORKER_INTERVAL", 5*time.Minute),
		parseIntEnv("CDN_QUOTA_RESET_BATCH_SIZE", 500),
	)

	// S9/S11: периодическая чистка опубликованного outbox и старого inbox.
	// Запускаем в оркестраторе (одного достаточно — таблицы общие).
	go outbox.RunCleanup(ctx, pool)

	<-stop
	log.Println("[vpn-orchestrator] shutting down...")
	appCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if producer != nil {
		_ = producer.Close()
	}
	_ = server.Shutdown(shutdownCtx)
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return fallback
}

func parseIntEnv(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return fallback
}

func parseBoolEnv(key string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return fallback
}
