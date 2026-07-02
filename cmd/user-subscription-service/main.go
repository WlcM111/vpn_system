package main

import (
	"context"
	"log"
	"log/slog"
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
	"vpn-platform/internal/user_subscription"
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

	repo := user_subscription.NewRepository(pool)
	svc := user_subscription.NewService(
		repo,
		strings.TrimSpace(os.Getenv("SUBSCRIPTION_PUBLIC_BASE_URL")),
		strings.TrimSpace(os.Getenv("SUBSCRIPTION_DEFAULT_COUNTRY")),
	)

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
		log.Println("[user-subscription] KAFKA_BROKERS not set, kafka disabled")
	}

	consumerWorkers := parseIntEnv("CONSUMER_WORKERS", 3)

	svc.SetProducer(producer)

	httpAddr := strings.TrimSpace(os.Getenv("USER_SUBSCRIPTION_HTTP_ADDR"))
	if httpAddr == "" {
		httpAddr = ":8083"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpserver.RegisterLivez(mux)
	httpserver.RegisterReadyz(mux, func(ctx context.Context) error {
		return pool.Ping(ctx)
	})

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           commonmetrics.HTTPMetricsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// S1: пул воркеров на партиции.
	if len(brokers) > 0 {
		for i := 0; i < consumerWorkers; i++ {
			go func() {
				r := commonkafka.NewReader(brokers, commonkafka.TopicSubscriptionCommands, "user-subscription-service")
				if err := user_subscription.RunSubscriptionCommandsConsumer(ctx, r, svc); err != nil {
					slog.Error("user-subscription commands consumer stopped", "err", err)
				}
			}()
			go func() {
				r := commonkafka.NewReader(brokers, commonkafka.TopicBillingEvents, "user-subscription-service")
				if err := user_subscription.RunBillingEventsConsumer(ctx, r, svc); err != nil {
					slog.Error("user-subscription billing consumer stopped", "err", err)
				}
			}()
		}
	}

	go func() {
		log.Printf("[user-subscription] http started on %s", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	go outbox.RunPublisher(ctx, pool, producer, "user-subscription-service")

	// Воркер напоминаний об истечении подписки (7/3/1 день + окончание для
	// платных; 1 день + окончание для триала). Шлёт через outbox, поэтому нужен
	// producer (Kafka). Интервал проверки — REMINDER_CHECK_INTERVAL (по умолч. 1h).
	if producer != nil {
		reminderInterval := time.Hour
		if raw := strings.TrimSpace(os.Getenv("REMINDER_CHECK_INTERVAL")); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil && d > 0 {
				reminderInterval = d
			}
		}
		go user_subscription.RunExpirationReminderWorker(ctx, svc, reminderInterval)
	}

	<-stop
	log.Println("[user-subscription] shutting down...")

	appCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if producer != nil {
		_ = producer.Close()
	}
	_ = httpServer.Shutdown(shutdownCtx)
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
