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

	"vpn-platform/internal/billing"
	"vpn-platform/internal/common/httpserver"
	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/logger"
	commonmetrics "vpn-platform/internal/common/metrics"
	"vpn-platform/internal/common/outbox"
	"vpn-platform/internal/common/postgres"
)

func main() {
	logger.Init()

	ctx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	commonmetrics.StartServer(ctx, ":9090")

	httpAddr := os.Getenv("BILLING_HTTP_ADDR")
	if strings.TrimSpace(httpAddr) == "" {
		httpAddr = ":8082"
	}

	dsn := os.Getenv("DATABASE_URL")
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
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
		log.Println("[billing] KAFKA_BROKERS not set, kafka disabled")
		producer = nil
	}

	consumerWorkers := parseIntEnv("CONSUMER_WORKERS", 3)

	repo := billing.NewRepository(pool)
	svc := billing.NewService(repo, producer)

	mux := http.NewServeMux()
	billing.NewHTTPHandlers(svc).Register(mux)

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

	// S1: пул воркеров на партиции (см. комментарий в vpn-orchestrator).
	if len(brokers) > 0 {
		for i := 0; i < consumerWorkers; i++ {
			go func() {
				r := commonkafka.NewReader(brokers, commonkafka.TopicBillingCommands, "billing-service")
				if err := billing.RunCommandConsumer(ctx, r, svc); err != nil {
					log.Printf("[billing] command consumer stopped: %v", err)
				}
			}()
			go func() {
				r := commonkafka.NewReader(brokers, commonkafka.TopicSubscriptionEvents, "billing-service")
				if err := billing.RunSubscriptionEventsConsumer(ctx, r, svc); err != nil {
					log.Printf("[billing] subscription events consumer stopped: %v", err)
				}
			}()
		}
	}

	go billing.RunRenewalScheduler(ctx, svc)
	go billing.RunGraceExpirationWorker(ctx, svc)
	go billing.RunRefundRetryWorker(ctx, svc)
	go outbox.RunPublisher(ctx, pool, producer, "billing-service")
	httpserver.RegisterLivez(mux)
	httpserver.RegisterReadyz(mux, func(ctx context.Context) error {
		return pool.Ping(ctx)
	})

	go func() {
		log.Printf("[billing] http server started on %s", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("billing http server error: %v", err)
		}
	}()

	<-stop
	log.Println("[billing] shutting down...")
	appCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if producer != nil {
		_ = producer.Close()
	}
	_ = server.Shutdown(shutdownCtx)
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
