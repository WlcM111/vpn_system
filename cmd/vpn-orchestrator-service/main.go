package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	kafkago "github.com/segmentio/kafka-go"
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

	var producer *commonkafka.Producer
	var subscriptionEventsReader *kafkago.Reader
	var vpnEventsReader *kafkago.Reader

	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersEnv != "" {
		brokers := strings.Split(brokersEnv, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}
		producer = commonkafka.NewProducer(brokers)
		subscriptionEventsReader = commonkafka.NewReader(brokers, commonkafka.TopicSubscriptionEvents, "vpn-orchestrator-service")
		vpnEventsReader = commonkafka.NewReader(brokers, commonkafka.TopicVPNEvents, "vpn-orchestrator-service")
	} else {
		log.Println("[vpn-orchestrator] KAFKA_BROKERS not set, kafka disabled")
	}

	repo := vpn_orchestrator.NewRepository(pool)
	svc := vpn_orchestrator.NewService(repo, producer, vpn_orchestrator.ServiceConfig{
		FeedFormat: strings.TrimSpace(os.Getenv("SUBSCRIPTION_FEED_FORMAT")),
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
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	if subscriptionEventsReader != nil {
		go func() {
			if err := vpn_orchestrator.RunSubscriptionEventsConsumer(ctx, subscriptionEventsReader, svc); err != nil {
				log.Printf("[vpn-orchestrator] subscription events consumer stopped: %v", err)
			}
		}()
	}
	if vpnEventsReader != nil {
		go func() {
			if err := vpn_orchestrator.RunVPNEventsConsumer(ctx, vpnEventsReader, svc); err != nil {
				log.Printf("[vpn-orchestrator] vpn events consumer stopped: %v", err)
			}
		}()
	}

	go func() {
		log.Printf("[vpn-orchestrator] http started on %s", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("vpn-orchestrator http server error: %v", err)
		}
	}()

	go outbox.RunPublisher(ctx, pool, producer, "vpn-orchestrator-service")

	<-stop
	log.Println("[vpn-orchestrator] shutting down...")
	appCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if subscriptionEventsReader != nil {
		_ = subscriptionEventsReader.Close()
	}
	if vpnEventsReader != nil {
		_ = vpnEventsReader.Close()
	}
	if producer != nil {
		_ = producer.Close()
	}
	_ = server.Shutdown(shutdownCtx)
}
