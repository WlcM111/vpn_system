package main

import (
	"context"
	"log/slog"
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
	"vpn-platform/internal/crypto_billing"

	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	// Логгер инициализируется первым, чтобы все ошибки старта были видны в JSON-формате.
	logger.Init()

	// Корневой контекст, отменяется при SIGINT/SIGTERM (graceful shutdown).
	ctx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	// Prometheus /metrics на отдельном порту :9090 (общий для всех сервисов проекта).
	commonmetrics.StartServer(ctx, ":9090")

	// Конфиг читается из env, при отсутствии обязательных секретов сервис падает.
	cfg, err := crypto_billing.LoadConfigFromEnv()
	if err != nil {
		slog.Error("failed to load crypto-billing config", "err", err)
		os.Exit(1)
	}

	// Postgres pool: тот же общий postgres.NewPool, что используют другие сервисы.
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		slog.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		slog.Error("failed to init postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Kafka producer и consumer crypto.commands. Если KAFKA_BROKERS пуст —
	// сервис продолжает работать "только с HTTP" (webhook будет принимать,
	// но не сможет публиковать события). На проде KAFKA_BROKERS обязательно задан.
	var producer *commonkafka.Producer
	var commandsReader *kafkago.Reader

	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersEnv != "" {
		brokers := strings.Split(brokersEnv, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}
		producer = commonkafka.NewProducer(brokers)
		commandsReader = commonkafka.NewReader(brokers, commonkafka.TopicCryptoCommands, "crypto-billing-service")
	} else {
		slog.Warn("crypto-billing KAFKA_BROKERS not set, kafka disabled")
	}

	// Доменные зависимости.
	repo := crypto_billing.NewRepository(pool)
	client := crypto_billing.NewCryptoBotClient(cfg.APIBase, cfg.APIToken)
	svc := crypto_billing.NewService(cfg, repo, client, producer)

	// HTTP-роутер. Бизнес-эндпоинты регистрирует HTTPHandlers, а livez/readyz —
	// общий пакет httpserver. readyz пингует БД, чтобы Kubernetes/compose могли
	// корректно определять готовность сервиса.
	mux := http.NewServeMux()
	crypto_billing.NewHTTPHandlers(svc, cfg).Register(mux)
	httpserver.RegisterLivez(mux)
	httpserver.RegisterReadyz(mux, func(ctx context.Context) error {
		return pool.Ping(ctx)
	})

	server := httpserver.New(cfg.HTTPAddr, mux)

	// Канал для сигналов завершения.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Запуск Kafka consumer'а в отдельной горутине.
	if commandsReader != nil {
		go func() {
			if err := crypto_billing.RunCommandConsumer(ctx, commandsReader, svc); err != nil {
				slog.Error("crypto-billing command consumer stopped", "err", err)
			}
		}()
	}

	// Запуск outbox-worker'а, который читает event_outbox и публикует в Kafka.
	// Если producer == nil (Kafka выключен) — worker корректно делает no-op.
	go outbox.RunPublisher(ctx, pool, producer, "crypto-billing-service")

	// Запуск HTTP-сервера.
	go func() {
		slog.Info("crypto-billing http server started", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("crypto-billing http server error", "err", err)
			os.Exit(1)
		}
	}()

	slog.Info("crypto-billing-service started")
	<-stop
	slog.Info("crypto-billing shutting down")
	appCancel()

	// Graceful shutdown: даём 10 секунд, чтобы текущие запросы и Kafka-сообщения
	// доехали до завершения.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if commandsReader != nil {
		_ = commandsReader.Close()
	}
	if producer != nil {
		_ = producer.Close()
	}
	_ = server.Shutdown(shutdownCtx)
}
