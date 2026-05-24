package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"vpn-platform/internal/common/logger"
	commonmetrics "vpn-platform/internal/common/metrics"
	"vpn-platform/internal/tg_bot_gateway"
)

func main() {
	logger.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	commonmetrics.StartServer(ctx, ":9090")

	app, err := tg_bot_gateway.NewApp()
	if err != nil {
		log.Fatalf("failed to create tg-bot-gateway app: %v", err)
	}

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := app.Run(); err != nil {
			log.Fatalf("app run error: %v", err)
		}
	}()

	log.Println("tg-bot-gateway started")
	<-stop
	log.Println("shutting down tg-bot-gateway...")
	cancel()
	app.Stop()
}
