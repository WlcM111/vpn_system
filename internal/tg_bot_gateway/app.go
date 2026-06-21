package tg_bot_gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/postgres"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
)

type App struct {
	bot        *tgbotapi.BotAPI
	stateStore StateStore
	backend    Backend

	kafkaProducer     *commonkafka.Producer
	notificationsRead *kafkago.Reader
	chatLocks         sync.Map
	stopOnce          sync.Once

	ctx    context.Context
	cancel context.CancelFunc
	pgPool *pgxpool.Pool
	stopCh chan struct{}
}

type chatLock struct {
	mu       sync.Mutex
	lastUsed time.Time
}

func (a *App) lockChat(chatID int64) func() {
	now := time.Now()
	v, _ := a.chatLocks.LoadOrStore(chatID, &chatLock{lastUsed: now})
	lock := v.(*chatLock)

	lock.mu.Lock()
	lock.lastUsed = now

	return func() {
		lock.lastUsed = time.Now()
		lock.mu.Unlock()
	}
}

func (a *App) cleanupChatLocks(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deadline := time.Now().Add(-10 * time.Minute)
			a.chatLocks.Range(func(key, value any) bool {
				lock, ok := value.(*chatLock)
				if !ok {
					a.chatLocks.Delete(key)
					return true
				}

				if lock.lastUsed.Before(deadline) {
					a.chatLocks.Delete(key)
				}
				return true
			})
		}
	}
}

func (a *App) kafkaEnabled() bool {
	return a.kafkaProducer != nil
}

func NewApp() (*App, error) {
	token := strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	if token == "" {
		return nil, ErrMissingToken
	}

	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, err
	}
	bot.Debug = false

	var pgPool *pgxpool.Pool
	stateStore := NewMemoryStateStore()

	if dsn := strings.TrimSpace(os.Getenv("DATABASE_URL")); dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := postgres.NewPool(ctx, dsn)
		if err != nil {
			return nil, err
		}

		pgPool = pool
		stateStore = NewPostgresStateStore(pool)
	}

	var kafkaProducer *commonkafka.Producer
	var notificationsReader *kafkago.Reader

	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersEnv != "" {
		brokers := strings.Split(brokersEnv, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}

		kafkaProducer = commonkafka.NewProducer(brokers)
		notificationsReader = commonkafka.NewReader(
			brokers,
			commonkafka.TopicTgNotifications,
			"tg-bot-gateway",
		)
	} else {
		log.Println("[tg-bot] KAFKA_BROKERS not set, work in pure local/mock mode")
	}

	directBaseURL := strings.TrimSpace(os.Getenv("TG_SUBSCRIPTION_DIRECT_BASE_URL"))
	if directBaseURL == "" {
		directBaseURL = strings.TrimSpace(os.Getenv("TG_SUBSCRIPTION_BASE_URL"))
	}

	backend := NewMockBackend(kafkaProducer, directBaseURL)
	if kafkaProducer == nil && strings.ToLower(strings.TrimSpace(os.Getenv("TG_ALLOW_MOCK_BACKEND"))) != "true" {
		return nil, errors.New("KAFKA_BROKERS is required unless TG_ALLOW_MOCK_BACKEND=true")
	}

	appCtx, appCancel := context.WithCancel(context.Background())

	return &App{
		bot:               bot,
		stateStore:        stateStore,
		backend:           backend,
		kafkaProducer:     kafkaProducer,
		notificationsRead: notificationsReader,
		stopCh:            make(chan struct{}),
		ctx:               appCtx,
		cancel:            appCancel,
		pgPool:            pgPool,
	}, nil
}

var ErrMissingToken = tgbotapi.Error{Message: "TELEGRAM_BOT_TOKEN is not set"}

func (a *App) Run() error {
	log.Printf("[tg-bot] authorized on account @%s", a.bot.Self.UserName)

	if a.notificationsRead != nil {
		go a.consumeNotifications(a.ctx)
	}

	go a.cleanupChatLocks(a.ctx)

	// Внутренний HTTP-эндпоинт массовой рассылки (только 127.0.0.1, по токену).
	// Если BROADCAST_TOKEN не задан — эндпоинт не поднимается.
	go a.startBroadcastServer(a.ctx)

	if webhookURL := strings.TrimSpace(os.Getenv("TG_WEBHOOK_URL")); webhookURL != "" {
		return a.runWebhookMode(webhookURL)
	}

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := a.bot.GetUpdatesChan(u)

	for {
		select {
		case update := <-updates:
			if update.Message == nil {
				continue
			}
			a.handleUpdate(a.ctx, update)

		case <-a.stopCh:
			return nil

		case <-a.ctx.Done():
			return nil
		}
	}
}

func (a *App) Stop() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})

	if a.cancel != nil {
		a.cancel()
	}

	if a.pgPool != nil {
		a.pgPool.Close()
	}

	if a.kafkaProducer != nil {
		if err := a.kafkaProducer.Close(); err != nil {
			log.Printf("[tg-bot] kafka producer close error: %v", err)
		}
	}

	if a.notificationsRead != nil {
		if err := a.notificationsRead.Close(); err != nil {
			log.Printf("[tg-bot] kafka notifications reader close error: %v", err)
		}
	}
}

func (a *App) runWebhookMode(webhookURL string) error {
	secret := strings.TrimSpace(os.Getenv("TG_WEBHOOK_SECRET"))

	ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
	defer cancel()

	if err := a.setTelegramWebhook(ctx, webhookURL, secret); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/telegram/webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if secret != "" && r.Header.Get("X-Telegram-Bot-Api-Secret-Token") != secret {
			http.NotFound(w, r)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

		var update tgbotapi.Update
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		if update.Message != nil {
			a.handleUpdate(a.ctx, update)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := strings.TrimSpace(os.Getenv("TG_WEBHOOK_ADDR"))
	if addr == "" {
		addr = ":8085"
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-a.ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("[tg-bot] webhook mode enabled url=%s addr=%s", webhookURL, addr)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (a *App) setTelegramWebhook(ctx context.Context, webhookURL string, secret string) error {
	payload := map[string]any{
		"url":                  webhookURL,
		"drop_pending_updates": true,
		"allowed_updates":      []string{"message"},
	}

	if secret != "" {
		payload["secret_token"] = secret
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal telegram webhook payload: %w", err)
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/setWebhook", a.bot.Token)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create telegram setWebhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("telegram setWebhook request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram setWebhook failed status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var telegramResp struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(respBody, &telegramResp); err != nil {
		return fmt.Errorf("decode telegram setWebhook response: %w", err)
	}

	if !telegramResp.OK {
		if telegramResp.Description == "" {
			telegramResp.Description = "unknown telegram setWebhook error"
		}
		return errors.New(telegramResp.Description)
	}

	return nil
}
