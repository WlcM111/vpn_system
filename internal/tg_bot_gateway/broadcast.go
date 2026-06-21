package tg_bot_gateway

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"vpn-platform/internal/common/httpserver"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ============================================================================
// Внутренняя ручка массовой рассылки (broadcast).
//
// Назначение: отправить одно сообщение ВСЕМ пользователям бота (уведомление,
// предупреждение, анонс). Ручка ВНУТРЕННЯЯ — наружу через nginx НЕ выставляется,
// порт пробрасывается только на 127.0.0.1 хоста; защищена токеном.
//
// Источник списка пользователей — таблица telegram_users (все, кто хоть раз
// взаимодействовал с сервисом). Отправка идёт напрямую через Telegram API
// (a.bot.Send), потому что только бот владеет этим API.
//
// Рассылка выполняется АСИНХРОННО: HTTP-ответ возвращается сразу (202 Accepted),
// а сообщения шлются в фоне с учётом rate-limit Telegram (~30 msg/sec).
// ============================================================================

// broadcastRequest — тело запроса на рассылку.
type broadcastRequest struct {
	Message   string `json:"message"`              // текст сообщения (обязательно)
	ParseMode string `json:"parse_mode,omitempty"` // "Markdown" | "HTML" | "" (по умолчанию без разметки)
}

// broadcastResponse — немедленный ответ (рассылка стартовала).
type broadcastResponse struct {
	Status          string `json:"status"`
	RecipientsTotal int    `json:"recipients_total"`
	Note            string `json:"note"`
}

// broadcastState защищает от параллельного запуска двух рассылок одновременно.
var broadcastInProgress int32

// startBroadcastServer поднимает внутренний HTTP-сервер для рассылки.
// Блокирующий вызов — запускать в горутине из Run(). Останавливается по ctx.
//
// Адрес берётся из BROADCAST_HTTP_ADDR (по умолчанию :8087 внутри контейнера).
// Токен — из BROADCAST_TOKEN (обязателен; без него сервер не стартует).
func (a *App) startBroadcastServer(ctx context.Context) {
	token := strings.TrimSpace(os.Getenv("BROADCAST_TOKEN"))
	if token == "" {
		log.Println("[tg-bot] BROADCAST_TOKEN not set, broadcast endpoint disabled")
		return
	}
	if a.pgPool == nil {
		log.Println("[tg-bot] no DB pool, broadcast endpoint disabled")
		return
	}

	addr := strings.TrimSpace(os.Getenv("BROADCAST_HTTP_ADDR"))
	if addr == "" {
		// Внутри контейнера слушаем на всех интерфейсах; наружу порт выставляется
		// только на 127.0.0.1 хоста через проброс в docker-compose.
		addr = ":8087"
	}

	mux := http.NewServeMux()
	httpserver.RegisterLivez(mux)
	mux.HandleFunc("/internal/broadcast", a.handleBroadcast(token))

	srv := httpserver.New(addr, mux)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("[tg-bot] broadcast endpoint listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[tg-bot] broadcast server error: %v", err)
	}
}

// handleBroadcast обрабатывает POST /internal/broadcast.
func (a *App) handleBroadcast(token string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Проверка токена: заголовок X-Broadcast-Token или Authorization: Bearer <token>.
		got := strings.TrimSpace(r.Header.Get("X-Broadcast-Token"))
		if got == "" {
			auth := strings.TrimSpace(r.Header.Get("Authorization"))
			got = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer"))
		}
		if got != token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var req broadcastRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		req.Message = strings.TrimSpace(req.Message)
		if req.Message == "" {
			http.Error(w, "message is required", http.StatusBadRequest)
			return
		}

		// не запускаем вторую рассылку, пока идёт первая
		if !atomic.CompareAndSwapInt32(&broadcastInProgress, 0, 1) {
			http.Error(w, "another broadcast is already in progress", http.StatusConflict)
			return
		}

		// читаем получателей
		recipients, err := a.loadAllUserIDs(r.Context())
		if err != nil {
			atomic.StoreInt32(&broadcastInProgress, 0)
			log.Printf("[tg-bot] broadcast: failed to load recipients: %v", err)
			http.Error(w, "failed to load recipients", http.StatusInternalServerError)
			return
		}

		// запускаем рассылку в фоне; HTTP-ответ отдаём сразу
		go func() {
			defer atomic.StoreInt32(&broadcastInProgress, 0)
			a.runBroadcast(a.ctx, recipients, req.Message, req.ParseMode)
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(broadcastResponse{
			Status:          "accepted",
			RecipientsTotal: len(recipients),
			Note:            "broadcast started in background",
		})
	}
}

// loadAllUserIDs читает все telegram_id из telegram_users.
func (a *App) loadAllUserIDs(ctx context.Context) ([]int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := a.pgPool.Query(queryCtx, `SELECT telegram_id FROM telegram_users`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// runBroadcast последовательно шлёт сообщение всем получателям с учётом
// rate-limit Telegram. Telegram допускает ~30 сообщений в секунду для бота;
// держим безопасный темп ~25/сек (пауза 40мс между сообщениями).
//
// Сообщения, которые не удалось доставить (пользователь заблокировал бота и
// т.п.), просто логируются — рассылка продолжается.
func (a *App) runBroadcast(ctx context.Context, recipients []int64, message, parseMode string) {
	const pauseBetween = 40 * time.Millisecond

	var sent, failed int
	start := time.Now()
	log.Printf("[tg-bot] broadcast started: recipients=%d", len(recipients))

	for _, id := range recipients {
		select {
		case <-ctx.Done():
			log.Printf("[tg-bot] broadcast cancelled: sent=%d failed=%d of %d", sent, failed, len(recipients))
			return
		default:
		}

		msg := tgbotapi.NewMessage(id, message)
		if parseMode != "" {
			msg.ParseMode = parseMode
		}
		msg.ReplyMarkup = mainMenuKeyboard()

		if _, err := a.bot.Send(msg); err != nil {
			failed++
			log.Printf("[tg-bot] broadcast: failed to send to %d: %v", id, err)
		} else {
			sent++
		}

		time.Sleep(pauseBetween)
	}

	log.Printf("[tg-bot] broadcast finished: sent=%d failed=%d of %d in %s",
		sent, failed, len(recipients), time.Since(start))
}
