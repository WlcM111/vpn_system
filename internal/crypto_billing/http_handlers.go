package crypto_billing

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type HTTPHandlers struct {
	service *Service
	cfg     Config
}

func NewHTTPHandlers(service *Service, cfg Config) *HTTPHandlers {
	return &HTTPHandlers{service: service, cfg: cfg}
}

// Register регистрирует все эндпоинты этого сервиса.
// /healthz — liveness, /webhooks/cryptobot/<TOKEN> — собственно webhook от CryptoBot.
// /livez и /readyz регистрируются отдельно в main.go через httpserver.RegisterLivez/Readyz,
// чтобы readyz пинговал реальный pgxpool.
func (h *HTTPHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/webhooks/cryptobot/", h.handleCryptoBotWebhook)
}

func (h *HTTPHandlers) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleCryptoBotWebhook обрабатывает webhook от CryptoBot.
// Структура защит идёт от дешёвых к дорогим:
//  1. Метод POST                  — копеечно, отсекает GET-роботов.
//  2. URL-токен (constant-time)   — копеечно, отсекает случайные запросы.
//  3. Чтение body с лимитом       — защита от DoS.
//  4. HMAC-подпись                — основной механизм аутентичности.
//  5. Передача в сервис           — здесь уже доверенный поток.
func (h *HTTPHandlers) handleCryptoBotWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Шаг 1: URL-токен. Это секрет, который мы прописываем в URL'е вебхука
	// (например, https://example.com/webhooks/cryptobot/abc123xyz). Знание URL'а
	// уже отсекает рандомные сканеры. subtle.ConstantTimeCompare — защита от timing-атак.
	actualToken := strings.Trim(strings.TrimPrefix(r.URL.Path, "/webhooks/cryptobot/"), "/")
	if subtle.ConstantTimeCompare([]byte(actualToken), []byte(h.cfg.WebhookToken)) != 1 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Шаг 2: чтение body с лимитом 1 МБ. CryptoBot вебхуки совсем небольшие
	// (несколько килобайт), 1 МБ — щедрая защита от DoS.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	// Шаг 3: HMAC-подпись. Если подписи нет или она невалидна — отдаём 401, чтобы
	// CryptoBot увидел проблему и не считал webhook успешным.
	sigHex := r.Header.Get("crypto-pay-api-signature")
	if sigHex == "" {
		http.Error(w, "missing signature", http.StatusBadRequest)
		return
	}
	if !VerifyWebhookSignature(h.cfg.APIToken, sigHex, raw) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	// Шаг 4: бизнес-логика. fingerprint = sha256(raw_body) используется для дедупа
	// внутри сервиса. Контекст с таймаутом 10 секунд — защита от зависших DB-запросов.
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])

	if err := h.service.ProcessWebhook(ctx, raw, fingerprint); err != nil {
		// Дубликат webhook'а — это НЕ ошибка для CryptoBot. Отдаём 200 OK,
		// чтобы он не ретраил тот же платёж бесконечно.
		if errors.Is(err, ErrDuplicateWebhook) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("duplicate"))
			return
		}
		// Инвойс не найден — теоретическая гонка. Отдаём 4xx, CryptoBot ретраит.
		// 422 более точное, чем 400 (тело валидное, но семантика — ресурс не готов).
		if errors.Is(err, ErrInvoiceNotFound) {
			slog.Warn("cryptobot webhook for unknown invoice (will retry)", "err", err)
			http.Error(w, "invoice not found, retry later", http.StatusUnprocessableEntity)
			return
		}
		// Любая другая ошибка — 500, CryptoBot ретраит. Логируем подробно.
		slog.Error("cryptobot webhook processing failed", "err", err)
		http.Error(w, "processing error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
