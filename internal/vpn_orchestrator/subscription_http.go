package vpn_orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"vpn-platform/internal/common/ratelimit"
	commonredis "vpn-platform/internal/common/redis"
)

type HTTPHandlers struct {
	service      *Service
	redis        *commonredis.Client
	localLimiter *ratelimit.Limiter
}

func NewHTTPHandlers(service *Service) *HTTPHandlers {
	return &HTTPHandlers{
		service:      service,
		redis:        commonredis.NewFromEnv(),
		localLimiter: ratelimit.New(time.Minute),
	}
}

func (h *HTTPHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/sub/", h.handleSubscriptionFeed)
	mux.HandleFunc("/s/", h.handleSubscriptionFeed)
	h.RegisterAdmin(mux)
}

func (h *HTTPHandlers) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *HTTPHandlers) handleSubscriptionFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := strings.TrimPrefix(r.URL.Path, "/sub/")
	if token == r.URL.Path {
		token = strings.TrimPrefix(r.URL.Path, "/s/")
	}
	token = strings.Trim(strings.TrimSpace(token), "/")
	if token == "" || strings.Contains(token, "/") {
		http.NotFound(w, r)
		return
	}

	if !h.allowSubscriptionRequest(r.Context(), token) {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	group := detectClientGroup(r)
	res, err := h.service.RenderSubscriptionFeedDetailed(ctx, token, group)
	if err != nil {
		switch {
		case errors.Is(err, ErrAccessDenied):
			// Подписка истекла или отозвана: отдаём пустой список серверов и
			// вежливое объяснение со ссылкой на бота вместо сухой ошибки 403.
			// Статус 200 обязателен — иначе клиент сочтёт обновление неудачным
			// и оставит у пользователя старые нерабочие конфиги.
			writeExpiredSubscription(w, h.service.cfg.FeedFormat)
		case errors.Is(err, ErrNoPoolItems):
			http.Error(w, "no vpn pool items configured", http.StatusServiceUnavailable)
		default:
			slog.Error("render subscription feed failed", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", res.ContentType)
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Profile-Title", "House VPN")
	w.Header().Set("Profile-Update-Interval", "24")
	// Сплит-роутинг: правила приезжают клиенту вместе с подпиской и
	// применяются автоматически. Пустое значение — роутинг не настроен,
	// подписка отдаётся как обычно (fail-open).
	if res.RoutingB64 != "" {
		w.Header().Set("routing", res.RoutingB64)
	}
	if res.Access != nil {
		until := res.Access.AccessUntil
		if res.Access.Status == "grace" {
			until = res.Access.GraceUntil
		}
		if until != nil {
			w.Header().Set("Subscription-Userinfo", fmt.Sprintf("upload=%d; download=%d; total=0; expire=%d", res.Uplink, res.Downlink, until.Unix()))
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(res.Body)
}

func (h *HTTPHandlers) allowSubscriptionRequest(ctx context.Context, token string) bool {
	limit := 30
	if raw := strings.TrimSpace(os.Getenv("SUBSCRIPTION_RATE_LIMIT_PER_MINUTE")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	// Redis недоступен или ошибка — используем локальный in-memory лимитер
	// (fail-safe вместо fail-open: грубая защита лучше, чем никакой).
	if h.redis == nil {
		return h.localLimiter.Allow(token, limit)
	}
	key := "ratelimit:subscription:" + token
	count, err := h.redis.Incr(ctx, key)
	if err != nil {
		return h.localLimiter.Allow(token, limit)
	}
	if count == 1 {
		_ = h.redis.Expire(ctx, key, time.Minute)
	}
	return count <= int64(limit)
}
