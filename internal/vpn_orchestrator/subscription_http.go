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

	res, err := h.service.RenderSubscriptionFeedDetailed(ctx, token)
	if err != nil {
		switch {
		case errors.Is(err, ErrAccessDenied):
			http.Error(w, "subscription is not active", http.StatusForbidden)
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
	w.Header().Set("Profile-Title", "VPN Platform")
	w.Header().Set("Profile-Update-Interval", "24")
	if res.Access != nil {
		until := res.Access.AccessUntil
		if res.Access.Status == "grace" {
			until = res.Access.GraceUntil
		}
		if until != nil {
			w.Header().Set("Subscription-Userinfo", fmt.Sprintf("upload=0; download=0; total=0; expire=%d", until.Unix()))
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
