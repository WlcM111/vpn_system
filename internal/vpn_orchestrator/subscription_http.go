package vpn_orchestrator

import (
	"context"
	"encoding/base64"
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

// ============================================================================
// Заголовки подписки для Happ.
//
// Формат заголовков — из официальной документации Happ:
// https://www.happ.su/main/dev-docs/app-management
//
//	profile-title          — имя подписки, plain или "base64:" + base64(UTF-8),
//	                         не длиннее 25 символов;
//	subscription-userinfo  — одна строка, поля через "; ":
//	                         upload, download, total (байты), expire (unix).
//
// Клиент показывает слева израсходованное (upload + download), справа после
// "/" — total.
// ============================================================================

// subscriptionProfileTitle — отображаемое имя ВСЕЙ подписки.
//
// Значок ускорения относится к подписке целиком, а не к отдельным
// конфигурациям: в имена конфигураций он не добавляется.
const subscriptionProfileTitle = "⚡ House VPN"

// cdnDisplayBytesPerNode — объём CDN-трафика, который показывается в Happ за
// каждый узел с активной CDN-конфигурацией. Итоговое total = n × это значение.
//
// ЕДИНИЦЫ. Значение десятичное (10^9), как принято в остальном проекте:
// defaultCDNQuotaLimitBytes = 20_000_000_000 назван «20 GB».
//
// ВНИМАНИЕ: это витрина, а НЕ лимит. Фактическое ограничение задаётся
// CDN_QUOTA_LIMIT_BYTES и здесь сознательно не используется: изменение витрины
// не должно менять тарифную политику.
const cdnDisplayBytesPerNode int64 = 10_000_000_000

// encodeProfileTitle кодирует имя подписки для HTTP-заголовка.
//
// Заголовки HTTP ограничены ISO-8859-1 (RFC 7230 §3.2.4), а «⚡» (U+26A1) в
// эту кодировку не входит: без base64 клиент получит искажённые байты.
func encodeProfileTitle(title string) string {
	return "base64:" + base64.StdEncoding.EncodeToString([]byte(title))
}

// cdnTotalBytes считает значение поля total для n узлов.
//
// n = 0 даёт 0: по соглашению subscription-userinfo это означает «объём не
// ограничен», и шкала расхода в клиенте не рисуется.
//
// Переполнение int64 недостижимо: при 10^10 байт на узел предел наступил бы
// примерно на 920 миллионах узлов.
func cdnTotalBytes(nodes int) int64 {
	if nodes <= 0 {
		return 0
	}
	return int64(nodes) * cdnDisplayBytesPerNode
}

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

	writeSubscriptionHeaders(w, res)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(res.Body)
}

// writeSubscriptionHeaders выставляет все заголовки успешного ответа подписки.
//
// Выделено из обработчика, чтобы состав заголовков проверялся тестом без
// поднятого сервиса: ошибка здесь не роняет запрос, а тихо ломает отображение
// в клиенте — такие дефекты замечают пользователи, а не мониторинг.
func writeSubscriptionHeaders(w http.ResponseWriter, res *SubscriptionFeedResult) {
	if res.ContentType != "" {
		w.Header().Set("Content-Type", res.ContentType)
	}
	// Расход обязан обновляться при каждом обновлении подписки. Кэш здесь —
	// прямая причина «зависшей» цифры у пользователя, поэтому запрет полный:
	// no-store для CDN и прокси, no-cache и Expires для старых клиентов.
	// ETag сознательно не выставляется: ответ 304 оставил бы у клиента
	// прежние значения upload/download.
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Profile-Title", encodeProfileTitle(subscriptionProfileTitle))
	// Интервал автообновления подписки в часах. 5 вместо суток: правки
	// манифеста маршрутизации и состава узлов доезжают до пользователей
	// в тот же день, а не на следующий.
	w.Header().Set("Profile-Update-Interval", "5")
	// Сплит-роутинг: правила приезжают клиенту вместе с подпиской и
	// применяются автоматически. Пустое значение — роутинг не настроен,
	// подписка отдаётся как обычно (fail-open).
	if res.RoutingB64 != "" {
		w.Header().Set("routing", res.RoutingB64)
	}
	if res.Access == nil {
		return
	}
	until := res.Access.AccessUntil
	if res.Access.Status == "grace" {
		until = res.Access.GraceUntil
	}
	if until == nil {
		return
	}
	// upload/download — только CDN-трафик за текущий период квоты
	// (Repository.SumCDNUsageForUser). total — n × объём на узел, где n
	// посчитано по фактически выданным CDN-ссылкам этого же ответа. Оба числа
	// и срок действия уходят одним заголовком, как требует документация Happ.
	w.Header().Set("Subscription-Userinfo", fmt.Sprintf(
		"upload=%d; download=%d; total=%d; expire=%d",
		res.Uplink, res.Downlink, cdnTotalBytes(res.CDNNodes), until.Unix()))
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
