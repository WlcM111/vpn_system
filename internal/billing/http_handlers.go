package billing

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type HTTPHandlers struct {
	service *Service
}

func NewHTTPHandlers(service *Service) *HTTPHandlers {
	return &HTTPHandlers{service: service}
}

func (h *HTTPHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/webhooks/yookassa/", h.handleYooKassaWebhook)
	mux.HandleFunc("/checkout/success", h.handleCheckoutSuccess)
	mux.HandleFunc("/checkout/fail", h.handleCheckoutFail)
}

func (h *HTTPHandlers) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *HTTPHandlers) handleYooKassaWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if strings.EqualFold(strings.TrimSpace(os.Getenv("YOOKASSA_ENFORCE_IP_CHECK")), "true") && !isTrustedYooKassaIP(r.RemoteAddr) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	expectedToken := strings.TrimSpace(os.Getenv("YOOKASSA_WEBHOOK_TOKEN"))
	if expectedToken == "" {
		slog.Error("YOOKASSA_WEBHOOK_TOKEN is not configured")
		http.Error(w, "webhook disabled", http.StatusServiceUnavailable)
		return
	}

	actualToken := strings.Trim(strings.TrimPrefix(r.URL.Path, "/webhooks/yookassa/"), "/")
	if actualToken == "" {
		actualToken = strings.TrimSpace(r.Header.Get("X-Webhook-Token"))
	}
	if subtle.ConstantTimeCompare([]byte(actualToken), []byte(expectedToken)) != 1 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	var notification yooKassaWebhookNotification
	if err := json.Unmarshal(raw, &notification); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])

	if err := h.service.ProcessWebhook(ctx, &notification, raw, fingerprint); err != nil {
		if errors.Is(err, ErrDuplicateWebhook) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("duplicate"))
			return
		}
		slog.Error("yookassa webhook processing failed", "err", err)
		http.Error(w, "processing error", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *HTTPHandlers) handleCheckoutSuccess(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Платеж подтвержден. Вернись в Telegram-бота."))
}

func (h *HTTPHandlers) handleCheckoutFail(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Платеж не был завершен. Вернись в Telegram-бота и попробуй снова."))
}

func isTrustedYooKassaIP(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return false
	}

	cidrs := []string{
		"185.71.76.0/27",
		"185.71.77.0/27",
		"77.75.153.0/25",
		"77.75.156.11/32",
		"77.75.156.35/32",
		"77.75.154.128/25",
		"2a02:5180::/32",
	}
	for _, raw := range cidrs {
		_, network, err := net.ParseCIDR(raw)
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}
