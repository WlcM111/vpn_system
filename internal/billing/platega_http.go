package billing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"vpn-platform/internal/billing/platega"
	commonmetrics "vpn-platform/internal/common/metrics"
)

// plategaCheckSlots ограничивает число одновременных фоновых сверок после
// колбэков. Если слотов нет, сверку выполнит воркер после истечения аренды.
var plategaCheckSlots = make(chan struct{}, 16)

// handlePlategaWebhook принимает колбэки Platega.
//
// Контракт (docs.platega.io, «Колбэки — общая информация», проверено
// 2026-11-05): POST на адрес из ЛК (Настройки → Callback URLs), заголовки
// X-MerchantId и X-Secret, ответ 200 в течение 30 секунд; без 200 Platega
// повторяет доставку примерно трижды (через 5, 10 и 15 минут); порядок не
// гарантирован, дубли нормальны.
//
// Ответ 200 отдаётся только после того, как колбэк надёжно зафиксирован в
// БД (заказ поставлен на сверку). Сверка статуса выполняется после ответа:
// её результат на ответ не влияет, а при сбое её повторит воркер.
func (h *HTTPHandlers) handlePlategaWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := h.service
	if !svc.plategaCfg.Enabled || svc.platega == nil {
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("disabled").Inc()
		http.Error(w, "webhook disabled", http.StatusServiceUnavailable)
		return
	}
	if !platega.AuthenticateCallback(r.Header, svc.plategaCfg.Client.MerchantID, svc.plategaCfg.Client.Secret) {
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("unauthorized").Inc()
		slog.Warn("platega callback rejected: authentication failed", "remote", remoteHost(r.RemoteAddr))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, platega.MaxCallbackBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("malformed").Inc()
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	cb, err := platega.ParseCallback(raw)
	if err != nil {
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("malformed").Inc()
		slog.Warn("platega callback rejected: malformed body", "err", err)
		http.Error(w, "bad callback", http.StatusBadRequest)
		return
	}

	sum := sha256.Sum256(raw)
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	orderID, paymentID, err := svc.RegisterPlategaCallback(ctx, cb, hex.EncodeToString(sum[:]))
	if err != nil {
		// Колбэк не зафиксирован — просим Platega повторить доставку.
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("error").Inc()
		slog.Error("platega callback registration failed", "payment_id", cb.ID, "err", err)
		http.Error(w, "temporary error", http.StatusInternalServerError)
		return
	}
	if orderID != "" {
		commonmetrics.BillingPlategaCallbacksTotal.WithLabelValues("accepted").Inc()
		slog.Info("platega callback accepted", "order_id", orderID, "payment_id", paymentID, "hint_status", cb.RawStatus)
		select {
		case plategaCheckSlots <- struct{}{}:
			go func() {
				defer func() { <-plategaCheckSlots }()
				checkCtx, cancelCheck := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancelCheck()
				if err := svc.CheckPlategaPayment(checkCtx, orderID, paymentID, 1); err != nil {
					slog.Warn("platega check after callback failed, reconciler will retry", "order_id", orderID, "err", err)
				}
			}()
		default:
			slog.Warn("platega check after callback deferred to reconciler", "order_id", orderID)
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func remoteHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// RunPlategaReconciler — воркер сверки заказов Platega. Колбэк — основной
// сигнал, но Platega повторяет его всего несколько раз; воркер гарантирует,
// что подтверждённая оплата будет найдена, даже если все колбэки потерялись
// (сервис был недоступен, сеть, ошибка прокси).
func RunPlategaReconciler(ctx context.Context, svc *Service) {
	tick := svc.plategaCfg.ReconcileTick
	if tick <= 0 {
		tick = 30 * time.Second
	}
	if !svc.plategaCfg.Enabled {
		slog.Info("platega reconciler: platega is not configured; only stale-creation sweep and metrics run")
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("platega reconciler stopped")
			return
		case <-ticker.C:
			passCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			if err := svc.ReconcilePlatega(passCtx); err != nil {
				slog.Error("platega reconcile pass failed", "err", err)
			}
			cancel()
		}
	}
}
