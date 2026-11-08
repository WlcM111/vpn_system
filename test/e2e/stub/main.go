// Stub платёжных API (YooKassa и Platega) для E2E-тестов.
//
// YooKassa — два эндпоинта, которые дёргает billing-service:
//
//	POST /payments        — создание платежа (checkout)
//	GET  /payments/{id}   — проверка статуса при обработке вебхука
//
// Все созданные платежи YooKassa считаются оплаченными: цель теста —
// проверить путь «оплата прошла → доступ выдан», а не поведение провайдера.
//
// Platega — по документированному контракту (docs.platega.io):
//
//	POST /transaction/process — создание транзакции (статус PENDING)
//	GET  /transaction/{id}    — статус транзакции
//
// и служебный эндпоинт теста, которого у Platega нет:
//
//	POST /stub/platega/status/{id}?status=CONFIRMED — сменить статус
//
// Заголовки X-MerchantId / X-Secret сверяются со STUB_PLATEGA_MERCHANT_ID /
// STUB_PLATEGA_SECRET, если они заданы.
//
// Запускается только в docker-compose.e2e.yml и никогда в проде.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type payment struct {
	ID            string         `json:"id"`
	Status        string         `json:"status"`
	Paid          bool           `json:"paid"`
	Amount        amount         `json:"amount"`
	Confirmation  confirm        `json:"confirmation"`
	PaymentMethod method         `json:"payment_method"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	CreatedAt     string         `json:"created_at"`
}

type amount struct {
	Value    string `json:"value"`
	Currency string `json:"currency"`
}

type confirm struct {
	Type            string `json:"type"`
	ConfirmationURL string `json:"confirmation_url"`
}

type method struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Saved bool   `json:"saved"`
}

type store struct {
	mu   sync.RWMutex
	data map[string]payment
}

func (s *store) put(p payment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[p.ID] = p
}

func (s *store) get(id string) (payment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.data[id]
	return p, ok
}

func main() {
	addr := os.Getenv("STUB_ADDR")
	if addr == "" {
		addr = ":8099"
	}

	st := &store{data: make(map[string]payment)}
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/payments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Amount   amount         `json:"amount"`
			Metadata map[string]any `json:"metadata"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		p := payment{
			ID:     uuid.NewString(),
			Status: "succeeded",
			Paid:   true,
			Amount: req.Amount,
			Confirmation: confirm{
				Type:            "redirect",
				ConfirmationURL: "https://stub.local/confirm",
			},
			PaymentMethod: method{ID: uuid.NewString(), Type: "bank_card", Saved: true},
			Metadata:      req.Metadata,
			CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		}
		st.put(p)

		log.Printf("[stub] создан платёж %s на %s %s", p.ID, p.Amount.Value, p.Amount.Currency)
		writeJSON(w, http.StatusOK, p)
	})

	mux.HandleFunc("/payments/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/payments/"), "/")
		if id == "" {
			http.Error(w, "payment id required", http.StatusBadRequest)
			return
		}

		// Возвраты в E2E не проверяем — отвечаем успехом, чтобы не ломать поток.
		if strings.HasSuffix(id, "/refunds") || strings.Contains(id, "/") {
			writeJSON(w, http.StatusOK, map[string]string{"status": "succeeded"})
			return
		}

		p, ok := st.get(id)
		if !ok {
			// Платёж мог быть создан не через стаб (например, тест шлёт свой ID).
			// Считаем его оплаченным — иначе вебхук не пройдёт проверку.
			p = payment{ID: id, Status: "succeeded", Paid: true}
		}
		writeJSON(w, http.StatusOK, p)
	})

	registerPlatega(mux)

	log.Printf("[stub] payment stub слушает %s", addr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("[stub] %v", err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// Platega
// ---------------------------------------------------------------------------

type plategaTx struct {
	ID       string
	Status   string
	Amount   json.Number
	Currency string
	Payload  string
}

type plategaStore struct {
	mu  sync.Mutex
	txs map[string]*plategaTx
}

func registerPlatega(mux *http.ServeMux) {
	st := &plategaStore{txs: map[string]*plategaTx{}}
	merchant := strings.TrimSpace(os.Getenv("STUB_PLATEGA_MERCHANT_ID"))
	secret := os.Getenv("STUB_PLATEGA_SECRET")
	authOK := func(r *http.Request) bool {
		if merchant == "" && secret == "" {
			return true
		}
		return strings.EqualFold(r.Header.Get("X-MerchantId"), merchant) && r.Header.Get("X-Secret") == secret
	}

	mux.HandleFunc("/transaction/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !authOK(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "UNAUTHORIZED", "message": "auth"})
			return
		}
		var req struct {
			PaymentMethod  int `json:"paymentMethod"`
			PaymentDetails struct {
				Amount   json.Number `json:"amount"`
				Currency string      `json:"currency"`
			} `json:"paymentDetails"`
			Payload  string `json:"payload"`
			Metadata struct {
				UserEmail string `json:"user_email"`
			} `json:"metadata"`
		}
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&req); err != nil || req.PaymentDetails.Amount == "" || req.Metadata.UserEmail == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "VALIDATION", "message": "bad request"})
			return
		}
		tx := &plategaTx{ID: uuid.NewString(), Status: "PENDING", Amount: req.PaymentDetails.Amount, Currency: req.PaymentDetails.Currency, Payload: req.Payload}
		st.mu.Lock()
		st.txs[tx.ID] = tx
		st.mu.Unlock()
		log.Printf("[stub] platega: создана транзакция %s на %s %s", tx.ID, tx.Amount, tx.Currency)
		writeJSON(w, http.StatusOK, map[string]any{
			"paymentMethod":  "SBPQR",
			"transactionId":  tx.ID,
			"redirect":       "https://stub.local/platega/" + tx.ID,
			"paymentDetails": string(tx.Amount) + " " + tx.Currency,
			"status":         "PENDING",
			"expiresIn":      "00:15:00",
			"merchantId":     merchant,
		})
	})

	mux.HandleFunc("/transaction/", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "UNAUTHORIZED", "message": "auth"})
			return
		}
		id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/transaction/"), "/")
		st.mu.Lock()
		tx, ok := st.txs[id]
		var cp plategaTx
		if ok {
			cp = *tx
		}
		st.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"code": "NOT_FOUND"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id":             cp.ID,
			"status":         cp.Status,
			"paymentDetails": map[string]any{"amount": cp.Amount, "currency": cp.Currency},
			"payload":        cp.Payload,
		})
	})

	mux.HandleFunc("/stub/platega/status/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/stub/platega/status/"), "/")
		status := strings.ToUpper(r.URL.Query().Get("status"))
		st.mu.Lock()
		tx, ok := st.txs[id]
		if ok {
			tx.Status = status
		}
		st.mu.Unlock()
		if !ok {
			http.Error(w, "unknown transaction", http.StatusNotFound)
			return
		}
		log.Printf("[stub] platega: транзакция %s → %s", id, status)
		w.WriteHeader(http.StatusNoContent)
	})
}
