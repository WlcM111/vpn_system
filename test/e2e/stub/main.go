// Stub YooKassa API для E2E-тестов.
//
// Обслуживает два эндпоинта, которые дёргает billing-service:
//
//	POST /payments        — создание платежа (checkout)
//	GET  /payments/{id}   — проверка статуса при обработке вебхука
//
// Все созданные платежи считаются оплаченными: цель теста — проверить путь
// «оплата прошла → доступ выдан», а не поведение платёжного провайдера.
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

	log.Printf("[stub] YooKassa stub слушает %s", addr)
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
