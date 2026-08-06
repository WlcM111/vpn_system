package vpn_orchestrator

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"strings"
)

func (h *HTTPHandlers) RegisterAdmin(mux *http.ServeMux) {
	mux.HandleFunc("/admin/nodes", h.withAdminAuth(h.handleAdminNodes))
	mux.HandleFunc("/admin/nodes/", h.withAdminAuth(h.handleAdminNodeByID))
	mux.HandleFunc("/admin/pool-items", h.withAdminAuth(h.handleAdminPoolItems))
	mux.HandleFunc("/admin/cdn-endpoints", h.withAdminAuth(h.handleAdminCDNEndpoints))
	mux.HandleFunc("/admin/cdn-endpoints/", h.withAdminAuth(h.handleAdminCDNEndpointByKey))
	mux.HandleFunc("/admin/grpc-endpoints", h.withAdminAuth(h.handleAdminGRPCEndpoints))
	mux.HandleFunc("/admin/grpc-endpoints/", h.withAdminAuth(h.handleAdminGRPCEndpointByKey))
	mux.HandleFunc("/admin/hysteria-endpoints", h.withAdminAuth(h.handleAdminHysteriaEndpoints))
	mux.HandleFunc("/admin/hysteria-endpoints/", h.withAdminAuth(h.handleAdminHysteriaEndpointByKey))
	mux.HandleFunc("/admin/users/", h.withAdminAuth(h.handleAdminUserActions))
}

func (h *HTTPHandlers) withAdminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := os.Getenv("ORCHESTRATOR_ADMIN_USER")
		pass := os.Getenv("ORCHESTRATOR_ADMIN_PASS")
		if strings.TrimSpace(user) == "" || strings.TrimSpace(pass) == "" {
			http.Error(w, "admin api disabled", http.StatusServiceUnavailable)
			return
		}
		actualUser, actualPass, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(actualUser), []byte(user)) != 1 || subtle.ConstantTimeCompare([]byte(actualPass), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="vpn-orchestrator-admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (h *HTTPHandlers) handleAdminNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AdminNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if err := h.service.repo.UpsertAdminNode(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPHandlers) handleAdminNodeByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nodeID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/nodes/"), "/")
	if nodeID == "" {
		http.NotFound(w, r)
		return
	}
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if err := h.service.repo.SetAdminNodeEnabled(r.Context(), nodeID, req.Enabled); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPHandlers) handleAdminPoolItems(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AdminPoolItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if err := h.service.repo.UpsertAdminPoolItem(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPHandlers) handleAdminUserActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/users/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "resync" {
		http.NotFound(w, r)
		return
	}
	telegramID, err := parseInt64(parts[0])
	if err != nil {
		http.Error(w, "bad telegram_id", http.StatusBadRequest)
		return
	}
	access, err := h.service.repo.GetAccessByTelegramID(r.Context(), telegramID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_, err = h.service.ensureUserCredentialsAndSync(r.Context(), access)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAdminCDNEndpoints — создание/обновление CDN-эндпоинта (POST).
func (h *HTTPHandlers) handleAdminCDNEndpoints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AdminCDNEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.CDNKey) == "" || strings.TrimSpace(req.Address) == "" {
		http.Error(w, "cdn_key and address are required", http.StatusBadRequest)
		return
	}
	if err := h.service.repo.UpsertAdminCDNEndpoint(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAdminCDNEndpointByKey — удаление CDN-эндпоинта по ключу (DELETE).
func (h *HTTPHandlers) handleAdminCDNEndpointByKey(w http.ResponseWriter, r *http.Request) {
	cdnKey := strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/cdn-endpoints/"), "/")
	if cdnKey == "" {
		http.Error(w, "cdn_key required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := h.service.repo.DeleteCDNEndpoint(r.Context(), cdnKey); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAdminGRPCEndpoints — создание/обновление gRPC-эндпоинта (POST).
func (h *HTTPHandlers) handleAdminGRPCEndpoints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AdminGRPCEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.GRPCKey) == "" || strings.TrimSpace(req.Address) == "" {
		http.Error(w, "grpc_key and address are required", http.StatusBadRequest)
		return
	}
	if err := h.service.repo.UpsertAdminGRPCEndpoint(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAdminGRPCEndpointByKey — удаление gRPC-эндпоинта по ключу (DELETE).
func (h *HTTPHandlers) handleAdminGRPCEndpointByKey(w http.ResponseWriter, r *http.Request) {
	grpcKey := strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/grpc-endpoints/"), "/")
	if grpcKey == "" {
		http.Error(w, "grpc_key required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := h.service.repo.DeleteGRPCEndpoint(r.Context(), grpcKey); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAdminHysteriaEndpoints — создание/обновление Hysteria-эндпоинта (POST).
func (h *HTTPHandlers) handleAdminHysteriaEndpoints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AdminHysteriaEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.HysteriaKey) == "" || strings.TrimSpace(req.Address) == "" {
		http.Error(w, "hysteria_key and address are required", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.ServerKey) == "" {
		http.Error(w, "server_key is required: hysteria link must point to the user's own node", http.StatusBadRequest)
		return
	}
	if err := h.service.repo.UpsertAdminHysteriaEndpoint(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAdminHysteriaEndpointByKey — удаление Hysteria-эндпоинта (DELETE).
func (h *HTTPHandlers) handleAdminHysteriaEndpointByKey(w http.ResponseWriter, r *http.Request) {
	hysteriaKey := strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/hysteria-endpoints/"), "/")
	if hysteriaKey == "" {
		http.Error(w, "hysteria_key required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := h.service.repo.DeleteHysteriaEndpoint(r.Context(), hysteriaKey); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
