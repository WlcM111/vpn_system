package vpn_orchestrator

import (
	"context"
	"time"
)

// ============================================================================
// Репозиторий gRPC-эндпоинтов (таблица vpn_grpc_endpoints).
// ============================================================================

// AdminGRPCEndpointRequest — тело запроса Admin API для создания/обновления gRPC.
// Привязка к серверу задаётся ServerKey; пустой ServerKey = глобальный эндпоинт.
type AdminGRPCEndpointRequest struct {
	GRPCKey    string `json:"grpc_key"`
	ServerKey  string `json:"server_key,omitempty"`
	Enabled    *bool  `json:"enabled,omitempty"`
	SortOrder  *int   `json:"sort_order,omitempty"`
	InboundTag string `json:"inbound_tag,omitempty"`

	Address     string `json:"address"`
	ServerName  string `json:"server_name,omitempty"`
	Host        string `json:"host,omitempty"`
	Port        *int   `json:"port,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	Mode        string `json:"mode,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	ALPN        string `json:"alpn,omitempty"`
	Remarks     string `json:"remarks,omitempty"`
}

// ListEnabledGRPCEndpoints возвращает все включённые gRPC-эндпоинты,
// отсортированные по sort_order, id. Привязанные и глобальные — вперемешку;
// выбор конкретного делает selectGRPCForServer.
func (r *Repository) ListEnabledGRPCEndpoints(ctx context.Context) ([]GRPCEndpoint, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT
			grpc_key, COALESCE(server_key, ''), enabled, sort_order, inbound_tag,
			address, server_name, host, port, service_name, mode, fingerprint, alpn, remarks
		FROM vpn_grpc_endpoints
		WHERE enabled = TRUE
		ORDER BY sort_order, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GRPCEndpoint
	for rows.Next() {
		var e GRPCEndpoint
		if err := rows.Scan(
			&e.GRPCKey, &e.ServerKey, &e.Enabled, &e.SortOrder, &e.InboundTag,
			&e.Address, &e.ServerName, &e.Host, &e.Port, &e.ServiceName, &e.Mode, &e.Fingerprint, &e.ALPN, &e.Remarks,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UpsertAdminGRPCEndpoint создаёт/обновляет gRPC-эндпоинт (Admin API).
func (r *Repository) UpsertAdminGRPCEndpoint(ctx context.Context, req AdminGRPCEndpointRequest) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	sortOrder := 100
	if req.SortOrder != nil {
		sortOrder = *req.SortOrder
	}
	port := 443
	if req.Port != nil && *req.Port > 0 {
		port = *req.Port
	}

	var serverKey any
	if req.ServerKey == "" {
		serverKey = nil
	} else {
		serverKey = req.ServerKey
	}

	_, err := r.pool.Exec(queryCtx, `
		INSERT INTO vpn_grpc_endpoints (
			grpc_key, server_key, enabled, sort_order, inbound_tag,
			address, server_name, host, port, service_name, mode, fingerprint, alpn, remarks
		) VALUES (
			$1, $2, $3, $4, COALESCE(NULLIF($5, ''), 'vless-grpc-cdn-in'),
			$6, $7, $8, $9,
			COALESCE(NULLIF($10, ''), 'api.grpc'),
			COALESCE(NULLIF($11, ''), 'gun'),
			COALESCE(NULLIF($12, ''), 'chrome'),
			COALESCE(NULLIF($13, ''), 'h2'),
			COALESCE(NULLIF($14, ''), 'race-src-grpc')
		)
		ON CONFLICT (grpc_key) DO UPDATE SET
			server_key = EXCLUDED.server_key,
			enabled = EXCLUDED.enabled,
			sort_order = EXCLUDED.sort_order,
			inbound_tag = EXCLUDED.inbound_tag,
			address = EXCLUDED.address,
			server_name = EXCLUDED.server_name,
			host = EXCLUDED.host,
			port = EXCLUDED.port,
			service_name = EXCLUDED.service_name,
			mode = EXCLUDED.mode,
			fingerprint = EXCLUDED.fingerprint,
			alpn = EXCLUDED.alpn,
			remarks = EXCLUDED.remarks,
			updated_at = now()
	`,
		req.GRPCKey, serverKey, enabled, sortOrder, req.InboundTag,
		req.Address, req.ServerName, req.Host, port, req.ServiceName, req.Mode, req.Fingerprint, req.ALPN, req.Remarks,
	)
	return err
}

// DeleteGRPCEndpoint удаляет gRPC-эндпоинт по grpc_key.
func (r *Repository) DeleteGRPCEndpoint(ctx context.Context, grpcKey string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.pool.Exec(queryCtx, `DELETE FROM vpn_grpc_endpoints WHERE grpc_key = $1`, grpcKey)
	return err
}
