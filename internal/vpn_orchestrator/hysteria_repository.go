package vpn_orchestrator

import (
	"context"
	"time"
)

// ============================================================================
// Репозиторий Hysteria2-эндпоинтов (таблица vpn_hysteria_endpoints).
// ============================================================================

// AdminHysteriaEndpointRequest — тело запроса Admin API.
type AdminHysteriaEndpointRequest struct {
	HysteriaKey string `json:"hysteria_key"`
	ServerKey   string `json:"server_key,omitempty"`
	Enabled     *bool  `json:"enabled,omitempty"`
	SortOrder   *int   `json:"sort_order,omitempty"`

	Address      string `json:"address"`
	Port         *int   `json:"port,omitempty"`
	SNI          string `json:"sni,omitempty"`
	Insecure     *bool  `json:"insecure,omitempty"`
	ObfsType     string `json:"obfs_type,omitempty"`
	ObfsPassword string `json:"obfs_password,omitempty"`
	Remarks      string `json:"remarks,omitempty"`
}

// ListEnabledHysteriaEndpoints возвращает включённые эндпоинты по sort_order, id.
func (r *Repository) ListEnabledHysteriaEndpoints(ctx context.Context) ([]HysteriaEndpoint, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT
			hysteria_key, COALESCE(server_key, ''), enabled, sort_order,
			address, port, sni, insecure, obfs_type, obfs_password, remarks
		FROM vpn_hysteria_endpoints
		WHERE enabled = TRUE
		ORDER BY sort_order, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HysteriaEndpoint
	for rows.Next() {
		var e HysteriaEndpoint
		if err := rows.Scan(
			&e.HysteriaKey, &e.ServerKey, &e.Enabled, &e.SortOrder,
			&e.Address, &e.Port, &e.SNI, &e.Insecure, &e.ObfsType, &e.ObfsPassword, &e.Remarks,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UpsertAdminHysteriaEndpoint создаёт/обновляет эндпоинт (Admin API).
func (r *Repository) UpsertAdminHysteriaEndpoint(ctx context.Context, req AdminHysteriaEndpointRequest) error {
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
	insecure := false
	if req.Insecure != nil {
		insecure = *req.Insecure
	}

	var serverKey any
	if req.ServerKey == "" {
		serverKey = nil
	} else {
		serverKey = req.ServerKey
	}

	_, err := r.pool.Exec(queryCtx, `
		INSERT INTO vpn_hysteria_endpoints (
			hysteria_key, server_key, enabled, sort_order,
			address, port, sni, insecure, obfs_type, obfs_password, remarks
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			COALESCE(NULLIF($11, ''), 'Hysteria')
		)
		ON CONFLICT (hysteria_key) DO UPDATE SET
			server_key = EXCLUDED.server_key,
			enabled = EXCLUDED.enabled,
			sort_order = EXCLUDED.sort_order,
			address = EXCLUDED.address,
			port = EXCLUDED.port,
			sni = EXCLUDED.sni,
			insecure = EXCLUDED.insecure,
			obfs_type = EXCLUDED.obfs_type,
			obfs_password = EXCLUDED.obfs_password,
			remarks = EXCLUDED.remarks,
			updated_at = now()
	`,
		req.HysteriaKey, serverKey, enabled, sortOrder,
		req.Address, port, req.SNI, insecure, req.ObfsType, req.ObfsPassword, req.Remarks,
	)
	return err
}

// DeleteHysteriaEndpoint удаляет эндпоинт по ключу.
func (r *Repository) DeleteHysteriaEndpoint(ctx context.Context, hysteriaKey string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.pool.Exec(queryCtx, `DELETE FROM vpn_hysteria_endpoints WHERE hysteria_key = $1`, hysteriaKey)
	return err
}
