package vpn_orchestrator

import (
	"context"
	"time"
)

// ============================================================================
// Репозиторий Reality-эндпоинтов (таблица vpn_reality_endpoints).
// ============================================================================

// AdminRealityEndpointRequest — тело запроса Admin API для создания или
// обновления Reality-эндпоинта.
//
// PublicKey и ServerName обязательны по смыслу: без них сборщик ссылки вернёт
// пустую строку и пользователь не получит конфигурацию. Приватного ключа здесь
// нет — он живёт только в config.json на узле.
type AdminRealityEndpointRequest struct {
	RealityKey string `json:"reality_key"`
	ServerKey  string `json:"server_key,omitempty"`
	Enabled    *bool  `json:"enabled,omitempty"`
	SortOrder  *int   `json:"sort_order,omitempty"`
	InboundTag string `json:"inbound_tag,omitempty"`

	Address     string `json:"address"`
	Port        *int   `json:"port,omitempty"`
	ServerName  string `json:"server_name"`
	PublicKey   string `json:"public_key"`
	ShortID     string `json:"short_id,omitempty"`
	SpiderX     string `json:"spider_x,omitempty"`
	Flow        string `json:"flow,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Remarks     string `json:"remarks,omitempty"`
}

// ListEnabledRealityEndpoints возвращает включённые Reality-эндпоинты,
// отсортированные по sort_order, id.
func (r *Repository) ListEnabledRealityEndpoints(ctx context.Context) ([]RealityEndpoint, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT
			reality_key, COALESCE(server_key, ''), enabled, sort_order, inbound_tag,
			address, port, server_name, public_key, short_id, spider_x, flow, fingerprint, remarks
		FROM vpn_reality_endpoints
		WHERE enabled = TRUE
		ORDER BY sort_order, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RealityEndpoint
	for rows.Next() {
		var e RealityEndpoint
		if err := rows.Scan(
			&e.RealityKey, &e.ServerKey, &e.Enabled, &e.SortOrder, &e.InboundTag,
			&e.Address, &e.Port, &e.ServerName, &e.PublicKey, &e.ShortID,
			&e.SpiderX, &e.Flow, &e.Fingerprint, &e.Remarks,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UpsertAdminRealityEndpoint создаёт или обновляет Reality-эндпоинт.
func (r *Repository) UpsertAdminRealityEndpoint(ctx context.Context, req AdminRealityEndpointRequest) error {
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
	port := 8443
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
		INSERT INTO vpn_reality_endpoints (
			reality_key, server_key, enabled, sort_order, inbound_tag,
			address, port, server_name, public_key, short_id, spider_x, flow, fingerprint, remarks
		) VALUES (
			$1, $2, $3, $4, COALESCE(NULLIF($5, ''), 'vless-reality-in'),
			$6, $7, $8, $9, $10,
			COALESCE(NULLIF($11, ''), '/'),
			COALESCE(NULLIF($12, ''), 'xtls-rprx-vision'),
			COALESCE(NULLIF($13, ''), 'chrome'),
			COALESCE(NULLIF($14, ''), 'reality')
		)
		ON CONFLICT (reality_key) DO UPDATE SET
			server_key = EXCLUDED.server_key,
			enabled = EXCLUDED.enabled,
			sort_order = EXCLUDED.sort_order,
			inbound_tag = EXCLUDED.inbound_tag,
			address = EXCLUDED.address,
			port = EXCLUDED.port,
			server_name = EXCLUDED.server_name,
			public_key = EXCLUDED.public_key,
			short_id = EXCLUDED.short_id,
			spider_x = EXCLUDED.spider_x,
			flow = EXCLUDED.flow,
			fingerprint = EXCLUDED.fingerprint,
			remarks = EXCLUDED.remarks,
			updated_at = now()
	`,
		req.RealityKey, serverKey, enabled, sortOrder, req.InboundTag,
		req.Address, port, req.ServerName, req.PublicKey, req.ShortID,
		req.SpiderX, req.Flow, req.Fingerprint, req.Remarks,
	)
	return err
}

// DeleteRealityEndpoint удаляет Reality-эндпоинт по reality_key.
func (r *Repository) DeleteRealityEndpoint(ctx context.Context, realityKey string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.pool.Exec(queryCtx, `DELETE FROM vpn_reality_endpoints WHERE reality_key = $1`, realityKey)
	return err
}
