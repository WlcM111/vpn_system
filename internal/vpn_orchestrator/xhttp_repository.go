package vpn_orchestrator

import (
	"context"
	"time"
)

// ============================================================================
// Репозиторий XHTTP-эндпоинтов (таблица vpn_xhttp_endpoints).
// ============================================================================

// AdminXHTTPEndpointRequest — тело запроса Admin API для создания или
// обновления XHTTP-эндпоинта.
//
// server_key, inbound_tag, address и path обязательны: без привязки к серверу
// эндпоинт не выберется, без инбаунда пользователя некуда регистрировать, без
// адреса и пути не собрать ссылку.
type AdminXHTTPEndpointRequest struct {
	XHTTPKey   string `json:"xhttp_key"`
	ServerKey  string `json:"server_key"`
	Enabled    *bool  `json:"enabled,omitempty"`
	SortOrder  *int   `json:"sort_order,omitempty"`
	InboundTag string `json:"inbound_tag"`

	Address     string `json:"address"`
	ServerName  string `json:"server_name,omitempty"`
	Host        string `json:"host,omitempty"`
	Port        *int   `json:"port,omitempty"`
	Path        string `json:"path"`
	Mode        string `json:"mode,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	ALPN        string `json:"alpn,omitempty"`
	Remarks     string `json:"remarks,omitempty"`
}

// ListEnabledXHTTPEndpoints возвращает включённые XHTTP-эндпоинты,
// отсортированные по sort_order, id.
func (r *Repository) ListEnabledXHTTPEndpoints(ctx context.Context) ([]XHTTPEndpoint, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT
			xhttp_key, COALESCE(server_key, ''), enabled, sort_order, inbound_tag,
			address, server_name, host, port, path, mode, fingerprint, alpn, remarks
		FROM vpn_xhttp_endpoints
		WHERE enabled = TRUE
		ORDER BY sort_order, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []XHTTPEndpoint
	for rows.Next() {
		var e XHTTPEndpoint
		if err := rows.Scan(
			&e.XHTTPKey, &e.ServerKey, &e.Enabled, &e.SortOrder, &e.InboundTag,
			&e.Address, &e.ServerName, &e.Host, &e.Port, &e.Path,
			&e.Mode, &e.Fingerprint, &e.ALPN, &e.Remarks,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UpsertAdminXHTTPEndpoint создаёт или обновляет XHTTP-эндпоинт.
func (r *Repository) UpsertAdminXHTTPEndpoint(ctx context.Context, req AdminXHTTPEndpointRequest) error {
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

	_, err := r.pool.Exec(queryCtx, `
		INSERT INTO vpn_xhttp_endpoints (
			xhttp_key, server_key, enabled, sort_order, inbound_tag,
			address, server_name, host, port, path, mode, fingerprint, alpn, remarks
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			COALESCE(NULLIF($11, ''), 'auto'),
			COALESCE(NULLIF($12, ''), 'chrome'),
			COALESCE(NULLIF($13, ''), 'h2,http/1.1'),
			$14
		)
		ON CONFLICT (xhttp_key) DO UPDATE SET
			server_key = EXCLUDED.server_key,
			enabled = EXCLUDED.enabled,
			sort_order = EXCLUDED.sort_order,
			inbound_tag = EXCLUDED.inbound_tag,
			address = EXCLUDED.address,
			server_name = EXCLUDED.server_name,
			host = EXCLUDED.host,
			port = EXCLUDED.port,
			path = EXCLUDED.path,
			mode = EXCLUDED.mode,
			fingerprint = EXCLUDED.fingerprint,
			alpn = EXCLUDED.alpn,
			remarks = EXCLUDED.remarks,
			updated_at = now()
	`,
		req.XHTTPKey, req.ServerKey, enabled, sortOrder, req.InboundTag,
		req.Address, req.ServerName, req.Host, port, req.Path,
		req.Mode, req.Fingerprint, req.ALPN, req.Remarks,
	)
	return err
}

// DeleteXHTTPEndpoint удаляет XHTTP-эндпоинт по xhttp_key.
func (r *Repository) DeleteXHTTPEndpoint(ctx context.Context, xhttpKey string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.pool.Exec(queryCtx, `DELETE FROM vpn_xhttp_endpoints WHERE xhttp_key = $1`, xhttpKey)
	return err
}
