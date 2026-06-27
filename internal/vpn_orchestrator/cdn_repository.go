package vpn_orchestrator

import (
	"context"
	"time"
)

// ============================================================================
// Репозиторий CDN-эндпоинтов (таблица vpn_cdn_endpoints).
// ============================================================================

// AdminCDNEndpointRequest — тело запроса Admin API для создания/обновления CDN.
// Привязка к серверу задаётся ServerKey; пустой ServerKey = глобальный CDN.
type AdminCDNEndpointRequest struct {
	CDNKey    string `json:"cdn_key"`
	ServerKey string `json:"server_key,omitempty"`
	Enabled   *bool  `json:"enabled,omitempty"`
	SortOrder *int   `json:"sort_order,omitempty"`

	Address     string `json:"address"`
	ServerName  string `json:"server_name,omitempty"`
	Host        string `json:"host,omitempty"`
	Port        *int   `json:"port,omitempty"`
	XHTTPPath   string `json:"xhttp_path,omitempty"`
	Mode        string `json:"mode,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	ALPN        string `json:"alpn,omitempty"`
	Remarks     string `json:"remarks,omitempty"`

	PaddingObfsMode      *bool  `json:"padding_obfs_mode,omitempty"`
	PaddingPlacement     string `json:"padding_placement,omitempty"`
	PaddingKey           string `json:"padding_key,omitempty"`
	PaddingMethod        string `json:"padding_method,omitempty"`
	ScMaxBufferedPosts   *int   `json:"sc_max_buffered_posts,omitempty"`
	ScMinPostsIntervalMs string `json:"sc_min_posts_interval_ms,omitempty"`
}

// ListEnabledCDNEndpoints возвращает все включённые CDN-эндпоинты,
// отсортированные по sort_order, id. Привязанные и глобальные — вперемешку;
// выбор нужного делает selectCDNForServer.
func (r *Repository) ListEnabledCDNEndpoints(ctx context.Context) ([]CDNEndpoint, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT
			cdn_key, COALESCE(server_key, ''), enabled, sort_order,
			address, server_name, host, port, xhttp_path, mode, fingerprint, alpn, remarks,
			padding_obfs_mode, padding_placement, padding_key, padding_method,
			sc_max_buffered_posts, sc_min_posts_interval_ms
		FROM vpn_cdn_endpoints
		WHERE enabled = TRUE
		ORDER BY sort_order, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CDNEndpoint
	for rows.Next() {
		var e CDNEndpoint
		if err := rows.Scan(
			&e.CDNKey, &e.ServerKey, &e.Enabled, &e.SortOrder,
			&e.Address, &e.ServerName, &e.Host, &e.Port, &e.XHTTPPath, &e.Mode, &e.Fingerprint, &e.ALPN, &e.Remarks,
			&e.PaddingObfsMode, &e.PaddingPlacement, &e.PaddingKey, &e.PaddingMethod,
			&e.ScMaxBufferedPosts, &e.ScMinPostsIntervalMs,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UpsertAdminCDNEndpoint создаёт или обновляет CDN-эндпоинт по cdn_key.
// Пустой ServerKey сохраняется как NULL (глобальный CDN).
func (r *Repository) UpsertAdminCDNEndpoint(ctx context.Context, req AdminCDNEndpointRequest) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// значения по умолчанию для необязательных полей
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
	paddingObfs := true
	if req.PaddingObfsMode != nil {
		paddingObfs = *req.PaddingObfsMode
	}
	scMax := 256
	if req.ScMaxBufferedPosts != nil && *req.ScMaxBufferedPosts > 0 {
		scMax = *req.ScMaxBufferedPosts
	}

	var serverKey any
	if req.ServerKey == "" {
		serverKey = nil
	} else {
		serverKey = req.ServerKey
	}

	_, err := r.pool.Exec(queryCtx, `
		INSERT INTO vpn_cdn_endpoints (
			cdn_key, server_key, enabled, sort_order,
			address, server_name, host, port, xhttp_path, mode, fingerprint, alpn, remarks,
			padding_obfs_mode, padding_placement, padding_key, padding_method,
			sc_max_buffered_posts, sc_min_posts_interval_ms
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, COALESCE(NULLIF($9, ''), '/api/uploadFile/'),
			COALESCE(NULLIF($10, ''), 'packet-up'),
			COALESCE(NULLIF($11, ''), 'chrome'),
			COALESCE(NULLIF($12, ''), 'h2,http/1.1'),
			COALESCE(NULLIF($13, ''), 'race-src-cdn'),
			$14,
			COALESCE(NULLIF($15, ''), 'cookie'),
			COALESCE(NULLIF($16, ''), 'ssid'),
			COALESCE(NULLIF($17, ''), 'tokenish'),
			$18,
			COALESCE(NULLIF($19, ''), '5')
		)
		ON CONFLICT (cdn_key) DO UPDATE SET
			server_key = EXCLUDED.server_key,
			enabled = EXCLUDED.enabled,
			sort_order = EXCLUDED.sort_order,
			address = EXCLUDED.address,
			server_name = EXCLUDED.server_name,
			host = EXCLUDED.host,
			port = EXCLUDED.port,
			xhttp_path = EXCLUDED.xhttp_path,
			mode = EXCLUDED.mode,
			fingerprint = EXCLUDED.fingerprint,
			alpn = EXCLUDED.alpn,
			remarks = EXCLUDED.remarks,
			padding_obfs_mode = EXCLUDED.padding_obfs_mode,
			padding_placement = EXCLUDED.padding_placement,
			padding_key = EXCLUDED.padding_key,
			padding_method = EXCLUDED.padding_method,
			sc_max_buffered_posts = EXCLUDED.sc_max_buffered_posts,
			sc_min_posts_interval_ms = EXCLUDED.sc_min_posts_interval_ms,
			updated_at = now()
	`,
		req.CDNKey, serverKey, enabled, sortOrder,
		req.Address, req.ServerName, req.Host, port, req.XHTTPPath, req.Mode, req.Fingerprint, req.ALPN, req.Remarks,
		paddingObfs, req.PaddingPlacement, req.PaddingKey, req.PaddingMethod,
		scMax, req.ScMinPostsIntervalMs,
	)
	return err
}

// DeleteCDNEndpoint удаляет CDN-эндпоинт по cdn_key.
func (r *Repository) DeleteCDNEndpoint(ctx context.Context, cdnKey string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.pool.Exec(queryCtx, `DELETE FROM vpn_cdn_endpoints WHERE cdn_key = $1`, cdnKey)
	return err
}
