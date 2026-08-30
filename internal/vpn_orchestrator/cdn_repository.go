package vpn_orchestrator

import (
	"context"
	"strings"
	"time"
)

// ============================================================================
// Репозиторий CDN-эндпоинтов (таблица vpn_cdn_endpoints).
// ============================================================================

// AdminCDNEndpointRequest — тело запроса Admin API для создания/обновления CDN.
// Привязка к серверу задаётся ServerKey; пустой ServerKey = глобальный CDN.
type AdminCDNEndpointRequest struct {
	CDNKey     string `json:"cdn_key"`
	ServerKey  string `json:"server_key,omitempty"`
	Enabled    *bool  `json:"enabled,omitempty"`
	SortOrder  *int   `json:"sort_order,omitempty"`
	InboundTag string `json:"inbound_tag,omitempty"`

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

	// Параметры восходящего потока (Xray-core PR #5414). Опущенное поле
	// сохраняется как пустое значение и в клиентскую ссылку не попадает,
	// поэтому старые вызовы Admin API продолжают работать без изменений.
	UplinkHTTPMethod    string `json:"uplink_http_method,omitempty"`
	UplinkDataPlacement string `json:"uplink_data_placement,omitempty"`
	UplinkDataKey       string `json:"uplink_data_key,omitempty"`
	UplinkChunkSize     *int   `json:"uplink_chunk_size,omitempty"`
	ScMaxEachPostBytes  *int   `json:"sc_max_each_post_bytes,omitempty"`
	SessionIDPlacement  string `json:"session_id_placement,omitempty"`
	SessionIDKey        string `json:"session_id_key,omitempty"`
	SeqPlacement        string `json:"seq_placement,omitempty"`
	SeqKey              string `json:"seq_key,omitempty"`

	// Параметры эталонной конфигурации из proxy-via-russian-cdn.
	XPaddingBytes  string `json:"x_padding_bytes,omitempty"`
	XPaddingHeader string `json:"x_padding_header,omitempty"`
	EnableXmux     bool   `json:"enable_xmux,omitempty"`
	XmuxJSON       string `json:"xmux_json,omitempty"`
}

// ListEnabledCDNEndpoints возвращает все включённые CDN-эндпоинты,
// отсортированные по sort_order, id. Привязанные и глобальные — вперемешку;
// выбор нужного делает selectCDNForServer.
func (r *Repository) ListEnabledCDNEndpoints(ctx context.Context) ([]CDNEndpoint, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT
			cdn_key, COALESCE(server_key, ''), enabled, sort_order, inbound_tag,
			address, server_name, host, port, xhttp_path, mode, fingerprint, alpn, remarks,
			padding_obfs_mode, padding_placement, padding_key, padding_method,
			sc_max_buffered_posts, sc_min_posts_interval_ms,
			uplink_http_method, uplink_data_placement, uplink_data_key,
			uplink_chunk_size, sc_max_each_post_bytes,
			session_id_placement, session_id_key, seq_placement, seq_key,
			x_padding_bytes, x_padding_header, enable_xmux, xmux_json
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
			&e.CDNKey, &e.ServerKey, &e.Enabled, &e.SortOrder, &e.InboundTag,
			&e.Address, &e.ServerName, &e.Host, &e.Port, &e.XHTTPPath, &e.Mode, &e.Fingerprint, &e.ALPN, &e.Remarks,
			&e.PaddingObfsMode, &e.PaddingPlacement, &e.PaddingKey, &e.PaddingMethod,
			&e.ScMaxBufferedPosts, &e.ScMinPostsIntervalMs,
			&e.UplinkHTTPMethod, &e.UplinkDataPlacement, &e.UplinkDataKey,
			&e.UplinkChunkSize, &e.ScMaxEachPostBytes,
			&e.SessionIDPlacement, &e.SessionIDKey, &e.SeqPlacement, &e.SeqKey,
			&e.XPaddingBytes, &e.XPaddingHeader, &e.EnableXmux, &e.XmuxJSON,
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

	// Ноль означает «не задано»: параметр не уедет в клиентскую ссылку,
	// применится дефолт ядра Xray. Отрицательные значения приводим к нулю.
	// xmux имеет смысл только парой «флаг + параметры»: одно без другого
	// в extra не уедет, поэтому нормализуем здесь.
	xmuxJSON := strings.TrimSpace(req.XmuxJSON)
	enableXmux := req.EnableXmux && xmuxJSON != ""
	if !enableXmux {
		xmuxJSON = ""
	}

	uplinkChunk := 0
	if req.UplinkChunkSize != nil && *req.UplinkChunkSize > 0 {
		uplinkChunk = *req.UplinkChunkSize
	}
	scMaxEachPost := 0
	if req.ScMaxEachPostBytes != nil && *req.ScMaxEachPostBytes > 0 {
		scMaxEachPost = *req.ScMaxEachPostBytes
	}

	var serverKey any
	if req.ServerKey == "" {
		serverKey = nil
	} else {
		serverKey = req.ServerKey
	}

	_, err := r.pool.Exec(queryCtx, `
		INSERT INTO vpn_cdn_endpoints (
			cdn_key, server_key, enabled, sort_order, inbound_tag,
			address, server_name, host, port, xhttp_path, mode, fingerprint, alpn, remarks,
			padding_obfs_mode, padding_placement, padding_key, padding_method,
			sc_max_buffered_posts, sc_min_posts_interval_ms,
			uplink_http_method, uplink_data_placement, uplink_data_key,
			uplink_chunk_size, sc_max_each_post_bytes,
			session_id_placement, session_id_key, seq_placement, seq_key,
			x_padding_bytes, x_padding_header, enable_xmux, xmux_json
		) VALUES (
			$1, $2, $3, $4, COALESCE(NULLIF($20, ''), 'vless-xhttp-cdn-in'),
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
			COALESCE(NULLIF($19, ''), '5'),
			$21, $22, $23, $24, $25, $26, $27, $28, $29,
			$30, $31, $32, $33
		)
		ON CONFLICT (cdn_key) DO UPDATE SET
			server_key = EXCLUDED.server_key,
			enabled = EXCLUDED.enabled,
			sort_order = EXCLUDED.sort_order,
			inbound_tag = EXCLUDED.inbound_tag,
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
			uplink_http_method = EXCLUDED.uplink_http_method,
			uplink_data_placement = EXCLUDED.uplink_data_placement,
			uplink_data_key = EXCLUDED.uplink_data_key,
			uplink_chunk_size = EXCLUDED.uplink_chunk_size,
			sc_max_each_post_bytes = EXCLUDED.sc_max_each_post_bytes,
			session_id_placement = EXCLUDED.session_id_placement,
			session_id_key = EXCLUDED.session_id_key,
			seq_placement = EXCLUDED.seq_placement,
			seq_key = EXCLUDED.seq_key,
			x_padding_bytes = EXCLUDED.x_padding_bytes,
			x_padding_header = EXCLUDED.x_padding_header,
			enable_xmux = EXCLUDED.enable_xmux,
			xmux_json = EXCLUDED.xmux_json,
			updated_at = now()
	`,
		req.CDNKey, serverKey, enabled, sortOrder,
		req.Address, req.ServerName, req.Host, port, req.XHTTPPath, req.Mode, req.Fingerprint, req.ALPN, req.Remarks,
		paddingObfs, req.PaddingPlacement, req.PaddingKey, req.PaddingMethod,
		scMax, req.ScMinPostsIntervalMs, req.InboundTag,
		req.UplinkHTTPMethod, req.UplinkDataPlacement, req.UplinkDataKey,
		uplinkChunk, scMaxEachPost,
		req.SessionIDPlacement, req.SessionIDKey, req.SeqPlacement, req.SeqKey,
		req.XPaddingBytes, req.XPaddingHeader, enableXmux, xmuxJSON,
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
