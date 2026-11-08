package vpn_orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrAccessNotFound = errors.New("subscription access not found")

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

type AccessState struct {
	TelegramID  int64
	Status      string
	AccessUntil *time.Time
	GraceUntil  *time.Time
	AccessRev   int64
	CountryCode string
}

type PoolItem struct {
	ID          int64
	ItemKey     string
	ServerKey   string
	NodeID      string
	CountryCode string
	Title       string
	ProfileType string
	Enabled     bool
	SortOrder   int

	// InboundTag и Flow — основной профиль пользователя на ноде (см.
	// ListEnabledPoolItems и buildUserProfiles).
	InboundTag string
	Flow       string
	Level      uint32
}

type UserCredential struct {
	TelegramID int64
	ItemKey    string
	ServerKey  string
	NodeID     string
	InboundTag string
	Email      string
	VLESSUUID  string
	AccessRev  int64
	Enabled    bool
}

type FeedItem struct {
	PoolItem   PoolItem
	Credential UserCredential
}

// ServerLoad — данные о ноде для балансировщика.
type ServerLoad struct {
	ServerKey   string
	CountryCode string
	Enabled     bool
	MaxUsers    int

	// ActiveUsers — выданные учётные записи. Ёмкостная величина: используется
	// как запасной вариант, пока нода не сообщила реальные подключения.
	ActiveUsers int

	// OnlineUsers — пользователи, реально передававшие данные за последний
	// интервал. Приходит в heartbeat от агента.
	OnlineUsers int

	// UplinkBps/DownlinkBps — скорость трафика ноды на момент последнего
	// heartbeat.
	UplinkBps   int64
	DownlinkBps int64

	// BandwidthMbps — полоса канала. 0 означает «не задана»: тогда давление
	// по каналу в расчёте не участвует.
	BandwidthMbps int

	Weight          int
	LastHeartbeatAt *time.Time

	// ProbeChecked/ProbeAlive — результат TCP-проверки VPN-портов ноды из центра.
	// Позволяет отличить блокировку канала Kafka («заблокировано») от реального
	// падения сервера («упало»).
	ProbeChecked bool
	ProbeAlive   bool
}

// AliveByHeartbeat — нода включена и шлёт свежий heartbeat (без учёта ёмкости).
func (s ServerLoad) AliveByHeartbeat(now time.Time, heartbeatTTL time.Duration) bool {
	if !s.Enabled {
		return false
	}
	// Нода только заведена, heartbeat ещё не приходил — считаем живой,
	// чтобы её можно было ввести в работу до первого heartbeat.
	if s.LastHeartbeatAt == nil {
		return true
	}
	return now.Sub(*s.LastHeartbeatAt) <= heartbeatTTL
}

// Alive — нода жива, если свежий heartbeat ИЛИ успешная TCP-проверка VPN-порта
// (сценарий blocked_controlplane, когда ТСПУ блокирует Kafka, но сама нода жива).
func (s ServerLoad) Alive(now time.Time, heartbeatTTL time.Duration) bool {
	if !s.Enabled {
		return false
	}
	if s.AliveByHeartbeat(now, heartbeatTTL) {
		return true
	}
	return s.ProbeChecked && s.ProbeAlive
}

// HasCapacity — есть ли свободные места по мягкому лимиту max_users.
func (s ServerLoad) HasCapacity() bool {
	return s.MaxUsers <= 0 || s.ActiveUsers < s.MaxUsers
}

// LoadScore — оценка загруженности ноды: чем меньше, тем свободнее.
func (s ServerLoad) LoadScore() float64 {
	w := s.Weight
	if w <= 0 {
		w = 1
	}

	conns := s.OnlineUsers
	if conns <= 0 {
		conns = s.ActiveUsers
	}
	score := float64(conns+1) / float64(w)

	if s.BandwidthMbps > 0 && s.MaxUsers > 0 {
		capacityBps := float64(s.BandwidthMbps) * 1_000_000
		usedBps := float64(s.UplinkBps + s.DownlinkBps)
		if usedBps > 0 {
			bwScore := usedBps / capacityBps * float64(s.MaxUsers+1) / float64(w)
			if bwScore > score {
				score = bwScore
			}
		}
	}

	return score
}

func (r *Repository) GetAccessByToken(ctx context.Context, token string) (*AccessState, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT
			us.telegram_id,
			us.status,
			us.expires_at,
			us.grace_until,
			us.access_rev,
			us.country_code
		FROM subscription_tokens st
		JOIN user_subscriptions us ON us.telegram_id = st.telegram_id
		WHERE st.token = $1 AND st.revoked_at IS NULL
	`, token)

	state, err := scanAccessState(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrAccessNotFound
		}
		return nil, fmt.Errorf("get access by token: %w", err)
	}

	return state, nil
}

// ListEnabledPoolItems возвращает включённые пул-айтемы включённых серверов.
//
// Инбаунд основного профиля берётся из настроек: пул-айтема, затем сервера
// (default_inbound_tag), а если оба пусты — у эндпоинтов самого сервера
// (Reality, затем XHTTP). Встроенного тега по умолчанию нет: пул-айтем, для
// которого инбаунд не нашёлся ни в одном из этих мест, не выдаётся — учётку
// без инбаунда нода отвергла бы целиком.
func (r *Repository) ListEnabledPoolItems(ctx context.Context) ([]PoolItem, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			pi.id,
			pi.item_key,
			pi.server_key,
			vs.node_id,
			pi.country_code,
			pi.title,
			pi.profile_type,
			pi.enabled,
			pi.sort_order,
			COALESCE(
				NULLIF(btrim(pi.inbound_tag), ''),
				NULLIF(btrim(vs.default_inbound_tag), ''),
				(SELECT NULLIF(btrim(re.inbound_tag), '') FROM vpn_reality_endpoints re
				 WHERE re.server_key = vs.server_key AND re.enabled
				 ORDER BY re.sort_order, re.id LIMIT 1),
				(SELECT NULLIF(btrim(xe.inbound_tag), '') FROM vpn_xhttp_endpoints xe
				 WHERE xe.server_key = vs.server_key AND xe.enabled
				 ORDER BY xe.sort_order, xe.id LIMIT 1),
				''
			),
			COALESCE(NULLIF(pi.flow, ''), vs.flow, ''),
			pi.level
		FROM vpn_pool_items pi
		JOIN vpn_servers vs ON vs.server_key = pi.server_key
		WHERE pi.enabled = true
		  AND vs.enabled = true
		  AND vs.node_id <> ''
		  AND vs.public_host <> ''
		ORDER BY pi.sort_order ASC, pi.id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list enabled pool items: %w", err)
	}
	defer rows.Close()

	items := make([]PoolItem, 0)
	for rows.Next() {
		var item PoolItem
		var level int
		if err := rows.Scan(
			&item.ID,
			&item.ItemKey,
			&item.ServerKey,
			&item.NodeID,
			&item.CountryCode,
			&item.Title,
			&item.ProfileType,
			&item.Enabled,
			&item.SortOrder,
			&item.InboundTag,
			&item.Flow,
			&level,
		); err != nil {
			return nil, fmt.Errorf("scan pool item: %w", err)
		}
		if item.InboundTag == "" {
			slog.Warn("pool item skipped: no inbound tag in pool item, server or its endpoints",
				"item_key", item.ItemKey, "server_key", item.ServerKey)
			continue
		}
		if level < 0 {
			level = 0
		}
		item.Level = uint32(level)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pool items: %w", err)
	}
	return items, nil
}

// LoadServersByCountry возвращает карту country_code -> ноды этой страны с метриками.
func (r *Repository) LoadServersByCountry(ctx context.Context) (map[string][]ServerLoad, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT s.server_key, s.country_code, s.enabled, s.max_users,
		       COALESCE(a.active_users, 0) AS active_users,
		       s.weight, s.last_heartbeat_at,
		       s.online_users, s.uplink_bps, s.downlink_bps, s.bandwidth_mbps
		FROM vpn_servers s
		LEFT JOIN (
			SELECT server_key, COUNT(*) AS active_users
			FROM vpn_user_node_credentials
			WHERE enabled = true
			GROUP BY server_key
		) a ON a.server_key = s.server_key
		WHERE s.enabled = true
	`)
	if err != nil {
		return nil, fmt.Errorf("load servers by country: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	out := make(map[string][]ServerLoad)
	for rows.Next() {
		var s ServerLoad
		if err := rows.Scan(&s.ServerKey, &s.CountryCode, &s.Enabled,
			&s.MaxUsers, &s.ActiveUsers, &s.Weight, &s.LastHeartbeatAt,
			&s.OnlineUsers, &s.UplinkBps, &s.DownlinkBps, &s.BandwidthMbps); err != nil {
			return nil, fmt.Errorf("scan server load: %w", err)
		}
		s.ProbeChecked, s.ProbeAlive = GetNodeProbe(s.ServerKey, now, 3*time.Minute)
		out[s.CountryCode] = append(out[s.CountryCode], s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate server loads: %w", err)
	}
	return out, nil
}

// GetUserServerForItem возвращает server_key, к которому пользователь уже привязан
// для item_key (sticky). Пустая строка — привязки нет.
func (r *Repository) GetUserServerForItem(ctx context.Context, telegramID int64, itemKey string) (string, error) {
	var serverKey string
	err := r.pool.QueryRow(ctx, `
		SELECT server_key FROM vpn_user_node_credentials
		WHERE telegram_id = $1 AND item_key = $2 AND enabled = true
	`, telegramID, itemKey).Scan(&serverKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get user server for item: %w", err)
	}
	return serverKey, nil
}

func (r *Repository) incActiveUsersInTx(ctx context.Context, tx pgx.Tx, serverKey string) error {
	if strings.TrimSpace(serverKey) == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE vpn_servers SET active_users = active_users + 1, updated_at = now() WHERE server_key = $1`, serverKey)
	if err != nil {
		return fmt.Errorf("inc active users %s: %w", serverKey, err)
	}
	return nil
}

func (r *Repository) decActiveUsersInTx(ctx context.Context, tx pgx.Tx, serverKey string) error {
	if strings.TrimSpace(serverKey) == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE vpn_servers SET active_users = GREATEST(active_users - 1, 0), updated_at = now() WHERE server_key = $1`, serverKey)
	if err != nil {
		return fmt.Errorf("dec active users %s: %w", serverKey, err)
	}
	return nil
}

func (r *Repository) EnsureCredentialsForItems(ctx context.Context, telegramID int64, accessRev int64, items []PoolItem) ([]FeedItem, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	out, err := r.EnsureCredentialsForItemsTx(ctx, tx, telegramID, accessRev, items)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return out, nil
}

// EnsureCredentialsForItemsTx — тело EnsureCredentialsForItems, но в переданной
// транзакции, без собственных Begin/Commit (C1).
func (r *Repository) EnsureCredentialsForItemsTx(ctx context.Context, tx pgx.Tx, telegramID int64, accessRev int64, items []PoolItem) ([]FeedItem, error) {
	out := make([]FeedItem, 0, len(items))
	for _, item := range items {
		newUUID := uuid.NewString()
		email := buildEmail(telegramID, item.ItemKey)

		var wasInserted bool
		var prevServerKey string
		row := tx.QueryRow(ctx, `
			INSERT INTO vpn_user_node_credentials (
				telegram_id, item_key, server_key, node_id, inbound_tag,
				email, vless_uuid, access_rev, enabled
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true)
			ON CONFLICT (telegram_id, item_key) DO UPDATE SET
				server_key = EXCLUDED.server_key,
				node_id = EXCLUDED.node_id,
				inbound_tag = EXCLUDED.inbound_tag,
				access_rev = GREATEST(vpn_user_node_credentials.access_rev, EXCLUDED.access_rev),
				enabled = true,
				updated_at = now()
			RETURNING telegram_id, item_key, server_key, node_id, inbound_tag,
			          email, vless_uuid, access_rev, enabled,
			          (xmax = 0) AS was_inserted,
			          (CASE WHEN xmax = 0 THEN '' ELSE vpn_user_node_credentials.server_key END) AS prev_server_key
		`, telegramID, item.ItemKey, item.ServerKey, item.NodeID, item.InboundTag, email, newUUID, accessRev)

		cred, err := scanCredentialWithMeta(row, &wasInserted, &prevServerKey)
		if err != nil {
			return nil, fmt.Errorf("ensure credential item_key=%s: %w", item.ItemKey, err)
		}

		// Обновляем active_users: новая учётка → +1; переезд на другую ноду → +1 новой, -1 старой;
		// та же нода (sticky) → ничего.
		if wasInserted {
			if err := r.incActiveUsersInTx(ctx, tx, item.ServerKey); err != nil {
				return nil, err
			}
		} else if prevServerKey != "" && prevServerKey != item.ServerKey {
			if err := r.incActiveUsersInTx(ctx, tx, item.ServerKey); err != nil {
				return nil, err
			}
			if err := r.decActiveUsersInTx(ctx, tx, prevServerKey); err != nil {
				return nil, err
			}
		}

		out = append(out, FeedItem{
			PoolItem:   item,
			Credential: *cred,
		})
	}

	return out, nil
}

func (r *Repository) DisableUserCredentials(ctx context.Context, telegramID int64, accessRev int64) ([]UserCredential, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	out, err := r.DisableUserCredentialsTx(ctx, tx, telegramID, accessRev)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return out, nil
}

// DisableUserCredentialsTx — тело DisableUserCredentials в переданной транзакции,
// без собственных Begin/Commit (C1).
func (r *Repository) DisableUserCredentialsTx(ctx context.Context, tx pgx.Tx, telegramID int64, accessRev int64) ([]UserCredential, error) {
	rows, err := tx.Query(ctx, `
		UPDATE vpn_user_node_credentials
		SET enabled = false,
		    access_rev = GREATEST(access_rev, $2),
		    updated_at = now()
		WHERE telegram_id = $1
		RETURNING telegram_id, item_key, server_key, node_id, inbound_tag,
		          email, vless_uuid, access_rev, enabled
	`, telegramID, accessRev)
	if err != nil {
		return nil, fmt.Errorf("disable credentials: %w", err)
	}
	defer rows.Close()

	var out []UserCredential
	for rows.Next() {
		cred, err := scanCredential(rows)
		if err != nil {
			return nil, fmt.Errorf("scan disabled credential: %w", err)
		}
		out = append(out, *cred)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate disabled credentials: %w", err)
	}

	rows.Close()

	// Декремент active_users на нодах, где отключены учётки.
	for _, c := range out {
		if c.ServerKey != "" {
			if err := r.decActiveUsersInTx(ctx, tx, c.ServerKey); err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}

func (r *Repository) UpsertAccessProjection(ctx context.Context, state *AccessState, eventType string, eventAt time.Time) error {
	if state == nil {
		return nil
	}
	if state.CountryCode == "" {
		state.CountryCode = "ALL"
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO vpn_user_access (
			telegram_id, status, access_until, grace_until, access_rev,
			country_code, last_event_type, last_event_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (telegram_id) DO UPDATE SET
			status = EXCLUDED.status,
			access_until = EXCLUDED.access_until,
			grace_until = EXCLUDED.grace_until,
			access_rev = EXCLUDED.access_rev,
			country_code = EXCLUDED.country_code,
			last_event_type = EXCLUDED.last_event_type,
			last_event_at = EXCLUDED.last_event_at,
			updated_at = now()
		WHERE vpn_user_access.access_rev <= EXCLUDED.access_rev
	`, state.TelegramID, state.Status, state.AccessUntil, state.GraceUntil, state.AccessRev, state.CountryCode, eventType, eventAt.UTC())
	if err != nil {
		return fmt.Errorf("upsert access projection: %w", err)
	}
	return nil
}

// UpsertAccessProjectionTx — то же, что UpsertAccessProjection, но в переданной
// транзакции (C1: отметка инбокса и сайд-эффект в одной tx).
func (r *Repository) UpsertAccessProjectionTx(ctx context.Context, tx pgx.Tx, state *AccessState, eventType string, eventAt time.Time) error {
	if state == nil {
		return nil
	}
	if state.CountryCode == "" {
		state.CountryCode = "ALL"
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO vpn_user_access (
			telegram_id, status, access_until, grace_until, access_rev,
			country_code, last_event_type, last_event_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (telegram_id) DO UPDATE SET
			status = EXCLUDED.status,
			access_until = EXCLUDED.access_until,
			grace_until = EXCLUDED.grace_until,
			access_rev = EXCLUDED.access_rev,
			country_code = EXCLUDED.country_code,
			last_event_type = EXCLUDED.last_event_type,
			last_event_at = EXCLUDED.last_event_at,
			updated_at = now()
		WHERE vpn_user_access.access_rev <= EXCLUDED.access_rev
	`, state.TelegramID, state.Status, state.AccessUntil, state.GraceUntil, state.AccessRev, state.CountryCode, eventType, eventAt.UTC())
	if err != nil {
		return fmt.Errorf("upsert access projection tx: %w", err)
	}
	return nil
}

func (r *Repository) UpdateNodeHeartbeat(ctx context.Context, nodeID string, serverKey string, at time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE vpn_servers
		SET last_heartbeat_at = $3,
		    updated_at = now()
		WHERE node_id = $1
		  AND ($2 = '' OR server_key = $2)
	`, nodeID, serverKey, at.UTC())
	if err != nil {
		return fmt.Errorf("update node heartbeat: %w", err)
	}
	return nil
}

// NodeLoadReport — показатели нагрузки, приходящие в heartbeat.
type NodeLoadReport struct {
	OnlineUsers int
	UplinkBps   int64
	DownlinkBps int64
}

func (r *Repository) UpdateNodeHeartbeatTx(ctx context.Context, tx pgx.Tx, nodeID string, serverKey string, at time.Time, load NodeLoadReport) error {
	_, err := tx.Exec(ctx, `
		UPDATE vpn_servers
		SET last_heartbeat_at = $3,
		    online_users = $4,
		    uplink_bps = $5,
		    downlink_bps = $6,
		    updated_at = now()
		WHERE node_id = $1
		  AND ($2 = '' OR server_key = $2)
	`, nodeID, serverKey, at.UTC(), load.OnlineUsers, load.UplinkBps, load.DownlinkBps)
	if err != nil {
		return fmt.Errorf("update node heartbeat tx: %w", err)
	}
	return nil
}

func (r *Repository) SaveNodeSyncResult(ctx context.Context, nodeID, serverKey string, telegramID int64, accessRev int64, commandID, eventType string, success bool, errText string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO vpn_node_sync_results (
			node_id, server_key, telegram_id, access_rev,
			command_id, event_type, success, error
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, nodeID, serverKey, telegramID, accessRev, commandID, eventType, success, errText)
	if err != nil {
		return fmt.Errorf("save node sync result: %w", err)
	}
	return nil
}

// SaveNodeSyncResultTx — версия для транзакции (C1).
func (r *Repository) SaveNodeSyncResultTx(ctx context.Context, tx pgx.Tx, nodeID, serverKey string, telegramID int64, accessRev int64, commandID, eventType string, success bool, errText string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO vpn_node_sync_results (
			node_id, server_key, telegram_id, access_rev,
			command_id, event_type, success, error
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, nodeID, serverKey, telegramID, accessRev, commandID, eventType, success, errText)
	if err != nil {
		return fmt.Errorf("save node sync result tx: %w", err)
	}
	return nil
}

func scanAccessState(row pgx.Row) (*AccessState, error) {
	var state AccessState
	var accessUntil sql.NullTime
	var graceUntil sql.NullTime
	var country sql.NullString

	if err := row.Scan(&state.TelegramID, &state.Status, &accessUntil, &graceUntil, &state.AccessRev, &country); err != nil {
		return nil, err
	}
	if accessUntil.Valid {
		t := accessUntil.Time.UTC()
		state.AccessUntil = &t
	}
	if graceUntil.Valid {
		t := graceUntil.Time.UTC()
		state.GraceUntil = &t
	}
	if country.Valid {
		state.CountryCode = country.String
	}
	return &state, nil
}

// ListActiveAccessesForReconcile возвращает все доступы со статусом trial/active/grace
// постранично (keyset по telegram_id).
func (r *Repository) ListActiveAccessesForReconcile(ctx context.Context, afterTelegramID int64, limit int) ([]AccessState, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := r.pool.Query(ctx, `
		SELECT telegram_id, status, expires_at, grace_until, access_rev, country_code
		FROM user_subscriptions
		WHERE (
		        (status IN ('trial', 'active') AND (expires_at IS NULL OR expires_at > now()))
		     OR (status = 'grace' AND (grace_until IS NULL OR grace_until > now()))
		      )
		  AND telegram_id > $1
		ORDER BY telegram_id
		LIMIT $2
	`, afterTelegramID, limit)
	if err != nil {
		return nil, fmt.Errorf("list active accesses for reconcile: %w", err)
	}
	defer rows.Close()

	var out []AccessState
	for rows.Next() {
		state, err := scanAccessState(rows)
		if err != nil {
			return nil, fmt.Errorf("scan access for reconcile: %w", err)
		}
		out = append(out, *state)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// WithAdvisoryLock выполняет fn, удерживая сессионный advisory-lock Postgres
// на ОДНОМ выделенном соединении пула.
func (r *Repository) WithAdvisoryLock(ctx context.Context, key int64, fn func(context.Context)) (bool, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire connection for advisory lock: %w", err)
	}
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		conn.Release()
		return false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !locked {
		conn.Release()
		return false, nil
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var released bool
		if err := conn.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released); err != nil || !released {
			_ = conn.Conn().Close(unlockCtx)
		}
		conn.Release()
	}()
	fn(ctx)
	return true, nil
}

// UserTrafficRow — суммарный трафик пользователя по всем узлам (байты).
type UserTrafficRow struct {
	Uplink   int64
	Downlink int64
}

// UpsertUserNodeTrafficTx сохраняет кумулятивный трафик пользователя НА ОДНОМ УЗЛЕ.
func (r *Repository) UpsertUserNodeTrafficTx(ctx context.Context, tx pgx.Tx, telegramID int64, nodeID string, uplink, downlink int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO vpn_user_traffic_node (telegram_id, node_id, uplink, downlink, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (telegram_id, node_id) DO UPDATE SET
			uplink = GREATEST(vpn_user_traffic_node.uplink, EXCLUDED.uplink),
			downlink = GREATEST(vpn_user_traffic_node.downlink, EXCLUDED.downlink),
			updated_at = now()
	`, telegramID, nodeID, uplink, downlink)
	if err != nil {
		return fmt.Errorf("upsert user node traffic tg=%d node=%s: %w", telegramID, nodeID, err)
	}
	return nil
}

// GetUserTraffic возвращает суммарный трафик пользователя по всем узлам.
func (r *Repository) GetUserTraffic(ctx context.Context, telegramID int64) (UserTrafficRow, error) {
	var row UserTrafficRow
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(uplink), 0), COALESCE(SUM(downlink), 0)
		FROM vpn_user_traffic_node
		WHERE telegram_id = $1
	`, telegramID).Scan(&row.Uplink, &row.Downlink)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return UserTrafficRow{}, nil
		}
		return UserTrafficRow{}, fmt.Errorf("get user traffic tg=%d: %w", telegramID, err)
	}
	return row, nil
}

func scanCredentialWithMeta(row pgx.Row, wasInserted *bool, prevServerKey *string) (*UserCredential, error) {
	var cred UserCredential
	if err := row.Scan(
		&cred.TelegramID,
		&cred.ItemKey,
		&cred.ServerKey,
		&cred.NodeID,
		&cred.InboundTag,
		&cred.Email,
		&cred.VLESSUUID,
		&cred.AccessRev,
		&cred.Enabled,
		wasInserted,
		prevServerKey,
	); err != nil {
		return nil, err
	}
	return &cred, nil
}

func scanCredential(row pgx.Row) (*UserCredential, error) {
	var cred UserCredential
	if err := row.Scan(
		&cred.TelegramID,
		&cred.ItemKey,
		&cred.ServerKey,
		&cred.NodeID,
		&cred.InboundTag,
		&cred.Email,
		&cred.VLESSUUID,
		&cred.AccessRev,
		&cred.Enabled,
	); err != nil {
		return nil, err
	}
	return &cred, nil
}

// ListEnabledCredentialsForNodeTx отдаёт включённые учётки пользователя на одном узле.
func (r *Repository) ListEnabledCredentialsForNodeTx(ctx context.Context, tx pgx.Tx, telegramID int64, nodeID string) ([]UserCredential, error) {
	rows, err := tx.Query(ctx, `
		SELECT telegram_id, item_key, server_key, node_id, inbound_tag,
		       email, vless_uuid, access_rev, enabled
		FROM vpn_user_node_credentials
		WHERE telegram_id = $1 AND node_id = $2 AND enabled = true
	`, telegramID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list credentials tg=%d node=%s: %w", telegramID, nodeID, err)
	}
	defer rows.Close()

	var out []UserCredential
	for rows.Next() {
		cred, err := scanCredential(rows)
		if err != nil {
			return nil, fmt.Errorf("scan credential: %w", err)
		}
		out = append(out, *cred)
	}
	return out, rows.Err()
}

func buildEmail(telegramID int64, itemKey string) string {
	clean := strings.NewReplacer("@", "-", "/", "-", " ", "-", ":", "-").Replace(itemKey)
	return "tg-" + strconv.FormatInt(telegramID, 10) + "-" + clean + "@vpn-platform.local"
}

func (r *Repository) MarkCredentialsSynced(ctx context.Context, telegramID int64, accessRev int64, nodeID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE vpn_user_node_credentials
		SET last_synced_rev = GREATEST(last_synced_rev, $2),
		    last_synced_at = now(),
		    updated_at = now()
		WHERE telegram_id = $1
		  AND access_rev <= $2
		  AND node_id = $3
	`, telegramID, accessRev, nodeID)
	if err != nil {
		return fmt.Errorf("mark credentials synced: %w", err)
	}
	return nil
}

// MarkCredentialsSyncedTx — версия для транзакции (C1).
func (r *Repository) MarkCredentialsSyncedTx(ctx context.Context, tx pgx.Tx, telegramID int64, accessRev int64, nodeID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE vpn_user_node_credentials
		SET last_synced_rev = GREATEST(last_synced_rev, $2),
		    last_synced_at = now(),
		    updated_at = now()
		WHERE telegram_id = $1
		  AND access_rev <= $2
		  AND node_id = $3
	`, telegramID, accessRev, nodeID)
	if err != nil {
		return fmt.Errorf("mark credentials synced tx: %w", err)
	}
	return nil
}

// DefaultNodeMaxUsers — лимит пользователей на ноду, когда при регистрации
// (POST /admin/nodes) max_users не передан или не больше нуля.
const DefaultNodeMaxUsers = 400

type AdminNodeRequest struct {
	ServerKey         string `json:"server_key"`
	NodeID            string `json:"node_id"`
	CountryCode       string `json:"country_code"`
	Title             string `json:"title"`
	PublicHost        string `json:"public_host"`
	Port              int    `json:"port"`
	Transport         string `json:"transport"`
	Security          string `json:"security"`
	DefaultInboundTag string `json:"default_inbound_tag"`
	HostHeader        string `json:"host_header"`
	SNI               string `json:"sni"`
	WSPath            string `json:"ws_path"`
	Flow              string `json:"flow"`
	MaxUsers          int    `json:"max_users"`
	Weight            int    `json:"weight"`
	BandwidthMbps     int    `json:"bandwidth_mbps"`
	Enabled           bool   `json:"enabled"`
}

type AdminPoolItemRequest struct {
	ItemKey     string `json:"item_key"`
	ServerKey   string `json:"server_key"`
	CountryCode string `json:"country_code"`
	Title       string `json:"title"`
	ProfileType string `json:"profile_type"`
	Enabled     bool   `json:"enabled"`
	SortOrder   int    `json:"sort_order"`
	InboundTag  string `json:"inbound_tag"`
	HostHeader  string `json:"host_header"`
	SNI         string `json:"sni"`
	WSPath      string `json:"ws_path"`
	Flow        string `json:"flow"`
	Level       int    `json:"level"`
}

func (r *Repository) UpsertAdminNode(ctx context.Context, req AdminNodeRequest) error {
	if req.ServerKey == "" || req.NodeID == "" || req.PublicHost == "" {
		return fmt.Errorf("server_key, node_id and public_host are required")
	}
	if req.Port <= 0 {
		req.Port = 443
	}
	if req.MaxUsers <= 0 {
		req.MaxUsers = DefaultNodeMaxUsers
	}
	if req.Weight <= 0 {
		req.Weight = 100
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO vpn_servers (
			server_key, node_id, country_code, title, public_host, port, transport, security,
			default_inbound_tag, host_header, sni, ws_path, flow, enabled, max_users, weight,
			bandwidth_mbps
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		ON CONFLICT (server_key) DO UPDATE SET
			node_id=EXCLUDED.node_id,
			country_code=EXCLUDED.country_code,
			title=EXCLUDED.title,
			public_host=EXCLUDED.public_host,
			port=EXCLUDED.port,
			transport=EXCLUDED.transport,
			security=EXCLUDED.security,
			default_inbound_tag=EXCLUDED.default_inbound_tag,
			host_header=EXCLUDED.host_header,
			sni=EXCLUDED.sni,
			ws_path=EXCLUDED.ws_path,
			flow=EXCLUDED.flow,
			enabled=EXCLUDED.enabled,
			max_users=EXCLUDED.max_users,
			weight=EXCLUDED.weight,
			bandwidth_mbps=EXCLUDED.bandwidth_mbps,
			updated_at=now()
	`, req.ServerKey, req.NodeID, req.CountryCode, req.Title, req.PublicHost, req.Port, req.Transport, req.Security, req.DefaultInboundTag, req.HostHeader, req.SNI, req.WSPath, req.Flow, req.Enabled, req.MaxUsers, req.Weight, req.BandwidthMbps)
	return err
}

func (r *Repository) SetAdminNodeEnabled(ctx context.Context, nodeID string, enabled bool) error {
	_, err := r.pool.Exec(ctx, `UPDATE vpn_servers SET enabled=$2, updated_at=now() WHERE node_id=$1`, nodeID, enabled)
	return err
}

func (r *Repository) UpsertAdminPoolItem(ctx context.Context, req AdminPoolItemRequest) error {
	if req.ItemKey == "" || req.ServerKey == "" {
		return fmt.Errorf("item_key and server_key are required")
	}
	if req.ProfileType == "" {
		req.ProfileType = "vless"
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO vpn_pool_items (
			item_key, server_key, country_code, title, profile_type, enabled, sort_order,
			inbound_tag, host_header, sni, ws_path, flow, level
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (item_key) DO UPDATE SET
			server_key=EXCLUDED.server_key,
			country_code=EXCLUDED.country_code,
			title=EXCLUDED.title,
			profile_type=EXCLUDED.profile_type,
			enabled=EXCLUDED.enabled,
			sort_order=EXCLUDED.sort_order,
			inbound_tag=EXCLUDED.inbound_tag,
			host_header=EXCLUDED.host_header,
			sni=EXCLUDED.sni,
			ws_path=EXCLUDED.ws_path,
			flow=EXCLUDED.flow,
			level=EXCLUDED.level,
			updated_at=now()
	`, req.ItemKey, req.ServerKey, req.CountryCode, req.Title, req.ProfileType, req.Enabled, req.SortOrder, req.InboundTag, req.HostHeader, req.SNI, req.WSPath, req.Flow, req.Level)
	return err
}

func (r *Repository) GetAccessByTelegramID(ctx context.Context, telegramID int64) (*AccessState, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT telegram_id, status, access_until, grace_until, access_rev, country_code
		FROM vpn_user_access
		WHERE telegram_id=$1
	`, telegramID)
	state, err := scanAccessState(row)
	if err != nil {
		return nil, fmt.Errorf("get access by telegram_id: %w", err)
	}
	return state, nil
}

func parseInt64(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}
