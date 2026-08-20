package vpn_orchestrator

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	commonkafka "vpn-platform/internal/common/kafka"
	commonmetrics "vpn-platform/internal/common/metrics"
	"vpn-platform/internal/common/outbox"
	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

var (
	ErrAccessDenied = errors.New("subscription access is not active")
	ErrNoPoolItems  = errors.New("no enabled vpn pool items")
)

type ServiceConfig struct {
	FeedFormat string

	// Балансировка (все значения приходят из env через main.go).
	NodeHeartbeatTTL time.Duration // нода жива, если heartbeat не старше этого
	DefaultMaxUsers  int           // лимит пользователей на ноду по умолчанию
	DefaultWeight    int           // вес ноды по умолчанию
	SoftOverflow     bool          // выдавать переполненную ноду вместо отказа
}

type Service struct {
	repo      *Repository
	producer  *commonkafka.Producer
	allocator *Allocator
	cfg       ServiceConfig
}

type SubscriptionFeedResult struct {
	Body        []byte
	ContentType string
	Access      *AccessState
	Uplink      int64
	Downlink    int64

	// RoutingB64 — payload для заголовка `routing` (base64). Пустая строка
	// означает «роутинг не отдаём» (fail-open).
	RoutingB64 string
}

func NewService(repo *Repository, producer *commonkafka.Producer, cfg ServiceConfig) *Service {
	cfg.FeedFormat = strings.ToLower(strings.TrimSpace(cfg.FeedFormat))
	if cfg.FeedFormat == "" {
		cfg.FeedFormat = "base64"
	}
	if cfg.NodeHeartbeatTTL <= 0 {
		cfg.NodeHeartbeatTTL = 90 * time.Second
	}
	return &Service{
		repo:      repo,
		producer:  producer,
		allocator: NewAllocator(cfg.NodeHeartbeatTTL, cfg.SoftOverflow),
		cfg:       cfg,
	}
}

func (s *Service) RenderSubscriptionFeedDetailed(ctx context.Context, token string, group clientGroup) (*SubscriptionFeedResult, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, ErrAccessDenied
	}

	access, err := s.repo.GetAccessByToken(ctx, token)
	if err != nil {
		if errors.Is(err, ErrAccessNotFound) {
			return nil, ErrAccessDenied
		}
		return nil, ErrAccessDenied
	}
	if !accessAllowed(access, time.Now().UTC()) {
		return nil, ErrAccessDenied
	}

	// P5: трафик пользователя для заголовка Subscription-Userinfo (не критично —
	// при ошибке просто отдадим нули, фид важнее).
	var trafficUp, trafficDown int64
	if tr, terr := s.repo.GetUserTraffic(ctx, access.TelegramID); terr == nil {
		trafficUp, trafficDown = tr.Uplink, tr.Downlink
	} else {
		slog.Warn("get user traffic failed", "telegram_id", access.TelegramID, "err", terr)
	}

	feedItems, err := s.ensureUserCredentials(ctx, access)
	if err != nil {
		return nil, ErrAccessDenied
	}
	if len(feedItems) == 0 {
		return nil, ErrAccessDenied
	}

	// Строки фида собираются блоками по странам: основной профиль страны и
	// сразу за ним все альтернативные транспорты ЭТОГО ЖЕ сервера (CDN → gRPC →
	// Hysteria). Каждая ссылка получает UUID своего сервера — см. feed_builder.go.
	lines := s.buildGroupedFeedLines(ctx, feedItems)
	if len(lines) == 0 {
		return nil, ErrAccessDenied
	}

	// Сплит-роутинг: манифест компилируется под формат клиента.
	// Fail-open — при любой проблеме строки пустые и всё работает как раньше.
	xrayB64, happB64 := globalRoutingCache.load()
	routingB64 := xrayB64
	if group == clientGroupHapp {
		routingB64 = happB64
	}

	// Для Happ/Incy роутинг дополнительно кладём в тело подписки deeplink'ом
	// с авто-активацией (.../onadd/) — так профиль применяется без действий
	// пользователя. Строки добавляются ДО кодирования тела.
	lines = append(lines, routingBodyLines(group, happB64)...)

	feed := strings.Join(lines, "\n") + "\n"
	contentType := "text/plain; charset=utf-8"

	switch s.cfg.FeedFormat {
	case "base64":
		return &SubscriptionFeedResult{Body: []byte(base64.StdEncoding.EncodeToString([]byte(feed))), ContentType: contentType, Access: access, Uplink: trafficUp, Downlink: trafficDown, RoutingB64: routingB64}, nil
	case "plain":
		return &SubscriptionFeedResult{Body: []byte(feed), ContentType: contentType, Access: access, Uplink: trafficUp, Downlink: trafficDown, RoutingB64: routingB64}, nil
	default:
		return nil, fmt.Errorf("unsupported SUBSCRIPTION_FEED_FORMAT: %s", s.cfg.FeedFormat)
	}
}

func accessAllowed(state *AccessState, now time.Time) bool {
	if state == nil {
		return false
	}
	switch state.Status {
	case "trial", "active":
		return state.AccessUntil != nil && state.AccessUntil.After(now)
	case "grace":
		return state.GraceUntil != nil && state.GraceUntil.After(now)
	default:
		return false
	}
}

func (s *Service) ensureUserCredentialsAndSyncTx(ctx context.Context, tx pgx.Tx, access *AccessState) ([]FeedItem, error) {
	items, err := s.selectBalancedItems(ctx, access.TelegramID)
	if err != nil {
		return nil, err
	}

	feedItems, err := s.repo.EnsureCredentialsForItemsTx(ctx, tx, access.TelegramID, access.AccessRev, items)
	if err != nil {
		return nil, err
	}

	// C1: публикация команд синхронизации — в той же транзакции. Ошибку публикации
	// теперь ВОЗВРАЩАЕМ (а не только логируем): при провале вся транзакция
	// откатывается, сообщение переобработается — так outbox-команда не потеряется.
	if err := s.publishSyncCommands(ctx, tx, access, feedItems); err != nil {
		return nil, fmt.Errorf("publish sync commands tg=%d: %w", access.TelegramID, err)
	}
	return feedItems, nil
}

// ensureUserCredentialsAndSync — обёртка над Tx-версией для вызовов ВНЕ инбокс-
// транзакции (admin HTTP, reconcile-воркер): открывает собственную транзакцию,
// делегирует в ...Tx, коммитит. Так C1-атомарность (креды + outbox-команда)
// сохраняется и для этих путей.
func (s *Service) ensureUserCredentialsAndSync(ctx context.Context, access *AccessState) ([]FeedItem, error) {
	tx, err := s.repo.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	feedItems, err := s.ensureUserCredentialsAndSyncTx(ctx, tx, access)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return feedItems, nil
}

func (s *Service) ensureUserCredentials(ctx context.Context, access *AccessState) ([]FeedItem, error) {
	items, err := s.selectBalancedItems(ctx, access.TelegramID)
	if err != nil {
		return nil, err
	}
	return s.repo.EnsureCredentialsForItems(ctx, access.TelegramID, access.AccessRev, items)
}

// selectBalancedItems загружает пул-айтемы и метрики нод, затем через аллокатор
// выбирает по одному профилю на страну (балансировка внутри страны).
func (s *Service) selectBalancedItems(ctx context.Context, telegramID int64) ([]PoolItem, error) {
	items, err := s.repo.ListEnabledPoolItems(ctx)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, ErrNoPoolItems
	}

	servers, err := s.repo.LoadServersByCountry(ctx)
	if err != nil {
		return nil, err
	}

	sticky := func(itemKey string) string {
		sk, err := s.repo.GetUserServerForItem(ctx, telegramID, itemKey)
		if err != nil {
			return ""
		}
		return sk
	}

	selected := s.allocator.Allocate(items, servers, sticky, time.Now().UTC())
	if len(selected) == 0 {
		return nil, ErrNoPoolItems
	}
	return selected, nil
}

// buildUserProfiles формирует профили пользователя для одного узла: основной
// (base) плюс CDN-вариант (тот же base, но с CDN inbound и Optional=true), если
// для сервера подобран CDN-эндпоинт. Дедуп по (inbound_tag|email): если CDN-inbound
// совпал с основным — второй профиль не добавляется. Единый источник логики для
// publishSyncCommands и publishRevokeCommands (IM-2: убирает дублирование).
func (s *Service) buildUserProfiles(base kafkacontracts.VPNNodeUserProfile, serverKey string, cdnEndpoints []CDNEndpoint, grpcEndpoints []GRPCEndpoint) []kafkacontracts.VPNNodeUserProfile {
	seen := make(map[string]struct{}, 3)
	out := make([]kafkacontracts.VPNNodeUserProfile, 0, 3)
	add := func(p kafkacontracts.VPNNodeUserProfile) {
		key := p.InboundTag + "|" + p.Email
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, p)
	}

	add(base)

	if endpoint, ok := selectCDNForServer(cdnEndpoints, serverKey); ok {
		cdnInbound := endpoint.InboundTag
		if cdnInbound == "" {
			cdnInbound = "vless-xhttp-cdn-in"
		}
		cdnProfile := base
		cdnProfile.InboundTag = cdnInbound
		cdnProfile.Optional = true
		add(cdnProfile)
	}

	// gRPC: регистрируем того же пользователя в gRPC-inbound, чтобы gRPC-ссылка из
	// фида работала. Optional=true — если на узле нет gRPC-inbound, node-agent
	// пропустит профиль без провала всей команды (симметрично CDN).
	if endpoint, ok := selectGRPCForServer(grpcEndpoints, serverKey); ok {
		grpcInbound := endpoint.InboundTag
		if grpcInbound == "" {
			grpcInbound = "vless-grpc-cdn-in"
		}
		grpcProfile := base
		grpcProfile.InboundTag = grpcInbound
		grpcProfile.Optional = true
		add(grpcProfile)
	}

	return out
}

func (s *Service) publishSyncCommands(ctx context.Context, tx pgx.Tx, access *AccessState, feedItems []FeedItem) error {
	if s.producer == nil {
		return nil
	}

	byNode := make(map[string][]FeedItem)
	serverByNode := make(map[string]string)
	for _, item := range feedItems {
		nodeID := item.PoolItem.NodeID
		if nodeID == "" {
			continue
		}
		byNode[nodeID] = append(byNode[nodeID], item)
		serverByNode[nodeID] = item.PoolItem.ServerKey
	}

	// Эндпоинты берём ТЕМИ ЖЕ загрузчиками, что и фид подписки: набор inbound'ов,
	// куда регистрируется пользователь, обязан совпадать с набором ссылок, которые
	// он получит. Разъезд здесь = ссылка есть, а UUID на узле не заведён.
	cdnEndpoints := s.loadCDNEndpoints(ctx)
	grpcEndpoints := s.loadGRPCEndpoints(ctx)

	for nodeID, nodeItems := range byNode {
		profiles := make([]kafkacontracts.VPNNodeUserProfile, 0, len(nodeItems)*2)
		for _, item := range nodeItems {
			base := kafkacontracts.VPNNodeUserProfile{
				ItemKey:     item.PoolItem.ItemKey,
				CountryCode: item.PoolItem.CountryCode,
				Title:       item.PoolItem.Title,
				ProfileType: item.PoolItem.ProfileType,
				InboundTag:  item.Credential.InboundTag,
				Email:       item.Credential.Email,
				VLESSUUID:   item.Credential.VLESSUUID,
				Flow:        item.PoolItem.Flow,
				Level:       item.PoolItem.Level,
				AccessUntil: access.AccessUntil,
			}
			profiles = append(profiles, s.buildUserProfiles(base, item.PoolItem.ServerKey, cdnEndpoints, grpcEndpoints)...)
		}

		cmd := kafkacontracts.NodeSyncUserCommand{
			Type:        kafkacontracts.VPNCommandNodeSyncUser,
			CommandID:   uuid.NewString(),
			NodeID:      nodeID,
			ServerKey:   serverByNode[nodeID],
			TelegramID:  access.TelegramID,
			AccessRev:   access.AccessRev,
			AccessUntil: access.AccessUntil,
			Profiles:    profiles,
			CreatedAt:   time.Now().UTC(),
		}
		if err := outbox.AddTx(ctx, tx, outbox.Event{
			AggregateType: "vpn-command",
			AggregateID:   nodeID,
			Topic:         commonkafka.TopicVPNCommands,
			MessageKey:    nodeID,
			EventType:     string(cmd.Type),
			Payload:       &cmd,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) publishRevokeCommands(ctx context.Context, tx pgx.Tx, telegramID int64, accessRev int64, reason string) error {
	if s.producer == nil {
		return nil
	}

	creds, err := s.repo.DisableUserCredentialsTx(ctx, tx, telegramID, accessRev)
	if err != nil {
		return err
	}
	byNode := make(map[string][]UserCredential)
	serverByNode := make(map[string]string)
	for _, cred := range creds {
		if cred.NodeID == "" {
			continue
		}
		byNode[cred.NodeID] = append(byNode[cred.NodeID], cred)
		serverByNode[cred.NodeID] = cred.ServerKey
	}

	// Те же эндпоинты и теми же загрузчиками, что и при выдаче: отзываем доступ
	// из всех inbound'ов, куда пользователь был заведён, иначе на узле остаётся
	// активный UUID после потери доступа.
	revokeCDNEndpoints := s.loadCDNEndpoints(ctx)
	revokeGRPCEndpoints := s.loadGRPCEndpoints(ctx)

	for nodeID, nodeCreds := range byNode {
		profiles := make([]kafkacontracts.VPNNodeUserProfile, 0, len(nodeCreds)*2)
		for _, cred := range nodeCreds {
			base := kafkacontracts.VPNNodeUserProfile{
				ItemKey:    cred.ItemKey,
				InboundTag: cred.InboundTag,
				Email:      cred.Email,
				VLESSUUID:  cred.VLESSUUID,
			}
			profiles = append(profiles, s.buildUserProfiles(base, cred.ServerKey, revokeCDNEndpoints, revokeGRPCEndpoints)...)
		}

		cmd := kafkacontracts.NodeRevokeUserCommand{
			Type:       kafkacontracts.VPNCommandNodeRevokeUser,
			CommandID:  uuid.NewString(),
			NodeID:     nodeID,
			ServerKey:  serverByNode[nodeID],
			TelegramID: telegramID,
			AccessRev:  accessRev,
			Reason:     reason,
			Profiles:   profiles,
			CreatedAt:  time.Now().UTC(),
		}
		if err := outbox.AddTx(ctx, tx, outbox.Event{
			AggregateType: "vpn-command",
			AggregateID:   nodeID,
			Topic:         commonkafka.TopicVPNCommands,
			MessageKey:    nodeID,
			EventType:     string(cmd.Type),
			Payload:       &cmd,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ApplyTrialStarted(ctx context.Context, tx pgx.Tx, event *kafkacontracts.SubscriptionTrialStartedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "trial", AccessUntil: &event.TrialUntil, AccessRev: event.AccessRev, CountryCode: event.Country}
	if err := s.repo.UpsertAccessProjectionTx(ctx, tx, access, string(event.Type), time.Now().UTC()); err != nil {
		return err
	}
	_, err := s.ensureUserCredentialsAndSyncTx(ctx, tx, access)
	return err
}

func (s *Service) ApplyActivated(ctx context.Context, tx pgx.Tx, event *kafkacontracts.SubscriptionActivatedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "active", AccessUntil: &event.ActiveUntil, AccessRev: event.AccessRev, CountryCode: event.Country}
	if err := s.repo.UpsertAccessProjectionTx(ctx, tx, access, string(event.Type), event.ActivatedAt); err != nil {
		return err
	}
	_, err := s.ensureUserCredentialsAndSyncTx(ctx, tx, access)
	return err
}

func (s *Service) ApplyCanceled(ctx context.Context, tx pgx.Tx, event *kafkacontracts.SubscriptionCanceledEvent) error {
	if event == nil {
		return nil
	}
	status := "expired"
	accessUntil := event.AccessUntil
	if event.CancelAtPeriodEnd && accessUntil != nil && accessUntil.After(time.Now().UTC()) {
		status = "active"
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: status, AccessUntil: accessUntil, AccessRev: event.AccessRev, CountryCode: "ALL"}
	if err := s.repo.UpsertAccessProjectionTx(ctx, tx, access, string(event.Type), event.CanceledAt); err != nil {
		return err
	}
	if status == "active" {
		_, err := s.ensureUserCredentialsAndSyncTx(ctx, tx, access)
		return err
	}
	return s.publishRevokeCommands(ctx, tx, event.TelegramID, event.AccessRev, "subscription_canceled")
}

func (s *Service) ApplyGraceStarted(ctx context.Context, tx pgx.Tx, event *kafkacontracts.SubscriptionGraceStartedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "grace", GraceUntil: &event.GraceUntil, AccessRev: event.AccessRev, CountryCode: "ALL"}
	if err := s.repo.UpsertAccessProjectionTx(ctx, tx, access, string(event.Type), time.Now().UTC()); err != nil {
		return err
	}
	_, err := s.ensureUserCredentialsAndSyncTx(ctx, tx, access)
	return err
}

func (s *Service) ApplySuspended(ctx context.Context, tx pgx.Tx, event *kafkacontracts.SubscriptionSuspendedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "expired", AccessUntil: &event.SuspendedAt, AccessRev: event.AccessRev, CountryCode: "ALL"}
	if err := s.repo.UpsertAccessProjectionTx(ctx, tx, access, string(event.Type), event.SuspendedAt); err != nil {
		return err
	}
	return s.publishRevokeCommands(ctx, tx, event.TelegramID, event.AccessRev, "subscription_suspended")
}

func (s *Service) ApplyNodeHeartbeat(ctx context.Context, tx pgx.Tx, event *kafkacontracts.VPNNodeHeartbeatEvent) error {
	if event == nil {
		return nil
	}
	return s.repo.UpdateNodeHeartbeatTx(ctx, tx, event.NodeID, event.ServerKey, event.CreatedAt,
		NodeLoadReport{
			OnlineUsers: event.OnlineUsers,
			UplinkBps:   event.UplinkBps,
			DownlinkBps: event.DownlinkBps,
		})
}

// ApplyNodeTraffic сохраняет кумулятивный трафик пользователей, присланный узлом.
func (s *Service) ApplyNodeTraffic(ctx context.Context, tx pgx.Tx, event *kafkacontracts.VPNNodeTrafficEvent) error {
	if event == nil || len(event.Items) == 0 {
		return nil
	}

	// Трафик хранится ПО УЗЛАМ: у одного пользователя своя учётка на каждой ноде,
	// и раньше значения разных нод затирали друг друга в одной строке по
	// telegram_id. Ключ (telegram_id, node_id) убирает конфликт, а суммирование
	// делается при чтении (Repository.GetUserTraffic).
	nodeID := event.NodeID
	if nodeID == "" {
		nodeID = event.ServerKey
	}
	if nodeID == "" {
		slog.Warn("node traffic event without node_id and server_key, skipped")
		return nil
	}

	// Суммарный кумулятивный трафик узла (для Prometheus-метрики / дашборда).
	var totalUplink, totalDownlink int64

	for _, item := range event.Items {
		if item.TelegramID == 0 {
			continue
		}
		if err := s.repo.UpsertUserNodeTrafficTx(ctx, tx, item.TelegramID, nodeID, item.Uplink, item.Downlink); err != nil {
			return err
		}
		totalUplink += item.Uplink
		totalDownlink += item.Downlink
	}

	// Выставляем метрику трафика по узлу. Лейбл service Prometheus добавит сам
	// при скрейпе. server_key используем и как country-лейбл (резолв страны здесь
	// избыточен — панель группирует по direction). Значения кумулятивны (Xray
	// не сбрасывает счётчики), поэтому в дашборде корректно работает rate().
	serverKey := event.ServerKey
	if serverKey == "" {
		serverKey = event.NodeID
	}
	commonmetrics.NodeTrafficBytes.WithLabelValues(serverKey, serverKey, "uplink").Set(float64(totalUplink))
	commonmetrics.NodeTrafficBytes.WithLabelValues(serverKey, serverKey, "downlink").Set(float64(totalDownlink))

	return nil
}

func (s *Service) ApplyNodeUserSynced(ctx context.Context, tx pgx.Tx, event *kafkacontracts.VPNNodeUserSyncedEvent) error {
	if event == nil {
		return nil
	}
	if err := s.repo.SaveNodeSyncResultTx(ctx, tx, event.NodeID, event.ServerKey, event.TelegramID, event.AccessRev, event.CommandID, string(event.Type), event.Success, event.Error); err != nil {
		return err
	}
	if event.Success {
		return s.repo.MarkCredentialsSyncedTx(ctx, tx, event.TelegramID, event.AccessRev, event.NodeID)
	}
	return nil
}

func (s *Service) ApplyNodeUserRevoked(ctx context.Context, tx pgx.Tx, event *kafkacontracts.VPNNodeUserRevokedEvent) error {
	if event == nil {
		return nil
	}
	return s.repo.SaveNodeSyncResultTx(ctx, tx, event.NodeID, event.ServerKey, event.TelegramID, event.AccessRev, event.CommandID, string(event.Type), event.Success, event.Error)
}

func (s *Service) ApplyNodeError(ctx context.Context, tx pgx.Tx, event *kafkacontracts.VPNNodeErrorEvent) error {
	if event == nil {
		return nil
	}
	return s.repo.SaveNodeSyncResultTx(ctx, tx, event.NodeID, event.ServerKey, 0, 0, event.CommandID, string(event.Type), false, event.Error)
}
