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

	// CDNQuota — политика лимита CDN-трафика (20 GB на пользователя на ноду).
	// Выключенная политика полностью сохраняет прежнее поведение.
	CDNQuota CDNQuotaPolicy
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

// buildCDNProfile строит CDN-профиль пользователя для узла.
//
// Ключевое отличие от прошлой версии: у CDN-учётки СОБСТВЕННЫЙ email. Xray
// ведёт счётчик трафика по email (user>>><email>>>>traffic>>>*), а не по
// инбаунду, поэтому при общем email CDN- и не-CDN-байты физически неразделимы
// и учесть квоту было невозможно. Суффикс -cdn изолирует счётчик, не затрагивая
// ни UUID, ни остальные транспорты.
//
// Возвращает (профиль, true), только если для сервера подобран CDN-эндпоинт и
// email удалось построить.
func buildCDNProfile(base kafkacontracts.VPNNodeUserProfile, serverKey string, cdnEndpoints []CDNEndpoint) (kafkacontracts.VPNNodeUserProfile, bool) {
	endpoint, ok := selectCDNForServer(cdnEndpoints, serverKey)
	if !ok {
		return kafkacontracts.VPNNodeUserProfile{}, false
	}
	cdnInbound := endpoint.InboundTag
	if cdnInbound == "" {
		cdnInbound = "vless-xhttp-cdn-in"
	}
	profile := base
	profile.InboundTag = cdnInbound
	profile.Optional = true
	// Если email построить не удалось — оставляем базовый: лучше потерять
	// точность учёта, чем выдать пользователю нерабочую учётку.
	if isolated := cdnEmail(base.Email); isolated != "" {
		profile.Email = isolated
	}
	return profile, true
}

// buildUserProfiles формирует профили пользователя для одного узла: основной
// (base) плюс CDN- и gRPC-варианты (Optional=true), если для сервера подобраны
// соответствующие эндпоинты. Дедуп по (inbound_tag|email). Единый источник
// логики для publishSyncCommands и publishRevokeCommands (IM-2).
//
// skipCDN=true исключает CDN-профиль из ЖЕЛАЕМОГО состояния. Используется, когда
// CDN-квота пользователя на этом узле исчерпана: иначе ближайший reconcile
// вернул бы отключённую учётку обратно. На путь ОТЗЫВА этот флаг не подаётся —
// снимать надо в том числе и CDN-учётку.
func (s *Service) buildUserProfiles(base kafkacontracts.VPNNodeUserProfile, serverKey string, cdnEndpoints []CDNEndpoint, grpcEndpoints []GRPCEndpoint, skipCDN bool) []kafkacontracts.VPNNodeUserProfile {
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

	if !skipCDN {
		if cdnProfile, ok := buildCDNProfile(base, serverKey, cdnEndpoints); ok {
			add(cdnProfile)
		}
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

	// Узлы, где CDN-квота пользователя исчерпана: их CDN-профиль в желаемое
	// состояние не попадает, поэтому reconcile не возвращает отключённый доступ.
	// Ошибка чтения не должна ронять выдачу основного доступа — тогда просто
	// работаем как раньше (fail-open по КВОТЕ, не по доступу).
	exhausted := map[string]struct{}{}
	if s.cfg.CDNQuota.Enabled && s.cfg.CDNQuota.Enforce {
		if got, err := s.repo.ExhaustedNodesForUser(ctx, access.TelegramID); err == nil {
			exhausted = got
		} else {
			slog.Warn("cdn quota: read exhausted nodes failed", "telegram_id", access.TelegramID, "err", err)
		}
	}

	for nodeID, nodeItems := range byNode {
		_, skipCDN := exhausted[nodeID]
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
			profiles = append(profiles, s.buildUserProfiles(base, item.PoolItem.ServerKey, cdnEndpoints, grpcEndpoints, skipCDN)...)
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
			// Отзыв: CDN-профиль включаем всегда (skipCDN=false) — иначе на узле
			// осталась бы активная CDN-учётка отозванного пользователя.
			profiles = append(profiles, s.buildUserProfiles(base, cred.ServerKey, revokeCDNEndpoints, revokeGRPCEndpoints, false)...)
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
	// Сброс CDN-квоты выполняется ДО пересборки desired state: иначе ниже по
	// коду мы бы прочитали ещё исчерпанное состояние и не вернули CDN-профиль.
	if err := s.resetCDNQuotaOnPayment(ctx, tx, event.TelegramID, event.PaymentID, event.ActivatedAt); err != nil {
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

	// Агрегаты одного отчёта: общий трафик и отдельно CDN-трафик по каждому
	// пользователю. Считаются в памяти по одному событию, в БД уходят готовыми.
	perUser := make(map[int64][2]int64, len(event.Items))
	cdnPerUser := make(map[int64]int64, len(event.Items))

	// Allowlist CDN-инбаундов строится из серверной конфигурации эндпоинтов.
	// Никаких догадок по имени: тега нет в списке — трафик не CDN.
	var cdnInbounds map[string]struct{}
	if s.cfg.CDNQuota.Enabled {
		cdnInbounds = cdnInboundAllowlist(s.loadCDNEndpoints(ctx))
	}

	for _, item := range event.Items {
		if item.TelegramID == 0 {
			continue
		}
		perUser[item.TelegramID] = [2]int64{
			perUser[item.TelegramID][0] + item.Uplink,
			perUser[item.TelegramID][1] + item.Downlink,
		}
		totalUplink += item.Uplink
		totalDownlink += item.Downlink

		// Классификация строго по inbound_tag учётки, сверенному с серверным
		// allowlist'ом. Пустой тег (агент старой версии либо учётка, общая
		// для нескольких инбаундов) и любой тег вне списка — трафик
		// неклассифицирован: в квоту он не идёт.
		if len(cdnInbounds) > 0 {
			if item.InboundTag == "" {
				// Ожидаемо ненулевая величина: основной и gRPC-профили делят
				// один email, поэтому агент осознанно не проставляет им тег.
				// Алертить надо на РЕЗКИЙ РОСТ относительно базовой линии, а
				// не на любое ненулевое значение.
				commonmetrics.CDNQuotaUnclassifiedTotal.WithLabelValues(nodeID).Inc()
			} else if _, isCDN := cdnInbounds[item.InboundTag]; isCDN {
				cdnPerUser[item.TelegramID] += item.Uplink + item.Downlink
			}
		}
	}

	// Общий трафик пишем ОДНОЙ строкой на пользователя: у него теперь несколько
	// учёток на узле (основная + CDN), и раньше вторая строка затирала первую
	// через GREATEST вместо того, чтобы сложиться. Сумма монотонных счётчиков
	// монотонна, поэтому GREATEST в апсерте продолжает защищать от повторов.
	for telegramID, tr := range perUser {
		if err := s.repo.UpsertUserNodeTrafficTx(ctx, tx, telegramID, nodeID, tr[0], tr[1]); err != nil {
			return err
		}
	}

	if err := s.applyCDNQuota(ctx, tx, nodeID, event.ServerKey, cdnPerUser, event.CreatedAt); err != nil {
		return err
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

// applyCDNQuota применяет наблюдения CDN-трафика к квотам узла и, если квота
// исчерпана и включено принуждение, снимает ТОЛЬКО CDN-учётку пользователя на
// этом узле. Основной и прочие транспорты не затрагиваются.
func (s *Service) applyCDNQuota(
	ctx context.Context,
	tx pgx.Tx,
	nodeID, serverKey string,
	cdnPerUser map[int64]int64,
	at time.Time,
) error {
	if !s.cfg.CDNQuota.Enabled || len(cdnPerUser) == 0 {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	periodKey := calendarPeriodKey(at)

	for telegramID, observed := range cdnPerUser {
		state, err := s.repo.ApplyObservationTx(ctx, tx, telegramID, nodeID, observed,
			s.cfg.CDNQuota.LimitBytes, periodKey, at)
		if err != nil {
			return err
		}
		if state.CounterRebased {
			commonmetrics.CDNQuotaCounterResetsTotal.WithLabelValues(nodeID).Inc()
			slog.Warn("cdn quota: node counter rolled back, row rebased",
				"telegram_id", telegramID, "node_id", nodeID, "observed", observed)
		}

		if !state.JustExhausted {
			continue
		}

		commonmetrics.CDNQuotaExhaustedTotal.WithLabelValues(nodeID).Inc()
		// Overshoot — насколько потребление перевалило за лимит к моменту
		// обнаружения. Величина ограничена частотой сбора и скоростью канала;
		// нулевого превышения при периодической телеметрии не бывает.
		if over := state.UsedBytes - state.LimitBytes; over > 0 {
			commonmetrics.CDNQuotaOvershootBytes.WithLabelValues(nodeID).Observe(float64(over))
		}
		slog.Info("cdn quota exhausted",
			"telegram_id", telegramID, "node_id", nodeID,
			"used_bytes", state.UsedBytes, "limit_bytes", state.LimitBytes,
			"period", state.PeriodKey, "revision", state.Revision)

		if !s.cfg.CDNQuota.Enforce {
			continue // shadow mode: считаем и наблюдаем, доступ не трогаем
		}
		if err := s.publishCDNRevoke(ctx, tx, telegramID, nodeID, serverKey, "cdn_quota_exhausted"); err != nil {
			return err
		}
	}
	return nil
}

// publishCDNRevoke шлёт узлу команду снять РОВНО CDN-учётку пользователя.
// Scope=listed_profiles обязателен: без него агент отзывает пользователя с узла
// целиком и вместе с CDN унёс бы оплаченный основной доступ.
func (s *Service) publishCDNRevoke(ctx context.Context, tx pgx.Tx, telegramID int64, nodeID, serverKey, reason string) error {
	creds, err := s.repo.ListEnabledCredentialsForNodeTx(ctx, tx, telegramID, nodeID)
	if err != nil {
		return err
	}
	if len(creds) == 0 {
		return nil
	}
	cdnEndpoints := s.loadCDNEndpoints(ctx)

	profiles := make([]kafkacontracts.VPNNodeUserProfile, 0, len(creds))
	maxRev := int64(0)
	for _, cred := range creds {
		if cred.AccessRev > maxRev {
			maxRev = cred.AccessRev
		}
		base := kafkacontracts.VPNNodeUserProfile{
			ItemKey:    cred.ItemKey,
			InboundTag: cred.InboundTag,
			Email:      cred.Email,
			VLESSUUID:  cred.VLESSUUID,
		}
		if cdnProfile, ok := buildCDNProfile(base, cred.ServerKey, cdnEndpoints); ok {
			profiles = append(profiles, cdnProfile)
		}
	}
	if len(profiles) == 0 {
		return nil
	}

	cmd := kafkacontracts.NodeRevokeUserCommand{
		Type:       kafkacontracts.VPNCommandNodeRevokeUser,
		CommandID:  uuid.NewString(),
		NodeID:     nodeID,
		ServerKey:  serverKey,
		TelegramID: telegramID,
		AccessRev:  maxRev,
		Reason:     reason,
		Scope:      kafkacontracts.RevokeScopeListedProfiles,
		Profiles:   profiles,
		CreatedAt:  time.Now().UTC(),
	}
	return outbox.AddTx(ctx, tx, outbox.Event{
		AggregateType: "vpn-command",
		AggregateID:   nodeID,
		Topic:         commonkafka.TopicVPNCommands,
		MessageKey:    nodeID,
		EventType:     string(cmd.Type),
		Payload:       &cmd,
	})
}

// resetCDNQuotaOnPayment открывает новый период квоты на всех узлах пользователя
// после подтверждённой оплаты или продления.
//
// Идемпотентность: ключ периода детерминирован от payment_id, повторное событие
// с тем же идентификатором ничего не сбрасывает. Пробный период, pending и
// неуспешный платёж сюда не доходят — обработчик вызывается только из
// ApplyActivated, то есть по подтверждённому событию платёжного контура.
func (s *Service) resetCDNQuotaOnPayment(ctx context.Context, tx pgx.Tx, telegramID int64, paymentID string, at time.Time) error {
	if !s.cfg.CDNQuota.Enabled {
		return nil
	}
	periodKey := paymentPeriodKey(paymentID)
	if periodKey == "" {
		return nil // активация без payment_id (реферальная награда) период не открывает
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	nodes, err := s.repo.ResetQuotaForUserTx(ctx, tx, telegramID, periodKey, s.cfg.CDNQuota.LimitBytes, at)
	if err != nil {
		return err
	}
	for _, nodeID := range nodes {
		commonmetrics.CDNQuotaResetTotal.WithLabelValues(nodeID, "payment").Inc()
	}
	if len(nodes) > 0 {
		slog.Info("cdn quota reset by payment", "telegram_id", telegramID, "nodes", len(nodes), "period", periodKey)
	}
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
