package vpn_orchestrator

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	commonkafka "vpn-platform/internal/common/kafka"
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

func (s *Service) RenderSubscriptionFeedDetailed(ctx context.Context, token string) (*SubscriptionFeedResult, error) {
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

	feedItems, err := s.ensureUserCredentials(ctx, access)
	if err != nil {
		return nil, ErrAccessDenied
	}
	if len(feedItems) == 0 {
		return nil, ErrAccessDenied
	}

	lines := make([]string, 0, len(feedItems)+1)
	for _, item := range feedItems {
		if strings.TrimSpace(item.URL) != "" {
			lines = append(lines, item.URL)
		}
	}
	if len(lines) == 0 {
		return nil, ErrAccessDenied
	}

	// CDN-конфигурации добавляются в общий фид с привязкой к серверам: для каждого
	// выданного пользователю сервера подбирается привязанный к нему CDN (или
	// глобальный/первый, если персональной привязки нет). UUID — тот же, что у
	// обычных конфигов. Пользователь импортирует одну ссылку подписки.
	lines = append(lines, s.cdnLinesForFeed(ctx, feedItems)...)

	feed := strings.Join(lines, "\n") + "\n"
	contentType := "text/plain; charset=utf-8"

	switch s.cfg.FeedFormat {
	case "base64":
		return &SubscriptionFeedResult{Body: []byte(base64.StdEncoding.EncodeToString([]byte(feed))), ContentType: contentType, Access: access}, nil
	case "plain":
		return &SubscriptionFeedResult{Body: []byte(feed), ContentType: contentType, Access: access}, nil
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

func (s *Service) ensureUserCredentialsAndSync(ctx context.Context, access *AccessState) ([]FeedItem, error) {
	items, err := s.selectBalancedItems(ctx, access.TelegramID)
	if err != nil {
		return nil, err
	}

	feedItems, err := s.repo.EnsureCredentialsForItems(ctx, access.TelegramID, access.AccessRev, items)
	if err != nil {
		return nil, err
	}

	if err := s.publishSyncCommands(ctx, access, feedItems); err != nil {
		log.Printf("[vpn-orchestrator] publish sync commands error telegram_id=%d err=%v", access.TelegramID, err)
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

func (s *Service) publishSyncCommands(ctx context.Context, access *AccessState, feedItems []FeedItem) error {
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

	// CDN-эндпоинты загружаем один раз для всех узлов: для каждого сервера
	// пользователя нужно зарегистрировать его UUID не только в основном inbound,
	// но и в CDN-inbound, иначе CDN-ссылка (XHTTP) не работает.
	cdnEndpoints, cdnErr := s.repo.ListEnabledCDNEndpoints(ctx)
	if cdnErr != nil {
		log.Printf("publishSyncCommands: load cdn endpoints failed: %v", cdnErr)
		cdnEndpoints = nil
	}

	for nodeID, nodeItems := range byNode {
		profiles := make([]kafkacontracts.VPNNodeUserProfile, 0, len(nodeItems)*2)
		// дедуп профилей по (inbound_tag|email): один пользователь не должен
		// попасть в один и тот же inbound дважды.
		seenProfile := make(map[string]struct{})
		addProfile := func(p kafkacontracts.VPNNodeUserProfile) {
			key := p.InboundTag + "|" + p.Email
			if _, ok := seenProfile[key]; ok {
				return
			}
			seenProfile[key] = struct{}{}
			profiles = append(profiles, p)
		}

		for _, item := range nodeItems {
			// 1) основной профиль (как было) — регистрация в основном inbound.
			addProfile(kafkacontracts.VPNNodeUserProfile{
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
			})

			// 2) CDN-профили: для CDN, подобранного этому серверу, регистрируем
			// тот же UUID/email в CDN-inbound. Тогда CDN-ссылка заработает.
			if endpoint, ok := selectCDNForServer(cdnEndpoints, item.PoolItem.ServerKey); ok {
				cdnInbound := endpoint.InboundTag
				if cdnInbound == "" {
					cdnInbound = "vless-xhttp-cdn-in"
				}
				addProfile(kafkacontracts.VPNNodeUserProfile{
					ItemKey:     item.PoolItem.ItemKey,
					CountryCode: item.PoolItem.CountryCode,
					Title:       item.PoolItem.Title,
					ProfileType: item.PoolItem.ProfileType,
					InboundTag:  cdnInbound,
					Email:       item.Credential.Email,
					VLESSUUID:   item.Credential.VLESSUUID,
					Flow:        item.PoolItem.Flow,
					Level:       item.PoolItem.Level,
					AccessUntil: access.AccessUntil,
					Optional:    true,
				})
			}
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
		tx, err := s.repo.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if err := outbox.AddTx(ctx, tx, outbox.Event{
			AggregateType: "vpn-command",
			AggregateID:   nodeID,
			Topic:         commonkafka.TopicVPNCommands,
			MessageKey:    nodeID,
			EventType:     string(cmd.Type),
			Payload:       &cmd,
		}); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) publishRevokeCommands(ctx context.Context, telegramID int64, accessRev int64, reason string) error {
	if s.producer == nil {
		return nil
	}

	creds, err := s.repo.DisableUserCredentials(ctx, telegramID, accessRev)
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

	// Те же CDN-эндпоинты, что и при выдаче: отзываем доступ и из CDN-inbound,
	// чтобы на узле не оставалось активного UUID после потери доступа.
	revokeCDNEndpoints, revokeCDNErr := s.repo.ListEnabledCDNEndpoints(ctx)
	if revokeCDNErr != nil {
		log.Printf("publishRevokeCommands: load cdn endpoints failed: %v", revokeCDNErr)
		revokeCDNEndpoints = nil
	}

	for nodeID, nodeCreds := range byNode {
		profiles := make([]kafkacontracts.VPNNodeUserProfile, 0, len(nodeCreds)*2)
		seenRevoke := make(map[string]struct{})
		addRevoke := func(p kafkacontracts.VPNNodeUserProfile) {
			key := p.InboundTag + "|" + p.Email
			if _, ok := seenRevoke[key]; ok {
				return
			}
			seenRevoke[key] = struct{}{}
			profiles = append(profiles, p)
		}

		for _, cred := range nodeCreds {
			// основной inbound
			addRevoke(kafkacontracts.VPNNodeUserProfile{
				ItemKey:    cred.ItemKey,
				InboundTag: cred.InboundTag,
				Email:      cred.Email,
				VLESSUUID:  cred.VLESSUUID,
			})
			// CDN-inbound (тот же подбор по серверу)
			if endpoint, ok := selectCDNForServer(revokeCDNEndpoints, cred.ServerKey); ok {
				cdnInbound := endpoint.InboundTag
				if cdnInbound == "" {
					cdnInbound = "vless-xhttp-cdn-in"
				}
				addRevoke(kafkacontracts.VPNNodeUserProfile{
					ItemKey:    cred.ItemKey,
					InboundTag: cdnInbound,
					Email:      cred.Email,
					VLESSUUID:  cred.VLESSUUID,
					Optional:   true,
				})
			}
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
		tx, err := s.repo.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if err := outbox.AddTx(ctx, tx, outbox.Event{
			AggregateType: "vpn-command",
			AggregateID:   nodeID,
			Topic:         commonkafka.TopicVPNCommands,
			MessageKey:    nodeID,
			EventType:     string(cmd.Type),
			Payload:       &cmd,
		}); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ApplyTrialStarted(ctx context.Context, event *kafkacontracts.SubscriptionTrialStartedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "trial", AccessUntil: &event.TrialUntil, AccessRev: event.AccessRev, CountryCode: event.Country}
	if err := s.repo.UpsertAccessProjection(ctx, access, string(event.Type), time.Now().UTC()); err != nil {
		return err
	}
	_, err := s.ensureUserCredentialsAndSync(ctx, access)
	return err
}

func (s *Service) ApplyActivated(ctx context.Context, event *kafkacontracts.SubscriptionActivatedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "active", AccessUntil: &event.ActiveUntil, AccessRev: event.AccessRev, CountryCode: event.Country}
	if err := s.repo.UpsertAccessProjection(ctx, access, string(event.Type), event.ActivatedAt); err != nil {
		return err
	}
	_, err := s.ensureUserCredentialsAndSync(ctx, access)
	return err
}

func (s *Service) ApplyCanceled(ctx context.Context, event *kafkacontracts.SubscriptionCanceledEvent) error {
	if event == nil {
		return nil
	}
	status := "expired"
	accessUntil := event.AccessUntil
	if event.CancelAtPeriodEnd && accessUntil != nil && accessUntil.After(time.Now().UTC()) {
		status = "active"
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: status, AccessUntil: accessUntil, AccessRev: event.AccessRev, CountryCode: "ALL"}
	if err := s.repo.UpsertAccessProjection(ctx, access, string(event.Type), event.CanceledAt); err != nil {
		return err
	}
	if status == "active" {
		_, err := s.ensureUserCredentialsAndSync(ctx, access)
		return err
	}
	return s.publishRevokeCommands(ctx, event.TelegramID, event.AccessRev, "subscription_canceled")
}

func (s *Service) ApplyGraceStarted(ctx context.Context, event *kafkacontracts.SubscriptionGraceStartedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "grace", GraceUntil: &event.GraceUntil, AccessRev: event.AccessRev, CountryCode: "ALL"}
	if err := s.repo.UpsertAccessProjection(ctx, access, string(event.Type), time.Now().UTC()); err != nil {
		return err
	}
	_, err := s.ensureUserCredentialsAndSync(ctx, access)
	return err
}

func (s *Service) ApplySuspended(ctx context.Context, event *kafkacontracts.SubscriptionSuspendedEvent) error {
	if event == nil {
		return nil
	}
	access := &AccessState{TelegramID: event.TelegramID, Status: "expired", AccessUntil: &event.SuspendedAt, AccessRev: event.AccessRev, CountryCode: "ALL"}
	if err := s.repo.UpsertAccessProjection(ctx, access, string(event.Type), event.SuspendedAt); err != nil {
		return err
	}
	return s.publishRevokeCommands(ctx, event.TelegramID, event.AccessRev, "subscription_suspended")
}

func (s *Service) ApplyNodeHeartbeat(ctx context.Context, event *kafkacontracts.VPNNodeHeartbeatEvent) error {
	if event == nil {
		return nil
	}
	return s.repo.UpdateNodeHeartbeat(ctx, event.NodeID, event.ServerKey, event.CreatedAt)
}

func (s *Service) ApplyNodeUserSynced(ctx context.Context, event *kafkacontracts.VPNNodeUserSyncedEvent) error {
	if event == nil {
		return nil
	}
	if err := s.repo.SaveNodeSyncResult(ctx, event.NodeID, event.ServerKey, event.TelegramID, event.AccessRev, event.CommandID, string(event.Type), event.Success, event.Error); err != nil {
		return err
	}
	if event.Success {
		return s.repo.MarkCredentialsSynced(ctx, event.TelegramID, event.AccessRev, event.NodeID)
	}
	return nil
}

func (s *Service) ApplyNodeUserRevoked(ctx context.Context, event *kafkacontracts.VPNNodeUserRevokedEvent) error {
	if event == nil {
		return nil
	}
	return s.repo.SaveNodeSyncResult(ctx, event.NodeID, event.ServerKey, event.TelegramID, event.AccessRev, event.CommandID, string(event.Type), event.Success, event.Error)
}

func (s *Service) ApplyNodeError(ctx context.Context, event *kafkacontracts.VPNNodeErrorEvent) error {
	if event == nil {
		return nil
	}
	return s.repo.SaveNodeSyncResult(ctx, event.NodeID, event.ServerKey, 0, 0, event.CommandID, string(event.Type), false, event.Error)
}
