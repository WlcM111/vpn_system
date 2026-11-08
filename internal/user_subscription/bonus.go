package user_subscription

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	commonmetrics "vpn-platform/internal/common/metrics"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	"github.com/jackc/pgx/v5"
)

// ============================================================================
// Бонусные дни по кампании оператора (scripts/bonus_campaign.sh).
//
// Начисление устроено так же, как реферальная награда (HandleReferralRedeem):
// срок продлевается в одной транзакции с событием активации для оркестратора
// и уведомлением пользователю — оба уходят через outbox. Отличия:
//
//   - начисление ровно одно на пару (пользователь, кампания): журнал
//     subscription_grants, source = 'admin', business_key = кампания. Повтор
//     команды, повторный запуск скрипта и гонка двух доставок дают одно
//     начисление и одно сообщение — инвариант держит UNIQUE в БД;
//   - срок считается от GREATEST(expires_at, now): у ушедших пользователей это
//     «N дней с текущей даты», у вернувшихся за это время — дни сверху;
//     имеющийся срок никогда не уменьшается;
//   - льготный период (grace) принадлежит платёжному контуру — как и триал,
//     бонус в нём не начисляется (deferred_grace, сообщение не отправляется);
//   - пока биллинг ещё может изменить подписку (повторы автосписания, его
//     льготный период, запланированное списание) — тоже нет (deferred_billing):
//     неуспешное списание позже перевело бы пользователя в grace/expired и
//     обрезало бы бонус. Команда отложенному не мешает следующему запуску
//     кампании: начисление держит журнал, а не дедупликация команд.
// ============================================================================

const (
	bonusGrantSource = "admin"
	maxBonusDays     = 366
	// Лимит Telegram — 4096 символов; запас — на подстановку {until}/{days}.
	maxBonusMessageRunes = 3500
)

var bonusCampaignRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,63}$`)

// ErrInvalidBonusCommand — команду нельзя выполнить ни при каком повторе;
// консьюмер отправляет её в DLT.
var ErrInvalidBonusCommand = errors.New("invalid grant bonus command")

// BonusGrantOutcome — подтверждённый результат начисления.
type BonusGrantOutcome string

const (
	BonusGranted         BonusGrantOutcome = "granted"
	BonusAlreadyGranted  BonusGrantOutcome = "already_granted"
	BonusDeferredGrace   BonusGrantOutcome = "deferred_grace"
	BonusDeferredBilling BonusGrantOutcome = "deferred_billing"
)

type BonusGrantResult struct {
	Outcome BonusGrantOutcome
	State   *SubscriptionState
}

// ValidateGrantBonusCommand проверяет команду до любых изменений.
func ValidateGrantBonusCommand(cmd *kafkacontracts.GrantBonusCommand) error {
	if cmd == nil {
		return fmt.Errorf("%w: empty command", ErrInvalidBonusCommand)
	}
	if cmd.TelegramID <= 0 {
		return fmt.Errorf("%w: telegram_id must be positive", ErrInvalidBonusCommand)
	}
	if cmd.Days < 1 || cmd.Days > maxBonusDays {
		return fmt.Errorf("%w: days must be 1..%d", ErrInvalidBonusCommand, maxBonusDays)
	}
	if !bonusCampaignRe.MatchString(cmd.Campaign) {
		return fmt.Errorf("%w: campaign must match %s", ErrInvalidBonusCommand, bonusCampaignRe.String())
	}
	if !utf8.ValidString(cmd.Message) || strings.TrimSpace(cmd.Message) == "" {
		return fmt.Errorf("%w: message must be non-empty UTF-8", ErrInvalidBonusCommand)
	}
	if utf8.RuneCountInString(cmd.Message) > maxBonusMessageRunes {
		return fmt.Errorf("%w: message longer than %d characters", ErrInvalidBonusCommand, maxBonusMessageRunes)
	}
	switch cmd.ParseMode {
	case "", "Markdown", "MarkdownV2", "HTML":
	default:
		return fmt.Errorf("%w: unsupported parse_mode", ErrInvalidBonusCommand)
	}
	return nil
}

// renderBonusMessage подставляет в текст кампании фактическую дату окончания
// доступа и срок начисления — из состояния, зафиксированного в той же
// транзакции, а не из расчёта скрипта.
func renderBonusMessage(tmpl string, until time.Time, days int) string {
	return strings.NewReplacer(
		"{until}", until.UTC().Format("02.01.2006"),
		"{days}", pluralDays(days),
	).Replace(tmpl)
}

// GrantBonusDaysTx начисляет бонусные дни кампании campaign. Строка подписки
// блокируется FOR UPDATE (с ленивым истечением), запись в журнал делается до
// продления: конфликт UNIQUE означает, что бонус уже начислен, и подписка не
// меняется.
func (r *Repository) GrantBonusDaysTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string, days int, campaign string) (BonusGrantResult, error) {
	if days < 1 || days > maxBonusDays {
		return BonusGrantResult{}, fmt.Errorf("grant bonus tx: days %d out of range", days)
	}
	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return BonusGrantResult{}, err
	}
	s, err := r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return BonusGrantResult{}, err
	}
	if s.Status == StatusGrace {
		return BonusGrantResult{Outcome: BonusDeferredGrace, State: s}, nil
	}
	live, err := r.billingRenewalLiveTx(ctx, tx, telegramID)
	if err != nil {
		return BonusGrantResult{}, err
	}
	if live {
		return BonusGrantResult{Outcome: BonusDeferredBilling, State: s}, nil
	}

	var until time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO subscription_grants (
			telegram_id, source, business_key, duration_days,
			effective_from, effective_until, granted_at
		)
		SELECT $1::bigint, $4::text, $2::text, $3::int,
		       b.base, b.base + make_interval(days => $3::int), now()
		FROM (
			SELECT GREATEST(COALESCE(expires_at, now()), now()) AS base
			FROM user_subscriptions
			WHERE telegram_id = $1::bigint
		) b
		ON CONFLICT (telegram_id, source, business_key) DO NOTHING
		RETURNING effective_until
	`, telegramID, campaign, days, bonusGrantSource).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return BonusGrantResult{Outcome: BonusAlreadyGranted, State: s}, nil
	}
	if err != nil {
		return BonusGrantResult{}, fmt.Errorf("record bonus grant tx: %w", err)
	}
	until = until.UTC()

	// Платёжный контур не трогаем: план, last_payment_id и автопродление
	// остаются как были. Статус — как у реферальной награды: доступ без
	// оплаты, но не пробный период; идущий триал остаётся триалом.
	if _, err := tx.Exec(ctx, `
		UPDATE user_subscriptions
		SET status = CASE WHEN status = 'trial' AND expires_at > now() THEN status ELSE 'active' END,
			started_at = COALESCE(started_at, now()),
			expires_at = $2,
			grace_until = NULL,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
	`, telegramID, until); err != nil {
		return BonusGrantResult{}, fmt.Errorf("extend subscription for bonus tx: %w", err)
	}
	if _, err := r.ensureTokenTx(ctx, tx, telegramID, &until); err != nil {
		return BonusGrantResult{}, err
	}

	s, err = r.getStateForUpdateTx(ctx, tx, telegramID)
	if err != nil {
		return BonusGrantResult{}, err
	}
	return BonusGrantResult{Outcome: BonusGranted, State: s}, nil
}

// billingRenewalLiveTx сообщает, может ли биллинг ещё изменить подписку
// пользователя: профиль автопродления в повторах, захвачен воркером, в своём
// льготном периоде или с запланированным списанием. Профиль блокируется
// FOR SHARE до конца транзакции — воркеры биллинга (FOR UPDATE SKIP LOCKED)
// его пропустят. Таблица принадлежит billing-service (общая БД, как у
// оркестратора с user_subscriptions); если её нет — биллинг ничего не изменит.
func (r *Repository) billingRenewalLiveTx(ctx context.Context, tx pgx.Tx, telegramID int64) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('billing_recurring_profiles') IS NOT NULL`).Scan(&exists); err != nil {
		return false, fmt.Errorf("check billing profiles table tx: %w", err)
	}
	if !exists {
		return false, nil
	}
	var live bool
	err := tx.QueryRow(ctx, `
		SELECT NOT (status IN ('expired', 'disabled')
		            OR (status = 'active' AND NOT (auto_renew_enabled AND payment_method_id <> ''
		                                           AND next_charge_at IS NOT NULL)))
		FROM billing_recurring_profiles
		WHERE telegram_id = $1
		FOR SHARE
	`, telegramID).Scan(&live)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check billing renewal tx: %w", err)
	}
	return live, nil
}

// HandleGrantBonus выполняет команду subscription.grant_bonus: начисление,
// событие активации для оркестратора и сообщение пользователю — одной
// транзакцией. Повтор и неначисление (бонус уже был, льготный период,
// автопродление в биллинге) сообщения не отправляют.
func (s *Service) HandleGrantBonus(ctx context.Context, cmd *kafkacontracts.GrantBonusCommand) error {
	if err := ValidateGrantBonusCommand(cmd); err != nil {
		return err
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	res, err := s.repo.GrantBonusDaysTx(ctx, tx, cmd.TelegramID, s.defaultCountry, cmd.Days, cmd.Campaign)
	if err != nil {
		return err
	}
	if res.Outcome != BonusGranted {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		commonmetrics.SubscriptionBonusGrantsTotal.WithLabelValues(string(res.Outcome)).Inc()
		slog.Info("bonus grant skipped", "campaign", cmd.Campaign, "telegram_id", cmd.TelegramID, "outcome", string(res.Outcome))
		return nil
	}

	state := res.State
	if state == nil || state.ExpiresAt == nil {
		return fmt.Errorf("bonus grant tg=%d: no expiry after grant", cmd.TelegramID)
	}

	// Событие активации — чтобы оркестратор выдал доступ на нодах под новый
	// срок (как у реферальной награды; без payment_id период CDN-квоты не
	// открывается).
	if err := s.publishSubscriptionEventTx(ctx, tx, &kafkacontracts.SubscriptionActivatedEvent{
		Type:        kafkacontracts.SubscriptionEventActivated,
		TelegramID:  cmd.TelegramID,
		PlanCode:    state.CurrentPlanCode,
		ActivatedAt: time.Now().UTC(),
		ActiveUntil: *state.ExpiresAt,
		DaysLeft:    state.DaysLeft,
		Country:     state.CountryCode,
		Source:      "bonus",
		AccessRev:   state.AccessRev,
	}); err != nil {
		return err
	}
	if err := s.notifyTx(ctx, tx, kafkacontracts.TgNotification{
		TelegramID: cmd.TelegramID,
		ParseMode:  cmd.ParseMode,
		Message:    renderBonusMessage(cmd.Message, *state.ExpiresAt, cmd.Days),
		Keyboard:   kafkacontracts.TgKeyboardMySubscriptionConfig,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	commonmetrics.SubscriptionBonusGrantsTotal.WithLabelValues(string(BonusGranted)).Inc()
	slog.Info("bonus granted",
		"campaign", cmd.Campaign, "telegram_id", cmd.TelegramID, "days", cmd.Days,
		"active_until", state.ExpiresAt.Format(time.RFC3339), "access_rev", state.AccessRev)
	return nil
}
