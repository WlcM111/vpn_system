package user_subscription

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
)

// ============================================================================
// Репозиторий реферальной программы.
//
// Доступные месяцы вычисляются, а не хранятся:
//   available = floor(converted_count / usersPerMonth) - sum(months_granted)
// После выдачи granted растёт, available -> 0 (идемпотентность).
// ============================================================================

const referralCodeAlphabet = "abcdefghjkmnpqrstuvwxyz23456789" // без 0/o/1/l/i
const referralCodeLength = 8

// EnsureReferralCode возвращает стабильный код пользователя, создавая при первом
// обращении. Гонка на создании разрешается через ON CONFLICT.
func (r *Repository) EnsureReferralCode(ctx context.Context, telegramID int64) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var code string
	err := r.pool.QueryRow(queryCtx,
		`SELECT code FROM referral_codes WHERE telegram_id = $1`, telegramID).Scan(&code)
	if err == nil {
		return code, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("select referral code: %w", err)
	}

	for attempt := 0; attempt < 5; attempt++ {
		candidate, genErr := generateReferralCode()
		if genErr != nil {
			return "", genErr
		}
		var stored string
		insErr := r.pool.QueryRow(queryCtx, `
			INSERT INTO referral_codes (telegram_id, code)
			VALUES ($1, $2)
			ON CONFLICT (telegram_id) DO UPDATE SET code = referral_codes.code
			RETURNING code
		`, telegramID, candidate).Scan(&stored)
		if insErr == nil {
			return stored, nil
		}
	}
	return "", errors.New("failed to allocate unique referral code")
}

// AttributeReferral фиксирует переход приглашённого по коду реферера (pending).
// Анти-фрод: код должен быть валиден; referrer != referee; привязка «прилипает».
// Возвращает (referrerTelegramID, attributed, error).
func (r *Repository) AttributeReferral(ctx context.Context, refereeTelegramID int64, referrerCode string) (int64, bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var referrerID int64
	err := r.pool.QueryRow(queryCtx,
		`SELECT telegram_id FROM referral_codes WHERE code = $1`, referrerCode).Scan(&referrerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil // невалидный код — молча игнорируем
	}
	if err != nil {
		return 0, false, fmt.Errorf("lookup referrer by code: %w", err)
	}
	if referrerID == refereeTelegramID {
		return 0, false, nil // самоприглашение
	}

	tag, err := r.pool.Exec(queryCtx, `
		INSERT INTO referrals (referee_telegram_id, referrer_telegram_id, status)
		VALUES ($1, $2, 'pending')
		ON CONFLICT (referee_telegram_id) DO NOTHING
	`, refereeTelegramID, referrerID)
	if err != nil {
		return 0, false, fmt.Errorf("insert referral: %w", err)
	}
	return referrerID, tag.RowsAffected() > 0, nil
}

// MarkReferralConvertedTx помечает приглашённого converted внутри переданной
// транзакции (транзакция активации платной подписки). Идемпотентно.
// Возвращает (referrerTelegramID, converted, error).
func (r *Repository) MarkReferralConvertedTx(ctx context.Context, tx pgx.Tx, refereeTelegramID int64) (int64, bool, error) {
	var referrerID int64
	err := tx.QueryRow(ctx, `
		UPDATE referrals
		SET status = 'converted', converted_at = now()
		WHERE referee_telegram_id = $1 AND status = 'pending'
		RETURNING referrer_telegram_id
	`, refereeTelegramID).Scan(&referrerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("mark referral converted: %w", err)
	}
	return referrerID, true, nil
}

// CountConvertedReferrals возвращает число приглашённых с платной подпиской.
func (r *Repository) CountConvertedReferrals(ctx context.Context, referrerID int64) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var n int
	err := r.pool.QueryRow(queryCtx, `
		SELECT COUNT(*) FROM referrals
		WHERE referrer_telegram_id = $1 AND status = 'converted'
	`, referrerID).Scan(&n)
	return n, err
}

// SumGrantedMonths возвращает сумму уже начисленных бесплатных месяцев.
func (r *Repository) SumGrantedMonths(ctx context.Context, referrerID int64) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var n int
	err := r.pool.QueryRow(queryCtx, `
		SELECT COALESCE(SUM(months_granted), 0) FROM referral_reward_grants
		WHERE referrer_telegram_id = $1
	`, referrerID).Scan(&n)
	return n, err
}

// RedeemReferralMonthsTx начисляет доступные месяцы: продлевает подписку и пишет
// reward_grant в одной транзакции, под advisory-lock (защита от двойного нажатия).
// Возвращает (grantedMonths, newExpiresAt). grantedMonths=0 — нечего начислять.
func (r *Repository) RedeemReferralMonthsTx(ctx context.Context, tx pgx.Tx, telegramID int64, country string, usersPerMonth, daysPerMonth int) (int, *time.Time, error) {
	if usersPerMonth <= 0 {
		usersPerMonth = 1
	}
	if daysPerMonth <= 0 {
		daysPerMonth = 30
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, referralRedeemLockKey(telegramID)); err != nil {
		return 0, nil, fmt.Errorf("advisory lock: %w", err)
	}

	var converted int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM referrals
		WHERE referrer_telegram_id = $1 AND status = 'converted'
	`, telegramID).Scan(&converted); err != nil {
		return 0, nil, fmt.Errorf("count converted: %w", err)
	}

	var granted int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(months_granted), 0) FROM referral_reward_grants
		WHERE referrer_telegram_id = $1
	`, telegramID).Scan(&granted); err != nil {
		return 0, nil, fmt.Errorf("sum granted: %w", err)
	}

	available := converted/usersPerMonth - granted
	if available <= 0 {
		return 0, nil, nil
	}

	if err := r.ensureUserAndStateTx(ctx, tx, telegramID, country); err != nil {
		return 0, nil, err
	}

	addDays := available * daysPerMonth
	var newExpiresAt time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE user_subscriptions
		SET expires_at = GREATEST(COALESCE(expires_at, now()), now()) + make_interval(days => $2),
			status = CASE WHEN status IN ('expired', 'none') THEN 'active' ELSE status END,
			access_rev = access_rev + 1,
			updated_at = now()
		WHERE telegram_id = $1
		RETURNING expires_at
	`, telegramID, addDays).Scan(&newExpiresAt); err != nil {
		return 0, nil, fmt.Errorf("extend subscription: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO referral_reward_grants (referrer_telegram_id, months_granted)
		VALUES ($1, $2)
	`, telegramID, available); err != nil {
		return 0, nil, fmt.Errorf("insert reward grant: %w", err)
	}

	return available, &newExpiresAt, nil
}

func generateReferralCode() (string, error) {
	b := make([]byte, referralCodeLength)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(referralCodeAlphabet))))
		if err != nil {
			return "", err
		}
		b[i] = referralCodeAlphabet[n.Int64()]
	}
	return string(b), nil
}

// referralRedeemLockKey — детерминированный ключ advisory-lock для сериализации
// начислений одного пользователя.
func referralRedeemLockKey(telegramID int64) int64 {
	return 0x5245460000000000 | (telegramID & 0x00000000FFFFFFFF)
}
