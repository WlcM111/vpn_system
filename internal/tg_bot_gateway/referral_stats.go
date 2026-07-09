package tg_bot_gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
)

// referralStatsView — данные для экрана реферала (читаются ботом напрямую из БД).
type referralStatsView struct {
	Code            string
	InvitedTotal    int
	AvailableMonths int
	UsersPerMonth   int
}

const botReferralCodeAlphabet = "abcdefghjkmnpqrstuvwxyz23456789"
const botReferralCodeLength = 8

// loadReferralStats читает статистику реферала напрямую из БД (у бота есть pgPool,
// как и для broadcast). Формула доступных месяцев дублирует user-subscription;
// авторитетное начисление — там под advisory-lock.
func (a *App) loadReferralStats(ctx context.Context, telegramID int64) (*referralStatsView, error) {
	if a.pgPool == nil {
		return nil, errors.New("referral: db is not configured")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	usersPerMonth := a.referralUsersPerMonth
	if usersPerMonth <= 0 {
		usersPerMonth = 5
	}

	code, err := a.ensureReferralCode(queryCtx, telegramID)
	if err != nil {
		return nil, err
	}

	var converted int
	if err := a.pgPool.QueryRow(queryCtx, `
		SELECT COUNT(*) FROM referrals
		WHERE referrer_telegram_id = $1 AND status = 'converted'
	`, telegramID).Scan(&converted); err != nil {
		return nil, err
	}

	var granted int
	if err := a.pgPool.QueryRow(queryCtx, `
		SELECT COALESCE(SUM(months_granted), 0) FROM referral_reward_grants
		WHERE referrer_telegram_id = $1
	`, telegramID).Scan(&granted); err != nil {
		return nil, err
	}

	available := converted/usersPerMonth - granted
	if available < 0 {
		available = 0
	}

	return &referralStatsView{
		Code:            code,
		InvitedTotal:    converted,
		AvailableMonths: available,
		UsersPerMonth:   usersPerMonth,
	}, nil
}

// ensureReferralCode возвращает код пользователя, создавая идемпотентно. Тот же
// ON CONFLICT (telegram_id), что и в user-subscription: какой код вставится первым,
// тот и «прилипнет» — обе стороны увидят один и тот же.
func (a *App) ensureReferralCode(ctx context.Context, telegramID int64) (string, error) {
	var code string
	err := a.pgPool.QueryRow(ctx,
		`SELECT code FROM referral_codes WHERE telegram_id = $1`, telegramID).Scan(&code)
	if err == nil {
		return code, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}

	for attempt := 0; attempt < 5; attempt++ {
		candidate, genErr := genBotReferralCode()
		if genErr != nil {
			return "", genErr
		}
		var stored string
		insErr := a.pgPool.QueryRow(ctx, `
			INSERT INTO referral_codes (telegram_id, code)
			VALUES ($1, $2)
			ON CONFLICT (telegram_id) DO UPDATE SET code = referral_codes.code
			RETURNING code
		`, telegramID, candidate).Scan(&stored)
		if insErr == nil {
			return stored, nil
		}
	}
	return "", fmt.Errorf("failed to ensure referral code for %d", telegramID)
}

func genBotReferralCode() (string, error) {
	b := make([]byte, botReferralCodeLength)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(botReferralCodeAlphabet))))
		if err != nil {
			return "", err
		}
		b[i] = botReferralCodeAlphabet[n.Int64()]
	}
	return string(b), nil
}
