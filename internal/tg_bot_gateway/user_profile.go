package tg_bot_gateway

import (
	"context"
	"log"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ============================================================================
// Сохранение профиля пользователя Telegram (ник и имя).
//
// Ник приходит в каждом апдейте, но раньше только писался в лог. Без него
// невозможно связаться с человеком или построить отчёт, не опрашивая API
// Telegram по каждому пользователю отдельно.
//
// Запись выполняется только при реальном изменении данных либо не чаще раза
// в час для отметки последней активности — условие WHERE в UPSERT отсекает
// холостые записи, поэтому на каждое сообщение в БД не ходим.
// ============================================================================

// rememberUserProfile сохраняет ник и имя пользователя.
// Ошибки только логирует: это вспомогательные данные, ломать обработку
// сообщения из-за них нельзя.
func (a *App) rememberUserProfile(ctx context.Context, from *tgbotapi.User) {
	if a.pgPool == nil || from == nil || from.ID == 0 {
		return
	}

	username := strings.TrimSpace(from.UserName)
	firstName := strings.TrimSpace(from.FirstName)
	lastName := strings.TrimSpace(from.LastName)

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := a.pgPool.Exec(queryCtx, `
		INSERT INTO telegram_users (telegram_id, username, first_name, last_name, last_seen_at)
		VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), now())
		ON CONFLICT (telegram_id) DO UPDATE SET
			username     = EXCLUDED.username,
			first_name   = EXCLUDED.first_name,
			last_name    = EXCLUDED.last_name,
			last_seen_at = now(),
			updated_at   = now()
		WHERE telegram_users.username    IS DISTINCT FROM EXCLUDED.username
		   OR telegram_users.first_name  IS DISTINCT FROM EXCLUDED.first_name
		   OR telegram_users.last_name   IS DISTINCT FROM EXCLUDED.last_name
		   OR telegram_users.last_seen_at IS NULL
		   OR telegram_users.last_seen_at < now() - interval '1 hour'
	`, from.ID, username, firstName, lastName)
	if err != nil {
		log.Printf("[tg-bot] remember user profile failed id=%d: %v", from.ID, err)
	}
}
