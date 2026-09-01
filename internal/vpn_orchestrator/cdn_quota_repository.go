package vpn_orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ============================================================================
// Хранилище CDN-квот.
//
// Все изменения — одним атомарным SQL-оператором. Read-modify-write в Go тут
// запрещён: отчёты приходят из нескольких партиций Kafka и обрабатываются
// параллельными экземплярами оркестратора, поэтому «прочитал, посчитал,
// записал» терял бы обновления.
// ============================================================================

// CDNQuotaState — состояние квоты одной пары (пользователь, нода).
type CDNQuotaState struct {
	TelegramID    int64
	NodeID        string
	PeriodKey     string
	UsedBytes     int64
	LimitBytes    int64
	State         string
	Revision      int64
	LastReportAt  *time.Time
	JustExhausted bool // перешла в exhausted именно этим вызовом
	// CounterRebased — узел прислал показание НИЖЕ сохранённой базы: рестарт
	// Xray с потерей состояния агента или переустановка агента. Строка
	// перебазирована, уже начисленное потребление сохранено.
	CounterRebased bool
}

const cdnQuotaStateActive = "active"
const cdnQuotaStateExhausted = "exhausted"

// ApplyObservationTx применяет новое наблюдение кумулятивных CDN-байтов пары
// (telegram_id, node_id) и возвращает актуальное состояние квоты.
//
// observed — сумма МОНОТОННЫХ кумулятивных счётчиков всех CDN-учёток
// пользователя на этом узле в рамках одного отчёта агента.
//
// Обрабатываемые аномалии:
//
//	повтор/переупорядочивание — observed не убывает (GREATEST), повторный
//	  отчёт не добавляет байтов;
//	сброс счётчика Xray или переустановка агента — observed приходит меньше
//	  сохранённого baseline; тогда перебазируемся на новое значение, сохранив
//	  уже начисленное потребление, вместо ухода used_bytes в минус;
//	пропуск отчётов — учёт кумулятивный, пропущенные интервалы «догоняются»
//	  следующим же отчётом.
//
// Переход в exhausted происходит здесь же, одним оператором: гонка «исчерпание
// одновременно со сбросом» разрешается детерминированно порядком строк в CTE.
func (r *Repository) ApplyObservationTx(
	ctx context.Context,
	tx pgx.Tx,
	telegramID int64,
	nodeID string,
	observed int64,
	observedUplink int64,
	limitBytes int64,
	periodKey string,
	at time.Time,
) (*CDNQuotaState, error) {
	if telegramID == 0 || nodeID == "" {
		return nil, fmt.Errorf("apply cdn observation: empty telegram_id or node_id")
	}
	if observed < 0 {
		return nil, fmt.Errorf("apply cdn observation: negative observed %d", observed)
	}
	if observedUplink < 0 {
		return nil, fmt.Errorf("apply cdn observation: negative observed uplink %d", observedUplink)
	}
	if observedUplink > observed {
		return nil, fmt.Errorf("apply cdn observation: observed uplink %d exceeds total %d", observedUplink, observed)
	}
	if limitBytes <= 0 {
		return nil, fmt.Errorf("apply cdn observation: non-positive limit %d", limitBytes)
	}

	var (
		st         CDNQuotaState
		prevState  string
		lastReport *time.Time
		rebased    bool
	)

	// INSERT ... ON CONFLICT DO UPDATE выполняется как единая атомарная операция
	// со строчной блокировкой, поэтому дополнительный SELECT ... FOR UPDATE не
	// нужен и только удлинял бы транзакцию.
	//
	// Первая вставка задаёт baseline = observed: существующим пользователям не
	// начисляется выдуманный исторический трафик, достоверный учёт начинается с
	// момента появления строки (period_started_at).
	// prev читается в том же снимке, что и апсерт: это единственный способ
	// узнать, откатился ли счётчик узла, — RETURNING отдаёт только новые
	// значения строки. Без этого сброс счётчика Xray был бы невидим в метриках.
	err := tx.QueryRow(ctx, `
		WITH prev AS (
			SELECT baseline_bytes AS prev_baseline
			FROM vpn_user_cdn_quota
			WHERE telegram_id = $1 AND node_id = $2
		),
		ups AS (
		INSERT INTO vpn_user_cdn_quota (
			telegram_id, node_id, period_key, period_started_at,
			baseline_bytes, observed_bytes,
			baseline_uplink_bytes, observed_uplink_bytes, limit_bytes,
			state, revision, last_report_at, created_at, updated_at
		)
		VALUES ($1, $2, $5, $6, $3, $3, $7, $7, $4, 'active', 0, $6, now(), now())
		ON CONFLICT (telegram_id, node_id) DO UPDATE SET
			-- Откат счётчика ниже базы = рестарт Xray или переустановка агента.
			-- Перебазируемся так, чтобы уже начисленное потребление сохранилось.
			baseline_bytes = CASE
				WHEN EXCLUDED.observed_bytes < vpn_user_cdn_quota.baseline_bytes
					THEN GREATEST($3 - (vpn_user_cdn_quota.observed_bytes - vpn_user_cdn_quota.baseline_bytes), 0)
				ELSE vpn_user_cdn_quota.baseline_bytes
			END,
			observed_bytes = CASE
				WHEN EXCLUDED.observed_bytes < vpn_user_cdn_quota.baseline_bytes
					THEN $3
				ELSE GREATEST(vpn_user_cdn_quota.observed_bytes, EXCLUDED.observed_bytes)
			END,
			-- Восходящая часть перебазируется по ТОМУ ЖЕ признаку отката, что и
			-- сумма: обе величины приходят одним отчётом, и расщеплять их по
			-- разным условиям значило бы получить отрицательный download.
			baseline_uplink_bytes = CASE
				WHEN EXCLUDED.observed_bytes < vpn_user_cdn_quota.baseline_bytes
					THEN GREATEST($7 - (vpn_user_cdn_quota.observed_uplink_bytes - vpn_user_cdn_quota.baseline_uplink_bytes), 0)
				ELSE vpn_user_cdn_quota.baseline_uplink_bytes
			END,
			observed_uplink_bytes = CASE
				WHEN EXCLUDED.observed_bytes < vpn_user_cdn_quota.baseline_bytes
					THEN $7
				ELSE GREATEST(vpn_user_cdn_quota.observed_uplink_bytes, EXCLUDED.observed_uplink_bytes)
			END,
			limit_bytes = EXCLUDED.limit_bytes,
			last_report_at = $6,
			updated_at = now()
		RETURNING
			period_key,
			GREATEST(observed_bytes - baseline_bytes, 0) AS used_bytes,
			limit_bytes,
			state,
			revision,
			last_report_at
		)
		SELECT ups.period_key, ups.used_bytes, ups.limit_bytes, ups.state,
		       ups.revision, ups.last_report_at,
		       COALESCE($3::bigint < prev.prev_baseline, false) AS rebased
		FROM ups LEFT JOIN prev ON true
	`, telegramID, nodeID, observed, limitBytes, periodKey, at.UTC(), observedUplink).
		Scan(&st.PeriodKey, &st.UsedBytes, &st.LimitBytes, &prevState, &st.Revision, &lastReport, &rebased)
	if err != nil {
		return nil, fmt.Errorf("apply cdn observation tg=%d node=%s: %w", telegramID, nodeID, err)
	}

	st.TelegramID = telegramID
	st.NodeID = nodeID
	st.State = prevState
	st.LastReportAt = lastReport
	st.CounterRebased = rebased

	// Переход active → exhausted выделен отдельным условным UPDATE, чтобы
	// состояние менялось ровно один раз: повторный отчёт после исчерпания
	// не трогает ни revision, ни exhausted_at.
	if prevState == cdnQuotaStateActive && st.UsedBytes >= st.LimitBytes {
		var newRev int64
		err := tx.QueryRow(ctx, `
			UPDATE vpn_user_cdn_quota
			SET state = 'exhausted',
			    exhausted_at = now(),
			    exhausted_reason = 'quota_exceeded',
			    revision = revision + 1,
			    updated_at = now()
			WHERE telegram_id = $1
			  AND node_id = $2
			  AND state = 'active'
			RETURNING revision
		`, telegramID, nodeID).Scan(&newRev)
		switch {
		case err == nil:
			st.State = cdnQuotaStateExhausted
			st.Revision = newRev
			st.JustExhausted = true
		case errors.Is(err, pgx.ErrNoRows):
			// Другой экземпляр успел перевести строку — состояние уже верное.
			st.State = cdnQuotaStateExhausted
		default:
			return nil, fmt.Errorf("mark cdn quota exhausted tg=%d node=%s: %w", telegramID, nodeID, err)
		}
	}

	return &st, nil
}

// ResetQuotaTx открывает новый период для пары (пользователь, нода).
//
// Идемпотентность обеспечена сравнением period_key: если ключ совпадает с
// текущим, строка не меняется и возвращается reset=false. Поэтому повтор
// события об оплате, replay старого подтверждения и повторный прогон месячного
// job'а дают ровно один период.
//
// baseline переносится на текущий observed: потребление нового периода
// начинается с нуля, а история кумулятива не теряется.
func (r *Repository) ResetQuotaTx(
	ctx context.Context,
	tx pgx.Tx,
	telegramID int64,
	nodeID string,
	periodKey string,
	limitBytes int64,
	at time.Time,
) (bool, error) {
	if periodKey == "" {
		return false, fmt.Errorf("reset cdn quota: empty period key")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE vpn_user_cdn_quota
		SET period_key = $3,
		    period_started_at = $5,
		    baseline_bytes = observed_bytes,
		    limit_bytes = $4,
		    state = 'active',
		    exhausted_at = NULL,
		    exhausted_reason = '',
		    revision = revision + 1,
		    updated_at = now()
		WHERE telegram_id = $1
		  AND node_id = $2
		  AND period_key <> $3
	`, telegramID, nodeID, periodKey, limitBytes, at.UTC())
	if err != nil {
		return false, fmt.Errorf("reset cdn quota tg=%d node=%s: %w", telegramID, nodeID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// ResetQuotaForUserTx сбрасывает квоты пользователя на ВСЕХ его узлах и
// возвращает список узлов, где сброс реально произошёл. Используется при
// подтверждённой оплате: продление обязано вернуть CDN на всех актуальных нодах
// ровно один раз.
func (r *Repository) ResetQuotaForUserTx(
	ctx context.Context,
	tx pgx.Tx,
	telegramID int64,
	periodKey string,
	limitBytes int64,
	at time.Time,
) ([]string, error) {
	if periodKey == "" {
		return nil, fmt.Errorf("reset cdn quota for user: empty period key")
	}
	rows, err := tx.Query(ctx, `
		UPDATE vpn_user_cdn_quota
		SET period_key = $2,
		    period_started_at = $4,
		    baseline_bytes = observed_bytes,
		    limit_bytes = $3,
		    state = 'active',
		    exhausted_at = NULL,
		    exhausted_reason = '',
		    revision = revision + 1,
		    updated_at = now()
		WHERE telegram_id = $1
		  AND period_key <> $2
		RETURNING node_id
	`, telegramID, periodKey, limitBytes, at.UTC())
	if err != nil {
		return nil, fmt.Errorf("reset cdn quota for user tg=%d: %w", telegramID, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("scan reset node: %w", err)
		}
		out = append(out, nodeID)
	}
	return out, rows.Err()
}

// ExhaustedNodesForUser возвращает узлы, на которых CDN-квота пользователя
// исчерпана. Пустой результат — обычное состояние, не ошибка.
//
// Читается на пути выдачи конфигураций: исчерпанная квота не должна
// восстанавливаться реконсайлом, поэтому CDN-профиль в desired state просто
// не попадает.
func (r *Repository) ExhaustedNodesForUser(ctx context.Context, telegramID int64) (map[string]struct{}, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT node_id
		FROM vpn_user_cdn_quota
		WHERE telegram_id = $1 AND state = 'exhausted'
	`, telegramID)
	if err != nil {
		return nil, fmt.Errorf("list exhausted cdn nodes tg=%d: %w", telegramID, err)
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("scan exhausted node: %w", err)
		}
		out[nodeID] = struct{}{}
	}
	return out, rows.Err()
}

// SumCDNUsageForUser возвращает расход CDN-трафика пользователя за текущий
// период, просуммированный по всем узлам, раздельно по направлениям.
//
// downlink выводится вычитанием: обе величины — high-water mark одного отчёта,
// перебазируются одновременно, поэтому разность неотрицательна по построению.
// GREATEST оставлен как страховка для строк, попавших в таблицу до миграции 0018:
// у них baseline_uplink_bytes и observed_uplink_bytes равны нулю.
//
// Ошибка чтения не критична для выдачи подписки: вызывающий код логирует её и
// отдаёт нули, потому что фид важнее заголовка.
func (r *Repository) SumCDNUsageForUser(ctx context.Context, telegramID int64) (uplink, downlink int64, err error) {
	if telegramID == 0 {
		return 0, 0, nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err = r.pool.QueryRow(queryCtx, `
		SELECT
			COALESCE(SUM(GREATEST(observed_uplink_bytes - baseline_uplink_bytes, 0)), 0),
			COALESCE(SUM(GREATEST(
				(observed_bytes - baseline_bytes)
				- (observed_uplink_bytes - baseline_uplink_bytes), 0)), 0)
		FROM vpn_user_cdn_quota
		WHERE telegram_id = $1
	`, telegramID).Scan(&uplink, &downlink)
	if err != nil {
		return 0, 0, fmt.Errorf("sum cdn usage tg=%d: %w", telegramID, err)
	}
	return uplink, downlink, nil
}

// CDNQuotaSummary — агрегаты для метрик. Кардинальность намеренно ограничена
// узлом: метки по telegram_id в Prometheus запрещены.
type CDNQuotaSummary struct {
	NodeID         string
	Rows           int
	ExhaustedRows  int
	UsedBytesTotal int64
	StaleRows      int
}

// SummarizeQuotas отдаёт срез по узлам для экспорта в Prometheus.
// staleAfter <= 0 отключает подсчёт «протухшей» телеметрии.
func (r *Repository) SummarizeQuotas(ctx context.Context, staleAfter time.Duration) ([]CDNQuotaSummary, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	stale := "interval '0'"
	if staleAfter > 0 {
		stale = "$1::interval"
	}

	rows, err := r.pool.Query(queryCtx, `
		SELECT node_id,
		       count(*),
		       count(*) FILTER (WHERE state = 'exhausted'),
		       COALESCE(SUM(GREATEST(observed_bytes - baseline_bytes, 0)), 0),
		       count(*) FILTER (
		           WHERE `+stale+` > interval '0'
		             AND (last_report_at IS NULL OR last_report_at < now() - `+stale+`)
		       )
		FROM vpn_user_cdn_quota
		GROUP BY node_id
	`, staleAfter.String())
	if err != nil {
		return nil, fmt.Errorf("summarize cdn quotas: %w", err)
	}
	defer rows.Close()

	var out []CDNQuotaSummary
	for rows.Next() {
		var s CDNQuotaSummary
		if err := rows.Scan(&s.NodeID, &s.Rows, &s.ExhaustedRows, &s.UsedBytesTotal, &s.StaleRows); err != nil {
			return nil, fmt.Errorf("scan cdn quota summary: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ListQuotasForCalendarReset отдаёт пары, у которых период отличается от
// целевого календарного, порционно (keyset-пагинация по (telegram_id, node_id)).
func (r *Repository) ListQuotasForCalendarReset(
	ctx context.Context,
	periodKey string,
	afterTelegramID int64,
	afterNodeID string,
	limit int,
) ([]CDNQuotaState, error) {
	if limit <= 0 {
		limit = 500
	}
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	rows, err := r.pool.Query(queryCtx, `
		SELECT telegram_id, node_id, period_key, state
		FROM vpn_user_cdn_quota
		WHERE period_key <> $1
		  AND (telegram_id, node_id) > ($2, $3)
		ORDER BY telegram_id, node_id
		LIMIT $4
	`, periodKey, afterTelegramID, afterNodeID, limit)
	if err != nil {
		return nil, fmt.Errorf("list quotas for calendar reset: %w", err)
	}
	defer rows.Close()

	var out []CDNQuotaState
	for rows.Next() {
		var s CDNQuotaState
		if err := rows.Scan(&s.TelegramID, &s.NodeID, &s.PeriodKey, &s.State); err != nil {
			return nil, fmt.Errorf("scan quota row: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
