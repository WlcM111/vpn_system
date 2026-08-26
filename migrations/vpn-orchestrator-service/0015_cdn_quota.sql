-- Квота CDN-трафика: 20 GB на пользователя на КАЖДОЙ ноде за период.
--
-- Почему отдельная таблица, а не колонка в vpn_user_traffic_node.
--   vpn_user_traffic_node хранит ОБЩИЙ трафик пользователя на узле и служит
--   только для отображения в клиенте (Subscription-Userinfo). Квота — это
--   контур принятия решений о доступе: у неё своя единица (только CDN-байты),
--   свой жизненный цикл (период), своё состояние (active/exhausted) и свои
--   требования к идемпотентности. Смешивать их — значит завязать выдачу
--   доступа на витрину.
--
-- Модель периода. Одна строка на пару (telegram_id, node_id); период задаётся
-- строковым ключом period_key:
--   'cal:YYYY-MM'   — плановый календарный сброс (00:00:00 UTC первого числа);
--   'pay:<payment>' — сброс по подтверждённой оплате/продлению;
--   'adm:<uuid>'    — административный сброс.
-- Сброс выполняется ТОЛЬКО при смене period_key, поэтому повтор события с тем
-- же payment_id или повторный прогон месячного job'а ничего не меняют —
-- идемпотентность обеспечивается сравнением ключа, а не флагом в приложении.
--
-- Модель учёта. node-agent присылает МОНОТОННЫЙ кумулятивный счётчик учётки
-- (он сам переживает рестарт Xray, см. Agent.accumulateTraffic). Поэтому:
--   observed_bytes — high-water mark кумулятива CDN-учёток на узле;
--   baseline_bytes — его значение на момент начала периода;
--   used_bytes     — разность, вычисляемая, а не накапливаемая сложением.
-- Такая модель устойчива к повторной доставке, переупорядочиванию и пропуску
-- отчётов: повторный отчёт не увеличивает потребление, а устаревший
-- отбрасывается GREATEST.
--
-- Идемпотентно: повторный прогон безопасен.

CREATE TABLE IF NOT EXISTS vpn_user_cdn_quota (
    telegram_id       BIGINT      NOT NULL,
    node_id           TEXT        NOT NULL,

    period_key        TEXT        NOT NULL,
    period_started_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Кумулятивные CDN-байты (uplink+downlink) по всем CDN-учёткам этой пары.
    baseline_bytes    BIGINT      NOT NULL DEFAULT 0,
    observed_bytes    BIGINT      NOT NULL DEFAULT 0,

    limit_bytes       BIGINT      NOT NULL,
    state             TEXT        NOT NULL DEFAULT 'active',
    exhausted_at      TIMESTAMPTZ NULL,
    exhausted_reason  TEXT        NOT NULL DEFAULT '',

    -- Ревизия строки: растёт при каждом сбросе и при смене состояния.
    -- Нужна для наблюдаемости и для отладки гонок.
    revision          BIGINT      NOT NULL DEFAULT 0,

    last_report_at    TIMESTAMPTZ NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (telegram_id, node_id),

    CONSTRAINT chk_vpn_cdn_quota_state
        CHECK (state IN ('active', 'exhausted')),
    CONSTRAINT chk_vpn_cdn_quota_nonneg
        CHECK (baseline_bytes >= 0 AND observed_bytes >= 0 AND limit_bytes > 0),
    -- observed не может быть меньше baseline: это ломало бы used_bytes.
    -- Сброс счётчика Xray/переустановка агента обрабатывается перебазированием
    -- (см. CDNQuotaRepository.ApplyObservation), а не отрицательным остатком.
    CONSTRAINT chk_vpn_cdn_quota_observed_ge_baseline
        CHECK (observed_bytes >= baseline_bytes)
);

-- Выборка исчерпанных квот (для метрик и для пропуска CDN при выдаче).
CREATE INDEX IF NOT EXISTS idx_vpn_user_cdn_quota_exhausted
    ON vpn_user_cdn_quota (telegram_id)
    WHERE state = 'exhausted';

-- Обход всех строк месячным job'ом сброса.
CREATE INDEX IF NOT EXISTS idx_vpn_user_cdn_quota_period
    ON vpn_user_cdn_quota (period_key);

-- Свежесть телеметрии: находим узлы, по которым давно не было CDN-отчётов.
CREATE INDEX IF NOT EXISTS idx_vpn_user_cdn_quota_last_report
    ON vpn_user_cdn_quota (node_id, last_report_at);

COMMENT ON TABLE vpn_user_cdn_quota IS
    'Per-user per-node CDN traffic quota; used_bytes = observed_bytes - baseline_bytes';
COMMENT ON COLUMN vpn_user_cdn_quota.period_key IS
    'cal:YYYY-MM | pay:<payment_id> | adm:<uuid>; reset happens only when it changes';