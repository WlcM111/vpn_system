package vpn_orchestrator

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ============================================================================
// Квота CDN-трафика: 20 GB на пользователя на КАЖДОЙ ноде за период.
//
// Область квоты — пара (пользователь, нода). На каждой ноде, где пользователю
// доступна CDN-конфигурация, у него собственные 20 GB; трафик разных нод не
// смешивается. Несколько CDN-учёток на одной ноде суммируются в одну квоту
// этой пары.
//
// КЛАССИФИКАЦИЯ. Единственный признак «это CDN» — inbound_tag учётной записи,
// пришедший от агента и сверенный с allowlist'ом, который строится из колонки
// vpn_cdn_endpoints.inbound_tag (источник истины на сервере). Не используются:
// отображаемое имя ноды, содержимое ссылки, позиция в списке, подстрока "cdn".
// Пустой или неизвестный inbound_tag НЕ попадает в квоту — по требованию
// «неоднозначный трафик запрещено записывать в CDN-квоту».
//
// ИЗМЕРЕНИЕ. Xray ведёт счётчик по email учётки, а не по инбаунду, поэтому
// разделение возможно только через отдельный email у CDN-профиля (cdnEmail).
// До миграции у CDN и основного профиля был общий email — такой трафик
// физически неразделим, и он остаётся неклассифицированным.
// ============================================================================

// defaultCDNQuotaLimitBytes — 20 GB в десятичном смысле, ровно как в задании.
// Значение вынесено в типизированную политику, а не разбросано по коду.
const defaultCDNQuotaLimitBytes int64 = 20_000_000_000

// cdnEmailSuffix — суффикс локальной части email для CDN-учётки.
// Он же служит признаком «эта учётка заведена под CDN» при чтении состояния.
const cdnEmailSuffix = "-cdn"

// CDNQuotaFailMode — поведение при длительной невозможности измерять CDN-трафик.
//
//	open  — CDN продолжает работать (доступность важнее точного учёта);
//	close — CDN отключается до восстановления телеметрии.
//
// В обоих режимах не-CDN доступ не затрагивается никогда.
type CDNQuotaFailMode string

const (
	CDNQuotaFailOpen  CDNQuotaFailMode = "open"
	CDNQuotaFailClose CDNQuotaFailMode = "close"
)

// CDNQuotaPolicy — конфигурация квоты, валидируемая при старте сервиса.
type CDNQuotaPolicy struct {
	// Enabled — включено ли ограничение. Выключено = только учёт (shadow mode):
	// потребление считается и видно в метриках, но доступ не снимается.
	Enabled bool

	// Enforce — снимать ли CDN-доступ при исчерпании. Разделено с Enabled,
	// чтобы можно было выкатить сбор метрик, свериться с реальностью и только
	// потом включить принуждение (см. план rollout).
	Enforce bool

	LimitBytes int64

	// TelemetryStaleAfter — через сколько отсутствие CDN-отчётов по узлу
	// считается сбоем телеметрии. 0 = проверка выключена.
	TelemetryStaleAfter time.Duration
	FailMode            CDNQuotaFailMode
}

// LoadCDNQuotaPolicyFromEnv собирает политику из окружения и валидирует её.
// Неоднозначные значения дают понятную ошибку запуска, а не тихий дефолт.
func LoadCDNQuotaPolicyFromEnv() (CDNQuotaPolicy, error) {
	p := CDNQuotaPolicy{
		Enabled:             envBoolDefault("CDN_QUOTA_ENABLED", false),
		Enforce:             envBoolDefault("CDN_QUOTA_ENFORCE", false),
		LimitBytes:          defaultCDNQuotaLimitBytes,
		TelemetryStaleAfter: 0,
		FailMode:            CDNQuotaFailOpen,
	}

	if raw := strings.TrimSpace(os.Getenv("CDN_QUOTA_LIMIT_BYTES")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v <= 0 {
			return p, fmt.Errorf("CDN_QUOTA_LIMIT_BYTES must be a positive integer, got %q", raw)
		}
		p.LimitBytes = v
	}

	if raw := strings.TrimSpace(os.Getenv("CDN_QUOTA_TELEMETRY_STALE_AFTER")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return p, fmt.Errorf("CDN_QUOTA_TELEMETRY_STALE_AFTER must be a duration like 45m, got %q", raw)
		}
		p.TelemetryStaleAfter = d
	}

	if raw := strings.ToLower(strings.TrimSpace(os.Getenv("CDN_QUOTA_FAIL_MODE"))); raw != "" {
		switch CDNQuotaFailMode(raw) {
		case CDNQuotaFailOpen, CDNQuotaFailClose:
			p.FailMode = CDNQuotaFailMode(raw)
		default:
			return p, fmt.Errorf("CDN_QUOTA_FAIL_MODE must be open or close, got %q", raw)
		}
	}

	if p.FailMode == CDNQuotaFailClose && p.TelemetryStaleAfter == 0 {
		return p, fmt.Errorf("CDN_QUOTA_FAIL_MODE=close requires CDN_QUOTA_TELEMETRY_STALE_AFTER")
	}
	if p.Enforce && !p.Enabled {
		return p, fmt.Errorf("CDN_QUOTA_ENFORCE=true requires CDN_QUOTA_ENABLED=true")
	}

	return p, nil
}

func envBoolDefault(key string, def bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch raw {
	case "":
		return def
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
}

// cdnEmail строит email CDN-учётки из email основного профиля.
//
// Отдельный email — единственный способ разделить CDN и не-CDN трафик: Xray
// считает байты по учётке (user>>><email>>>>traffic>>>*), а не по инбаунду.
// Суффикс добавляется к локальной части, домен сохраняется:
//
//	tg-1-lt-main-1-ws@vpn-platform.local → tg-1-lt-main-1-ws-cdn@vpn-platform.local
//
// Возвращает "" для пустого или некорректного входа — вызывающий код тогда
// оставляет старое поведение (общий email) вместо выдачи битой учётки.
func cdnEmail(baseEmail string) string {
	baseEmail = strings.TrimSpace(baseEmail)
	at := strings.LastIndex(baseEmail, "@")
	if at <= 0 || at == len(baseEmail)-1 {
		return ""
	}
	local, domain := baseEmail[:at], baseEmail[at+1:]
	if strings.HasSuffix(local, cdnEmailSuffix) {
		return baseEmail
	}
	return local + cdnEmailSuffix + "@" + domain
}

// cdnInboundAllowlist собирает множество inbound-тегов, считающихся CDN.
//
// Источник — серверная конфигурация CDN-эндпоинтов (vpn_cdn_endpoints или
// env-фолбэк). Не имя, не подстрока, не догадка: если тега нет в этом
// множестве, трафик по нему в квоту не идёт.
func cdnInboundAllowlist(endpoints []CDNEndpoint) map[string]struct{} {
	out := make(map[string]struct{}, len(endpoints))
	for _, e := range endpoints {
		tag := strings.TrimSpace(e.InboundTag)
		if tag == "" {
			// Дефолт совпадает с тем, что подставляет buildUserProfiles при
			// пустом теге; иначе классификация разъехалась бы с выдачей.
			tag = "vless-xhttp-cdn-in"
		}
		out[tag] = struct{}{}
	}
	return out
}

// calendarPeriodKey — ключ планового месячного периода в UTC.
// Сброс наступает в 00:00:00 UTC первого числа: с этого момента у всех строк
// ключ отличается от сохранённого, и первый же проход job'а их перебазирует.
func calendarPeriodKey(now time.Time) string {
	return "cal:" + now.UTC().Format("2006-01")
}

// paymentPeriodKey — ключ периода, открытого подтверждённым платежом.
// Один payment_id не может открыть два периода: ключ детерминирован.
func paymentPeriodKey(paymentID string) string {
	paymentID = strings.TrimSpace(paymentID)
	if paymentID == "" {
		return ""
	}
	return "pay:" + paymentID
}
