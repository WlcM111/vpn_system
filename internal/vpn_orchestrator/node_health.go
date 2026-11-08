package vpn_orchestrator

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// NodeHealthState описывает состояние узла с разделением «заблокировано» и «упало».
type NodeHealthState string

const (
	// NodeStateHealthy — и Kafka-heartbeat свежий, и VPN-порт отвечает по TCP.
	NodeStateHealthy NodeHealthState = "healthy"
	// NodeStateBlockedDataplane — Kafka-heartbeat свежий (нода жива), но прямое
	// TCP-соединение из центра в РФ до VPN-порта ноды блокируется ТСПУ.
	NodeStateBlockedDataplane NodeHealthState = "blocked_dataplane"
	// NodeStateBlockedControlplane — Kafka-heartbeat устарел (канал нода→центр
	// заблокирован), но VPN-порт ноды отвечает по TCP (нода жива и обслуживает клиентов).
	NodeStateBlockedControlplane NodeHealthState = "blocked_controlplane"
	// NodeStateDown — ни Kafka-heartbeat, ни TCP-проверка VPN-портов не проходят.
	NodeStateDown NodeHealthState = "down"
)

var allNodeHealthStates = []NodeHealthState{
	NodeStateHealthy,
	NodeStateBlockedDataplane,
	NodeStateBlockedControlplane,
	NodeStateDown,
}

// ClassifyNodeHealth отличает «заблокировано» (blocked) от «упало» (down):
//   - если есть свежий heartbeat ИЛИ успешная TCP-проба — нода считается ЖИВОЙ (up=true);
//   - если один из каналов работает, а второй недоступен — выставляется blocked=true;
//   - только когда мертвы оба канала — нода считается упавшей (up=false, state=down).
func ClassifyNodeHealth(heartbeatAlive, probeChecked, probeAlive bool) (state NodeHealthState, up, blocked bool) {
	if heartbeatAlive {
		if probeChecked && !probeAlive {
			return NodeStateBlockedDataplane, true, true
		}
		return NodeStateHealthy, true, false
	}
	if probeChecked && probeAlive {
		return NodeStateBlockedControlplane, true, true
	}
	return NodeStateDown, false, false
}

type nodeProbeEntry struct {
	alive     bool
	checkedAt time.Time
}

type nodeProbeStore struct {
	mu      sync.RWMutex
	entries map[string]nodeProbeEntry
}

var globalNodeProbeStore = &nodeProbeStore{
	entries: make(map[string]nodeProbeEntry),
}

// RecordNodeProbe сохраняет результат TCP-проверки VPN-порта ноды.
func RecordNodeProbe(serverKey string, alive bool, at time.Time) {
	serverKey = strings.TrimSpace(serverKey)
	if serverKey == "" {
		return
	}
	globalNodeProbeStore.mu.Lock()
	globalNodeProbeStore.entries[serverKey] = nodeProbeEntry{alive: alive, checkedAt: at}
	globalNodeProbeStore.mu.Unlock()
}

// GetNodeProbe возвращает (checked, alive) для server_key, если замер не старше maxAge.
func GetNodeProbe(serverKey string, now time.Time, maxAge time.Duration) (checked, alive bool) {
	serverKey = strings.TrimSpace(serverKey)
	if serverKey == "" {
		return false, false
	}
	if maxAge <= 0 {
		maxAge = 3 * time.Minute
	}
	globalNodeProbeStore.mu.RLock()
	entry, ok := globalNodeProbeStore.entries[serverKey]
	globalNodeProbeStore.mu.RUnlock()
	if !ok || entry.checkedAt.IsZero() || now.Sub(entry.checkedAt) > maxAge {
		return false, false
	}
	return true, entry.alive
}

func isReservedTestHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	lower := strings.ToLower(strings.TrimSpace(host))
	return strings.HasSuffix(lower, ".invalid") || strings.HasSuffix(lower, ".example") || strings.HasSuffix(lower, ".example.com")
}

// ProbeTCPTargets проверяет, открывается ли TCP-соединение хотя бы к одному
// из целевых адресов ("host:port") за отведённый timeout.
//
// Адреса проверяются параллельно: на заблокированном узле каждая попытка
// упирается в таймаут, и последовательный перебор занимал бы N×timeout.
// Отмена родительского контекста (остановка, общий дедлайн сборщика) не
// считается результатом проверки: возвращается checked=false, чтобы не
// записать узлу ложное «недоступен».
func ProbeTCPTargets(ctx context.Context, addrs []string, timeout time.Duration) (checked, alive bool) {
	var valid []string
	seen := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" || isReservedTestHost(a) {
			continue
		}
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		valid = append(valid, a)
	}
	if len(valid) == 0 {
		return false, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel() // прерывает оставшиеся попытки после первой удачной

	// Буфер на все попытки: горутины не блокируются, даже когда результат
	// уже не читают.
	results := make(chan bool, len(valid))
	var dialer net.Dialer
	for _, addr := range valid {
		go func(addr string) {
			conn, err := dialer.DialContext(probeCtx, "tcp", addr)
			if err != nil {
				results <- false
				return
			}
			_ = conn.Close()
			results <- true
		}(addr)
	}
	for range valid {
		if <-results {
			return true, true
		}
	}
	if ctx.Err() != nil {
		return false, false
	}
	return true, false
}

func formatHostPort(host string, port int, defaultPort int) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if port <= 0 {
		port = defaultPort
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// PreserveCountriesOnMassDrop защищает подписку от массового выпадения рабочих нод,
// когда из-за блокировки ТСПУ на стыке с центром в РФ устаревает heartbeat всех нод
// страны. Если в стране есть включённые администратором пул-айтемы, но аллокатор не
// выбрал ни одного, сохраняется один резервный пул-айтем страны (sticky-нода
// пользователя либо первая по sort_order).
func PreserveCountriesOnMassDrop(candidates []PoolItem, allocated []PoolItem, sticky stickyResolver) []PoolItem {
	if len(candidates) == 0 {
		return allocated
	}

	allocatedCountries := make(map[string]bool, len(allocated))
	for _, it := range allocated {
		allocatedCountries[it.CountryCode] = true
	}

	byCountry := make(map[string][]PoolItem)
	var countryOrder []string
	seenCountry := make(map[string]bool)
	for _, it := range candidates {
		if !it.Enabled {
			continue
		}
		if !seenCountry[it.CountryCode] {
			seenCountry[it.CountryCode] = true
			countryOrder = append(countryOrder, it.CountryCode)
		}
		byCountry[it.CountryCode] = append(byCountry[it.CountryCode], it)
	}

	out := make([]PoolItem, 0, len(countryOrder))
	out = append(out, allocated...)

	for _, country := range countryOrder {
		if allocatedCountries[country] {
			continue
		}
		list := byCountry[country]
		if len(list) == 0 {
			continue
		}
		var pick *PoolItem
		if sticky != nil {
			for i := range list {
				bound := sticky(list[i].ItemKey)
				if bound != "" && bound == list[i].ServerKey {
					pick = &list[i]
					break
				}
			}
		}
		if pick == nil {
			pick = &list[0]
		}
		out = append(out, *pick)
	}

	return (&Allocator{}).SortPoolItems(out)
}

// FilterPoolItemsForTrial оставляет для пробной подписки (trial) только один
// целевой сервер/локацию, заданную в настройках (по умолчанию Румыния: "RO" / "ro-main-1").
func FilterPoolItemsForTrial(items []PoolItem, trialTarget string) []PoolItem {
	target := strings.TrimSpace(trialTarget)
	if target == "" {
		target = "RO"
	}
	targetLower := strings.ToLower(target)
	targetUpper := strings.ToUpper(target)

	var enabled []PoolItem
	for _, it := range items {
		if it.Enabled {
			enabled = append(enabled, it)
		}
	}
	if len(enabled) == 0 {
		return nil
	}

	// 1. Точное совпадение по server_key, node_id или item_key (например, "ro-main-1" или "ro-main").
	var exact []PoolItem
	for _, it := range enabled {
		if strings.EqualFold(it.ServerKey, target) ||
			strings.EqualFold(it.NodeID, target) ||
			strings.EqualFold(it.ItemKey, target) {
			exact = append(exact, it)
		}
	}
	if len(exact) > 0 {
		return exact
	}

	// 2. Совпадение по коду страны (например, "RO", либо префикс "RO-" в "ro-main-1").
	countryTarget := targetUpper
	if idx := strings.IndexByte(countryTarget, '-'); idx == 2 {
		countryTarget = countryTarget[:2]
	}
	var byCountry []PoolItem
	for _, it := range enabled {
		if strings.EqualFold(it.CountryCode, targetUpper) ||
			strings.EqualFold(it.CountryCode, countryTarget) {
			byCountry = append(byCountry, it)
		}
	}
	if len(byCountry) > 0 {
		return byCountry
	}

	// 3. Совпадение по названию (например, "Румыния") — только если по коду
	// страны ничего не нашлось и цель длиннее кода страны: двухбуквенная
	// подстрока совпала бы с чужими названиями ("ro" есть в «Europe»,
	// «Toronto», «Rotterdam»), и пробная подписка ушла бы не на тот сервер.
	if utf8.RuneCountInString(target) > 2 {
		var byTitle []PoolItem
		for _, it := range enabled {
			if strings.Contains(strings.ToLower(it.Title), targetLower) {
				byTitle = append(byTitle, it)
			}
		}
		if len(byTitle) > 0 {
			return byTitle
		}
	}

	// 4. Безопасный фолбэк: если целевая страна ещё не заведена в БД (например, в E2E-тесте
	// с единственной тестовой нодой), отдаём первый включённый пул-айтем, но строго один.
	sorted := (&Allocator{}).SortPoolItems(enabled)
	return sorted[:1]
}
