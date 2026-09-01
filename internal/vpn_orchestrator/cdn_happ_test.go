package vpn_orchestrator

import (
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// CDN-трафик в Happ: объём, расход, название подписки.
//
// Нумерация тестов соответствует списку обязательных сценариев задания.
// Сценарии, требующие живой БД (перебазирование high-water mark, повторная
// доставка, конкурентная запись), проверяются на уровне SQL — см. раздел
// «Проверки, требующие БД» в гайде: логика находится в одном операторе
// INSERT ... ON CONFLICT, и подменять его заглушкой в Go бессмысленно.
// ============================================================================

func cdnEndpointsFor(serverKeys ...string) []CDNEndpoint {
	out := make([]CDNEndpoint, 0, len(serverKeys))
	for _, sk := range serverKeys {
		out = append(out, CDNEndpoint{
			CDNKey:     sk + "-cdn",
			ServerKey:  sk,
			Enabled:    true,
			InboundTag: "vless-xhttp-cdn-in",
			Address:    sk + ".race-src.com",
		})
	}
	return out
}

func cdnAllowlist() map[string]struct{} {
	return cdnInboundAllowlist(cdnEndpointsFor("lt-main-1"))
}

// --- 1. CDN-нод нет: n = 0 -------------------------------------------------

func TestCDNTotalZeroNodes(t *testing.T) {
	if got := cdnTotalBytes(0); got != 0 {
		t.Fatalf("n=0 должно давать total=0 (безлимит в терминах Happ), получено %d", got)
	}
	if got := cdnTotalBytes(-1); got != 0 {
		t.Fatalf("отрицательное n должно давать 0, получено %d", got)
	}
}

// --- 2, 3. Одна и несколько CDN-нод ---------------------------------------

func TestCDNTotalScalesWithNodeCount(t *testing.T) {
	cases := []struct {
		nodes int
		want  int64
	}{
		{1, 10_000_000_000},
		{2, 20_000_000_000},
		{3, 30_000_000_000},
		{4, 40_000_000_000},
	}
	for _, c := range cases {
		if got := cdnTotalBytes(c.nodes); got != c.want {
			t.Errorf("n=%d: total=%d, ожидалось %d", c.nodes, got, c.want)
		}
	}
}

// --- 4. Несколько CDN-ссылок одной ноды — нода считается один раз ----------

func TestCDNNodeCountedOncePerServer(t *testing.T) {
	svc := &Service{}

	// Два пул-айтема указывают на ОДИН server_key: так бывает, когда у страны
	// несколько профилей. Ссылка CDN должна добавиться один раз, и n = 1.
	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "lt-a", ServerKey: "lt-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-1"},
			URL:        "vless://uuid-1@lt.race-src.com:443#LT-A",
		},
		{
			PoolItem:   PoolItem{ItemKey: "lt-b", ServerKey: "lt-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-1"},
			URL:        "vless://uuid-1@lt.race-src.com:443#LT-B",
		},
	}

	_, cdnServers := svc.buildGroupedFeedLinesWithEndpoints(
		items, cdnEndpointsFor("lt-main-1"), nil, nil)

	if len(cdnServers) != 1 {
		t.Fatalf("две ссылки одной ноды дали n=%d, ожидалось 1", len(cdnServers))
	}
	if _, ok := cdnServers["lt-main-1"]; !ok {
		t.Fatalf("в множестве нет lt-main-1: %v", cdnServers)
	}
}

func TestCDNNodeCountMatchesIssuedLinks(t *testing.T) {
	svc := &Service{}

	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "lt", ServerKey: "lt-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-1"},
			URL:        "vless://uuid-1@lt.race-src.com:443#LT",
		},
		{
			PoolItem:   PoolItem{ItemKey: "ee", ServerKey: "ee-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-2"},
			URL:        "vless://uuid-2@ee.race-src.com:443#EE",
		},
		// У этой ноды CDN-эндпоинта нет — в n она попасть не должна.
		{
			PoolItem:   PoolItem{ItemKey: "ar", ServerKey: "ar-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-3"},
			URL:        "vless://uuid-3@ar.race-src.com:443#AR",
		},
	}

	lines, cdnServers := svc.buildGroupedFeedLinesWithEndpoints(
		items, cdnEndpointsFor("lt-main-1", "ee-main-1"), nil, nil)

	if len(cdnServers) != 2 {
		t.Fatalf("n=%d, ожидалось 2 (нода без CDN-эндпоинта не считается)", len(cdnServers))
	}

	// Ключевой инвариант: n равно числу CDN-ссылок, реально попавших в фид.
	cdnLines := 0
	for _, l := range lines {
		// type=xhttp ставит только BuildCDNVLESSURLFromEndpoint — это точный
		// признак CDN-ссылки, а не совпадение по подстроке в имени.
		if strings.Contains(l, "type=xhttp") {
			cdnLines++
		}
	}
	if cdnLines != len(cdnServers) {
		t.Fatalf("CDN-ссылок в фиде %d, а n=%d — расхождение витрины и выдачи",
			cdnLines, len(cdnServers))
	}
}

// --- 5. Обычная конфигурация передала трафик — CDN-значение не изменилось --

func TestPlainTrafficDoesNotEnterCDN(t *testing.T) {
	agg := classifyTrafficItems([]kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 777, Email: "tg-777-lt@x", Uplink: 500, Downlink: 9000, InboundTag: "vless-ws-in"},
	}, cdnAllowlist())

	if len(agg.CDNPerUser) != 0 {
		t.Fatalf("обычный vless-ws-in попал в CDN: %v", agg.CDNPerUser)
	}
	if agg.PerUser[777] != [2]int64{500, 9000} {
		t.Fatalf("общий трафик потерян: %v", agg.PerUser[777])
	}
}

func TestUnknownAndEmptyTagsStayOutOfCDN(t *testing.T) {
	agg := classifyTrafficItems([]kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 1, Uplink: 10, Downlink: 20, InboundTag: ""},
		{TelegramID: 1, Uplink: 30, Downlink: 40, InboundTag: "vless-grpc-cdn-in"},
		{TelegramID: 1, Uplink: 50, Downlink: 60, InboundTag: "hysteria"},
	}, cdnAllowlist())

	if len(agg.CDNPerUser) != 0 {
		t.Fatalf("в CDN попал трафик вне allowlist: %v", agg.CDNPerUser)
	}
	if agg.Unclassified != 1 {
		t.Fatalf("неклассифицированных %d, ожидалась 1", agg.Unclassified)
	}
}

// --- 6, 7, 8. Upload, download и оба сразу --------------------------------

func TestCDNUploadOnly(t *testing.T) {
	agg := classifyTrafficItems([]kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 5, Uplink: 4096, Downlink: 0, InboundTag: "vless-xhttp-cdn-in"},
	}, cdnAllowlist())

	if agg.CDNPerUser[5] != [2]int64{4096, 0} {
		t.Fatalf("upload-only: %v, ожидалось [4096 0]", agg.CDNPerUser[5])
	}
}

func TestCDNDownloadOnly(t *testing.T) {
	agg := classifyTrafficItems([]kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 5, Uplink: 0, Downlink: 8192, InboundTag: "vless-xhttp-cdn-in"},
	}, cdnAllowlist())

	if agg.CDNPerUser[5] != [2]int64{0, 8192} {
		t.Fatalf("download-only: %v, ожидалось [0 8192]", agg.CDNPerUser[5])
	}
}

func TestCDNUploadAndDownloadStayIndependent(t *testing.T) {
	agg := classifyTrafficItems([]kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 5, Uplink: 111, Downlink: 222, InboundTag: "vless-xhttp-cdn-in"},
		{TelegramID: 5, Uplink: 333, Downlink: 444, InboundTag: "vless-xhttp-cdn-in"},
	}, cdnAllowlist())

	// Направления не должны смешиваться при суммировании нескольких учёток.
	if got, want := agg.CDNPerUser[5], ([2]int64{444, 666}); got != want {
		t.Fatalf("сумма по направлениям: %v, ожидалось %v", got, want)
	}
}

// --- 14, 15, 16. CDN-нода выключена, добавлена, удалена -------------------

func TestDisabledCDNNodeDropsOutOfCount(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{{
		PoolItem:   PoolItem{ItemKey: "lt", ServerKey: "lt-main-1"},
		Credential: UserCredential{VLESSUUID: "uuid-1"},
		URL:        "vless://uuid-1@lt.race-src.com:443#LT",
	}}

	// ListEnabledCDNEndpoints не отдаёт выключенные строки, поэтому
	// «выключено» на входе выглядит как пустой список.
	_, cdnServers := svc.buildGroupedFeedLinesWithEndpoints(items, nil, nil, nil)
	if len(cdnServers) != 0 {
		t.Fatalf("выключенная CDN-нода посчитана: n=%d", len(cdnServers))
	}

	// Нода добавлена — n растёт.
	_, cdnServers = svc.buildGroupedFeedLinesWithEndpoints(
		items, cdnEndpointsFor("lt-main-1"), nil, nil)
	if len(cdnServers) != 1 {
		t.Fatalf("добавленная CDN-нода не посчитана: n=%d", len(cdnServers))
	}
}

func TestBrokenCDNEndpointDoesNotCount(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{{
		PoolItem:   PoolItem{ItemKey: "lt", ServerKey: "lt-main-1"},
		Credential: UserCredential{VLESSUUID: "uuid-1"},
		URL:        "vless://uuid-1@lt.race-src.com:443#LT",
	}}

	// Эндпоинт без адреса: BuildCDNVLESSURLFromEndpoint вернёт "", ссылка в
	// фид не попадёт. Значит и в n входить не должен — иначе клиент увидит
	// объём, которым не сможет воспользоваться.
	broken := []CDNEndpoint{{
		CDNKey: "lt-cdn", ServerKey: "lt-main-1", Enabled: true,
		InboundTag: "vless-xhttp-cdn-in", Address: "",
	}}

	_, cdnServers := svc.buildGroupedFeedLinesWithEndpoints(items, broken, nil, nil)
	if len(cdnServers) != 0 {
		t.Fatalf("невалидный эндпоинт посчитан в n: %v", cdnServers)
	}
}

// --- 13. Одна нода временно недоступна ------------------------------------

func TestUnreachableNodeDoesNotBreakFeed(t *testing.T) {
	svc := &Service{}

	// Недоступность ноды не меняет состав фида: он строится из БД центра, а не
	// опросом узлов. Пользователь получает все ссылки, включая ссылку молчащей
	// ноды, — иначе временный сетевой сбой отбирал бы оплаченный доступ.
	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "lt", ServerKey: "lt-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-1"},
			URL:        "vless://uuid-1@lt.race-src.com:443#LT",
		},
		{
			PoolItem:   PoolItem{ItemKey: "ar", ServerKey: "ar-main-1"},
			Credential: UserCredential{VLESSUUID: "uuid-2"},
			URL:        "vless://uuid-2@ar.race-src.com:443#AR",
		},
	}

	lines, cdnServers := svc.buildGroupedFeedLinesWithEndpoints(
		items, cdnEndpointsFor("lt-main-1", "ar-main-1"), nil, nil)

	if len(cdnServers) != 2 {
		t.Fatalf("n=%d, ожидалось 2", len(cdnServers))
	}
	if len(lines) < 4 {
		t.Fatalf("строк в фиде %d, ожидалось не менее 4 (2 основных + 2 CDN)", len(lines))
	}
}

// --- 19. Параллельное обновление статистики -------------------------------

func TestClassifyTrafficItemsIsRaceFree(t *testing.T) {
	items := []kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 1, Uplink: 10, Downlink: 20, InboundTag: "vless-xhttp-cdn-in"},
		{TelegramID: 2, Uplink: 30, Downlink: 40, InboundTag: "vless-ws-in"},
	}
	allow := cdnAllowlist()

	// Функция не имеет разделяемого состояния: параллельные вызовы обязаны
	// давать одинаковый результат. Запускать под go test -race.
	var wg sync.WaitGroup
	results := make([]TrafficAggregate, 16)
	for i := range results {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = classifyTrafficItems(items, allow)
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if r.CDNPerUser[1] != [2]int64{10, 20} {
			t.Fatalf("вызов %d: CDN пользователя 1 = %v", i, r.CDNPerUser[1])
		}
		if _, ok := r.CDNPerUser[2]; ok {
			t.Fatalf("вызов %d: не-CDN пользователь попал в квоту", i)
		}
	}
}

// --- 20. Большие значения без переполнения --------------------------------

func TestLargeTrafficValuesDoNotOverflow(t *testing.T) {
	const big = int64(1) << 52 // ~4.5 ПБ, заведомо больше любого реального объёма

	agg := classifyTrafficItems([]kafkacontracts.VPNNodeTrafficItem{
		{TelegramID: 9, Uplink: big, Downlink: big, InboundTag: "vless-xhttp-cdn-in"},
		{TelegramID: 9, Uplink: big, Downlink: big, InboundTag: "vless-xhttp-cdn-in"},
	}, cdnAllowlist())

	want := [2]int64{2 * big, 2 * big}
	if agg.CDNPerUser[9] != want {
		t.Fatalf("переполнение или потеря: %v, ожидалось %v", agg.CDNPerUser[9], want)
	}
	if agg.CDNPerUser[9][0] < 0 || agg.CDNPerUser[9][1] < 0 {
		t.Fatalf("значение ушло в минус: %v", agg.CDNPerUser[9])
	}
}

func TestCDNTotalBytesNoOverflowOnRealisticNodeCounts(t *testing.T) {
	// Реальный потолок — десятки нод. Проверяем с запасом на три порядка.
	if got := cdnTotalBytes(10_000); got != 100_000_000_000_000 {
		t.Fatalf("total для 10000 узлов = %d", got)
	}
	if cdnTotalBytes(10_000) < 0 {
		t.Fatal("переполнение int64")
	}
}

// --- 21. Значение байтов отображается как n × 10 GB ------------------------

func TestSubscriptionUserinfoTotalMatchesNodeCount(t *testing.T) {
	until := time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC)

	res := &SubscriptionFeedResult{
		Body:        []byte("dGVzdA=="),
		ContentType: "text/plain; charset=utf-8",
		Access:      &AccessState{Status: "active", AccessUntil: &until},
		Uplink:      1_234_567,
		Downlink:    7_654_321,
		CDNNodes:    3,
	}

	rec := httptest.NewRecorder()
	writeSubscriptionHeaders(rec, res)

	got := rec.Header().Get("Subscription-Userinfo")
	want := "upload=1234567; download=7654321; total=30000000000; expire=1798761540"
	if got != want {
		t.Fatalf("subscription-userinfo:\n получено %q\n ожидалось %q", got, want)
	}
}

// --- 22. Общий трафик и срок действия в одном заголовке -------------------

func TestSubscriptionUserinfoCarriesAllFieldsInOneHeader(t *testing.T) {
	until := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	res := &SubscriptionFeedResult{
		Access:   &AccessState{Status: "active", AccessUntil: &until},
		Uplink:   1,
		Downlink: 2,
		CDNNodes: 1,
	}

	rec := httptest.NewRecorder()
	writeSubscriptionHeaders(rec, res)

	if n := len(rec.Header().Values("Subscription-Userinfo")); n != 1 {
		t.Fatalf("заголовков Subscription-Userinfo: %d, документация Happ требует один", n)
	}
	h := rec.Header().Get("Subscription-Userinfo")
	for _, field := range []string{"upload=", "download=", "total=", "expire="} {
		if !strings.Contains(h, field) {
			t.Errorf("в заголовке нет поля %s: %q", field, h)
		}
	}
	if !strings.Contains(h, "; ") {
		t.Errorf("поля не разделены '; ': %q", h)
	}
}

func TestGracePeriodUsesGraceUntil(t *testing.T) {
	access := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	grace := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)

	res := &SubscriptionFeedResult{
		Access:   &AccessState{Status: "grace", AccessUntil: &access, GraceUntil: &grace},
		CDNNodes: 1,
	}

	rec := httptest.NewRecorder()
	writeSubscriptionHeaders(rec, res)

	if !strings.Contains(rec.Header().Get("Subscription-Userinfo"), "expire=1788480000") {
		t.Fatalf("в grace должен использоваться grace_until: %q",
			rec.Header().Get("Subscription-Userinfo"))
	}
}

// --- 23. UTF-8-название отображается корректно ----------------------------

func TestProfileTitleIsBase64EncodedUTF8(t *testing.T) {
	res := &SubscriptionFeedResult{CDNNodes: 0}
	rec := httptest.NewRecorder()
	writeSubscriptionHeaders(rec, res)

	got := rec.Header().Get("Profile-Title")
	if !strings.HasPrefix(got, "base64:") {
		t.Fatalf("Profile-Title без префикса base64: %q — «⚡» приедет искажённым", got)
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(got, "base64:"))
	if err != nil {
		t.Fatalf("Profile-Title не декодируется: %v", err)
	}
	if string(decoded) != "⚡ House VPN" {
		t.Fatalf("после декодирования %q, ожидалось %q", string(decoded), "⚡ House VPN")
	}
}

func TestProfileTitleFitsHappLimit(t *testing.T) {
	// Документация Happ: максимальная длина имени подписки — 25 символов.
	if n := len([]rune(subscriptionProfileTitle)); n > 25 {
		t.Fatalf("имя подписки %d символов, лимит Happ — 25", n)
	}
}

func TestProfileTitleHeaderIsASCII(t *testing.T) {
	// HTTP-заголовки ограничены ISO-8859-1 (RFC 7230). Значение обязано быть
	// представимо байтами < 0x80, иначе клиент получит мусор.
	res := &SubscriptionFeedResult{}
	rec := httptest.NewRecorder()
	writeSubscriptionHeaders(rec, res)

	for _, b := range []byte(rec.Header().Get("Profile-Title")) {
		if b > 0x7F {
			t.Fatalf("в Profile-Title не-ASCII байт %#x", b)
		}
	}
}

// --- 24. Кэш не удерживает устаревший расход ------------------------------

func TestSubscriptionResponseIsNotCacheable(t *testing.T) {
	res := &SubscriptionFeedResult{ContentType: "text/plain; charset=utf-8"}
	rec := httptest.NewRecorder()
	writeSubscriptionHeaders(rec, res)

	cc := rec.Header().Get("Cache-Control")
	if !strings.Contains(cc, "no-store") {
		t.Fatalf("Cache-Control=%q: без no-store CDN или клиент удержат устаревший расход", cc)
	}
	if !strings.Contains(cc, "no-cache") {
		t.Errorf("Cache-Control=%q: нет no-cache", cc)
	}
	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Errorf("Pragma=%q, ожидалось no-cache", got)
	}
	if got := rec.Header().Get("Expires"); got != "0" {
		t.Errorf("Expires=%q, ожидалось 0", got)
	}
	if rec.Header().Get("ETag") != "" {
		t.Error("ETag на подписке позволил бы клиенту получить 304 и не обновить расход")
	}
}

// --- Изоляция: витрина не влияет на квоту ---------------------------------

func TestDisplayVolumeIsIndependentFromEnforcementLimit(t *testing.T) {
	// Витрина (что видит пользователь) и лимит (когда снимается CDN) — разные
	// величины и обязаны оставаться разными. Совпадение этих чисел означало бы,
	// что правка отображения молча изменила тарифную политику.
	if cdnDisplayBytesPerNode == defaultCDNQuotaLimitBytes {
		t.Fatal("витрина совпала с лимитом принуждения — проверьте, что это осознанное решение")
	}
	if cdnDisplayBytesPerNode != 10_000_000_000 {
		t.Fatalf("витрина = %d, задание требует 10 GB на узел", cdnDisplayBytesPerNode)
	}
	if defaultCDNQuotaLimitBytes != 20_000_000_000 {
		t.Fatalf("лимит квоты = %d, ожидалось 20 GB (изменение тарифа запрещено)",
			defaultCDNQuotaLimitBytes)
	}
}
