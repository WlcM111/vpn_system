package vpn_orchestrator

import (
	"net/url"
	"strings"
	"testing"
)

// baseRealityEndpoint — заполненный эндпоинт, от которого отталкиваются тесты.
func baseRealityEndpoint() RealityEndpoint {
	return RealityEndpoint{
		RealityKey:  "ee-main-1-reality",
		ServerKey:   "ee-main-1",
		Enabled:     true,
		SortOrder:   50,
		InboundTag:  "vless-reality-in",
		Address:     "13.143.176.187",
		Port:        8443,
		ServerName:  "dzen.ru",
		PublicKey:   "YqHW8a4iAc1SZYpTrFVoOQg1F3yAdX1tWXuROZUCsEU",
		ShortID:     "6ba85179e30d4fc2",
		SpiderX:     "/",
		Flow:        "xtls-rprx-vision",
		Fingerprint: "chrome",
		Remarks:     "🇪🇪 Эстония — Reality",
	}
}

const testUUID = "11111111-2222-3333-4444-555555555555"

// queryOf разбирает query-часть vless://-ссылки.
func queryOf(t *testing.T, link string) url.Values {
	t.Helper()
	i := strings.Index(link, "?")
	if i < 0 {
		t.Fatalf("в ссылке нет query: %s", link)
	}
	frag := link[i+1:]
	if h := strings.Index(frag, "#"); h >= 0 {
		frag = frag[:h]
	}
	v, err := url.ParseQuery(frag)
	if err != nil {
		t.Fatalf("query не разбирается: %v", err)
	}
	return v
}

// Ссылка содержит все параметры, без которых Reality не работает.
func TestRealityURLHasRequiredParams(t *testing.T) {
	link := BuildRealityVLESSURLFromEndpoint(baseRealityEndpoint(), testUUID)
	if link == "" {
		t.Fatal("ссылка не собралась")
	}
	if !strings.HasPrefix(link, "vless://"+testUUID+"@13.143.176.187:8443?") {
		t.Errorf("неверный префикс: %s", link)
	}

	q := queryOf(t, link)
	want := map[string]string{
		"encryption": "none",
		"security":   "reality",
		"type":       "tcp",
		"sni":        "dzen.ru",
		"pbk":        "YqHW8a4iAc1SZYpTrFVoOQg1F3yAdX1tWXuROZUCsEU",
		"sid":        "6ba85179e30d4fc2",
		"fp":         "chrome",
		"spx":        "/",
		"flow":       "xtls-rprx-vision",
	}
	for k, v := range want {
		if got := q.Get(k); got != v {
			t.Errorf("%s = %q, ожидалось %q", k, got, v)
		}
	}
}

// Без публичного ключа ссылка бесполезна: клиент не пройдёт рукопожатие,
// поэтому выдавать её нельзя.
func TestRealityURLEmptyWithoutPublicKey(t *testing.T) {
	e := baseRealityEndpoint()
	e.PublicKey = ""
	if got := BuildRealityVLESSURLFromEndpoint(e, testUUID); got != "" {
		t.Errorf("ожидалась пустая ссылка, получено: %s", got)
	}
}

// Без SNI клиент не знает, под какой сайт маскироваться.
func TestRealityURLEmptyWithoutServerName(t *testing.T) {
	e := baseRealityEndpoint()
	e.ServerName = ""
	if got := BuildRealityVLESSURLFromEndpoint(e, testUUID); got != "" {
		t.Errorf("ожидалась пустая ссылка, получено: %s", got)
	}
}

// Пустые адрес и UUID обрабатываются так же, как в остальных транспортах.
func TestRealityURLEmptyInputs(t *testing.T) {
	e := baseRealityEndpoint()
	e.Address = ""
	if got := BuildRealityVLESSURLFromEndpoint(e, testUUID); got != "" {
		t.Errorf("пустой адрес: ожидалась пустая ссылка, получено %s", got)
	}
	if got := BuildRealityVLESSURLFromEndpoint(baseRealityEndpoint(), ""); got != "" {
		t.Errorf("пустой UUID: ожидалась пустая ссылка, получено %s", got)
	}
}

// Незаданные необязательные поля подставляются значениями по умолчанию.
func TestRealityURLDefaults(t *testing.T) {
	e := baseRealityEndpoint()
	e.Port = 0
	e.Fingerprint = ""
	e.SpiderX = ""
	e.Remarks = ""

	link := BuildRealityVLESSURLFromEndpoint(e, testUUID)
	if !strings.Contains(link, ":8443?") {
		t.Errorf("порт по умолчанию не 8443: %s", link)
	}
	q := queryOf(t, link)
	if q.Get("fp") != "chrome" {
		t.Errorf("fp = %q, ожидалось chrome", q.Get("fp"))
	}
	if q.Get("spx") != "/" {
		t.Errorf("spx = %q, ожидалось /", q.Get("spx"))
	}
	if !strings.Contains(link, "#reality") {
		t.Errorf("remarks по умолчанию не подставлены: %s", link)
	}
}

// Пустой shortId не должен превращаться в параметр sid= без значения:
// часть клиентов трактует это как ошибку конфигурации.
func TestRealityURLOmitsEmptyShortID(t *testing.T) {
	e := baseRealityEndpoint()
	e.ShortID = ""
	link := BuildRealityVLESSURLFromEndpoint(e, testUUID)
	if strings.Contains(link, "sid=") {
		t.Errorf("пустой sid попал в ссылку: %s", link)
	}
}

// Пустой flow означает обычный TLS-прокси; параметр в этом случае лишний.
func TestRealityURLOmitsEmptyFlow(t *testing.T) {
	e := baseRealityEndpoint()
	e.Flow = ""
	link := BuildRealityVLESSURLFromEndpoint(e, testUUID)
	if strings.Contains(link, "flow=") {
		t.Errorf("пустой flow попал в ссылку: %s", link)
	}
}

// Кириллица и эмодзи в названии профиля кодируются как во всех остальных
// транспортах, иначе клиент покажет искажённый текст.
func TestRealityURLEscapesRemarks(t *testing.T) {
	link := BuildRealityVLESSURLFromEndpoint(baseRealityEndpoint(), testUUID)
	i := strings.Index(link, "#")
	if i < 0 {
		t.Fatal("нет фрагмента с названием")
	}
	frag := link[i+1:]
	if strings.ContainsAny(frag, " ") {
		t.Errorf("пробел не экранирован: %s", frag)
	}
	decoded, err := url.QueryUnescape(frag)
	if err != nil {
		t.Fatalf("фрагмент не декодируется: %v", err)
	}
	if decoded != "🇪🇪 Эстония — Reality" {
		t.Errorf("название после декодирования = %q", decoded)
	}
}

// Эндпоинт выбирается строго по своему узлу: адрес Reality — это IP
// конкретной машины, и подстановка чужого увела бы пользователя не туда.
func TestSelectRealityForServerExactMatch(t *testing.T) {
	eps := []RealityEndpoint{
		{RealityKey: "ee", ServerKey: "ee-main-1", Address: "13.143.176.187"},
		{RealityKey: "us", ServerKey: "us-main-1", Address: "74.208.61.5"},
	}
	got, ok := selectRealityForServer(eps, "us-main-1")
	if !ok || got.Address != "74.208.61.5" {
		t.Errorf("выбран не тот эндпоинт: %+v ok=%v", got, ok)
	}
}

// Глобального фолбэка у Reality нет: эндпоинт без server_key не подходит
// никакому узлу.
func TestSelectRealityNoGlobalFallback(t *testing.T) {
	eps := []RealityEndpoint{
		{RealityKey: "global", ServerKey: "", Address: "1.2.3.4"},
	}
	if _, ok := selectRealityForServer(eps, "ee-main-1"); ok {
		t.Error("эндпоинт без server_key не должен подходить узлу")
	}
}

// Неизвестный узел и пустой список не дают совпадения.
func TestSelectRealityMisses(t *testing.T) {
	eps := []RealityEndpoint{{RealityKey: "ee", ServerKey: "ee-main-1"}}
	if _, ok := selectRealityForServer(eps, "lt-main-1"); ok {
		t.Error("нашлось совпадение для чужого узла")
	}
	if _, ok := selectRealityForServer(nil, "ee-main-1"); ok {
		t.Error("пустой список дал совпадение")
	}
	if _, ok := selectRealityForServer(eps, ""); ok {
		t.Error("пустой server_key дал совпадение")
	}
}

// Reality-строка попадает в фид ровно для того узла, к которому привязана.
func TestRealityLinkInFeedForItsNodeOnly(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "ee-main", ServerKey: "ee-main-1", CountryCode: "EE", Title: "🇪🇪 Эстония"},
			Credential: UserCredential{VLESSUUID: testUUID, ServerKey: "ee-main-1"},
		},
		{
			PoolItem:   PoolItem{ItemKey: "us-main-1-ws", ServerKey: "us-main-1", CountryCode: "US", Title: "🇺🇸 США"},
			Credential: UserCredential{VLESSUUID: testUUID, ServerKey: "us-main-1"},
		},
	}
	reality := []RealityEndpoint{baseRealityEndpoint()} // только ee-main-1

	lines, _ := svc.buildGroupedFeedLinesWithEndpoints(items, nil, nil, nil, reality)

	n := 0
	for _, l := range lines {
		if strings.Contains(l, "security=reality") {
			n++
			if !strings.Contains(l, "13.143.176.187") {
				t.Errorf("Reality-строка ведёт не на свой узел: %s", l)
			}
		}
	}
	if n != 1 {
		t.Errorf("Reality-строк в фиде: %d, ожидалась 1", n)
	}
}

// Пустой список эндпоинтов не ломает сборку фида: остальные транспорты
// продолжают работать.
func TestFeedWithoutRealityEndpoints(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "ee-main", ServerKey: "ee-main-1", CountryCode: "EE", Title: "🇪🇪 Эстония"},
			Credential: UserCredential{VLESSUUID: testUUID, ServerKey: "ee-main-1"},
		},
	}
	lines, _ := svc.buildGroupedFeedLinesWithEndpoints(items, nil, nil, nil, nil)
	for _, l := range lines {
		if strings.Contains(l, "security=reality") {
			t.Errorf("Reality-строка без эндпоинтов: %s", l)
		}
	}
}

// Эндпоинт с незаполненным публичным ключом не должен давать строку в фиде:
// иначе пользователь получит профиль, который не подключается.
func TestBrokenRealityEndpointProducesNoLine(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "ee-main", ServerKey: "ee-main-1", CountryCode: "EE", Title: "🇪🇪 Эстония"},
			Credential: UserCredential{VLESSUUID: testUUID, ServerKey: "ee-main-1"},
		},
	}
	broken := baseRealityEndpoint()
	broken.PublicKey = ""

	lines, _ := svc.buildGroupedFeedLinesWithEndpoints(items, nil, nil, nil, []RealityEndpoint{broken})
	for _, l := range lines {
		if strings.Contains(l, "security=reality") {
			t.Errorf("битый эндпоинт дал строку: %s", l)
		}
	}
}

// Профиль синхронизации для узла с Reality содержит отдельный inbound и
// помечен Optional: на узлах без этого инбаунда агент его пропустит.
func TestRealityProfileIsOptional(t *testing.T) {
	svc := &Service{}
	base := baseProfile()
	profiles := svc.buildUserProfiles(base, "ee-main-1", nil, nil, []RealityEndpoint{baseRealityEndpoint()}, false)

	var found bool
	for _, p := range profiles {
		if p.InboundTag == "vless-reality-in" {
			found = true
			if !p.Optional {
				t.Error("Reality-профиль должен быть Optional")
			}
			if p.VLESSUUID != base.VLESSUUID {
				t.Error("UUID должен совпадать с базовым профилем")
			}
		}
	}
	if !found {
		t.Error("Reality-профиль не добавлен")
	}
}

// Без эндпоинта на узле Reality-профиль не появляется.
func TestNoRealityProfileWithoutEndpoint(t *testing.T) {
	svc := &Service{}
	profiles := svc.buildUserProfiles(baseProfile(), "lt-main-1", nil, nil, []RealityEndpoint{baseRealityEndpoint()}, false)
	for _, p := range profiles {
		if p.InboundTag == "vless-reality-in" {
			t.Error("Reality-профиль появился на узле без эндпоинта")
		}
	}
}
