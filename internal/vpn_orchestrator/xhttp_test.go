package vpn_orchestrator

import (
	"net/url"
	"strings"
	"testing"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

func testXHTTPEndpoint() XHTTPEndpoint {
	return XHTTPEndpoint{
		XHTTPKey: "ro-main-1-xhttp", ServerKey: "ro-main-1", Enabled: true,
		InboundTag: "vless-xhttp-in", Address: "ro.race-src.com", ServerName: "ro.race-src.com",
		Host: "ro.race-src.com", Port: 443, Path: "/a1b2c3d4e5f60718", Mode: "auto",
		Fingerprint: "chrome", ALPN: "h2,http/1.1", Remarks: "🇷🇴 Румыния — XHTTP",
	}
}

func TestBuildXHTTPVLESSURLFromEndpoint(t *testing.T) {
	e := testXHTTPEndpoint()
	raw := BuildXHTTPVLESSURLFromEndpoint(e, testUUID)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("ссылка не разбирается: %v (%s)", err, raw)
	}
	if u.Scheme != "vless" || u.User.Username() != testUUID || u.Host != "ro.race-src.com:443" {
		t.Fatalf("схема/UUID/адрес: %s", raw)
	}
	q := u.Query()
	want := map[string]string{
		"encryption": "none", "security": "tls", "type": "xhttp",
		"sni": "ro.race-src.com", "host": "ro.race-src.com", "path": "/a1b2c3d4e5f60718",
		"mode": "auto", "fp": "chrome", "alpn": "h2,http/1.1",
	}
	for k, v := range want {
		if got := q.Get(k); got != v {
			t.Errorf("%s = %q, ожидалось %q", k, got, v)
		}
	}
	for _, k := range []string{"flow", "extra", "pbk", "sid"} {
		if q.Has(k) {
			t.Errorf("лишний параметр %s в XHTTP-ссылке: %s", k, raw)
		}
	}
	if u.Fragment != "🇷🇴 Румыния — XHTTP" {
		t.Errorf("название = %q", u.Fragment)
	}
	if strings.Contains(raw[strings.Index(raw, "#"):], "+") {
		t.Errorf("пробелы в названии закодированы плюсами: %s", raw)
	}
}

func TestBuildXHTTPVLESSURLDefaultsAndRequiredFields(t *testing.T) {
	min := XHTTPEndpoint{ServerKey: "ro-main-1", InboundTag: "vless-xhttp-in", Address: "ro.race-src.com", Path: "/p"}
	q := queryOf(t, BuildXHTTPVLESSURLFromEndpoint(min, testUUID))
	for k, v := range map[string]string{"sni": "ro.race-src.com", "host": "ro.race-src.com", "mode": "auto", "fp": "chrome", "alpn": "h2,http/1.1"} {
		if got := q.Get(k); got != v {
			t.Errorf("значение по умолчанию %s = %q, ожидалось %q", k, got, v)
		}
	}
	if !strings.Contains(BuildXHTTPVLESSURLFromEndpoint(min, testUUID), "@ro.race-src.com:443?") {
		t.Error("порт по умолчанию не 443")
	}

	for name, e := range map[string]XHTTPEndpoint{
		"без адреса":            {Address: "", Path: "/p"},
		"без пути":              {Address: "ro.race-src.com", Path: ""},
		"путь не с косой черты": {Address: "ro.race-src.com", Path: "p"},
	} {
		if got := BuildXHTTPVLESSURLFromEndpoint(e, testUUID); got != "" {
			t.Errorf("%s: ожидалась пустая ссылка, получено %q", name, got)
		}
	}
	if got := BuildXHTTPVLESSURLFromEndpoint(min, ""); got != "" {
		t.Errorf("без UUID: ожидалась пустая ссылка, получено %q", got)
	}
}

// Эндпоинт чужого сервера не подходит: UUID у каждого сервера свой.
func TestSelectXHTTPForServerHasNoFallback(t *testing.T) {
	eps := []XHTTPEndpoint{testXHTTPEndpoint(), {XHTTPKey: "global", ServerKey: ""}}
	if got, ok := selectXHTTPForServer(eps, "ro-main-1"); !ok || got.XHTTPKey != "ro-main-1-xhttp" {
		t.Fatalf("свой эндпоинт не выбран: %+v %v", got, ok)
	}
	if _, ok := selectXHTTPForServer(eps, "ee-main-1"); ok {
		t.Fatal("выбран эндпоинт чужого сервера или глобальный")
	}
	if _, ok := selectXHTTPForServer(eps, ""); ok {
		t.Fatal("пустой server_key дал совпадение")
	}
}

func TestXHTTPConfigSwitch(t *testing.T) {
	for v, want := range map[string]bool{"": true, "true": true, "1": true, "TRUE": true, "false": false, "0": false} {
		t.Setenv("XHTTP_CONFIG_ENABLED", v)
		if got := xhttpConfigEnabled(); got != want {
			t.Errorf("XHTTP_CONFIG_ENABLED=%q: %v, ожидалось %v", v, got, want)
		}
	}
}

// Ссылки сервера — только из его эндпоинтов, в постоянном порядке
// Reality → XHTTP → Hysteria2 → CDN. Отдельной «основной» ссылки нет.
func TestFeedLinksComeOnlyFromEndpointsInFixedOrder(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{
		{
			PoolItem:   PoolItem{ItemKey: "ro-main", ServerKey: "ro-main-1", CountryCode: "RO", InboundTag: "vless-reality-in"},
			Credential: UserCredential{VLESSUUID: "uuid-ro", ServerKey: "ro-main-1"},
		},
		{
			PoolItem:   PoolItem{ItemKey: "ee-main", ServerKey: "ee-main-1", CountryCode: "EE", InboundTag: "vless-reality-in"},
			Credential: UserCredential{VLESSUUID: "uuid-ee", ServerKey: "ee-main-1"},
		},
	}
	realityRO := baseRealityEndpoint()
	realityRO.RealityKey, realityRO.ServerKey, realityRO.Address = "ro", "ro-main-1", "91.200.220.180"
	realityEE := baseRealityEndpoint()
	xhttpEE := testXHTTPEndpoint()
	xhttpEE.XHTTPKey, xhttpEE.ServerKey, xhttpEE.Address = "ee", "ee-main-1", "ee.race-src.com"
	hy := []HysteriaEndpoint{
		{HysteriaKey: "ee-hy", ServerKey: "ee-main-1", Address: "ee.race-src.com", Enabled: true},
		{HysteriaKey: "ro-hy", ServerKey: "ro-main-1", Address: "ro.race-src.com", Enabled: true},
	}
	cdn := []CDNEndpoint{{CDNKey: "yacdn-ee", ServerKey: "ee-main-1", Enabled: true, InboundTag: "vless-xhttp-cdn-in", Address: "yacdn.race-src.com"}}

	lines, cdnServers := svc.buildGroupedFeedLinesWithEndpoints(items, cdn,
		[]XHTTPEndpoint{xhttpEE, testXHTTPEndpoint()}, hy, []RealityEndpoint{realityEE, realityRO})

	type kind struct{ scheme, typ, host string }
	got := make([]kind, 0, len(lines))
	for _, l := range lines {
		u, err := url.Parse(l)
		if err != nil {
			t.Fatalf("строка не разбирается: %s", l)
		}
		got = append(got, kind{u.Scheme, u.Query().Get("type") + "/" + u.Query().Get("security"), u.Hostname()})
	}
	want := []kind{
		{"vless", "tcp/reality", "91.200.220.180"},
		{"vless", "xhttp/tls", "ro.race-src.com"},
		{"hysteria2", "/", "ro.race-src.com"},
		{"vless", "tcp/reality", "13.143.176.187"},
		{"vless", "xhttp/tls", "ee.race-src.com"},
		{"hysteria2", "/", "ee.race-src.com"},
		{"vless", "xhttp/tls", "yacdn.race-src.com"},
	}
	if len(got) != len(want) {
		t.Fatalf("строк %d, ожидалось %d: %v", len(got), len(want), lines)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("строка %d: %+v, ожидалось %+v", i, got[i], want[i])
		}
	}
	for i, l := range lines {
		uuid := "uuid-ro"
		if i >= 3 {
			uuid = "uuid-ee"
		}
		if !strings.Contains(l, "://"+uuid+"@") {
			t.Errorf("строка %d получила чужой UUID: %s", i, l)
		}
	}
	if len(cdnServers) != 1 {
		t.Errorf("CDN-узлов %d, ожидался 1", len(cdnServers))
	}
}

// Сервер без эндпоинтов не даёт ни одной строки (основной ссылки из полей
// сервера больше нет).
func TestServerWithoutEndpointsHasNoLines(t *testing.T) {
	svc := &Service{}
	items := []FeedItem{{
		PoolItem:   PoolItem{ItemKey: "lt", ServerKey: "lt-main-1", InboundTag: "vless-reality-in"},
		Credential: UserCredential{VLESSUUID: "uuid-1", ServerKey: "lt-main-1"},
	}}
	if lines, _ := svc.buildGroupedFeedLinesWithEndpoints(items, nil, nil, nil, nil); len(lines) != 0 {
		t.Fatalf("сервер без эндпоинтов дал строки: %v", lines)
	}
}

func profileByTag(profiles []kafkacontracts.VPNNodeUserProfile, tag string) (kafkacontracts.VPNNodeUserProfile, int) {
	var found kafkacontracts.VPNNodeUserProfile
	n := 0
	for _, p := range profiles {
		if p.InboundTag == tag {
			found = p
			n++
		}
	}
	return found, n
}

// Основной профиль на инбаунде Reality и Reality-эндпоинт дают ОДНУ учётку:
// flow — у эндпоинта (как в ссылке), обязательность — у основного профиля.
// XHTTP — отдельная учётка того же пользователя без flow. CDN сохраняет свой
// email и flow основного профиля.
func TestBuildUserProfilesThreeProfilesPerNode(t *testing.T) {
	svc := &Service{}
	base := kafkacontracts.VPNNodeUserProfile{
		ItemKey: "ee-main", InboundTag: "vless-reality-in", Email: "tg-1-ee-main@vpn-platform.local",
		VLESSUUID: testUUID, Flow: "",
	}
	reality := baseRealityEndpoint() // ee-main-1, flow xtls-rprx-vision
	xhttp := testXHTTPEndpoint()
	xhttp.ServerKey = "ee-main-1"
	cdn := []CDNEndpoint{{CDNKey: "yacdn-ee", ServerKey: "ee-main-1", Enabled: true, InboundTag: "vless-xhttp-cdn-in", Address: "yacdn.race-src.com"}}

	profiles := svc.buildUserProfiles(base, "ee-main-1", cdn, []XHTTPEndpoint{xhttp}, []RealityEndpoint{reality}, false)
	if len(profiles) != 3 {
		t.Fatalf("профилей %d, ожидалось 3 (Reality, XHTTP, CDN): %+v", len(profiles), profiles)
	}

	r, n := profileByTag(profiles, "vless-reality-in")
	if n != 1 {
		t.Fatalf("учёток в Reality-инбаунде %d, ожидалась 1", n)
	}
	if r.Flow != "xtls-rprx-vision" || r.Optional || r.Email != base.Email || r.VLESSUUID != testUUID {
		t.Errorf("Reality-учётка: flow=%q optional=%v email=%q", r.Flow, r.Optional, r.Email)
	}
	if linkFlow := queryOf(t, BuildRealityVLESSURLFromEndpoint(reality, testUUID)).Get("flow"); r.Flow != linkFlow {
		t.Errorf("flow учётки %q расходится с flow ссылки %q", r.Flow, linkFlow)
	}

	x, n := profileByTag(profiles, "vless-xhttp-in")
	if n != 1 || x.Flow != "" || !x.Optional || x.Email != base.Email || x.VLESSUUID != testUUID {
		t.Errorf("XHTTP-учётка: n=%d flow=%q optional=%v email=%q", n, x.Flow, x.Optional, x.Email)
	}

	c, n := profileByTag(profiles, "vless-xhttp-cdn-in")
	if n != 1 || c.Email != cdnEmail(base.Email) || c.Flow != "" || !c.Optional {
		t.Errorf("CDN-учётка: n=%d email=%q flow=%q optional=%v", n, c.Email, c.Flow, c.Optional)
	}
}

// Эндпоинт без тега инбаунда не даёт профиля: встроенного тега по умолчанию
// нет, учётку некуда регистрировать.
func TestEndpointWithoutInboundTagGivesNoProfile(t *testing.T) {
	svc := &Service{}
	base := kafkacontracts.VPNNodeUserProfile{InboundTag: "vless-reality-in", Email: "e@x", VLESSUUID: testUUID}
	reality := baseRealityEndpoint()
	reality.InboundTag = ""
	xhttp := testXHTTPEndpoint()
	xhttp.ServerKey, xhttp.InboundTag = "ee-main-1", " "

	profiles := svc.buildUserProfiles(base, "ee-main-1", nil, []XHTTPEndpoint{xhttp}, []RealityEndpoint{reality}, false)
	if len(profiles) != 1 || profiles[0].InboundTag != "vless-reality-in" || profiles[0].Flow != "" {
		t.Fatalf("ожидался только основной профиль без изменений, получено %+v", profiles)
	}
}

// Отзыв снимает и XHTTP-учётку: она строится тем же buildUserProfiles.
func TestRevokeProfilesIncludeXHTTP(t *testing.T) {
	svc := &Service{}
	base := kafkacontracts.VPNNodeUserProfile{InboundTag: "vless-reality-in", Email: "e@x", VLESSUUID: testUUID}
	xhttp := testXHTTPEndpoint()
	xhttp.ServerKey = "ee-main-1"
	profiles := svc.buildUserProfiles(base, "ee-main-1", nil, []XHTTPEndpoint{xhttp}, []RealityEndpoint{baseRealityEndpoint()}, false)
	if _, n := profileByTag(profiles, "vless-xhttp-in"); n != 1 {
		t.Fatalf("XHTTP-учётки нет в наборе отзыва: %+v", profiles)
	}
}
