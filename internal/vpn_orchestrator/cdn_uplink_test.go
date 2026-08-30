package vpn_orchestrator

import (
	"net/url"
	"strings"
	"testing"
)

// Эталон обратной совместимости: ссылка, снятая с production 29.08.2026
// командой `curl ... | base64 -d | grep 'type=xhttp'`. После добавления
// колонок uplink_* она обязана остаться байт в байт такой же, пока новые
// поля пустые — иначе у пользователей на руках перестанут работать уже
// выданные конфигурации.
const productionExtraJSON = `{"xPaddingObfsMode":true,"xPaddingPlacement":"cookie",` +
	`"xPaddingKey":"ssid","xPaddingMethod":"tokenish",` +
	`"scMaxBufferedPosts":256,"scMinPostsIntervalMs":"5"}`

// baseEndpoint повторяет строку cdn-global из production на момент правки.
func baseEndpoint() CDNEndpoint {
	return CDNEndpoint{
		CDNKey:               "cdn-global",
		ServerKey:            "ee-main-1",
		Enabled:              true,
		SortOrder:            100,
		InboundTag:           "vless-xhttp-cdn-in",
		Address:              "cdn.example.net",
		ServerName:           "cdn.example.net",
		Host:                 "cdn.example.net",
		Port:                 443,
		XHTTPPath:            "/api/uploadFile/",
		Mode:                 "packet-up",
		Fingerprint:          "chrome",
		ALPN:                 "h2,http/1.1",
		Remarks:              "cdn",
		PaddingObfsMode:      true,
		PaddingPlacement:     "cookie",
		PaddingKey:           "ssid",
		PaddingMethod:        "tokenish",
		ScMaxBufferedPosts:   256,
		ScMinPostsIntervalMs: "5",
	}
}

// extraFromURL достаёт и раскодирует значение параметра extra из vless://-ссылки.
func extraFromURL(t *testing.T, link string) string {
	t.Helper()
	idx := strings.Index(link, "?")
	if idx < 0 {
		t.Fatalf("в ссылке нет query-части: %s", link)
	}
	frag := link[idx+1:]
	if h := strings.Index(frag, "#"); h >= 0 {
		frag = frag[:h]
	}
	values, err := url.ParseQuery(frag)
	if err != nil {
		t.Fatalf("query не разбирается: %v", err)
	}
	extra := values.Get("extra")
	if extra == "" {
		t.Fatal("параметр extra отсутствует")
	}
	return extra
}

// Пустые новые поля не должны попадать в extra: ссылка остаётся прежней.
func TestBuildCDNVLESSURL_BackwardCompatible(t *testing.T) {
	link := BuildCDNVLESSURLFromEndpoint(baseEndpoint(), "11111111-2222-3333-4444-555555555555")
	if link == "" {
		t.Fatal("ссылка не собралась")
	}

	got := extraFromURL(t, link)
	if got != productionExtraJSON {
		t.Errorf("extra изменился, старые конфигурации сломаются\nполучено: %s\nожидалось: %s", got, productionExtraJSON)
	}

	for _, key := range []string{
		"uplinkHTTPMethod", "uplinkDataPlacement", "uplinkDataKey",
		"uplinkChunkSize", "scMaxEachPostBytes",
		"sessionIDPlacement", "sessionIDKey", "seqPlacement", "seqKey",
	} {
		if strings.Contains(got, key) {
			t.Errorf("незаданное поле %s попало в extra: %s", key, got)
		}
	}
}

// Заполненные поля должны появиться в extra ровно с теми именами, которые
// понимает ядро Xray (Xray-core PR #5414).
func TestBuildCDNVLESSURL_UplinkGetMode(t *testing.T) {
	e := baseEndpoint()
	e.UplinkHTTPMethod = "GET"
	e.UplinkDataPlacement = "header"
	e.UplinkDataKey = "X-Data"
	e.UplinkChunkSize = 4096
	e.ScMaxEachPostBytes = 4096

	got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))

	want := []string{
		`"uplinkHTTPMethod":"GET"`,
		`"uplinkDataPlacement":"header"`,
		`"uplinkDataKey":"X-Data"`,
		`"uplinkChunkSize":4096`,
		`"scMaxEachPostBytes":4096`,
	}
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("в extra нет %s\nполучено: %s", w, got)
		}
	}

	// Базовая часть обязана сохраниться целиком.
	if !strings.HasPrefix(got, `{"xPaddingObfsMode":true,"xPaddingPlacement":"cookie"`) {
		t.Errorf("базовая часть extra нарушена: %s", got)
	}

	// Незаданные поля по-прежнему не должны появляться.
	for _, key := range []string{"sessionIDPlacement", "seqPlacement", "sessionIDKey", "seqKey"} {
		if strings.Contains(got, key) {
			t.Errorf("незаданное поле %s попало в extra: %s", key, got)
		}
	}
}

// Вынос идентификатора сессии из пути — приём против CDN, которые режут
// запросы к путям, не похожим на статический файл.
func TestBuildCDNVLESSURL_SessionOutOfPath(t *testing.T) {
	e := baseEndpoint()
	e.XHTTPPath = "/api/uploadFile/chunk.js"
	e.SessionIDPlacement = "query"
	e.SessionIDKey = "sid"
	e.SeqPlacement = "query"
	e.SeqKey = "n"

	link := BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555")
	got := extraFromURL(t, link)

	for _, w := range []string{
		`"sessionIDPlacement":"query"`,
		`"sessionIDKey":"sid"`,
		`"seqPlacement":"query"`,
		`"seqKey":"n"`,
	} {
		if !strings.Contains(got, w) {
			t.Errorf("в extra нет %s\nполучено: %s", w, got)
		}
	}

	if !strings.Contains(link, url.QueryEscape("/api/uploadFile/chunk.js")) {
		t.Errorf("путь не попал в ссылку: %s", link)
	}
}

// Нулевые и отрицательные размеры трактуются как «не задано».
func TestBuildCDNVLESSURL_ZeroSizesOmitted(t *testing.T) {
	e := baseEndpoint()
	e.UplinkChunkSize = 0
	e.ScMaxEachPostBytes = -1

	got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))

	if strings.Contains(got, "uplinkChunkSize") {
		t.Errorf("нулевой uplinkChunkSize попал в extra: %s", got)
	}
	if strings.Contains(got, "scMaxEachPostBytes") {
		t.Errorf("отрицательный scMaxEachPostBytes попал в extra: %s", got)
	}
}

// Пробельные значения не должны превращаться в пустые ключи в JSON.
func TestBuildCDNVLESSURL_BlankStringsOmitted(t *testing.T) {
	e := baseEndpoint()
	e.UplinkHTTPMethod = "   "
	e.UplinkDataKey = "\t"
	e.SessionIDPlacement = " "

	got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))
	if got != productionExtraJSON {
		t.Errorf("пробельные значения изменили extra\nполучено: %s\nожидалось: %s", got, productionExtraJSON)
	}
}

// Собранный extra обязан оставаться корректным JSON при любом наборе полей.
func TestBuildCDNVLESSURL_ExtraIsValidJSON(t *testing.T) {
	cases := map[string]func(*CDNEndpoint){
		"пусто":      func(e *CDNEndpoint) {},
		"только GET": func(e *CDNEndpoint) { e.UplinkHTTPMethod = "GET" },
		"только seq": func(e *CDNEndpoint) { e.SeqPlacement = "query"; e.SeqKey = "n" },
		"всё сразу": func(e *CDNEndpoint) {
			e.UplinkHTTPMethod = "GET"
			e.UplinkDataPlacement = "header"
			e.UplinkDataKey = "X-Data"
			e.UplinkChunkSize = 4096
			e.ScMaxEachPostBytes = 4096
			e.SessionIDPlacement = "query"
			e.SessionIDKey = "sid"
			e.SeqPlacement = "query"
			e.SeqKey = "n"
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := baseEndpoint()
			mutate(&e)
			got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))

			if !strings.HasPrefix(got, "{") || !strings.HasSuffix(got, "}") {
				t.Fatalf("extra не выглядит как JSON-объект: %s", got)
			}
			if strings.Contains(got, ",,") || strings.Contains(got, "{,") || strings.Contains(got, ",}") {
				t.Fatalf("в extra лишняя запятая: %s", got)
			}
		})
	}
}

// Пустой адрес или UUID по-прежнему дают пустую ссылку.
func TestBuildCDNVLESSURL_EmptyInputs(t *testing.T) {
	e := baseEndpoint()
	e.Address = ""
	if got := BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"); got != "" {
		t.Errorf("при пустом адресе ожидалась пустая ссылка, получено: %s", got)
	}

	if got := BuildCDNVLESSURLFromEndpoint(baseEndpoint(), ""); got != "" {
		t.Errorf("при пустом UUID ожидалась пустая ссылка, получено: %s", got)
	}
}

// Параметры xmux уезжают в extra только парой: флаг без параметров и
// параметры без флага одинаково бессмысленны.
func TestBuildCDNVLESSURL_XmuxPairOnly(t *testing.T) {
	xmux := `{"maxConcurrency":"0","maxConnections":2,"cMaxReuseTimes":0,` +
		`"hMaxRequestTimes":"100-200","hMaxReusableSecs":"300-600","hKeepAlivePeriod":0}`

	t.Run("флаг без параметров", func(t *testing.T) {
		e := baseEndpoint()
		e.EnableXmux = true
		got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))
		if strings.Contains(got, "xmux") {
			t.Errorf("xmux попал в extra без параметров: %s", got)
		}
	})

	t.Run("параметры без флага", func(t *testing.T) {
		e := baseEndpoint()
		e.XmuxJSON = xmux
		got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))
		if strings.Contains(got, "xmux") {
			t.Errorf("xmux попал в extra без флага: %s", got)
		}
	})

	t.Run("флаг и параметры вместе", func(t *testing.T) {
		e := baseEndpoint()
		e.EnableXmux = true
		e.XmuxJSON = xmux
		got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))
		for _, w := range []string{`"enableXmux":true`, `"maxConnections":2`, `"hMaxRequestTimes":"100-200"`} {
			if !strings.Contains(got, w) {
				t.Errorf("в extra нет %s\nполучено: %s", w, got)
			}
		}
	})
}

// Обфускация padding из эталона попадает в extra при заполнении полей.
func TestBuildCDNVLESSURL_PaddingEtalon(t *testing.T) {
	e := baseEndpoint()
	e.PaddingPlacement = "queryInHeader"
	e.PaddingKey = "_dc"
	e.XPaddingBytes = "100-1000"
	e.XPaddingHeader = "X-Cache"

	got := extraFromURL(t, BuildCDNVLESSURLFromEndpoint(e, "11111111-2222-3333-4444-555555555555"))
	for _, w := range []string{
		`"xPaddingPlacement":"queryInHeader"`,
		`"xPaddingKey":"_dc"`,
		`"xPaddingBytes":"100-1000"`,
		`"xPaddingHeader":"X-Cache"`,
	} {
		if !strings.Contains(got, w) {
			t.Errorf("в extra нет %s\nполучено: %s", w, got)
		}
	}
}
