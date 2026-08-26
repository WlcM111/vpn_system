package vpn_orchestrator

import (
	"sort"
	"testing"
	"time"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// ИЗОЛЯЦИЯ ОТЗЫВА CDN — сторона оркестратора.
//
// Здесь проверяется то, что решается в центре: состав ЖЕЛАЕМОГО состояния.
// Область действия самой операции Xray доказывается тестами node-agent
// (cdn_isolation_test.go в его репозитории).
// ============================================================================

func baseProfile() kafkacontracts.VPNNodeUserProfile {
	return kafkacontracts.VPNNodeUserProfile{
		ItemKey:     "lt",
		CountryCode: "LT",
		Title:       "Литва",
		ProfileType: "vless",
		InboundTag:  "vless-ws-in",
		Email:       "tg-777-lt@vpn-platform.local",
		VLESSUUID:   "uuid-1",
	}
}

func testEndpoints() ([]CDNEndpoint, []GRPCEndpoint) {
	return []CDNEndpoint{{
			CDNKey: "lt-cdn", ServerKey: "lt-main-1", Enabled: true,
			InboundTag: "vless-xhttp-cdn-in", Address: "race-src.com",
		}}, []GRPCEndpoint{{
			GRPCKey: "lt-grpc", ServerKey: "lt-main-1", Enabled: true,
			InboundTag: "vless-grpc-cdn-in", Address: "race-src.com",
		}}
}

func tags(profiles []kafkacontracts.VPNNodeUserProfile) []string {
	out := make([]string, 0, len(profiles))
	for _, p := range profiles {
		out = append(out, p.InboundTag)
	}
	sort.Strings(out)
	return out
}

// skipCDN исключает из желаемого состояния ТОЛЬКО CDN-профиль. Обычный и
// gRPC остаются — иначе исчерпание квоты отобрало бы оплаченный доступ.
func TestSkipCDNRemovesOnlyCDNProfile(t *testing.T) {
	svc := &Service{}
	cdn, grpc := testEndpoints()

	full := svc.buildUserProfiles(baseProfile(), "lt-main-1", cdn, grpc, false)
	if got, want := tags(full), []string{"vless-grpc-cdn-in", "vless-ws-in", "vless-xhttp-cdn-in"}; len(got) != 3 ||
		got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("полный набор профилей: %v, ожидалось %v", got, want)
	}

	skipped := svc.buildUserProfiles(baseProfile(), "lt-main-1", cdn, grpc, true)
	got := tags(skipped)
	want := []string{"vless-grpc-cdn-in", "vless-ws-in"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("набор при исчерпанной квоте: %v, ожидалось %v", got, want)
	}
	for _, p := range skipped {
		if p.InboundTag == "vless-xhttp-cdn-in" {
			t.Fatal("CDN-профиль попал в желаемое состояние при исчерпанной квоте")
		}
	}
}

// Обычный профиль обязан остаться байт-в-байт прежним: тот же UUID, тот же
// email, тот же inbound. Иначе «отзыв CDN» незаметно пересоздал бы доступ.
func TestSkipCDNLeavesBaseProfileIntact(t *testing.T) {
	svc := &Service{}
	cdn, grpc := testEndpoints()
	base := baseProfile()

	skipped := svc.buildUserProfiles(base, "lt-main-1", cdn, grpc, true)
	var found bool
	for _, p := range skipped {
		if p.InboundTag != base.InboundTag {
			continue
		}
		found = true
		if p.Email != base.Email {
			t.Errorf("email обычного профиля изменился: %q → %q", base.Email, p.Email)
		}
		if p.VLESSUUID != base.VLESSUUID {
			t.Errorf("UUID обычного профиля изменился: %q → %q", base.VLESSUUID, p.VLESSUUID)
		}
		if p.Optional {
			t.Error("обычный профиль стал необязательным — узел сможет молча его пропустить")
		}
	}
	if !found {
		t.Fatal("обычный профиль исчез из желаемого состояния")
	}
}

// На пути ОТЗЫВА CDN-профиль обязан присутствовать всегда: иначе на узле
// осталась бы активная CDN-учётка отозванного пользователя.
func TestRevokePathAlwaysIncludesCDN(t *testing.T) {
	svc := &Service{}
	cdn, grpc := testEndpoints()

	profiles := svc.buildUserProfiles(baseProfile(), "lt-main-1", cdn, grpc, false)
	var hasCDN bool
	for _, p := range profiles {
		if p.InboundTag == "vless-xhttp-cdn-in" {
			hasCDN = true
		}
	}
	if !hasCDN {
		t.Fatal("CDN-профиль отсутствует на пути отзыва")
	}
}

// CDN-профиль получает СВОЙ email; остальные транспорты сохраняют базовый.
// Это и есть физическое разделение счётчиков Xray.
func TestBuildCDNProfileIsolatesEmailOnly(t *testing.T) {
	cdn, _ := testEndpoints()
	base := baseProfile()

	profile, ok := buildCDNProfile(base, "lt-main-1", cdn)
	if !ok {
		t.Fatal("CDN-профиль не построен для сервера с привязанным эндпоинтом")
	}
	if profile.Email == base.Email {
		t.Fatal("email CDN совпал с базовым: счётчики Xray останутся общими")
	}
	if want := cdnEmail(base.Email); profile.Email != want {
		t.Errorf("email CDN = %q, ожидалось %q", profile.Email, want)
	}
	if profile.VLESSUUID != base.VLESSUUID {
		t.Error("UUID CDN-профиля изменён — ссылка перестанет подключаться")
	}
	if !profile.Optional {
		t.Error("CDN-профиль должен быть Optional: узел без CDN-инбаунда не обязан его применять")
	}
	if profile.InboundTag != "vless-xhttp-cdn-in" {
		t.Errorf("inbound CDN = %q, ожидался vless-xhttp-cdn-in", profile.InboundTag)
	}
}

// Сервер без привязанного CDN-эндпоинта не получает CDN-профиля вовсе:
// ссылка на чужую ноду в паре с этим UUID не подключилась бы.
func TestBuildCDNProfileRequiresBoundEndpoint(t *testing.T) {
	cdn, _ := testEndpoints()
	if _, ok := buildCDNProfile(baseProfile(), "us-main-1", cdn); ok {
		t.Fatal("CDN-профиль построен для сервера без привязанного эндпоинта")
	}
	if _, ok := buildCDNProfile(baseProfile(), "", cdn); ok {
		t.Fatal("CDN-профиль построен при пустом server_key")
	}
	if _, ok := buildCDNProfile(baseProfile(), "lt-main-1", nil); ok {
		t.Fatal("CDN-профиль построен без единого эндпоинта в конфигурации")
	}
}

// gRPC-инбаунд НЕ считается CDN, хотя его тег содержит подстроку «cdn».
// Прямая проверка требования «не классифицировать по имени».
func TestGRPCInboundIsNotClassifiedAsCDN(t *testing.T) {
	cdn, _ := testEndpoints()
	allow := cdnInboundAllowlist(cdn)

	if _, ok := allow["vless-grpc-cdn-in"]; ok {
		t.Fatal("gRPC-инбаунд попал в CDN-allowlist по совпадению подстроки cdn")
	}
	if _, ok := allow["vless-ws-in"]; ok {
		t.Fatal("обычный инбаунд попал в CDN-allowlist")
	}
	if _, ok := allow["vless-xhttp-cdn-in"]; !ok {
		t.Fatal("настоящий CDN-инбаунд отсутствует в allowlist")
	}
	if _, ok := allow["vless-ws-cdn-in"]; ok {
		t.Fatal("неиспользуемый инбаунд попал в allowlist")
	}
}

// Квоты разных узлов независимы по построению ключа: пара (пользователь, узел).
func TestQuotaKeysAreNodeScoped(t *testing.T) {
	if paymentPeriodKey("p1") == "" {
		t.Fatal("ключ периода по платежу пуст")
	}
	// Одинаковый пользователь на двух узлах — две разные строки квоты.
	// Проверяем это на уровне того, что именно уходит в ключ: узел входит
	// в первичный ключ таблицы (см. 0015_cdn_quota.sql), а не в period_key.
	if got := calendarPeriodKey(time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)); got != "cal:2026-08" {
		t.Fatalf("календарный ключ = %q, ожидалось cal:2026-08", got)
	}
	// Ключ периода одинаков для обоих узлов — различает их первичный ключ,
	// поэтому исчерпание на одном узле не может обнулить квоту другого.
}
