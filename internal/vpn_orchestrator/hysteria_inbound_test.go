package vpn_orchestrator

import (
	"encoding/json"
	"strings"
	"testing"

	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Hysteria2 внутри Xray (нода-вход каскада): учётка пользователя
// регистрируется в Hysteria2-инбаунде Xray тем же email и UUID. У обычных нод
// (inbound_tag пустой) набор профилей не меняется.
// ============================================================================

func cascadeHysteriaEndpoint() HysteriaEndpoint {
	return HysteriaEndpoint{
		HysteriaKey: "cas-main-1-hysteria", ServerKey: "cas-main-1", Enabled: true,
		Address: "cas.race-src.com", Port: 443, SNI: "cas.race-src.com",
		Remarks: "🔀 Каскад - Маленький пинг (игры)", InboundTag: "hysteria-in",
	}
}

func cascadeBaseProfile() kafkacontracts.VPNNodeUserProfile {
	return kafkacontracts.VPNNodeUserProfile{
		ItemKey: "cas-main", InboundTag: "vless-reality-in",
		Email: "tg-1-cas-main@vpn-platform.local", VLESSUUID: testUUID,
	}
}

func TestHysteriaInboundProfileForCascadeNode(t *testing.T) {
	svc := &Service{}
	reality := baseRealityEndpoint()
	reality.ServerKey = "cas-main-1"
	xhttp := testXHTTPEndpoint()
	xhttp.ServerKey = "cas-main-1"

	profiles := svc.buildUserProfilesWithHysteria(cascadeBaseProfile(), "cas-main-1", nil,
		[]XHTTPEndpoint{xhttp}, []RealityEndpoint{reality}, []HysteriaEndpoint{cascadeHysteriaEndpoint()}, false)
	if len(profiles) != 3 {
		t.Fatalf("профилей %d, ожидалось 3 (Reality, XHTTP, Hysteria2): %+v", len(profiles), profiles)
	}
	h, n := profileByTag(profiles, "hysteria-in")
	if n != 1 {
		t.Fatalf("учёток в Hysteria2-инбаунде %d, ожидалась 1", n)
	}
	if h.Protocol != kafkacontracts.ProfileProtocolHysteria || h.Flow != "" || !h.Optional ||
		h.Email != cascadeBaseProfile().Email || h.VLESSUUID != testUUID {
		t.Errorf("Hysteria2-учётка: protocol=%q flow=%q optional=%v email=%q", h.Protocol, h.Flow, h.Optional, h.Email)
	}
	for _, p := range profiles {
		if p.InboundTag != "hysteria-in" && p.Protocol != kafkacontracts.ProfileProtocolVLESS {
			t.Errorf("VLESS-профиль %s с протоколом %q", p.InboundTag, p.Protocol)
		}
	}
}

// Обычная нода: inbound_tag пустой — профилей ровно столько же, сколько без
// Hysteria2-эндпоинтов.
func TestHysteriaWithoutInboundTagAddsNoProfile(t *testing.T) {
	svc := &Service{}
	h := cascadeHysteriaEndpoint()
	h.ServerKey, h.InboundTag = "ee-main-1", " "
	base := cascadeBaseProfile()
	with := svc.buildUserProfilesWithHysteria(base, "ee-main-1", nil, nil, []RealityEndpoint{baseRealityEndpoint()}, []HysteriaEndpoint{h}, false)
	without := svc.buildUserProfiles(base, "ee-main-1", nil, nil, []RealityEndpoint{baseRealityEndpoint()}, false)
	if len(with) != len(without) {
		t.Fatalf("Hysteria2 без inbound_tag добавил профиль: %+v", with)
	}
	for i := range with {
		if with[i] != without[i] {
			t.Errorf("профиль %d изменился: %+v / %+v", i, with[i], without[i])
		}
	}
}

// Эндпоинт чужого сервера профиля не даёт.
func TestHysteriaInboundOtherServerIgnored(t *testing.T) {
	svc := &Service{}
	profiles := svc.buildUserProfilesWithHysteria(cascadeBaseProfile(), "ro-main-1", nil, nil, nil,
		[]HysteriaEndpoint{cascadeHysteriaEndpoint()}, false)
	if _, n := profileByTag(profiles, "hysteria-in"); n != 0 {
		t.Fatalf("профиль чужого Hysteria2-эндпоинта: %+v", profiles)
	}
}

// Ссылка Hysteria2 от тега инбаунда не зависит: пароль — UUID, адрес и SNI —
// эндпоинта.
func TestHysteriaInboundTagDoesNotChangeLink(t *testing.T) {
	e := cascadeHysteriaEndpoint()
	withTag := BuildHysteriaURL(e, testUUID)
	e.InboundTag = ""
	if withTag != BuildHysteriaURL(e, testUUID) {
		t.Fatalf("ссылка зависит от inbound_tag: %s", withTag)
	}
	if !strings.HasPrefix(withTag, "hysteria2://"+testUUID+"@cas.race-src.com:443?sni=cas.race-src.com") {
		t.Fatalf("ссылка: %s", withTag)
	}
}

// Поле protocol в команде: у VLESS отсутствует (старые агенты видят прежний
// формат), у Hysteria2 — "hysteria".
func TestProfileProtocolJSON(t *testing.T) {
	vless, err := json.Marshal(cascadeBaseProfile())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(vless), `"protocol"`) {
		t.Errorf("у VLESS-профиля есть поле protocol: %s", vless)
	}
	p := cascadeBaseProfile()
	p.Protocol = kafkacontracts.ProfileProtocolHysteria
	hy, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(hy), `"protocol":"hysteria"`) {
		t.Errorf("у Hysteria2-профиля нет protocol=hysteria: %s", hy)
	}
}
