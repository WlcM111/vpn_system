package vpn_orchestrator

import (
	"encoding/json"
	"testing"
)

func TestXHTTPExtraDefaultsAndValidation(t *testing.T) {
	if got, err := buildXHTTPExtra("", 0, "", false, ""); err != nil || got != "" {
		t.Fatalf("legacy endpoint changed: %q, %v", got, err)
	}
	cases := []struct {
		name, padding, interval, xmux string
		post                          int
		enabled                       bool
	}{
		{name: "injected JSON", padding: `100\",\"downloadSettings\":{}`},
		{name: "reversed", padding: "1000-100"},
		{name: "negative", interval: "-1"},
		{name: "overflow", interval: "2147483648"},
		{name: "negative POST", post: -1},
		{name: "malformed XMUX", enabled: true, xmux: `{broken}`},
		{name: "array XMUX", enabled: true, xmux: `[]`},
		{name: "null XMUX", enabled: true, xmux: `null`},
		{name: "empty XMUX", enabled: true, xmux: `{}`},
		{name: "conflicting limits", enabled: true, xmux: `{"maxConcurrency":"16-32","maxConnections":2}`},
		{name: "unknown field", enabled: true, xmux: `{"downloadSettings":{}}`},
		{name: "invalid range type", enabled: true, xmux: `{"maxConcurrency":true}`},
		{name: "quoted keepalive", enabled: true, xmux: `{"hKeepAlivePeriod":"0"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildXHTTPExtra(tc.padding, tc.post, tc.interval, tc.enabled, tc.xmux); err == nil {
				t.Fatal("invalid transport configuration accepted")
			}
		})
	}
}

func TestXHTTPExtraValidJSON(t *testing.T) {
	got, err := buildXHTTPExtra("100-1000", 1000000, "10-50", true,
		`{"maxConcurrency":"16-32","maxConnections":0,"cMaxReuseTimes":"64-128","hMaxRequestTimes":"600-900","hMaxReusableSecs":"1800-3000","hKeepAlivePeriod":0}`)
	if err != nil {
		t.Fatal(err)
	}
	var extra map[string]any
	if err := json.Unmarshal([]byte(got), &extra); err != nil {
		t.Fatal(err)
	}
	if extra["xPaddingBytes"] != "100-1000" || extra["scMaxEachPostBytes"] != float64(1000000) {
		t.Fatalf("wrong transport settings: %s", got)
	}
	if _, exists := extra["enableXmux"]; exists {
		t.Fatal("application flag sent as Xray setting")
	}
	xmux, ok := extra["xmux"].(map[string]any)
	if !ok || xmux["maxConcurrency"] != "16-32" {
		t.Fatalf("wrong XMUX: %s", got)
	}
}
