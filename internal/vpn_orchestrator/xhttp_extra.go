package vpn_orchestrator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// xhttpRange validates the nonnegative int32 ranges accepted by Xray.
func xhttpRange(value string) error {
	parts := strings.Split(value, "-")
	if len(parts) > 2 || value == "" {
		return fmt.Errorf("expected a nonnegative integer or min-max range")
	}
	var first int64
	for i, part := range parts {
		if part == "" || strings.IndexFunc(part, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return fmt.Errorf("invalid range")
		}
		n, err := strconv.ParseInt(part, 10, 32)
		if err != nil || n < 0 || (i > 0 && n < first) {
			return fmt.Errorf("invalid or reversed range")
		}
		first = n
	}
	return nil
}

func buildXHTTPExtra(padding string, maxPost int, interval string, enableXmux bool, xmuxJSON string) (string, error) {
	extra := make(map[string]any)
	for key, value := range map[string]string{
		"xPaddingBytes":        strings.TrimSpace(padding),
		"scMinPostsIntervalMs": strings.TrimSpace(interval),
	} {
		if value == "" {
			continue
		}
		if err := xhttpRange(value); err != nil {
			return "", fmt.Errorf("%s: %w", key, err)
		}
		extra[key] = value
	}
	if maxPost < 0 || int64(maxPost) > 2147483647 {
		return "", fmt.Errorf("sc_max_each_post_bytes must fit a nonnegative int32")
	}
	if maxPost > 0 {
		extra["scMaxEachPostBytes"] = maxPost
	}
	if enableXmux {
		var xmux map[string]json.RawMessage
		if err := json.Unmarshal([]byte(xmuxJSON), &xmux); err != nil || len(xmux) == 0 {
			return "", fmt.Errorf("enabled xmux_json must be a nonempty JSON object")
		}
		allowed := map[string]bool{
			"maxConcurrency": true, "maxConnections": true, "cMaxReuseTimes": true,
			"hMaxRequestTimes": true, "hMaxReusableSecs": true, "hKeepAlivePeriod": true,
		}
		positive := make(map[string]bool)
		for key, raw := range xmux {
			if !allowed[key] {
				return "", fmt.Errorf("unknown XMUX field %q", key)
			}
			value := string(raw)
			if key == "hKeepAlivePeriod" && strings.HasPrefix(value, "\"") {
				return "", fmt.Errorf("xmux.hKeepAlivePeriod must be a JSON integer")
			}
			if strings.HasPrefix(value, "\"") {
				if err := json.Unmarshal(raw, &value); err != nil {
					return "", fmt.Errorf("invalid XMUX field %q", key)
				}
			}
			if err := xhttpRange(value); err != nil {
				return "", fmt.Errorf("xmux.%s: %w", key, err)
			}
			if key == "hKeepAlivePeriod" && strings.Contains(value, "-") {
				return "", fmt.Errorf("xmux.hKeepAlivePeriod must be an integer")
			}
			parts := strings.Split(value, "-")
			n, _ := strconv.ParseInt(parts[len(parts)-1], 10, 32)
			positive[key] = n > 0
		}
		if positive["maxConcurrency"] && positive["maxConnections"] {
			return "", fmt.Errorf("xmux maxConcurrency and maxConnections cannot both be positive")
		}
		// Xray enables XMUX through this object; enableXmux is an application-side flag.
		extra["xmux"] = xmux
	}
	if len(extra) == 0 {
		return "", nil
	}
	data, err := json.Marshal(extra)
	if err != nil {
		return "", fmt.Errorf("marshal XHTTP extra: %w", err)
	}
	return string(data), nil
}
