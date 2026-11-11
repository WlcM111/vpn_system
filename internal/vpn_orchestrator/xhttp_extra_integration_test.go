//go:build integration

package vpn_orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestXHTTPExtraMigrationAndRepository(t *testing.T) {
	pool := startOrchestratorDB(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO vpn_servers(server_key,node_id,country_code,title) VALUES ('relay-test','relay-test','US','test')`)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(pool)
	post := 1000000
	req := AdminXHTTPEndpointRequest{XHTTPKey: "relay-xhttp", ServerKey: "relay-test", InboundTag: "vless-xhttp-in", Address: "192.0.2.1", Path: "/test", XPaddingBytes: "100-1000", ScMaxEachPostBytes: &post, ScMinPostsIntervalMs: "10-50", EnableXmux: true, XmuxJSON: `{"maxConcurrency":"16-32","maxConnections":0}`}
	if err := repo.UpsertAdminXHTTPEndpoint(ctx, req); err != nil {
		t.Fatal(err)
	}
	rows, err := repo.ListEnabledXHTTPEndpoints(ctx)
	if err != nil || len(rows) != 1 || rows[0].ScMaxEachPostBytes != post || !rows[0].EnableXmux {
		t.Fatalf("round trip: %+v, %v", rows, err)
	}
	// Reapplying the migration must neither overwrite settings nor fail.
	sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "vpn-orchestrator-service", "0024_xhttp_padding_xmux.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	req.XmuxJSON = `{"maxConcurrency":32,"maxConnections":4}`
	if err := repo.UpsertAdminXHTTPEndpoint(ctx, req); err == nil {
		t.Fatal("conflicting XMUX limits accepted")
	}
	rows, err = repo.ListEnabledXHTTPEndpoints(ctx)
	if err != nil || len(rows) != 1 || rows[0].XmuxJSON != `{"maxConcurrency":"16-32","maxConnections":0}` {
		t.Fatalf("rejected update changed database: %+v, %v", rows, err)
	}
	for _, value := range []string{`[]`, `null`, `{broken}`, `{}`} {
		if _, err := pool.Exec(ctx, `UPDATE vpn_xhttp_endpoints SET xmux_json=$1, enable_xmux=true WHERE xhttp_key='relay-xhttp'`, value); err == nil {
			t.Fatalf("database constraint accepted %q", value)
		}
	}
}
