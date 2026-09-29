package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

func config(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	return Config{Mode: "demo", DBPath: filepath.Join(root, "dsh.db"), Workspace: filepath.Join(root, "workspace"), SystemPrompt: "测试助手"}
}
func awaitTurn(t *testing.T, a *App, id string) []session.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := a.Harness.ListEvents(ctx, id, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.EventType == "turn/end" {
				return events
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("turn did not finish")
		case <-ticker.C:
		}
	}
}
func TestBootToolTurnAndRestart(t *testing.T) {
	ctx := context.Background()
	cfg := config(t)
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id, err := a.Harness.CreateSession(ctx, "持久化验收")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Harness.Prompt(ctx, id, "使用工具", "once"); err != nil {
		t.Fatal(err)
	}
	events := awaitTurn(t, a, id)
	calls, results, steps := 0, 0, 0
	for _, e := range events {
		switch e.EventType {
		case "tool/call":
			calls++
		case "tool/result":
			results++
		case "step/start":
			steps++
		}
	}
	if calls != 1 || results != 1 || steps != 2 {
		t.Fatalf("calls=%d results=%d steps=%d", calls, results, steps)
	}
	if err = a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	a, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	list, err := a.Harness.ListSessions(ctx)
	if err != nil || len(list) != 1 || list[0].ID != id {
		t.Fatalf("list=%v err=%v", list, err)
	}
	after, err := a.Harness.ListEvents(ctx, id, 0, 1000)
	if err != nil || len(after) != len(events) {
		t.Fatalf("recovery changed completed history %d -> %d: %v", len(events), len(after), err)
	}
}
func TestRejectDatabaseCaseAlias(t *testing.T) {
	cfg := config(t)
	if err := os.MkdirAll(cfg.Workspace, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(cfg.Workspace), "WORKSPACE")
	rootInfo, err := os.Stat(cfg.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	aliasInfo, err := os.Stat(alias)
	if os.IsNotExist(err) {
		t.Skip("case-sensitive filesystem")
	}
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(rootInfo, aliasInfo) {
		t.Skip("alias is a different directory")
	}
	cfg.DBPath = filepath.Join(alias, "secret.db")
	if _, err := Open(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("database case alias accepted: %v", err)
	}
	if _, err := os.Stat(cfg.DBPath); !os.IsNotExist(err) {
		t.Fatalf("database file was created before isolation: %v", err)
	}
}

func TestRejectDangerousConfiguration(t *testing.T) {
	for _, address := range []string{":8080", "0.0.0.0:8080", "localhost:8080", "[::]:8080", "10.0.0.1:8080"} {
		if ValidateListenAddress(address) == nil {
			t.Fatalf("accepted %s", address)
		}
	}
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080"} {
		if err := ValidateListenAddress(address); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config(t)
	cfg.DBPath = filepath.Join(cfg.Workspace, "secret.db")
	if _, err := Open(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("workspace database accepted: %v", err)
	}
	cfg = config(t)
	cfg.APIKey = "secret-key"
	if _, err := Open(context.Background(), cfg); err == nil || strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("ambiguous demo mode: %v", err)
	}
	cfg = config(t)
	cfg.Mode = "openai"
	if _, err := Open(context.Background(), cfg); err == nil {
		t.Fatal("missing credentials accepted")
	}
}
