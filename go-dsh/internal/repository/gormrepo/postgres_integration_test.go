//go:build integration

package gormrepo_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestPostgresMigrationAndEventStore(t *testing.T) {
	dsn := os.Getenv("DSH_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DSH_TEST_POSTGRES_DSN is not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../../db/migrations/postgres/000001_session.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	surfaceDown, err := os.ReadFile("../../../db/migrations/postgres/000002_ordered_surface.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Exec(string(surfaceDown)).Error
	_ = db.Exec(string(down)).Error
	up, err := os.ReadFile("../../../db/migrations/postgres/000001_session.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(up)).Error; err != nil {
		t.Fatalf("apply postgres migration: %v", err)
	}
	surfaceUp, err := os.ReadFile("../../../db/migrations/postgres/000002_ordered_surface.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(surfaceUp)).Error; err != nil {
		t.Fatalf("apply ordered surface migration: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Exec(string(surfaceDown)).Error
		_ = db.Exec(string(down)).Error
	})

	store := gormrepo.NewEventStore(db)
	ctx := context.Background()
	if err := store.Create(ctx, session.NewSession{ID: "pg-session", TenantID: "tenant", WorkspaceID: "workspace"}); err != nil {
		t.Fatal(err)
	}
	epoch, head, err := store.ClaimWriter(ctx, "pg-session")
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.Append(ctx, "pg-session", epoch, head, []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     "session/created",
		EventID:       "pg-event-1",
		ReplayPolicy:  session.ReplayRequired,
		Data:          json.RawMessage(`{"dialect":"postgres"}`),
		Extensions:    json.RawMessage(`{"future":true}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !sameJSON(events[0].Extensions, json.RawMessage(`{"future":true}`)) {
		t.Fatalf("unexpected events: %#v", events)
	}
	loaded, err := store.Load(ctx, "pg-session", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || !sameJSON(loaded[0].Extensions, json.RawMessage(`{"future":true}`)) {
		t.Fatalf("unexpected loaded events: %#v", loaded)
	}
}

func sameJSON(left, right json.RawMessage) bool {
	var leftValue any
	var rightValue any
	return json.Unmarshal(left, &leftValue) == nil &&
		json.Unmarshal(right, &rightValue) == nil &&
		reflect.DeepEqual(leftValue, rightValue)
}
