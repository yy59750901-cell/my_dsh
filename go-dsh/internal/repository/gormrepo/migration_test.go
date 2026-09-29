package gormrepo_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSQLiteInitialMigrationUpAndDown(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:migration?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	up, err := os.ReadFile("../../../db/migrations/sqlite/000001_session.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(up)).Error; err != nil {
		t.Fatalf("apply up migration: %v", err)
	}

	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('sessions', 'session_events', 'session_projections')").Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("table count = %d, want 3", count)
	}

	now := time.Now().UTC()
	if err := db.Exec(`INSERT INTO sessions
		(id, tenant_id, workspace_id, status, last_seq, actor_epoch, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"legacy-session", "tenant", "workspace", "idle", 1, 0, now, now,
	).Error; err != nil {
		t.Fatalf("insert legacy session: %v", err)
	}
	if err := db.Exec(`INSERT INTO session_events
		(session_id, seq, event_id, type, schema_major, schema_minor, replay_policy,
		 occurred_at, committed_at, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"legacy-session", 1, "legacy-event", "user/message", 1, 0, "required",
		now, now, `{"text":"legacy"}`, now,
	).Error; err != nil {
		t.Fatalf("insert legacy event: %v", err)
	}

	surfaceUp, err := os.ReadFile("../../../db/migrations/sqlite/000002_ordered_surface.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(surfaceUp)).Error; err != nil {
		t.Fatalf("upgrade existing schema with ordered surface: %v", err)
	}
	var surfaceColumns int64
	if err := db.Raw("SELECT COUNT(*) FROM pragma_table_info('session_events') WHERE name = 'surface_op'").Scan(&surfaceColumns).Error; err != nil {
		t.Fatal(err)
	}
	if surfaceColumns != 1 {
		t.Fatalf("surface_op column count = %d, want 1", surfaceColumns)
	}
	store := gormrepo.NewEventStore(db)
	epoch, head, err := store.ClaimWriter(context.Background(), "legacy-session")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := session.Replay(context.Background(), store, "legacy-session", epoch, head, session.ReplayOptions{})
	if err != nil {
		t.Fatalf("replay migrated legacy event: %v", err)
	}
	if nodes := snapshot.Surface().Nodes(); len(nodes) != 1 || nodes[0] != 1 {
		t.Fatalf("legacy surface nodes = %v, want [1]", nodes)
	}

	surfaceDown, err := os.ReadFile("../../../db/migrations/sqlite/000002_ordered_surface.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(surfaceDown)).Error; err != nil {
		t.Fatalf("apply ordered surface down migration: %v", err)
	}

	down, err := os.ReadFile("../../../db/migrations/sqlite/000001_session.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(down)).Error; err != nil {
		t.Fatalf("apply down migration: %v", err)
	}
}
