package gormrepo_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEventStoreAppendUsesExpectedSequenceAndWriterEpoch(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:event-store?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}

	store := gormrepo.NewEventStore(db)
	ctx := context.Background()
	if err := store.Create(ctx, session.NewSession{
		ID: "ses-001", TenantID: "tenant-001", WorkspaceID: "ws-001", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	epoch, head, err := store.ClaimWriter(ctx, "ses-001")
	if err != nil {
		t.Fatal(err)
	}
	if head != 0 {
		t.Fatalf("head = %d, want 0", head)
	}

	appended, err := store.Append(ctx, "ses-001", epoch, head, []session.NewEvent{
		{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     "session/created",
			EventID:       "evt-001",
			ReplayPolicy:  session.ReplayRequired,
			Data:          json.RawMessage(`{"workspaceId":"ws-001"}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(appended) != 1 || appended[0].Seq != 1 {
		t.Fatalf("appended = %#v", appended)
	}

	if _, err := store.Append(ctx, "ses-001", epoch, 0, []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     "turn/start",
		EventID:       "evt-002",
		ReplayPolicy:  session.ReplayRequired,
		Data:          json.RawMessage(`{}`),
	}}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale append error = %v, want conflict", err)
	}

	newEpoch, newHead, err := store.ClaimWriter(ctx, "ses-001")
	if err != nil {
		t.Fatal(err)
	}
	if newEpoch <= epoch || newHead != 1 {
		t.Fatalf("claim = (%d,%d), previous epoch %d", newEpoch, newHead, epoch)
	}
	if _, err := store.Append(ctx, "ses-001", epoch, newHead, []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     "turn/start",
		EventID:       "evt-003",
		ReplayPolicy:  session.ReplayRequired,
		Data:          json.RawMessage(`{}`),
	}}); !errors.Is(err, session.ErrWriterFenced) {
		t.Fatalf("fenced append error = %v, want writer fenced", err)
	}

	loaded, err := store.Load(ctx, "ses-001", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.ValidateSequence(loaded, 0); err != nil {
		t.Fatal(err)
	}
}

func TestEventStoreRoundTripsSurfaceMetadata(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:surface-metadata?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	store := gormrepo.NewEventStore(db)
	ctx := context.Background()
	if err := store.Create(ctx, session.NewSession{ID: "surface-session", TenantID: "tenant-001", WorkspaceID: "ws-001"}); err != nil {
		t.Fatal(err)
	}
	epoch, head, err := store.ClaimWriter(ctx, "surface-session")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "surface-session", epoch, head, []session.NewEvent{
		{
			SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor},
			EventType:     "user/message",
			EventID:       "surface-event-1",
			ReplayPolicy:  session.ReplayRequired,
			Data:          json.RawMessage(`{"role":"user","content":[]}`),
			SurfaceOp:     session.AppendSurfaceOp(),
		},
		{
			SchemaVersion:   session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor},
			EventType:       "assistant/message",
			EventID:         "surface-event-2",
			ReplayPolicy:    session.ReplayRequired,
			Data:            json.RawMessage(`{"message":{"role":"assistant","content":[{"type":"text","text":"summary"}]}}`),
			SourceEventSeqs: []uint64{1},
			SurfaceOp:       session.ReplaceSurfaceOp(1, 1),
		},
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, "surface-session", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].SurfaceOp == nil || loaded[0].SurfaceOp.Kind != session.SurfaceOpAppend {
		t.Fatalf("append surface operation not preserved: %#v", loaded)
	}
	if loaded[1].SurfaceOp == nil || *loaded[1].SurfaceOp != *session.ReplaceSurfaceOp(1, 1) {
		t.Fatalf("replacement operation not preserved: %#v", loaded[1].SurfaceOp)
	}
	if len(loaded[1].SourceEventSeqs) != 1 || loaded[1].SourceEventSeqs[0] != 1 {
		t.Fatalf("source event sequences not preserved: %v", loaded[1].SourceEventSeqs)
	}
}

func TestEventStoreValidatesForkBoundary(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:fork-boundary?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	store := gormrepo.NewEventStore(db)
	ctx := context.Background()
	if err := store.Create(ctx, session.NewSession{ID: "parent", TenantID: "tenant-001", WorkspaceID: "ws-001"}); err != nil {
		t.Fatal(err)
	}

	forkAtZero := uint64(0)
	if err := store.Create(ctx, session.NewSession{
		ID: "child", TenantID: "tenant-001", WorkspaceID: "ws-001", ParentID: "parent", ForkSeq: &forkAtZero,
	}); err != nil {
		t.Fatalf("valid fork rejected: %v", err)
	}

	missingBoundary := session.NewSession{ID: "invalid", TenantID: "tenant-001", WorkspaceID: "ws-001", ParentID: "parent"}
	if err := store.Create(ctx, missingBoundary); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("missing fork boundary error = %v", err)
	}

	futureBoundary := uint64(1)
	if err := store.Create(ctx, session.NewSession{
		ID: "future", TenantID: "tenant-001", WorkspaceID: "ws-001", ParentID: "parent", ForkSeq: &futureBoundary,
	}); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("future fork boundary error = %v", err)
	}
}

func TestEventStoreListsOnlyResumableSessionsWithStableCursor(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:resumable-sessions?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	store := gormrepo.NewEventStore(db)
	ctx := context.Background()
	for _, sessionID := range []string{"session-a", "session-b", "session-c"} {
		if err := store.Create(ctx, session.NewSession{ID: sessionID, TenantID: "tenant-001", WorkspaceID: "ws-001"}); err != nil {
			t.Fatal(err)
		}
	}
	archivedAt := time.Now().UTC()
	if err := db.Model(&gormrepo.SessionModel{}).Where("id = ?", "session-b").Update("archived_at", archivedAt).Error; err != nil {
		t.Fatal(err)
	}

	first, err := store.ListResumableSessionIDs(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.SessionIDs, []string{"session-a"}) || first.NextCursor != "session-a" {
		t.Fatalf("first page = %+v", first)
	}
	second, err := store.ListResumableSessionIDs(ctx, first.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.SessionIDs, []string{"session-c"}) || second.NextCursor != "session-c" {
		t.Fatalf("second page = %+v", second)
	}
	last, err := store.ListResumableSessionIDs(ctx, second.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(last.SessionIDs) != 0 || last.NextCursor != "" {
		t.Fatalf("last page = %+v", last)
	}
}
