package db

import (
	"context"
	"path/filepath"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestMigrationIdempotencyAndChecksum(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	db, _ := database.DB()
	defer db.Close()
	for range 2 {
		if err := Migrate(context.Background(), database); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := database.Raw("SELECT count(*) FROM dsh_schema_migrations").Scan(&count).Error; err != nil || count != 2 {
		t.Fatalf("count %d err %v", count, err)
	}
	if !database.Migrator().HasColumn("session_events", "surface_op") {
		t.Fatal("missing surface column")
	}
	if err := database.Exec("UPDATE dsh_schema_migrations SET checksum = ?", "altered").Error; err != nil {
		t.Fatal(err)
	}
	if err := Migrate(context.Background(), database); err == nil {
		t.Fatal("changed migration accepted")
	}
}
