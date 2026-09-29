// Package db embeds immutable SQL migrations so the server is a standalone binary.
package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

//go:embed migrations/*/*.up.sql
var migrations embed.FS

// Migrate only applies forward migrations. Existing unversioned schemas fail
// closed instead of silently assuming their contents or dropping user data.
func Migrate(ctx context.Context, database *gorm.DB) error {
	dialect := database.Dialector.Name()
	if dialect != "sqlite" && dialect != "postgres" {
		return fmt.Errorf("unsupported database dialect")
	}
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if dialect == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(734092851)").Error; err != nil {
				return err
			}
		}
		if err := tx.Exec("CREATE TABLE IF NOT EXISTS dsh_schema_migrations (version VARCHAR(100) PRIMARY KEY, checksum VARCHAR(64) NOT NULL)").Error; err != nil {
			return err
		}
		entries, err := migrations.ReadDir("migrations/" + dialect)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".up.sql") {
				continue
			}
			data, err := migrations.ReadFile("migrations/" + dialect + "/" + entry.Name())
			if err != nil {
				return err
			}
			checksum := fmt.Sprintf("%x", sha256.Sum256(data))
			var rows []struct{ Checksum string }
			if err := tx.Raw("SELECT checksum FROM dsh_schema_migrations WHERE version = ?", entry.Name()).Scan(&rows).Error; err != nil {
				return err
			}
			if len(rows) != 0 {
				if rows[0].Checksum != checksum {
					return fmt.Errorf("migration checksum mismatch: %s", entry.Name())
				}
				continue
			}
			if err := tx.Exec(string(data)).Error; err != nil {
				return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
			}
			if err := tx.Exec("INSERT INTO dsh_schema_migrations (version, checksum) VALUES (?, ?)", entry.Name(), checksum).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
