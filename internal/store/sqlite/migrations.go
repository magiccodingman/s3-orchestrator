// -------------------------------------------------------------------------------
// SQLite Migrations - Schema Initialization and Version Management
//
// Author: Alex Freidah
//
// Manages the SQLite schema lifecycle. Embeds the consolidated schema DDL,
// applies it on first run, and performs supported in-place upgrades for existing
// single-instance deployments.
// -------------------------------------------------------------------------------

package sqlite

import (
	"context"
	"database/sql"
	_ "embed" // required for //go:embed directive on migrationFS
	"fmt"
	"log/slog"

	"github.com/afreidah/s3-orchestrator/internal/observe/logfmt"
)

//go:embed schema.sql
var schemaSQL string

// expectedSchemaVersion is the SQLite schema version this binary expects.
// Bump this when the embedded schema.sql is updated.
const expectedSchemaVersion = 3

// RunMigrations applies the embedded SQLite schema if the database has not
// been initialised yet. If schema_version already exists and the version
// matches, this is a no-op. If the version does not match, an error is
// returned so the operator can take corrective action.
func (s *Store) RunMigrations(ctx context.Context) error {
	version, exists, err := s.currentSchemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("check schema version: %w", err)
	}

	if exists {
		switch version {
		case expectedSchemaVersion:
			slog.InfoContext(ctx, "SQLite schema up to date", logfmt.Component("sqlite_store"), "version", version)
			return nil
		case 2:
			return s.migrateV2ToV3(ctx)
		default:
			return fmt.Errorf("SQLite schema version %d does not match expected %d  -  manual migration required", version, expectedSchemaVersion)
		}
	}

	// Fresh database: apply the full schema inside a transaction.
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply sqlite schema: %w", err)
	}

	slog.InfoContext(ctx, "SQLite schema applied",
		logfmt.Component("sqlite_store"),
		"version", expectedSchemaVersion,
	)
	return nil
}

// migrateV2ToV3 adds nullable compression metadata. Existing rows retain NULL
// and therefore keep their historical uncompressed interpretation.
func (s *Store) migrateV2ToV3(ctx context.Context) error {
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		statements := []string{
			"ALTER TABLE object_locations ADD COLUMN compression_algorithm TEXT",
			"ALTER TABLE object_locations ADD COLUMN compression_level INTEGER",
			"ALTER TABLE object_locations ADD COLUMN compression_version INTEGER",
			"ALTER TABLE object_locations ADD COLUMN logical_size INTEGER",
			"ALTER TABLE pending_objects ADD COLUMN compression_algorithm TEXT",
			"ALTER TABLE pending_objects ADD COLUMN compression_level INTEGER",
			"ALTER TABLE pending_objects ADD COLUMN compression_version INTEGER",
			"ALTER TABLE pending_objects ADD COLUMN logical_size INTEGER",
			"DELETE FROM schema_version",
			"INSERT INTO schema_version (version) VALUES (3)",
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("migrate sqlite schema v2 to v3: %w", err)
	}
	slog.InfoContext(ctx, "SQLite schema migrated", logfmt.Component("sqlite_store"), "from_version", 2, "to_version", 3)
	return nil
}

// VerifySchemaVersion checks that the database schema version matches what
// this binary expects. Returns an error if schema_version is missing or if
// the recorded version is older than expected. Logs a warning if the schema
// is newer (possible downgrade).
func (s *Store) VerifySchemaVersion(ctx context.Context) error {
	version, exists, err := s.currentSchemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("query schema version: %w", err)
	}
	if !exists {
		return fmt.Errorf("schema_version table does not exist  -  database not initialised")
	}

	if version < expectedSchemaVersion {
		return fmt.Errorf(
			"SQLite schema version %d is older than expected %d  -  migrations may have partially failed",
			version, expectedSchemaVersion,
		)
	}
	if version > expectedSchemaVersion {
		return fmt.Errorf(
			"SQLite schema version %d is newer than expected %d  -  binary is outdated",
			version, expectedSchemaVersion,
		)
	}
	return nil
}

// currentSchemaVersion returns the version from the schema_version table.
// If the table does not exist, exists is false and version is 0.
func (s *Store) currentSchemaVersion(ctx context.Context) (version int, exists bool, err error) {
	// Check whether the schema_version table exists.
	var count int
	err = s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_version'",
	).Scan(&count)
	if err != nil {
		return 0, false, fmt.Errorf("check sqlite_master: %w", err)
	}
	if count == 0 {
		return 0, false, nil
	}

	err = s.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0) FROM schema_version",
	).Scan(&version)
	if err != nil && err != sql.ErrNoRows {
		return 0, true, fmt.Errorf("read schema_version: %w", err)
	}
	return version, true, nil
}
