// -------------------------------------------------------------------------------
// SQLite Compression Metadata Tests
//
// Author: Alex Freidah
//
// Covers representation round trips, replica propagation, logical listing size,
// and the in-place v2-to-v3 migration for existing single-instance databases.
// -------------------------------------------------------------------------------

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/afreidah/s3-orchestrator/internal/config"
	"github.com/afreidah/s3-orchestrator/internal/store/core"
)

func TestCompressionMetadata_RoundTripAndReplica(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	ctx := context.Background()
	meta := &core.EncryptionMeta{
		CompressionAlgorithm: "zstd",
		CompressionLevel:     3,
		CompressionVersion:   1,
		LogicalSize:          8192,
		ContentHash:          "logical-hash",
	}
	if _, err := s.RecordObject(ctx, "bucket/compressed", "backend-a", 311, meta); err != nil {
		t.Fatalf("RecordObject: %v", err)
	}

	locs, err := s.GetAllObjectLocations(ctx, "bucket/compressed")
	if err != nil {
		t.Fatalf("GetAllObjectLocations: %v", err)
	}
	if len(locs) != 1 {
		t.Fatalf("locations = %d, want 1", len(locs))
	}
	assertCompressionLocation(t, locs[0], "backend-a", 311, 8192)

	physicalSize, inserted, err := s.RecordReplica(ctx, "bucket/compressed", "backend-b", "backend-a")
	if err != nil {
		t.Fatalf("RecordReplica: %v", err)
	}
	if !inserted || physicalSize != 311 {
		t.Fatalf("RecordReplica inserted=%v size=%d, want true/311", inserted, physicalSize)
	}
	locs, err = s.GetAllObjectLocations(ctx, "bucket/compressed")
	if err != nil {
		t.Fatalf("GetAllObjectLocations after replica: %v", err)
	}
	if len(locs) != 2 {
		t.Fatalf("locations after replica = %d, want 2", len(locs))
	}
	for _, loc := range locs {
		assertCompressionLocation(t, loc, loc.BackendName, 311, 8192)
	}

	page, err := s.ListObjects(ctx, "bucket/", "", 10)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(page.Objects) != 1 || page.Objects[0].SizeBytes != 8192 {
		t.Fatalf("logical listing = %+v, want one 8192-byte object", page.Objects)
	}
}

func assertCompressionLocation(t *testing.T, loc core.ObjectLocation, backend string, physical, logical int64) {
	t.Helper()
	if loc.BackendName != backend || loc.SizeBytes != physical || loc.ClientSize() != logical ||
		loc.CompressionAlgorithm != "zstd" || loc.CompressionLevel != 3 ||
		loc.CompressionVersion != 1 || loc.LogicalSize != logical || loc.ContentHash != "logical-hash" {
		t.Fatalf("location = %+v", loc)
	}
}

func TestRunMigrations_V2ToV3PreservesLegacyRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy sqlite: %v", err)
	}
	legacyDDL := `
CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version(version) VALUES (2);
CREATE TABLE backend_quotas (
    backend_name TEXT PRIMARY KEY,
    bytes_used INTEGER NOT NULL DEFAULT 0,
    bytes_limit INTEGER NOT NULL,
    orphan_bytes INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT
);
CREATE TABLE object_locations (
    object_key TEXT NOT NULL,
    backend_name TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    size_bytes INTEGER NOT NULL,
    encrypted INTEGER NOT NULL DEFAULT 0,
    encryption_key BLOB,
    key_id TEXT,
    plaintext_size INTEGER,
    content_hash TEXT,
    created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (object_key, backend_name)
);
CREATE TABLE pending_objects (
    intent_id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    backend_name TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    size_bytes INTEGER NOT NULL,
    encrypted INTEGER NOT NULL DEFAULT 0,
    encryption_key BLOB,
    key_id TEXT,
    plaintext_size INTEGER,
    content_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
INSERT INTO backend_quotas(backend_name, bytes_used, bytes_limit) VALUES ('backend-a', 123, 1048576);
INSERT INTO object_locations(object_key, backend_name, size_bytes) VALUES ('bucket/legacy', 'backend-a', 123);
`
	if _, err := db.ExecContext(ctx, legacyDDL); err != nil {
		_ = db.Close()
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy sqlite: %v", err)
	}

	s, err := NewStore(ctx, &config.DatabaseConfig{Driver: "sqlite", Path: path}, nil)
	if err != nil {
		t.Fatalf("NewStore migration: %v", err)
	}
	defer s.Close()
	if err := s.VerifySchemaVersion(ctx); err != nil {
		t.Fatalf("VerifySchemaVersion: %v", err)
	}

	locs, err := s.GetAllObjectLocations(ctx, "bucket/legacy")
	if err != nil {
		t.Fatalf("GetAllObjectLocations: %v", err)
	}
	if len(locs) != 1 || locs[0].Compressed() || locs[0].ClientSize() != 123 {
		t.Fatalf("legacy location changed meaning: %+v", locs)
	}
	unencrypted, err := s.ListUnencryptedLocations(ctx, 10, 0)
	if err != nil {
		t.Fatalf("ListUnencryptedLocations on NULL metadata: %v", err)
	}
	if len(unencrypted) != 1 || unencrypted[0].CompressionAlgorithm != "" || unencrypted[0].LogicalSize != 0 {
		t.Fatalf("legacy admin row = %+v", unencrypted)
	}
}
