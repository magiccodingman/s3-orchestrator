-- -----------------------------------------------------------------------------
-- Compression Representation Metadata
--
-- Author: Alex Freidah
--
-- Adds nullable representation fields to committed and pending object rows.
-- NULL preserves every pre-compression row, so upgrades require no data rewrite.
-- -----------------------------------------------------------------------------

-- +goose Up
ALTER TABLE object_locations
    ADD COLUMN compression_algorithm TEXT,
    ADD COLUMN compression_level INTEGER,
    ADD COLUMN compression_version INTEGER,
    ADD COLUMN logical_size BIGINT;
ALTER TABLE pending_objects
    ADD COLUMN compression_algorithm TEXT,
    ADD COLUMN compression_level INTEGER,
    ADD COLUMN compression_version INTEGER,
    ADD COLUMN logical_size BIGINT;

-- +goose Down
ALTER TABLE pending_objects
    DROP COLUMN logical_size,
    DROP COLUMN compression_version,
    DROP COLUMN compression_level,
    DROP COLUMN compression_algorithm;
ALTER TABLE object_locations
    DROP COLUMN logical_size,
    DROP COLUMN compression_version,
    DROP COLUMN compression_level,
    DROP COLUMN compression_algorithm;
