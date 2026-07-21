// -------------------------------------------------------------------------------
// Core Compression Representation Tests
//
// Author: Alex Freidah
// -------------------------------------------------------------------------------

package core

import "testing"

func TestObjectLocationClientSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		loc  ObjectLocation
		want int64
	}{
		{name: "plain", loc: ObjectLocation{SizeBytes: 100}, want: 100},
		{name: "encrypted legacy", loc: ObjectLocation{SizeBytes: 140, Encrypted: true, PlaintextSize: 100}, want: 100},
		{name: "compressed", loc: ObjectLocation{SizeBytes: 40, CompressionAlgorithm: "zstd", LogicalSize: 100}, want: 100},
		{name: "compressed then encrypted", loc: ObjectLocation{SizeBytes: 80, Encrypted: true, PlaintextSize: 40, CompressionAlgorithm: "zstd", LogicalSize: 100}, want: 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.loc.ClientSize(); got != tc.want {
				t.Fatalf("ClientSize() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRepresentationMetaIncludesCompression(t *testing.T) {
	t.Parallel()

	loc := ObjectLocation{
		SizeBytes:            80,
		Encrypted:            true,
		EncryptionKey:        []byte("wrapped"),
		KeyID:                "key-1",
		PlaintextSize:        40,
		ContentHash:          "hash",
		CompressionAlgorithm: "zstd",
		CompressionLevel:     3,
		CompressionVersion:   1,
		LogicalSize:          100,
	}
	meta := loc.RepresentationMeta()
	if meta == nil || !meta.Encrypted || !meta.Compressed() || meta.LogicalSize != 100 || meta.PlaintextSize != 40 {
		t.Fatalf("RepresentationMeta() = %+v", meta)
	}
}
