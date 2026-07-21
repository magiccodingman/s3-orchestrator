// -------------------------------------------------------------------------------
// Multipart Final-Object Compression Tests
//
// Author: Alex Freidah
//
// Pins the final assembly representation: multipart parts remain ordinary
// temporary objects, while the completed object is one replayable Zstandard
// frame with logical-size metadata for the original concatenated plaintext.
// -------------------------------------------------------------------------------

package multipart

import (
	"bytes"
	"io"
	"testing"

	"github.com/afreidah/s3-orchestrator/internal/compression"
)

func TestPrepareAssembledRepresentation_CompressesFinalObject(t *testing.T) {
	t.Parallel()

	codec := compression.New(3)
	mp := &Manager{compressor: codec, compressWrites: true, compressionLevel: 3}
	plaintext := bytes.Repeat([]byte("multipart-final-object\n"), 4096)
	representation, err := mp.prepareAssembledRepresentation(bytes.NewReader(plaintext), int64(len(plaintext)))
	if err != nil {
		t.Fatalf("prepareAssembledRepresentation: %v", err)
	}
	defer representation.Cleanup()
	if representation.size >= int64(len(plaintext)) {
		t.Fatalf("stored size = %d, want smaller than %d", representation.size, len(plaintext))
	}
	if representation.meta == nil || representation.meta.CompressionAlgorithm != compression.AlgorithmZstd ||
		representation.meta.CompressionLevel != 3 || representation.meta.CompressionVersion != compression.FormatVersion ||
		representation.meta.LogicalSize != int64(len(plaintext)) {
		t.Fatalf("compression metadata = %+v", representation.meta)
	}

	decoded, err := codec.NewReader(io.NopCloser(representation.body))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("read decoded representation: %v", err)
	}
	if err := decoded.Close(); err != nil {
		t.Fatalf("close decoded representation: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatal("decoded final representation does not match concatenated plaintext")
	}
}
