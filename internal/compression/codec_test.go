// -------------------------------------------------------------------------------
// Compression - Streaming Zstandard Codec Tests
//
// Author: Alex Freidah
//
// Covers round trips for empty, compressible, and incompressible payloads plus
// malformed-frame rejection. The tests consume the public replayable-body API
// so they also pin the handoff between compression and write failover.
// -------------------------------------------------------------------------------

package compression

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
)

func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty"},
		{name: "compressible", data: bytes.Repeat([]byte("transparent compression\n"), 4096)},
		{name: "incompressible", data: randomBytes(256 * 1024)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			codec := New(3)
			body, err := codec.Compress(bytes.NewReader(tc.data), int64(len(tc.data)))
			if err != nil {
				t.Fatalf("Compress() error = %v", err)
			}
			defer body.Cleanup()
			stored, err := body.Reader()
			if err != nil {
				t.Fatalf("Reader() error = %v", err)
			}
			decoded, err := codec.NewReader(io.NopCloser(stored))
			if err != nil {
				t.Fatalf("NewReader() error = %v", err)
			}
			defer decoded.Close()
			got, err := io.ReadAll(decoded)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			if !bytes.Equal(got, tc.data) {
				t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(tc.data))
			}
			if body.Size() <= 0 {
				t.Fatalf("compressed size = %d, want positive frame size", body.Size())
			}
		})
	}
}

func TestCodecRejectsMalformedFrame(t *testing.T) {
	t.Parallel()
	decoded, err := New(3).NewReader(io.NopCloser(bytes.NewReader([]byte("not a zstd frame"))))
	if err != nil {
		return
	}
	defer decoded.Close()
	if _, err := io.ReadAll(decoded); err == nil {
		t.Fatal("malformed frame unexpectedly decoded")
	}
}

func randomBytes(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rand.Uint32())
	}
	return data
}

func TestCodecReaderClosesSource(t *testing.T) {
	t.Parallel()

	codec := New(3)
	body, err := codec.Compress(bytes.NewReader([]byte("close propagation")), 17)
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	defer body.Cleanup()
	stored, err := body.Reader()
	if err != nil {
		t.Fatalf("Reader() error = %v", err)
	}
	source := &trackingReadCloser{Reader: stored}
	decoded, err := codec.NewReader(source)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if err := decoded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !source.closed {
		t.Fatal("decoder close did not close its source")
	}
}

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}
