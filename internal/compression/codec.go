// -------------------------------------------------------------------------------
// Compression - Streaming Zstandard Codec
//
// Author: Alex Freidah
//
// Implements the single at-rest compression format supported by the
// orchestrator. Compression is streamed into the existing replayable
// memory-or-tempfile body so backend failover can retry without retaining
// whole objects on the heap; decompression remains a bounded streaming reader.
// -------------------------------------------------------------------------------

package compression

import (
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/afreidah/s3-orchestrator/internal/util/bufpool"
	"github.com/afreidah/s3-orchestrator/internal/util/materialize"
)

// AlgorithmZstd is the persisted algorithm identifier for Zstandard objects.
const AlgorithmZstd = "zstd"

// FormatVersion is the persisted orchestrator compression-container version.
// Version 1 stores one ordinary Zstandard frame for the complete object.
const FormatVersion = 1

// Codec compresses and decompresses the orchestrator's Zstandard format.
type Codec struct {
	level zstd.EncoderLevel
}

// New creates a codec using a standard Zstandard level. Operator-facing
// validation is owned by config; EncoderLevelFromZstd maps it for the library.
func New(level int) *Codec {
	return &Codec{level: zstd.EncoderLevelFromZstd(level)}
}

// Compress streams src through Zstandard into a replayable materialized body.
// sizeHint selects the existing memory-versus-tempfile policy; the returned
// body's Size reports the actual compressed byte count.
func (c *Codec) Compress(src io.Reader, sizeHint int64) (*materialize.Body, error) {
	pr, pw := io.Pipe()
	producerDone := make(chan error, 1)

	go func() {
		enc, err := zstd.NewWriter(pw,
			zstd.WithEncoderLevel(c.level),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			_ = pw.CloseWithError(err)
			producerDone <- err
			return
		}

		_, copyErr := bufpool.Copy(enc, src)
		closeErr := enc.Close()
		err = errors.Join(copyErr, closeErr)
		_ = pw.CloseWithError(err)
		producerDone <- err
	}()

	body, consumeErr := materialize.New(pr, sizeHint, nil)
	_ = pr.Close()
	producerErr := <-producerDone
	if err := errors.Join(consumeErr, producerErr); err != nil {
		if body != nil {
			body.Cleanup()
		}
		return nil, fmt.Errorf("compress zstd stream: %w", err)
	}
	return body, nil
}

// NewReader returns a streaming reader for one stored Zstandard frame. Closing
// it releases decoder resources and closes the stored source body.
func (c *Codec) NewReader(src io.ReadCloser) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(src, zstd.WithDecoderConcurrency(1))
	if err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("open zstd stream: %w", err)
	}
	return &decoderReadCloser{Decoder: dec, source: src}, nil
}

type decoderReadCloser struct {
	*zstd.Decoder
	source io.Closer
}

func (d *decoderReadCloser) Close() error {
	d.Decoder.Close()
	return d.source.Close()
}
