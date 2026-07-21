// -------------------------------------------------------------------------------
// Object Manager - PUT
//
// Author: Alex Freidah
//
// PutObject orchestration: body materialization, optional whole-object Zstandard
// compression, write failover across eligible backends, per-attempt encryption,
// pending-intent recovery, and drain-race close. Compression is prepared once and
// replayed across retries; encryption retains fresh-nonce retry semantics.
// -------------------------------------------------------------------------------

package object

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/afreidah/s3-orchestrator/internal/compression"
	"github.com/afreidah/s3-orchestrator/internal/observe"
	"github.com/afreidah/s3-orchestrator/internal/observe/telemetry"
	"github.com/afreidah/s3-orchestrator/internal/store/core"
	"github.com/afreidah/s3-orchestrator/internal/util/materialize"

	"go.opentelemetry.io/otel/trace"
)

// preparedPutBody is the replayable representation that enters optional
// encryption. StorageInputSize preserves the historical declared size for
// ordinary writes and records the actual encoded size for compressed writes;
// LogicalSize is always the client-visible byte count.
type preparedPutBody struct {
	Body             *materialize.Body
	StorageInputSize int64
	LogicalSize      int64
	ContentHash      string
	Meta             *core.EncryptionMeta
}

func (p *preparedPutBody) Cleanup() { p.Body.Cleanup() }

// PutObject uploads an object to the first backend with available quota.
func (o *Manager) PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (string, error) {
	const operation = "PutObject"
	start := time.Now()
	ctx, span := telemetry.StartSpan(ctx, managerSpanPrefix+operation,
		telemetry.AttrObjectKey.String(key), telemetry.AttrObjectSize.Int64(size))
	defer span.End()

	// Preserve the early historical eligibility check for uncompressed writes.
	// Compressed writes cannot know their backend ingress size until the stream
	// has been encoded, so they perform the authoritative check below.
	var eligible []string
	if !o.compressWrites {
		eligible = o.core.EligibleForWrite(1, 0, size)
		if len(eligible) == 0 {
			telemetry.UsageLimitRejectionsTotal.WithLabelValues(operation, "write").Inc()
			observe.MarkSpanError(span, "usage limits exceeded on all backends")
			return "", core.ErrInsufficientStorage
		}
	}

	prepared, err := o.preparePutBody(span, body, size)
	if err != nil {
		return "", err
	}
	defer prepared.Cleanup()

	// Compression materially changes backend ingress and quota size. Use the
	// encoded size for the authoritative policy check and backend selection.
	if prepared.Meta != nil && prepared.Meta.Compressed() {
		eligible = o.core.EligibleForWrite(1, 0, prepared.Body.Size())
		if len(eligible) == 0 {
			telemetry.UsageLimitRejectionsTotal.WithLabelValues(operation, "write").Inc()
			observe.MarkSpanError(span, "compressed object exceeds backend limits")
			return "", core.ErrInsufficientStorage
		}
	}

	var dekState putEncryptState
	var failedBackends []string
	var lastErr error
	for len(eligible) > 0 {
		res := o.attemptPutOnBackend(ctx, span, operation, key, prepared, contentType, metadata, &dekState, eligible)
		if res.fatalErr != nil {
			return "", res.fatalErr
		}
		if res.putErr == nil {
			o.finalizePutSuccess(ctx, span, operation, key, res.backend, res.storedSize, prepared.LogicalSize, start, failedBackends)
			return res.etag, nil
		}
		lastErr = res.putErr
		failedBackends = append(failedBackends, res.backend)
		eligible = withoutBackend(eligible, res.backend)
		o.log.WarnContext(ctx, "PutObject: backend write failed, trying next",
			"key", key, "failed_backend", res.backend, "error", res.putErr,
			"remaining_backends", len(eligible))
	}
	observe.RecordSpanError(span, lastErr)
	return "", lastErr
}

type putAttemptResult struct {
	backend    string
	etag       string
	storedSize int64
	fatalErr   error
	putErr     error
}

func (o *Manager) preparePutBody(span trace.Span, body io.Reader, size int64) (*preparedPutBody, error) {
	if !o.compressWrites {
		mb, contentHash, err := o.bufferPutBody(span, body, size)
		if err != nil {
			return nil, err
		}
		return &preparedPutBody{Body: mb, StorageInputSize: size, LogicalSize: size, ContentHash: contentHash}, nil
	}

	// Stream the inbound plaintext directly through the hasher and compressor
	// into the replayable encoded body. We never retain both a plaintext and a
	// compressed materialization, which bounds disk/RAM use to one stored copy.
	var hasher hash.Hash
	icfg := o.integrityCfg.Load()
	if icfg != nil && icfg.Enabled {
		hasher = newSHA256()
		body = io.TeeReader(body, hasher)
	}
	compressed, err := o.compressor.Compress(body, size)
	if err != nil {
		observe.RecordSpanError(span, err)
		return nil, err
	}
	return &preparedPutBody{
		Body:             compressed,
		StorageInputSize: compressed.Size(),
		LogicalSize:      size,
		ContentHash:      sha256Hex(hasher),
		Meta: &core.EncryptionMeta{
			CompressionAlgorithm: compression.AlgorithmZstd,
			CompressionLevel:     o.compressionLevel,
			CompressionVersion:   compression.FormatVersion,
			LogicalSize:          size,
		},
	}, nil
}

func (o *Manager) bufferPutBody(span trace.Span, body io.Reader, size int64) (*materialize.Body, string, error) {
	var hasher hash.Hash
	icfg := o.integrityCfg.Load()
	if icfg != nil && icfg.Enabled {
		hasher = newSHA256()
	}
	mb, err := materialize.New(body, size, hasher)
	if err != nil {
		observe.RecordSpanError(span, err)
		return nil, "", fmt.Errorf("buffer request body: %w", err)
	}
	return mb, sha256Hex(hasher), nil
}

func (o *Manager) attemptPutOnBackend(ctx context.Context, span trace.Span, operation, key string, prepared *preparedPutBody, contentType string, metadata map[string]string, dekState *putEncryptState, eligible []string) putAttemptResult {
	inputSize := prepared.StorageInputSize
	backendName, err := o.coord.SelectBackendForWrite(ctx, inputSize, eligible)
	if err != nil {
		return putAttemptResult{fatalErr: o.core.ClassifyWriteError(span, operation, err)}
	}
	span.SetAttributes(telemetry.AttrBackendName.String(backendName))
	be, err := o.core.GetBackend(backendName)
	if err != nil {
		observe.RecordSpanError(span, err)
		return putAttemptResult{backend: backendName, fatalErr: err}
	}

	uploadBody, uploadSize, enc, err := o.buildPutPayload(ctx, prepared, dekState)
	if err != nil {
		observe.RecordSpanError(span, err)
		return putAttemptResult{backend: backendName, fatalErr: err}
	}
	intentID, err := o.coord.InsertPendingIntent(ctx, key, backendName, uploadSize, enc)
	if err != nil {
		observe.RecordSpanError(span, err)
		return putAttemptResult{backend: backendName, fatalErr: err}
	}

	bctx, bcancel := o.core.WithTimeout(ctx)
	etag, err := be.PutObject(bctx, key, uploadBody, uploadSize, contentType, metadata)
	bcancel()
	if err != nil {
		o.core.Acct().APICall(backendName)
		return putAttemptResult{backend: backendName, putErr: err}
	}
	if o.core.IsDraining(backendName) {
		o.log.WarnContext(ctx, "drain started mid-write; aborting commit on draining backend", "key", key, "backend", backendName)
		telemetry.DrainRaceAbortedTotal.Inc()
		o.coord.RecoverFromRecordFailure(ctx, be, backendName, key, "drain_race_aborted", uploadSize)
		return putAttemptResult{backend: backendName, putErr: errDrainRaceAborted}
	}
	if err := o.coord.RecordObjectAndPromoteIntent(ctx, span, key, backendName, uploadSize, enc, intentID); err != nil {
		return putAttemptResult{backend: backendName, fatalErr: err}
	}
	return putAttemptResult{backend: backendName, etag: etag, storedSize: uploadSize}
}

var errDrainRaceAborted = errors.New("aborted: drain started mid-write")

func (o *Manager) buildPutPayload(ctx context.Context, prepared *preparedPutBody, dekState *putEncryptState) (io.Reader, int64, *core.EncryptionMeta, error) {
	input, err := prepared.Body.Reader()
	if err != nil {
		return nil, 0, nil, err
	}
	inputSize := prepared.StorageInputSize
	if o.encryptor != nil {
		uploadBody, uploadSize, enc, err := encryptForPut(ctx, o.encryptor, input, inputSize, dekState)
		if err != nil {
			return nil, 0, nil, err
		}
		mergeRepresentationMeta(enc, prepared.Meta, prepared.ContentHash)
		return uploadBody, uploadSize, enc, nil
	}
	enc := cloneRepresentationMeta(prepared.Meta)
	if prepared.ContentHash != "" {
		if enc == nil {
			enc = &core.EncryptionMeta{}
		}
		enc.ContentHash = prepared.ContentHash
	}
	return input, inputSize, enc, nil
}

func cloneRepresentationMeta(src *core.EncryptionMeta) *core.EncryptionMeta {
	if src == nil {
		return nil
	}
	clone := *src
	return &clone
}

func mergeRepresentationMeta(dst, src *core.EncryptionMeta, contentHash string) {
	if src != nil {
		dst.CompressionAlgorithm = src.CompressionAlgorithm
		dst.CompressionLevel = src.CompressionLevel
		dst.CompressionVersion = src.CompressionVersion
		dst.LogicalSize = src.LogicalSize
	}
	dst.ContentHash = contentHash
}

func withoutBackend(eligible []string, name string) []string {
	remaining := make([]string, 0, len(eligible)-1)
	for _, n := range eligible {
		if n != name {
			remaining = append(remaining, n)
		}
	}
	return remaining
}
