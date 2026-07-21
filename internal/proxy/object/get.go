// -------------------------------------------------------------------------------
// Object Manager - GET
//
// Author: Alex Freidah
//
// GetObject orchestration: cache lookup, failover, physical-byte accounting,
// transparent decrypt/decompress transforms, logical ranges, integrity checking,
// and streaming cache population. Stored and client-visible sizes stay separate.
// -------------------------------------------------------------------------------

package object

import (
	"bytes"
	"context"
	"fmt"
	"io"

	s3be "github.com/afreidah/s3-orchestrator/internal/backend"
	objcache "github.com/afreidah/s3-orchestrator/internal/cache"
	"github.com/afreidah/s3-orchestrator/internal/compression"
	"github.com/afreidah/s3-orchestrator/internal/encryption"
	"github.com/afreidah/s3-orchestrator/internal/observe/telemetry"
	pobserve "github.com/afreidah/s3-orchestrator/internal/proxy/observe"
	"github.com/afreidah/s3-orchestrator/internal/proxy/readpath"
	"github.com/afreidah/s3-orchestrator/internal/store/core"
	"github.com/afreidah/s3-orchestrator/internal/util/ioutilx"
)

type objectReadResult struct {
	result     *s3be.GetObjectResult
	storedSize int64
}

// GetObject retrieves an object with failover and restores its original bytes.
func (o *Manager) GetObject(ctx context.Context, key, rangeHeader string) (*s3be.GetObjectResult, error) {
	if cached, ok := o.tryGetObjectCache(ctx, key, rangeHeader); ok {
		return cached, nil
	}

	winner, backendName, err := readpath.Read(ctx, o.failover, "GetObject", key,
		func(ctx context.Context, beName string, loc *core.ObjectLocation, be s3be.ObjectBackend) (readpath.ProbeResult[*objectReadResult], error) {
			return o.getObjectAttempt(ctx, key, rangeHeader, beName, be, loc)
		})
	if err != nil {
		return nil, err
	}
	o.core.Acct().Egress(backendName, winner.storedSize)
	result := winner.result
	pobserve.GetCompleted(ctx, key, backendName, result.Size)
	if err := o.populateObjectCache(key, rangeHeader, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Manager) tryGetObjectCache(ctx context.Context, key, rangeHeader string) (*s3be.GetObjectResult, bool) {
	if o.objectCache == nil || rangeHeader != "" {
		return nil, false
	}
	entry, ok := o.objectCache.Get(key)
	if !ok {
		return nil, false
	}
	pobserve.GetCompleted(ctx, key, "cache", int64(len(entry.Data)))
	return &s3be.GetObjectResult{Body: io.NopCloser(bytes.NewReader(entry.Data)), Size: int64(len(entry.Data)), ContentType: entry.ContentType, ETag: entry.ETag, Metadata: entry.Metadata}, true
}

func (o *Manager) getObjectAttempt(ctx context.Context, key, rangeHeader, beName string, be s3be.ObjectBackend, loc *core.ObjectLocation) (readpath.ProbeResult[*objectReadResult], error) {
	var fail readpath.ProbeResult[*objectReadResult]
	if !o.core.Usage().WithinLimits(beName, 1, 0, 0) {
		return fail, fmt.Errorf("backend %s: %w", beName, readpath.ErrUsageLimitSkip)
	}
	if loc == nil && (o.encryptor != nil || o.compressWrites) {
		// Encryption and compression both require persisted representation
		// metadata. Never leak ciphertext or compressed backend bytes during a
		// metadata-store outage.
		return fail, core.ErrServiceUnavailable
	}
	if loc != nil && loc.Compressed() && rangeHeader != "" {
		if _, _, ok := ParsePlaintextRange(rangeHeader, loc.LogicalSize); !ok {
			return fail, &core.S3Error{StatusCode: 416, Code: "InvalidRange", Message: "The requested range is not satisfiable"}
		}
	}

	actualRange, rng, ptStart, ptEnd := o.resolveBackendRange(rangeHeader, loc)
	r, cancel, err := o.core.GetWithTimeout(ctx, be, key, actualRange)
	if err != nil {
		o.core.Acct().APICall(beName)
		return fail, err
	}
	storedSize := r.Size
	if !o.core.Usage().WithinLimits(beName, 1, storedSize, 0) {
		_ = r.Body.Close()
		cancel()
		o.core.Acct().APICall(beName)
		return fail, fmt.Errorf("backend %s egress: %w", beName, readpath.ErrUsageLimitSkip)
	}
	if err := o.buildClientReader(ctx, r, loc, key, beName, be, rangeHeader, rng, ptStart, ptEnd, storedSize); err != nil {
		_ = r.Body.Close()
		cancel()
		return fail, err
	}
	r.Body = ioutilx.WithCancel(r.Body, cancel)
	value := &objectReadResult{result: r, storedSize: storedSize}
	return readpath.ProbeResult[*objectReadResult]{Value: value, Size: r.Size, Cleanup: func() { _ = r.Body.Close() }}, nil
}

func (o *Manager) resolveBackendRange(rangeHeader string, loc *core.ObjectLocation) (string, *encryption.RangeResult, int64, int64) {
	if loc != nil && loc.Compressed() {
		if rangeHeader == "" {
			return "", nil, 0, 0
		}
		start, end, _ := ParsePlaintextRange(rangeHeader, loc.LogicalSize)
		return "", nil, start, end
	}
	if loc == nil || !loc.Encrypted || rangeHeader == "" {
		return rangeHeader, nil, 0, 0
	}
	ptStart, ptEnd, ok := ParsePlaintextRange(rangeHeader, loc.PlaintextSize)
	if !ok {
		return rangeHeader, nil, 0, 0
	}
	rng, _ := encryption.CiphertextRange(ptStart, ptEnd, o.encryptor.ChunkSize())
	if rng == nil {
		return rangeHeader, nil, ptStart, ptEnd
	}
	return rng.BackendRange, rng, ptStart, ptEnd
}

func (o *Manager) buildClientReader(ctx context.Context, r *s3be.GetObjectResult, loc *core.ObjectLocation, key, beName string, be s3be.ObjectBackend, rangeHeader string, rng *encryption.RangeResult, ptStart, ptEnd, storedSize int64) error {
	if loc != nil && loc.Encrypted && o.encryptor != nil {
		if err := decryptResponse(ctx, o.encryptor, r, loc, rng, ptStart, ptEnd); err != nil {
			return err
		}
	}
	if loc != nil && loc.Compressed() {
		if loc.CompressionAlgorithm != compression.AlgorithmZstd || loc.CompressionVersion != compression.FormatVersion {
			return fmt.Errorf("unsupported compression representation %q version %d", loc.CompressionAlgorithm, loc.CompressionVersion)
		}
		decoded, err := o.compressor.NewReader(r.Body)
		if err != nil {
			return err
		}
		r.Body = decoded
		r.Size = loc.LogicalSize
		r.ContentRange = ""
		if rangeHeader != "" {
			if ptStart > 0 {
				if _, err := io.CopyN(io.Discard, r.Body, ptStart); err != nil {
					return fmt.Errorf("seek compressed range: %w", err)
				}
			}
			length := ptEnd - ptStart + 1
			r.Body = ioutilx.ReadCloser(io.LimitReader(r.Body, length), r.Body)
			r.Size = length
			r.ContentRange = fmt.Sprintf("bytes %d-%d/%d", ptStart, ptEnd, loc.LogicalSize)
		}
	}
	if rangeHeader == "" {
		o.maybeWrapIntegrityReader(ctx, r, loc, key, beName, be, storedSize)
	}
	return nil
}

func (o *Manager) maybeWrapIntegrityReader(ctx context.Context, r *s3be.GetObjectResult, loc *core.ObjectLocation, key, beName string, be s3be.ObjectBackend, storedSize int64) {
	icfg := o.integrityCfg.Load()
	if icfg == nil || !icfg.Enabled || !icfg.VerifyOnRead {
		return
	}
	expectedHash := ""
	if loc != nil {
		expectedHash = loc.ContentHash
	}
	if expectedHash == "" {
		return
	}
	vr := NewVerifyingReader(r.Body)
	vr.SetVerification(expectedHash, func(expected, actual string) {
		o.log.ErrorContext(ctx, "integrity check failed on read", "key", key, "backend", beName, "expected_hash", expected, "actual_hash", actual)
		telemetry.IntegrityErrorsTotal.WithLabelValues("read").Inc()
		o.coord.DeleteOrEnqueue(ctx, be, beName, key, "integrity_failed", storedSize)
	})
	r.Body = vr
}

func (o *Manager) populateObjectCache(key, rangeHeader string, result *s3be.GetObjectResult) error {
	if o.objectCache == nil || rangeHeader != "" || result.Size <= 0 || !o.objectCache.Admit(result.Size) {
		return nil
	}
	meta := objcache.EntryMeta{ContentType: result.ContentType, ETag: result.ETag, Metadata: result.Metadata}
	result.Body = newCacheTeeBody(result.Body, result.Size, func(data []byte) { o.objectCache.PutBytes(key, data, meta) })
	return nil
}
