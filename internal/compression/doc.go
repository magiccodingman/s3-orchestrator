// Package compression provides the transparent at-rest compression stage used
// by object writes and reads. The package deliberately exposes one concrete
// Zstandard codec rather than a generic algorithm registry: stored metadata
// identifies the format version, while construction and policy remain in the
// configuration and dependency-injection layers.
package compression
