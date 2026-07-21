// -------------------------------------------------------------------------------
// Compression Configuration
//
// Author: Alex Freidah
//
// Defines the optional transparent at-rest Zstandard stage. The first format
// intentionally has one algorithm and one whole-object frame; the configured
// level affects new writes only and defaults to the balanced Zstandard level 3.
// -------------------------------------------------------------------------------

package config

// CompressionConfig controls transparent compression before optional
// encryption. Existing objects retain the representation recorded with their
// location row, so enabling or changing the level never rewrites stored data.
type CompressionConfig struct {
	Enabled bool `yaml:"enabled"`
	Level   int  `yaml:"level"`
}

func (c *CompressionConfig) setDefaultsAndValidate() []error {
	if c.Level == 0 {
		c.Level = 3
	}
	if c.Level < 1 || c.Level > 19 {
		return []error{ErrCompressionLevelRange}
	}
	return nil
}
