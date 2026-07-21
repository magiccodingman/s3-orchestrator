// -------------------------------------------------------------------------------
// Compression Configuration Tests
//
// Author: Alex Freidah
//
// Pins the disabled-by-default behavior, level-3 default, accepted boundaries,
// validation sentinel, and restart-required classification used by SIGHUP.
// -------------------------------------------------------------------------------

package config

import (
	"errors"
	"slices"
	"testing"
)

func TestCompressionConfigDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	cfg := CompressionConfig{}
	if errs := cfg.setDefaultsAndValidate(); len(errs) != 0 {
		t.Fatalf("default validation errors = %v", errs)
	}
	if cfg.Enabled {
		t.Fatal("compression should remain disabled by default")
	}
	if cfg.Level != 3 {
		t.Fatalf("default level = %d, want 3", cfg.Level)
	}
	for _, level := range []int{1, 3, 19} {
		candidate := CompressionConfig{Enabled: true, Level: level}
		if errs := candidate.setDefaultsAndValidate(); len(errs) != 0 {
			t.Fatalf("level %d validation errors = %v", level, errs)
		}
	}
	for _, level := range []int{-1, 20} {
		candidate := CompressionConfig{Enabled: true, Level: level}
		errs := candidate.setDefaultsAndValidate()
		if len(errs) != 1 || !errors.Is(errs[0], ErrCompressionLevelRange) {
			t.Fatalf("level %d errors = %v", level, errs)
		}
	}
}

func TestCompressionConfigRequiresRestart(t *testing.T) {
	t.Parallel()
	oldCfg := &Config{Compression: CompressionConfig{Level: 3}}
	newCfg := &Config{Compression: CompressionConfig{Enabled: true, Level: 3}}
	if got := NonReloadableFieldsChanged(oldCfg, newCfg); !slices.Contains(got, "compression") {
		t.Fatalf("changed fields = %v, want compression", got)
	}
}
