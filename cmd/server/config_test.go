package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestRunRejectsInvalidConfigFile(t *testing.T) {
	for _, tt := range []struct {
		name     string
		contents string
		want     string
	}{
		{name: "unknown field", contents: "unknown: true\n", want: "invalid keys"},
		{name: "malformed yaml", contents: "kafka: [\n", want: "load config"},
		{name: "enabled missing credentials", contents: "kafka:\n  enabled: true\n  auth: plain\n", want: "required for plain"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tt.contents), 0o600); err != nil {
				t.Fatal(err)
			}
			err := run(t.Context(), func(key string) string {
				if key == "CONFIG_FILE" {
					return path
				}
				return ""
			}, zap.NewNop())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunProcessReturnsNonzeroForMissingExplicitConfig(t *testing.T) {
	t.Setenv("CONFIG_FILE", filepath.Join(t.TempDir(), "missing.yaml"))
	if got := runProcess(); got != 1 {
		t.Fatalf("runProcess = %d, want nonzero for missing explicit config", got)
	}
}
