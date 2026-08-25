package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadPreservesFilesystemErrorChain(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing", "broker.yaml")
	_, err := Load(missing)
	if err == nil {
		t.Fatal("Load returned nil error for missing config")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("errors.Is(err, os.ErrNotExist) = false: %v", err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("errors.As(err, *os.PathError) = false: %T %v", err, err)
	}
	if pathErr.Path != missing {
		t.Fatalf("PathError.Path = %q, want %q", pathErr.Path, missing)
	}
}

func TestLoadPreservesYAMLParseErrorChain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.yaml")
	if err := os.WriteFile(path, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load returned nil error for invalid config shape")
	}
	var typeErr *yaml.TypeError
	if !errors.As(err, &typeErr) {
		t.Fatalf("errors.As(err, *yaml.TypeError) = false: %T %v", err, err)
	}
}
