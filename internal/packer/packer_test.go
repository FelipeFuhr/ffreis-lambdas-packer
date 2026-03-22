package packer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizePrefix(t *testing.T) {
	t.Parallel()

	got, err := NormalizePrefix("lambdas/dev")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "lambdas/dev/" {
		t.Fatalf("got %q, want %q", got, "lambdas/dev/")
	}
}

func TestDiscoverLocalArtifactsPrefersZip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fnDir := filepath.Join(dir, "users")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "bootstrap"), []byte("bin"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "bootstrap.zip"), []byte("zip"), 0o644); err != nil {
		t.Fatal(err)
	}

	arts, err := DiscoverLocalArtifacts(dir, "p/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1", len(arts))
	}
	if arts[0].ZipPath == "" || arts[0].RawPath != "" {
		t.Fatalf("expected zip artifact, got: %#v", arts[0])
	}
}

