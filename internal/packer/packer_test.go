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

func TestDiscoverLocalArtifactsSingleFileFallback(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fnDir := filepath.Join(dir, "orders")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// No bootstrap or bootstrap.zip — only a single regular file.
	if err := os.WriteFile(filepath.Join(fnDir, "handler"), []byte("bin"), 0o644); err != nil {
		t.Fatal(err)
	}

	arts, err := DiscoverLocalArtifacts(dir, "p/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1", len(arts))
	}
	if arts[0].RawPath == "" || arts[0].ZipPath != "" {
		t.Fatalf("expected raw artifact, got: %#v", arts[0])
	}
}

func TestDiscoverLocalArtifactsMultipleFilesError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fnDir := filepath.Join(dir, "payments")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Two files with no recognised name → ambiguous, must error.
	if err := os.WriteFile(filepath.Join(fnDir, "file1"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "file2"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := DiscoverLocalArtifacts(dir, "p/")
	if err == nil {
		t.Fatal("expected error for ambiguous artifacts, got nil")
	}
}

func TestDiscoverLocalArtifactsNoArtifactsError(t *testing.T) {
	t.Parallel()

	// Empty dir with no subdirectories → no artifacts.
	dir := t.TempDir()

	_, err := DiscoverLocalArtifacts(dir, "p/")
	if err == nil {
		t.Fatal("expected error for empty artifact dir, got nil")
	}
}

func TestBuildPlanNoDelete(t *testing.T) {
	t.Parallel()

	local := []LocalArtifact{
		{Function: "fn1", Key: "p/fn1.zip"},
	}
	remote := map[string]struct{}{
		"p/fn1.zip": {},
		"p/fn2.zip": {},
	}

	plan := BuildPlan(local, remote, true /* noDelete */)

	if len(plan.Uploads) != 1 {
		t.Fatalf("got %d uploads, want 1", len(plan.Uploads))
	}
	if len(plan.Deletes) != 0 {
		t.Fatalf("got %d deletes, want 0 (noDelete=true)", len(plan.Deletes))
	}
}

func TestBuildPlanWithDelete(t *testing.T) {
	t.Parallel()

	local := []LocalArtifact{
		{Function: "fn1", Key: "p/fn1.zip"},
	}
	remote := map[string]struct{}{
		"p/fn1.zip": {},
		"p/fn2.zip": {},
		"p/fn3.zip": {},
	}

	plan := BuildPlan(local, remote, false /* noDelete */)

	if len(plan.Uploads) != 1 {
		t.Fatalf("got %d uploads, want 1", len(plan.Uploads))
	}
	if len(plan.Deletes) != 2 {
		t.Fatalf("got %d deletes, want 2", len(plan.Deletes))
	}
	// Deletes must be sorted.
	if plan.Deletes[0] != "p/fn2.zip" || plan.Deletes[1] != "p/fn3.zip" {
		t.Fatalf("unexpected delete list: %v", plan.Deletes)
	}
}

func TestBuildPlanDeleteEmpty(t *testing.T) {
	t.Parallel()

	local := []LocalArtifact{
		{Function: "fn1", Key: "p/fn1.zip"},
		{Function: "fn2", Key: "p/fn2.zip"},
	}
	remote := map[string]struct{}{
		"p/fn1.zip": {},
	}

	plan := BuildPlan(local, remote, false /* noDelete */)

	if len(plan.Uploads) != 2 {
		t.Fatalf("got %d uploads, want 2", len(plan.Uploads))
	}
	if len(plan.Deletes) != 0 {
		t.Fatalf("got %d deletes, want 0 (nothing to delete)", len(plan.Deletes))
	}
}
