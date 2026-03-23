package packer

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestNormalizePrefixTrimsLeadingSlashAndSpace(t *testing.T) {
	t.Parallel()

	got, err := NormalizePrefix(" /lambdas/dev ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "lambdas/dev/" {
		t.Fatalf("got %q, want %q", got, "lambdas/dev/")
	}
}

func TestNormalizePrefixRejectsEmpty(t *testing.T) {
	t.Parallel()

	_, err := NormalizePrefix("   ")
	if err == nil {
		t.Fatal("expected error, got nil")
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

func TestDiscoverLocalArtifactsSkipsNonDirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "README.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	fnDir := filepath.Join(dir, "users")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "bootstrap.zip"), []byte("zip"), 0o644); err != nil {
		t.Fatal(err)
	}

	arts, err := DiscoverLocalArtifacts(dir, "p/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(arts) != 1 || arts[0].Function != "users" {
		t.Fatalf("unexpected artifacts: %#v", arts)
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

type fakeS3 struct {
	putCalls    []s3.PutObjectInput
	putBodies   [][]byte
	deleteCalls [][]string

	listCalls []*s3.ListObjectsV2Input
}

func (f *fakeS3) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if params == nil {
		return nil, nil
	}
	body, _ := ioReadAll(params.Body)
	f.putCalls = append(f.putCalls, *params)
	f.putBodies = append(f.putBodies, body)
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) DeleteObjects(_ context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	var keys []string
	if params != nil && params.Delete != nil {
		for _, obj := range params.Delete.Objects {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}
	f.deleteCalls = append(f.deleteCalls, keys)
	return &s3.DeleteObjectsOutput{}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.listCalls = append(f.listCalls, params)
	// Page 1: no continuation token → truncated
	if params == nil || params.ContinuationToken == nil {
		return &s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: strPtr("p/a.zip")},
				{Key: strPtr("p/ignore.txt")},
			},
			IsTruncated:           boolPtr(true),
			NextContinuationToken: strPtr("t1"),
		}, nil
	}
	// Page 2
	return &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{Key: strPtr("p/c.zip")},
		},
		IsTruncated: boolPtr(false),
	}, nil
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

func ioReadAll(r io.Reader) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	return io.ReadAll(r)
}

func TestPutArtifactZipsRaw(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rawPath := filepath.Join(dir, "bootstrap")
	if err := os.WriteFile(rawPath, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	s3c := &fakeS3{}
	a := LocalArtifact{RawPath: rawPath, Key: "p/fn.zip"}
	if err := PutArtifact(context.Background(), s3c, "bucket", a); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(s3c.putCalls) != 1 {
		t.Fatalf("got %d put calls, want 1", len(s3c.putCalls))
	}
	if s3c.putCalls[0].ContentType == nil || *s3c.putCalls[0].ContentType != "application/zip" {
		t.Fatalf("unexpected content type: %#v", s3c.putCalls[0].ContentType)
	}

	zdata := s3c.putBodies[0]
	zr, err := zip.NewReader(bytes.NewReader(zdata), int64(len(zdata)))
	if err != nil {
		t.Fatalf("zip reader: %v", err)
	}
	if len(zr.File) != 1 {
		t.Fatalf("got %d zip entries, want 1", len(zr.File))
	}
	if zr.File[0].Name != "bootstrap" {
		t.Fatalf("got zip entry %q, want %q", zr.File[0].Name, "bootstrap")
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatalf("open zip entry: %v", err)
	}
	defer rc.Close()
	payload, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read zip entry: %v", err)
	}
	if string(payload) != "hello" {
		t.Fatalf("got payload %q, want %q", string(payload), "hello")
	}
}

func TestPutArtifactUploadsZipAsIs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	zipPath := filepath.Join(dir, "bootstrap.zip")
	if err := os.WriteFile(zipPath, []byte("zip-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	s3c := &fakeS3{}
	a := LocalArtifact{ZipPath: zipPath, Key: "p/fn.zip"}
	if err := PutArtifact(context.Background(), s3c, "bucket", a); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := string(s3c.putBodies[0]); got != "zip-bytes" {
		t.Fatalf("got %q, want %q", got, "zip-bytes")
	}
}

func TestDeleteKeysBatches(t *testing.T) {
	t.Parallel()

	var keys []string
	for i := 0; i < 1001; i++ {
		keys = append(keys, "p/k"+strconv.Itoa(i)+".zip")
	}

	s3c := &fakeS3{}
	if err := DeleteKeys(context.Background(), s3c, "bucket", keys); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s3c.deleteCalls) != 2 {
		t.Fatalf("got %d delete calls, want 2", len(s3c.deleteCalls))
	}
	if len(s3c.deleteCalls[0]) != 1000 || len(s3c.deleteCalls[1]) != 1 {
		t.Fatalf("unexpected batch sizes: %d and %d", len(s3c.deleteCalls[0]), len(s3c.deleteCalls[1]))
	}
}

func TestListRemoteZipsFiltersAndPaginates(t *testing.T) {
	t.Parallel()

	s3c := &fakeS3{}
	remote, err := ListRemoteZips(context.Background(), s3c, "bucket", "p/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var keys []string
	for k := range remote {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "p/a.zip" || keys[1] != "p/c.zip" {
		t.Fatalf("unexpected remote keys: %v", keys)
	}
}

type fakeS3DeleteError struct {
	*fakeS3
}

func (f *fakeS3DeleteError) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{
		Errors: []types.Error{
			{Key: strPtr("p/bad.zip"), Message: strPtr("nope")},
		},
	}, nil
}

func TestDeleteKeysSurfacesDeleteErrors(t *testing.T) {
	t.Parallel()

	s3c := &fakeS3DeleteError{fakeS3: &fakeS3{}}
	err := DeleteKeys(context.Background(), s3c, "bucket", []string{"p/bad.zip"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
