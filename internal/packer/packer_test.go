package packer

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
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
		t.Fatalf(testUnexpectedErrorFmt, err)
	}
	if got != testPrefixDev {
		t.Fatalf("got %q, want %q", got, testPrefixDev)
	}
}

func TestNormalizePrefixTrimsLeadingSlashAndSpace(t *testing.T) {
	t.Parallel()

	got, err := NormalizePrefix(" /lambdas/dev ")
	if err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
	}
	if got != testPrefixDev {
		t.Fatalf("got %q, want %q", got, testPrefixDev)
	}
}

func TestNormalizePrefixRejectsEmpty(t *testing.T) {
	t.Parallel()

	_, err := NormalizePrefix("   ")
	if err == nil {
		t.Fatal(testExpectedErrorGotNil)
	}
}

func TestDiscoverLocalArtifactsPrefersZip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fnDir := filepath.Join(dir, "users")
	mustMkdirAll(t, fnDir)
	mustWriteFile(t, filepath.Join(fnDir, artifactBootstrap), []byte("bin"))
	mustWriteFile(t, filepath.Join(fnDir, artifactBootstrapZip), []byte("zip"))

	arts, err := DiscoverLocalArtifacts(dir, testPrefixP)
	if err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
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
	mustMkdirAll(t, fnDir)
	// No bootstrap or bootstrap.zip — only a single regular file.
	mustWriteFile(t, filepath.Join(fnDir, "handler"), []byte("bin"))

	arts, err := DiscoverLocalArtifacts(dir, testPrefixP)
	if err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
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
	mustMkdirAll(t, fnDir)
	// Two files with no recognised name → ambiguous, must error.
	mustWriteFile(t, filepath.Join(fnDir, "file1"), []byte("a"))
	mustWriteFile(t, filepath.Join(fnDir, "file2"), []byte("b"))

	_, err := DiscoverLocalArtifacts(dir, testPrefixP)
	if err == nil {
		t.Fatal("expected error for ambiguous artifacts, got nil")
	}
}

func TestDiscoverLocalArtifactsNoArtifactsError(t *testing.T) {
	t.Parallel()

	// Empty dir with no subdirectories → no artifacts.
	dir := t.TempDir()

	_, err := DiscoverLocalArtifacts(dir, testPrefixP)
	if err == nil {
		t.Fatal("expected error for empty artifact dir, got nil")
	}
}

func TestDiscoverLocalArtifactsSkipsNonDirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "README.txt"), []byte("x"))

	fnDir := filepath.Join(dir, "users")
	mustMkdirAll(t, fnDir)
	mustWriteFile(t, filepath.Join(fnDir, artifactBootstrapZip), []byte("zip"))

	arts, err := DiscoverLocalArtifacts(dir, testPrefixP)
	if err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
	}
	if len(arts) != 1 || arts[0].Function != "users" {
		t.Fatalf("unexpected artifacts: %#v", arts)
	}
}

func TestBuildPlanNoDelete(t *testing.T) {
	t.Parallel()

	local := []LocalArtifact{
		{Function: "fn1", Key: testKeyFn1Zip},
	}
	remote := map[string]struct{}{
		testKeyFn1Zip: {},
		testKeyFn2Zip: {},
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
		{Function: "fn1", Key: testKeyFn1Zip},
	}
	remote := map[string]struct{}{
		testKeyFn1Zip: {},
		testKeyFn2Zip: {},
		testKeyFn3Zip: {},
	}

	plan := BuildPlan(local, remote, false /* noDelete */)

	if len(plan.Uploads) != 1 {
		t.Fatalf("got %d uploads, want 1", len(plan.Uploads))
	}
	if len(plan.Deletes) != 2 {
		t.Fatalf("got %d deletes, want 2", len(plan.Deletes))
	}
	// Deletes must be sorted.
	if plan.Deletes[0] != testKeyFn2Zip || plan.Deletes[1] != testKeyFn3Zip {
		t.Fatalf("unexpected delete list: %v", plan.Deletes)
	}
}

func TestBuildPlanDeleteEmpty(t *testing.T) {
	t.Parallel()

	local := []LocalArtifact{
		{Function: "fn1", Key: testKeyFn1Zip},
		{Function: "fn2", Key: testKeyFn2Zip},
	}
	remote := map[string]struct{}{
		testKeyFn1Zip: {},
	}

	plan := BuildPlan(local, remote, false /* noDelete */)

	if len(plan.Uploads) != 2 {
		t.Fatalf("got %d uploads, want 2", len(plan.Uploads))
	}
	if len(plan.Deletes) != 0 {
		t.Fatalf("got %d deletes, want 0 (nothing to delete)", len(plan.Deletes))
	}
}

func TestPutArtifact_ErrWhenNoPaths(t *testing.T) {
	t.Parallel()

	err := PutArtifact(context.Background(), nil, "bucket", LocalArtifact{Function: "fn", Key: "p/fn.zip"})
	if err == nil {
		t.Fatalf("err = nil, want error")
	}
}

func TestDeleteKeys_EmptyNoOp(t *testing.T) {
	t.Parallel()

	if err := DeleteKeys(context.Background(), nil, "bucket", nil); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestBatchKeys_DefaultSizeWhenInvalid(t *testing.T) {
	t.Parallel()

	keys := make([]string, 1001)
	for i := range keys {
		keys[i] = "k"
	}
	batches := batchKeys(keys, 0)
	if len(batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2", len(batches))
	}
	if len(batches[0]) != 1000 || len(batches[1]) != 1 {
		t.Fatalf("batch sizes = %d/%d, want 1000/1", len(batches[0]), len(batches[1]))
	}
}

func TestBatchKeys_SplitsBySize(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c", "d", "e"}
	batches := batchKeys(keys, 2)
	if len(batches) != 3 {
		t.Fatalf("len(batches) = %d, want 3", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 1 {
		t.Fatalf("unexpected batch sizes: %d/%d/%d", len(batches[0]), len(batches[1]), len(batches[2]))
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
	rawPath := filepath.Join(dir, artifactBootstrap)
	mustWriteFile(t, rawPath, []byte("hello"))

	s3c := &fakeS3{}
	a := LocalArtifact{RawPath: rawPath, Key: testKeyFnZip}
	if err := PutArtifact(context.Background(), s3c, testBucket, a); err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
	}

	if len(s3c.putCalls) != 1 {
		t.Fatalf("got %d put calls, want 1", len(s3c.putCalls))
	}
	if s3c.putCalls[0].ContentType == nil || *s3c.putCalls[0].ContentType != contentTypeZip {
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
	if zr.File[0].Name != artifactBootstrap {
		t.Fatalf("got zip entry %q, want %q", zr.File[0].Name, artifactBootstrap)
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
	zipPath := filepath.Join(dir, artifactBootstrapZip)
	mustWriteFile(t, zipPath, []byte("zip-bytes"))

	s3c := &fakeS3{}
	a := LocalArtifact{ZipPath: zipPath, Key: testKeyFnZip}
	if err := PutArtifact(context.Background(), s3c, testBucket, a); err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
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
	if err := DeleteKeys(context.Background(), s3c, testBucket, keys); err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
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
	remote, err := ListRemoteZips(context.Background(), s3c, testBucket, testPrefixP)
	if err != nil {
		t.Fatalf(testUnexpectedErrorFmt, err)
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
			{Key: strPtr(testKeyBadZip), Message: strPtr("nope")},
		},
	}, nil
}

func TestDeleteKeysSurfacesDeleteErrors(t *testing.T) {
	t.Parallel()

	s3c := &fakeS3DeleteError{fakeS3: &fakeS3{}}
	err := DeleteKeys(context.Background(), s3c, testBucket, []string{testKeyBadZip})
	if err == nil {
		t.Fatal(testExpectedErrorGotNil)
	}
}
