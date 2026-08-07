package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeS3Client is a test double for s3ClientAPI. It records calls and lets
// each test script the response/error for each of the three S3 operations
// run/runSingleFile/runSync depend on, without touching real AWS.
type fakeS3Client struct {
	putCalls    []s3.PutObjectInput
	putErr      error
	deleteCalls []s3.DeleteObjectsInput
	deleteErr   error
	listKeys    []string
	listErr     error
}

func (f *fakeS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if params.Body != nil {
		_, _ = io.Copy(io.Discard, params.Body)
	}
	f.putCalls = append(f.putCalls, *params)
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3Client) DeleteObjects(_ context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.deleteCalls = append(f.deleteCalls, *params)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (f *fakeS3Client) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	objs := make([]types.Object, 0, len(f.listKeys))
	for _, k := range f.listKeys {
		key := k
		objs = append(objs, types.Object{Key: &key})
	}
	return &s3.ListObjectsV2Output{Contents: objs}, nil
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// whatever was written to it.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	_ = w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return buf.String()
}

// captureStderr mirrors captureStdout for os.Stderr.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old }()

	fn()

	_ = w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	return buf.String()
}

func writeZip(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("fake-zip-contents"), 0o600); err != nil {
		t.Fatalf("write zip: %v", err)
	}
}

func TestRunSingleFile_DryRun_PrintsPlanWithoutUpload(t *testing.T) {
	fake := &fakeS3Client{}
	opts := options{bucket: "b", file: "dist/x.zip", key: "monitor-lambda/x.zip", dryRun: true}

	var code int
	out := captureStdout(t, func() {
		code = runSingleFile(context.Background(), fake, opts)
	})

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if len(fake.putCalls) != 0 {
		t.Fatalf("expected no PutObject calls in dry-run, got %d", len(fake.putCalls))
	}
	for _, want := range []string{"lambdas-packer (dry-run)", "key: monitor-lambda/x.zip", "file: dist/x.zip"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q, got: %s", want, out)
		}
	}
}

func TestRunSingleFile_Success_UploadsArtifact(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "x.zip")
	writeZip(t, zipPath)

	fake := &fakeS3Client{}
	opts := options{bucket: "b", file: zipPath, key: "monitor-lambda/x.zip"}

	var code int
	out := captureStdout(t, func() {
		code = runSingleFile(context.Background(), fake, opts)
	})

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if len(fake.putCalls) != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", len(fake.putCalls))
	}
	if aws.ToString(fake.putCalls[0].Key) != "monitor-lambda/x.zip" {
		t.Fatalf("unexpected key: %v", fake.putCalls[0].Key)
	}
	if !strings.Contains(out, "uploaded: s3://b/monitor-lambda/x.zip") {
		t.Fatalf("stdout missing upload confirmation, got: %s", out)
	}
}

func TestRunSingleFile_UploadFails_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "x.zip")
	writeZip(t, zipPath)

	fake := &fakeS3Client{putErr: errBoom}
	opts := options{bucket: "b", file: zipPath, key: "monitor-lambda/x.zip"}

	var code int
	errOut := captureStderr(t, func() {
		code = runSingleFile(context.Background(), fake, opts)
	})

	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "upload failed") {
		t.Fatalf("stderr missing 'upload failed', got: %s", errOut)
	}
}

func TestRunSync_DryRun_PrintsPlanWithoutChanges(t *testing.T) {
	dir := t.TempDir()
	writeZip(t, filepath.Join(dir, "fn1", "bootstrap.zip"))

	fake := &fakeS3Client{listKeys: []string{"lambdas/dev/fn2.zip"}}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: dir, dryRun: true}

	var code int
	out := captureStdout(t, func() {
		code = runSync(context.Background(), fake, opts)
	})

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if len(fake.putCalls) != 0 || len(fake.deleteCalls) != 0 {
		t.Fatalf("expected no S3 mutations in dry-run, got puts=%d deletes=%d", len(fake.putCalls), len(fake.deleteCalls))
	}
	if !strings.Contains(out, "uploads: 1") || !strings.Contains(out, "deletes: 1") {
		t.Fatalf("stdout missing expected plan counts, got: %s", out)
	}
}

func TestRunSync_Success_UploadsAndDeletes(t *testing.T) {
	dir := t.TempDir()
	writeZip(t, filepath.Join(dir, "fn1", "bootstrap.zip"))

	fake := &fakeS3Client{listKeys: []string{"lambdas/dev/fn2.zip"}}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: dir}

	var code int
	out := captureStdout(t, func() {
		code = runSync(context.Background(), fake, opts)
	})

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if len(fake.putCalls) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(fake.putCalls))
	}
	if len(fake.deleteCalls) != 1 || len(fake.deleteCalls[0].Delete.Objects) != 1 {
		t.Fatalf("expected 1 delete batch with 1 key, got %+v", fake.deleteCalls)
	}
	if !strings.Contains(out, "done: uploaded=1 deleted=1") {
		t.Fatalf("stdout missing completion summary, got: %s", out)
	}
}

func TestRunSync_NoDelete_SkipsRemoteExtras(t *testing.T) {
	dir := t.TempDir()
	writeZip(t, filepath.Join(dir, "fn1", "bootstrap.zip"))

	fake := &fakeS3Client{listKeys: []string{"lambdas/dev/fn2.zip"}}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: dir, noDelete: true}

	code := runSync(context.Background(), fake, opts)

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if len(fake.deleteCalls) != 0 {
		t.Fatalf("expected no deletes with --no-delete, got %d", len(fake.deleteCalls))
	}
}

func TestRunSync_DiscoverLocalArtifactsFails_ReturnsError(t *testing.T) {
	fake := &fakeS3Client{}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: filepath.Join(t.TempDir(), "does-not-exist")}

	var code int
	errOut := captureStderr(t, func() {
		code = runSync(context.Background(), fake, opts)
	})

	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "artifact discovery failed") {
		t.Fatalf("stderr missing discovery error, got: %s", errOut)
	}
}

func TestRunSync_ListRemoteZipsFails_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	writeZip(t, filepath.Join(dir, "fn1", "bootstrap.zip"))

	fake := &fakeS3Client{listErr: errBoom}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: dir}

	var code int
	errOut := captureStderr(t, func() {
		code = runSync(context.Background(), fake, opts)
	})

	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "failed listing s3://b/lambdas/dev/") {
		t.Fatalf("stderr missing list error, got: %s", errOut)
	}
}

func TestRunSync_PutArtifactFails_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	writeZip(t, filepath.Join(dir, "fn1", "bootstrap.zip"))

	fake := &fakeS3Client{putErr: errBoom}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: dir}

	var code int
	errOut := captureStderr(t, func() {
		code = runSync(context.Background(), fake, opts)
	})

	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "upload failed for") {
		t.Fatalf("stderr missing upload error, got: %s", errOut)
	}
}

func TestRunSync_DeleteKeysFails_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	writeZip(t, filepath.Join(dir, "fn1", "bootstrap.zip"))

	fake := &fakeS3Client{listKeys: []string{"lambdas/dev/fn2.zip"}, deleteErr: errBoom}
	opts := options{bucket: "b", prefix: "lambdas/dev/", artifactDir: dir}

	var code int
	errOut := captureStderr(t, func() {
		code = runSync(context.Background(), fake, opts)
	})

	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "delete failed") {
		t.Fatalf("stderr missing delete error, got: %s", errOut)
	}
}

func TestRun_MissingBucket_ReturnsUsageErrorWithoutAWSCall(t *testing.T) {
	// run() calls parseArgs first; a parse failure must short-circuit before
	// any AWS config loading or network call, so this is safe to exercise
	// directly via the public entrypoint with no credentials configured.
	var code int
	errOut := captureStderr(t, func() {
		code = run([]string{"--prefix", "lambdas/dev/"})
	})

	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
	if !strings.Contains(errOut, "--bucket is required") {
		t.Fatalf("stderr missing usage error, got: %s", errOut)
	}
}

func TestLoadAWSConfig_NoRegion_UsesDefault(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "")

	cfg, err := loadAWSConfig(context.Background(), "")
	if err != nil {
		t.Fatalf("loadAWSConfig error = %v", err)
	}
	_ = cfg // no explicit region requested: just verify the default-config path succeeds
}

var errBoom = &fakeError{"boom"}

type fakeError struct{ msg string }

func (e *fakeError) Error() string { return e.msg }
