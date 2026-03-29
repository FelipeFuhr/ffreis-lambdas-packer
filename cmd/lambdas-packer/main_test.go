package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/felipefuhr/ffreis-lambdas-packer/internal/packer"
)

func TestParseArgs_RequiresBucket(t *testing.T) {
	t.Parallel()

	_, err := parseArgs([]string{"--prefix", "lambdas/dev/"})
	if err == nil || !strings.Contains(err.Error(), "--bucket is required") {
		t.Fatalf("err = %v, want --bucket is required", err)
	}
}

func TestParseArgs_RequiresPrefix(t *testing.T) {
	t.Parallel()

	_, err := parseArgs([]string{"--bucket", "b"})
	if err == nil || !strings.Contains(err.Error(), "--prefix is required") {
		t.Fatalf("err = %v, want --prefix is required", err)
	}
}

func TestParseArgs_Success(t *testing.T) {
	t.Parallel()

	opts, err := parseArgs([]string{
		"--bucket", "b",
		"--prefix", "lambdas/dev/",
		"--artifact-dir", "x",
		"--region", "us-east-1",
		"--dry-run",
		"--no-delete",
	})
	if err != nil {
		t.Fatalf("parseArgs error = %v", err)
	}
	if opts.bucket != "b" || opts.prefix != "lambdas/dev/" || opts.artifactDir != "x" || opts.region != "us-east-1" || !opts.dryRun || !opts.noDelete {
		t.Fatalf("unexpected opts: %#v", opts)
	}
}

func TestLoadAWSConfig_SetsRegionWhenProvided(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	cfg, err := loadAWSConfig(context.Background(), "us-east-1")
	if err != nil {
		t.Fatalf("loadAWSConfig error = %v", err)
	}
	if cfg.Region != "us-east-1" {
		t.Fatalf("Region = %q, want us-east-1", cfg.Region)
	}
}

func TestPrintPlan_OutputsDryRunAndCounts(t *testing.T) {
	plan := packer.Plan{
		Uploads: []packer.LocalArtifact{{Function: "fn1", Key: "p/fn1.zip"}},
		Deletes: []string{"p/fn2.zip", "p/fn3.zip"},
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer r.Close()

	oldStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	printPlan(plan, "bucket", "p/", true)
	_ = w.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	got := buf.String()

	for _, want := range []string{
		"lambdas-packer (dry-run)",
		"bucket: bucket",
		"prefix: p/",
		"uploads: 1",
		"deletes: 2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stdout missing %q\n--- stdout ---\n%s", want, got)
		}
	}
}
