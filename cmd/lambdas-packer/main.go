// Package main provides the `lambdas-packer` CLI.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/FelipeFuhr/ffreis-lambdas-packer/internal/packer"
)

// s3ClientAPI is the minimal surface run/runSingleFile/runSync need from the
// AWS S3 SDK client. Defined as an interface (ports-and-adapters) so tests can
// substitute a fake instead of exercising real AWS calls.
type s3ClientAPI interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type options struct {
	bucket      string
	prefix      string
	artifactDir string
	file        string
	key         string
	region      string
	dryRun      bool
	noDelete    bool
}

func (o options) singleFileMode() bool {
	return o.file != "" || o.key != ""
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	opts, err := parseArgs(args)
	if err != nil {
		writeLine(os.Stderr, err.Error())
		return 2
	}

	ctx := context.Background()

	awsCfg, err := loadAWSConfig(ctx, opts.region)
	if err != nil {
		writeErrorLine(os.Stderr, "failed to load AWS config: ", err)
		return 1
	}

	s3Client := s3.NewFromConfig(awsCfg)

	if opts.singleFileMode() {
		return runSingleFile(ctx, s3Client, opts)
	}
	return runSync(ctx, s3Client, opts)
}

func runSingleFile(ctx context.Context, s3Client s3ClientAPI, opts options) int {
	if opts.dryRun {
		writeLine(os.Stdout, cliName+" (dry-run)")
		writeLine(os.Stdout, labelBucket+": "+opts.bucket)
		writeLine(os.Stdout, "key: "+opts.key)
		writeLine(os.Stdout, "file: "+opts.file)
		return 0
	}
	art := packer.LocalArtifact{Function: opts.key, ZipPath: opts.file, Key: opts.key}
	if err := packer.PutArtifact(ctx, s3Client, opts.bucket, art); err != nil {
		writeErrorLine(os.Stderr, "upload failed: ", err)
		return 1
	}
	writeLine(os.Stdout, "uploaded: s3://"+opts.bucket+"/"+opts.key)
	return 0
}

func runSync(ctx context.Context, s3Client s3ClientAPI, opts options) int {
	prefix, err := packer.NormalizePrefix(opts.prefix)
	if err != nil {
		writeLine(os.Stderr, err.Error())
		return 2
	}

	local, err := packer.DiscoverLocalArtifacts(opts.artifactDir, prefix)
	if err != nil {
		writeErrorLine(os.Stderr, "artifact discovery failed: ", err)
		return 1
	}

	remote, err := packer.ListRemoteZips(ctx, s3Client, opts.bucket, prefix)
	if err != nil {
		writeErrorLine(os.Stderr, "failed listing s3://"+opts.bucket+"/"+prefix+": ", err)
		return 1
	}

	plan := packer.BuildPlan(local, remote, opts.noDelete)
	printPlan(plan, opts.bucket, prefix, opts.dryRun)

	if opts.dryRun {
		return 0
	}

	for _, a := range plan.Uploads {
		if err := packer.PutArtifact(ctx, s3Client, opts.bucket, a); err != nil {
			writeErrorLine(os.Stderr, "upload failed for "+a.Key+": ", err)
			return 1
		}
	}
	if err := packer.DeleteKeys(ctx, s3Client, opts.bucket, plan.Deletes); err != nil {
		writeErrorLine(os.Stderr, "delete failed: ", err)
		return 1
	}

	writeLine(os.Stdout, "done: uploaded="+strconv.Itoa(len(plan.Uploads))+" deleted="+strconv.Itoa(len(plan.Deletes)))
	return 0
}

func parseArgs(args []string) (options, error) {
	var opts options
	fs := flag.NewFlagSet(cliName, flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&opts.bucket, flagBucket, "", "S3 bucket name (required)")
	fs.StringVar(&opts.prefix, flagPrefix, "", "S3 key prefix for sync mode (e.g. lambdas/dev/); required unless --file/--key are set")
	fs.StringVar(&opts.artifactDir, flagArtifactDir, "lambdas/target/lambda", "Artifact dir containing */bootstrap.zip or */bootstrap (sync mode)")
	fs.StringVar(&opts.file, flagFile, "", "Local zip file to upload (single-file mode; requires --key)")
	fs.StringVar(&opts.key, flagKey, "", "Exact S3 key for the upload (single-file mode; requires --file)")
	fs.StringVar(&opts.region, flagRegion, "", "AWS region override (optional)")
	fs.BoolVar(&opts.dryRun, flagDryRun, false, "Print planned actions without changing S3")
	fs.BoolVar(&opts.noDelete, flagNoDelete, false, "Upload/update only (do not delete remote extras; sync mode only)")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}

	if opts.bucket == "" {
		return options{}, fmt.Errorf("--bucket is required")
	}
	if opts.file != "" || opts.key != "" {
		if opts.file == "" {
			return options{}, fmt.Errorf("--file is required when --key is set")
		}
		if opts.key == "" {
			return options{}, fmt.Errorf("--key is required when --file is set")
		}
	} else if opts.prefix == "" {
		return options{}, fmt.Errorf("--prefix is required for sync mode (or use --file + --key for single-file upload)")
	}
	return opts, nil
}

func loadAWSConfig(ctx context.Context, region string) (aws.Config, error) {
	if region != "" {
		return config.LoadDefaultConfig(ctx, config.WithRegion(region))
	}
	return config.LoadDefaultConfig(ctx)
}

func printPlan(plan packer.Plan, bucket, prefix string, dryRun bool) {
	mode := modeApply
	if dryRun {
		mode = modeDryRun
	}
	writeLine(os.Stdout, cliName+" ("+mode+")")
	writeLine(os.Stdout, labelBucket+": "+bucket)
	writeLine(os.Stdout, labelPrefix+": "+prefix)
	writeLine(os.Stdout, "uploads: "+strconv.Itoa(len(plan.Uploads)))
	writeLine(os.Stdout, "deletes: "+strconv.Itoa(len(plan.Deletes)))
}

func writeLine(w io.Writer, line string) {
	_, _ = io.WriteString(w, line+"\n") //nolint:errcheck // best-effort: stdout/stderr write failures (e.g. broken pipe) are intentionally ignored
}

func writeErrorLine(w io.Writer, prefix string, err error) {
	writeLine(w, prefix+err.Error())
}
