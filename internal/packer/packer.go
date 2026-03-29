// Package packer provides artifact discovery, (optional) zipping, and S3 sync planning.
package packer

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3PutDeleteClient interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type LocalArtifact struct {
	Function string
	// If ZipPath is non-empty, it points to an already zipped artifact.
	ZipPath string
	// If RawPath is non-empty, it points to a file that must be zipped before upload.
	RawPath string
	Key     string
}

type Plan struct {
	Uploads []LocalArtifact
	Deletes []string
}

func NormalizePrefix(prefix string) (string, error) {
	p := strings.TrimSpace(prefix)
	if p == "" {
		return "", errors.New("prefix must be non-empty")
	}
	p = strings.TrimPrefix(p, "/")
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p, nil
}

func DiscoverLocalArtifacts(artifactDir, prefix string) ([]LocalArtifact, error) {
	entries, err := os.ReadDir(artifactDir)
	if err != nil {
		return nil, err
	}

	out := make([]LocalArtifact, 0, len(entries))
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		fn := ent.Name()
		fnDir := filepath.Join(artifactDir, fn)

		art, ok, err := discoverArtifactForFunctionDir(fnDir, fn, prefix)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, art)
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no artifacts found under %q", artifactDir)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func discoverArtifactForFunctionDir(fnDir, function, prefix string) (LocalArtifact, bool, error) {
	key := prefix + function + artifactZipExt

	zipPath := filepath.Join(fnDir, artifactBootstrapZip)
	exists, err := statExists(zipPath)
	if err != nil {
		return LocalArtifact{}, false, err
	}
	if exists {
		return LocalArtifact{Function: function, ZipPath: zipPath, Key: key}, true, nil
	}

	rawPath := filepath.Join(fnDir, artifactBootstrap)
	exists, err = statExists(rawPath)
	if err != nil {
		return LocalArtifact{}, false, err
	}
	if exists {
		return LocalArtifact{Function: function, RawPath: rawPath, Key: key}, true, nil
	}

	single, ok, err := singleRegularFile(fnDir)
	if err != nil {
		return LocalArtifact{}, false, err
	}
	if ok {
		return LocalArtifact{Function: function, RawPath: single, Key: key}, true, nil
	}

	return LocalArtifact{}, false, fmt.Errorf("no artifact found for %q (expected bootstrap.zip, bootstrap, or single file)", fnDir)
}

func statExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func singleRegularFile(dir string) (string, bool, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", false, err
	}

	var candidate string
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		info, err := f.Info()
		if err != nil {
			return "", false, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if candidate != "" {
			return "", false, nil
		}
		candidate = filepath.Join(dir, f.Name())
	}
	if candidate == "" {
		return "", false, nil
	}
	return candidate, true, nil
}

func ListRemoteZips(ctx context.Context, client s3.ListObjectsV2APIClient, bucket, prefix string) (map[string]struct{}, error) {
	out := map[string]struct{}{}

	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key
			if strings.HasSuffix(key, artifactZipExt) {
				out[key] = struct{}{}
			}
		}
	}
	return out, nil
}

func BuildPlan(local []LocalArtifact, remote map[string]struct{}, noDelete bool) Plan {
	desired := map[string]struct{}{}
	for _, a := range local {
		desired[a.Key] = struct{}{}
	}

	var deletes []string
	if !noDelete {
		for key := range remote {
			if _, ok := desired[key]; !ok {
				deletes = append(deletes, key)
			}
		}
		sort.Strings(deletes)
	}

	return Plan{Uploads: local, Deletes: deletes}
}

func PutArtifact(ctx context.Context, client s3PutDeleteClient, bucket string, a LocalArtifact) error {
	switch {
	case a.ZipPath != "":
		f, err := os.Open(a.ZipPath)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(a.Key),
			Body:        f,
			ContentType: aws.String(contentTypeZip),
		})
		return err
	case a.RawPath != "":
		return putZippedRaw(ctx, client, bucket, a.Key, a.RawPath)
	default:
		return errors.New("artifact has neither ZipPath nor RawPath")
	}
}

func putZippedRaw(ctx context.Context, client s3PutDeleteClient, bucket, key, rawPath string) error {
	rawFile, err := os.Open(rawPath)
	if err != nil {
		return err
	}
	defer rawFile.Close()

	pr, pw := io.Pipe()
	zw := zip.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer zw.Close()

		entryName := filepath.Base(rawPath)
		w, err := zw.Create(entryName)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(w, rawFile); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
	}()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        pr,
		ContentType: aws.String(contentTypeZip),
	})
	return err
}

func DeleteKeys(ctx context.Context, client s3PutDeleteClient, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	for _, batch := range batchKeys(keys, defaultDeleteBatchSize) {
		if err := deleteObjects(ctx, client, bucket, batch); err != nil {
			return err
		}
	}
	return nil
}

func deleteObjects(ctx context.Context, client s3PutDeleteClient, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	var objs []types.ObjectIdentifier
	for _, k := range keys {
		k := k
		objs = append(objs, types.ObjectIdentifier{Key: &k})
	}

	out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: objs, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return err
	}
	if len(out.Errors) > 0 {
		var parts []string
		for _, e := range out.Errors {
			if e.Key == nil || e.Message == nil {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s: %s", *e.Key, *e.Message))
		}
		if len(parts) > 0 {
			return fmt.Errorf("%s: %s", errDeleteErrorsPrefix, strings.Join(parts, "; "))
		}
		return errors.New(errDeleteErrorsPrefix)
	}
	return nil
}

func batchKeys(keys []string, size int) [][]string {
	if size <= 0 {
		size = defaultDeleteBatchSize
	}
	var out [][]string
	for len(keys) > 0 {
		if len(keys) <= size {
			out = append(out, keys)
			break
		}
		out = append(out, keys[:size])
		keys = keys[size:]
	}
	return out
}
