package main

import "testing"

func TestParseArgsRequiresBucket(t *testing.T) {
	t.Parallel()

	_, err := parseArgs([]string{"--prefix", "p/"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseArgsRequiresPrefix(t *testing.T) {
	t.Parallel()

	_, err := parseArgs([]string{"--bucket", "b"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseArgsOK(t *testing.T) {
	t.Parallel()

	opts, err := parseArgs([]string{"--bucket", "b", "--prefix", "p/"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.bucket != "b" || opts.prefix != "p/" {
		t.Fatalf("unexpected opts: %#v", opts)
	}
}

