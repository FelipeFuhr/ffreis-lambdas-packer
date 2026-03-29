package main

import "testing"

const testExpectedErrorGotNil = "expected error, got nil"

func TestParseArgsRequiresBucket(t *testing.T) {
	t.Parallel()

	_, err := parseArgs([]string{"--prefix", "p/"})
	if err == nil {
		t.Fatal(testExpectedErrorGotNil)
	}
}

func TestParseArgsRequiresPrefix(t *testing.T) {
	t.Parallel()

	_, err := parseArgs([]string{"--bucket", "b"})
	if err == nil {
		t.Fatal(testExpectedErrorGotNil)
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
