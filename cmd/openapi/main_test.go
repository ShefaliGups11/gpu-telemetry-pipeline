package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"
)

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestMainFn(t *testing.T) {
	fatalExit = func(int) {}
	defer func() { fatalExit = os.Exit }()
	main()
}

func TestWriteSpec(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSpec(&buf); err != nil {
		t.Fatal(err)
	}
	var spec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatal(spec["openapi"])
	}
}

func TestOsExit(t *testing.T) {
	osExit(nil)
	var code int
	fatalExit = func(c int) { code = c }
	defer func() { fatalExit = os.Exit }()
	osExit(io.ErrClosedPipe)
	if code != 1 {
		t.Fatalf("code %d", code)
	}
	if err := writeSpec(errWriter{}); err == nil {
		t.Fatal("expected write error")
	}
}
