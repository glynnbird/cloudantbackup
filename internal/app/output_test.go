package backup

import (
	"bufio"
	"bytes"
	"testing"
)

func TestStdoutOutputWriterWritesHeaderAndResult(t *testing.T) {
	var buf bytes.Buffer
	writer := &stdoutOutputWriter{
		writer: bufio.NewWriter(&buf),
	}

	if err := writer.WriteHeader(ModeFull); err != nil {
		t.Fatalf("unexpected header error: %v", err)
	}
	if err := writer.WriteResult([]byte(`[{"_id":"doc1"}]`)); err != nil {
		t.Fatalf("unexpected result error: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}

	got := buf.String()
	want := "{\"name\":\"@cloudant/couchbackup\",\"version\":\"1.0.0\",\"mode\":\"full\"}\n[{\"_id\":\"doc1\"}]\n"
	if got != want {
		t.Fatalf("unexpected output.\nwant: %q\ngot:  %q", want, got)
	}
}

func TestStdoutOutputWriterFlushNilWriter(t *testing.T) {
	writer := &stdoutOutputWriter{}
	if err := writer.Flush(); err != nil {
		t.Fatalf("expected nil flush error, got %v", err)
	}
}

// Made with Bob
