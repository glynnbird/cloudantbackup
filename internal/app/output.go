package backup

import (
	"bufio"
	"fmt"
	"os"
)

type (
	outputWriter interface {
		WriteHeader(mode string) error
		WriteResult(result []byte) error
	}

	stdoutOutputWriter struct {
		writer *bufio.Writer
	}
)

func newStdoutOutputWriter() *stdoutOutputWriter {
	return &stdoutOutputWriter{
		writer: bufio.NewWriterSize(os.Stdout, 64*1024),
	}
}

func (w *stdoutOutputWriter) WriteHeader(mode string) error {
	if _, err := fmt.Fprintf(w.writer, `{"name":"@cloudant/couchbackup","version":"1.0.0","mode":"%v"}`, mode); err != nil {
		return err
	}
	return w.writer.WriteByte('\n')
}

func (w *stdoutOutputWriter) WriteResult(result []byte) error {
	if _, err := w.writer.Write(result); err != nil {
		return err
	}
	return w.writer.WriteByte('\n')
}

func (w *stdoutOutputWriter) Flush() error {
	if w.writer == nil {
		return nil
	}
	return w.writer.Flush()
}

// Made with Bob
