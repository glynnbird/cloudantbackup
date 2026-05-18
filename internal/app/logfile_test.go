package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogFileWriteAndLoadPendingBatches(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "backup.log")

	lf, err := NewLogFile(filename)
	if err != nil {
		t.Fatalf("unexpected error creating logfile: %v", err)
	}

	batch1 := NewBatch(1, []string{"a", "b"})
	batch2 := NewBatch(2, []string{"c"})

	if err := lf.WriteNewBatch(batch1); err != nil {
		t.Fatalf("unexpected error writing batch1: %v", err)
	}
	if err := lf.WriteNewBatch(batch2); err != nil {
		t.Fatalf("unexpected error writing batch2: %v", err)
	}
	if err := lf.WriteDoneBatch(1); err != nil {
		t.Fatalf("unexpected error writing done batch: %v", err)
	}
	if err := lf.WriteChangesComplete(); err != nil {
		t.Fatalf("unexpected error writing changes complete: %v", err)
	}
	if err := lf.Close(); err != nil {
		t.Fatalf("unexpected error closing logfile: %v", err)
	}

	reopened, err := NewLogFile(filename)
	if err != nil {
		t.Fatalf("unexpected error reopening logfile: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	batches, err := reopened.Load(500)
	if err != nil {
		t.Fatalf("unexpected error loading logfile: %v", err)
	}

	if len(batches) != 1 {
		t.Fatalf("expected 1 pending batch, got %d", len(batches))
	}
	if batches[0].batchID != 2 {
		t.Fatalf("expected pending batch id 2, got %d", batches[0].batchID)
	}
	if len(batches[0].docs) != 1 {
		t.Fatalf("expected 1 doc in pending batch, got %d", len(batches[0].docs))
	}
	if batches[0].docs[0].ID == nil || *batches[0].docs[0].ID != "c" {
		t.Fatalf("expected pending doc id c, got %#v", batches[0].docs[0].ID)
	}
}

func TestLogFileLoadRequiresChangesComplete(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "backup.log")

	content := ":t batch1 [{\"id\":\"a\"}]\n"
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		t.Fatalf("unexpected error writing fixture: %v", err)
	}

	lf, err := NewLogFile(filename)
	if err != nil {
		t.Fatalf("unexpected error opening logfile: %v", err)
	}
	t.Cleanup(func() {
		_ = lf.Close()
	})

	_, err = lf.Load(10)
	if err == nil {
		t.Fatal("expected error when changes feed is not complete")
	}
	if err.Error() != "cannot resume - changes feed not complete" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogFileLoadRejectsMoreDoneBatchesThanExist(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "backup.log")

	content := strings.Join([]string{
		`:t batch1 [{"id":"a"}]`,
		`:d batch1`,
		`:changes_complete`,
		"",
	}, "\n")
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		t.Fatalf("unexpected error writing fixture: %v", err)
	}

	lf, err := NewLogFile(filename)
	if err != nil {
		t.Fatalf("unexpected error opening logfile: %v", err)
	}
	t.Cleanup(func() {
		_ = lf.Close()
	})

	_, err = lf.Load(10)
	if err == nil {
		t.Fatal("expected error when done batches are not fewer than total batches")
	}
	if err.Error() != "cannot resume - more batches done than exist" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogFileProcessDoneLine(t *testing.T) {
	lf := &LogFile{}

	batchID, err := lf.processDoneLine(":d batch42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batchID != 42 {
		t.Fatalf("expected batch id 42, got %d", batchID)
	}

	batchID, err = lf.processDoneLine("not valid")
	if err != nil {
		t.Fatalf("unexpected error for invalid line: %v", err)
	}
	if batchID != -1 {
		t.Fatalf("expected -1 for invalid line, got %d", batchID)
	}
}

func TestLogFileCloseNilHandle(t *testing.T) {
	lf := &LogFile{}
	if err := lf.Close(); err != nil {
		t.Fatalf("expected nil error closing nil handle, got %v", err)
	}
}

func TestValidateLogState(t *testing.T) {
	lf := &LogFile{}
	batches := []Batch{
		{batchID: 1},
		{batchID: 2},
	}

	if err := lf.validateLogState(false, batches, map[int]bool{}); err == nil || err.Error() != "cannot resume - changes feed not complete" {
		t.Fatalf("expected changes feed incomplete error, got %v", err)
	}

	if err := lf.validateLogState(true, batches[:1], map[int]bool{1: true}); err == nil || err.Error() != "cannot resume - more batches done than exist" {
		t.Fatalf("expected more batches done than exist error, got %v", err)
	}

	if err := lf.validateLogState(true, batches, map[int]bool{1: true}); err != nil {
		t.Fatalf("expected valid state, got %v", err)
	}
}

func TestFilterPendingBatches(t *testing.T) {
	lf := &LogFile{}
	batches := []Batch{
		{batchID: 1},
		{batchID: 2},
		{batchID: 3},
	}

	pending := lf.filterPendingBatches(batches, map[int]bool{
		1: true,
		3: true,
	})

	if len(pending) != 1 {
		t.Fatalf("expected 1 pending batch, got %d", len(pending))
	}
	if pending[0].batchID != 2 {
		t.Fatalf("expected pending batch id 2, got %d", pending[0].batchID)
	}
}

func TestParseLogFileScannerError(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "backup.log")
	if err := os.WriteFile(filename, []byte(strings.Repeat("x", 70*1024)), 0644); err != nil {
		t.Fatalf("unexpected error writing fixture: %v", err)
	}

	rc, err := os.Open(filename)
	if err != nil {
		t.Fatalf("unexpected error opening fixture: %v", err)
	}
	t.Cleanup(func() {
		_ = rc.Close()
	})

	lf := &LogFile{filename: filename}
	_, _, _, err = lf.parseLogFile(rc, 10)
	if err == nil {
		t.Fatal("expected scanner error for oversized token")
	}
}
