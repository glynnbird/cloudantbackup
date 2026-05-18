package backup

import "testing"

func TestNewBatch(t *testing.T) {
	batch := NewBatch(7, []string{"doc1", "doc2"})

	if batch.batchID != 7 {
		t.Fatalf("expected batch id 7, got %d", batch.batchID)
	}
	if len(batch.docs) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(batch.docs))
	}
	if batch.docs[0].ID == nil || *batch.docs[0].ID != "doc1" {
		t.Fatalf("expected first doc id doc1, got %#v", batch.docs[0].ID)
	}
	if batch.docs[1].ID == nil || *batch.docs[1].ID != "doc2" {
		t.Fatalf("expected second doc id doc2, got %#v", batch.docs[1].ID)
	}
}

func TestNewBatchFromLogLine(t *testing.T) {
	batch, err := NewBatchFromLogLine(`:t batch56 [{"id":"a"},{"id":"b"}]`, 500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if batch.batchID != 56 {
		t.Fatalf("expected batch id 56, got %d", batch.batchID)
	}
	if len(batch.docs) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(batch.docs))
	}
	if batch.docs[0].ID == nil || *batch.docs[0].ID != "a" {
		t.Fatalf("expected first doc id a, got %#v", batch.docs[0].ID)
	}
	if batch.docs[1].ID == nil || *batch.docs[1].ID != "b" {
		t.Fatalf("expected second doc id b, got %#v", batch.docs[1].ID)
	}
}

func TestNewBatchFromLogLineInvalid(t *testing.T) {
	_, err := NewBatchFromLogLine(`not a batch line`, 10)
	if err == nil {
		t.Fatal("expected error for invalid log line")
	}
}

func TestToLogString(t *testing.T) {
	batch := NewBatch(3, []string{"x", "y"})

	got := batch.ToLogString()
	want := `[{"id":"x"},{"id":"y"}]`

	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}
