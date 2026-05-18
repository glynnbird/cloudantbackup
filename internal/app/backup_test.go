package backup

import (
	"context"
	"testing"
)

func TestDispatchBatchToWorker(t *testing.T) {
	cb := &CloudantBackup{
		buffer:       make([]string, 3),
		bufferLen:    2,
		jobsChan:     make(chan Batch, 1),
		errorsChan:   make(chan error, 1),
		changesCount: 0,
		batchID:      4,
	}
	cb.buffer[0] = "doc1"
	cb.buffer[1] = "doc2"

	if err := cb.dispatchBatchToWorker(context.Background()); err != nil {
		t.Fatalf("unexpected error dispatching batch: %v", err)
	}

	select {
	case batch := <-cb.jobsChan:
		if batch.batchID != 4 {
			t.Fatalf("expected batch id 4, got %d", batch.batchID)
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
	default:
		t.Fatal("expected a batch to be dispatched")
	}

	if cb.batchID != 5 {
		t.Fatalf("expected batch id to advance to 5, got %d", cb.batchID)
	}
	if cb.changesCount != 2 {
		t.Fatalf("expected changes count 2, got %d", cb.changesCount)
	}
	if cb.bufferLen != 0 {
		t.Fatalf("expected bufferLen reset to 0, got %d", cb.bufferLen)
	}
}

func TestDispatchBatchToWorkerEmptyBuffer(t *testing.T) {
	cb := &CloudantBackup{
		buffer:     make([]string, 1),
		bufferLen:  0,
		jobsChan:   make(chan Batch, 1),
		errorsChan: make(chan error, 1),
		batchID:    9,
	}

	if err := cb.dispatchBatchToWorker(context.Background()); err != nil {
		t.Fatalf("unexpected error dispatching empty batch: %v", err)
	}

	select {
	case batch := <-cb.jobsChan:
		t.Fatalf("did not expect dispatched batch, got %+v", batch)
	default:
	}

	if cb.batchID != 9 {
		t.Fatalf("expected batch id unchanged, got %d", cb.batchID)
	}
}
