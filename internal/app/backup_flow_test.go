package backup

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

func TestSpoolChangesFeedDispatchesBatches(t *testing.T) {
	service := &fakeCloudantService{
		changesStream: io.NopCloser(strings.NewReader(`{"results":[{"id":"doc1"},{"id":"doc2"}],"last_seq":"2-g1AAA"}`)),
	}
	output := &fakeOutputWriter{}
	cb, err := NewWithDeps(&AppConfig{
		DatabaseName: "mydb",
		Parallelism:  1,
		BufferSize:   2,
		Mode:         ModeFull,
		Since:        "0",
	}, service, output)
	if err != nil {
		t.Fatalf("unexpected error creating backup: %v", err)
	}

	if err := cb.SpoolChangesFeed(context.Background()); err != nil {
		t.Fatalf("unexpected error spooling changes: %v", err)
	}

	select {
	case batch := <-cb.jobsChan:
		if batch.batchID != 1 {
			t.Fatalf("expected batch id 1, got %d", batch.batchID)
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
		t.Fatal("expected dispatched batch")
	}

	if cb.changesCount != 2 {
		t.Fatalf("expected changes count 2, got %d", cb.changesCount)
	}
}

func TestFetchDocsWorkerWritesResult(t *testing.T) {
	docID := "doc1"
	doc := cloudantv1.Document{
		ID: &docID,
	}
	service := &fakeCloudantService{
		bulkGetResult: &cloudantv1.BulkGetResult{
			Results: []cloudantv1.BulkGetResultItem{
				{
					ID: &docID,
					Docs: []cloudantv1.BulkGetResultDocument{
						{Ok: &doc},
					},
				},
			},
		},
	}
	output := &fakeOutputWriter{}
	cb, err := NewWithDeps(&AppConfig{
		DatabaseName: "mydb",
		Parallelism:  1,
		BufferSize:   2,
		Mode:         ModeFull,
		Since:        "0",
	}, service, output)
	if err != nil {
		t.Fatalf("unexpected error creating backup: %v", err)
	}

	cb.wgWorker.Add(1)
	go cb.fetchDocsWorker(context.Background(), func() {})

	cb.resultsChan = make(chan ResultSet, 1)

	cb.jobsChan <- *NewBatch(1, []string{"doc1"})
	close(cb.jobsChan)
	cb.wgWorker.Wait()

	select {
	case result := <-cb.resultsChan:
		if result.batchID != 1 {
			t.Fatalf("expected batch id 1, got %d", result.batchID)
		}
		if result.docCount != 1 {
			t.Fatalf("expected doc count 1, got %d", result.docCount)
		}
		if !strings.Contains(string(result.result), "\"doc1\"") {
			t.Fatalf("expected marshalled result to contain doc1, got %s", string(result.result))
		}
	default:
		t.Fatal("expected worker result")
	}
}

func TestStatsCollectorWritesHeaderAndResults(t *testing.T) {
	output := &fakeOutputWriter{}
	cb, err := NewWithDeps(&AppConfig{
		DatabaseName: "mydb",
		Parallelism:  1,
		BufferSize:   2,
		Mode:         ModeShallow,
		Since:        "0",
	}, &fakeCloudantService{}, output)
	if err != nil {
		t.Fatalf("unexpected error creating backup: %v", err)
	}

	cb.wgCollector.Add(1)
	go cb.statsCollector(context.Background(), func() {})

	cb.resultsChan <- ResultSet{
		result:   []byte(`[{"_id":"doc1"}]`),
		docCount: 1,
		batchID:  1,
	}
	close(cb.resultsChan)
	cb.wgCollector.Wait()

	if len(output.headers) != 1 || output.headers[0] != ModeShallow {
		t.Fatalf("expected one header for mode %s, got %#v", ModeShallow, output.headers)
	}
	if len(output.results) != 1 || string(output.results[0]) != `[{"_id":"doc1"}]` {
		t.Fatalf("unexpected output results: %#v", output.results)
	}
}

func TestSpoolChangesFeedHonorsCancelledContext(t *testing.T) {
	service := &fakeCloudantService{
		changesStream: io.NopCloser(strings.NewReader(`{"results":[{"id":"doc1"}],"last_seq":"1-g1AAA"}`)),
	}
	cb, err := NewWithDeps(&AppConfig{
		DatabaseName: "mydb",
		Parallelism:  1,
		BufferSize:   1,
		Mode:         ModeFull,
		Since:        "0",
	}, service, &fakeOutputWriter{})
	if err != nil {
		t.Fatalf("unexpected error creating backup: %v", err)
	}

	cb.jobsChan = make(chan Batch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cb.SpoolChangesFeed(ctx)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestFetchDocsWorkerHonorsCancelledContextOnResultSend(t *testing.T) {
	docID := "doc1"
	doc := cloudantv1.Document{
		ID: &docID,
	}
	service := &fakeCloudantService{
		bulkGetResult: &cloudantv1.BulkGetResult{
			Results: []cloudantv1.BulkGetResultItem{
				{
					ID: &docID,
					Docs: []cloudantv1.BulkGetResultDocument{
						{Ok: &doc},
					},
				},
			},
		},
	}
	cb, err := NewWithDeps(&AppConfig{
		DatabaseName: "mydb",
		Parallelism:  1,
		BufferSize:   2,
		Mode:         ModeFull,
		Since:        "0",
	}, service, &fakeOutputWriter{})
	if err != nil {
		t.Fatalf("unexpected error creating backup: %v", err)
	}

	cb.resultsChan = make(chan ResultSet)
	ctx, cancel := context.WithCancel(context.Background())

	cb.wgWorker.Add(1)
	go cb.fetchDocsWorker(ctx, cancel)

	cb.jobsChan <- *NewBatch(1, []string{"doc1"})

	cancel()
	close(cb.jobsChan)
	cb.wgWorker.Wait()

	select {
	case result := <-cb.resultsChan:
		t.Fatalf("did not expect published result after cancellation: %+v", result)
	default:
	}
}
