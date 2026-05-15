package backup

import (
	"io"
	"strings"
	"testing"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

type fakeCloudantService struct {
	changesStream io.ReadCloser
	changesErr    error
	bulkGetResult *cloudantv1.BulkGetResult
	bulkGetErr    error
	lastBulkDocs  []cloudantv1.BulkGetQueryDocument
}

func (f *fakeCloudantService) PostChangesAsStream(*cloudantv1.PostChangesOptions) (io.ReadCloser, *core.DetailedResponse, error) {
	return f.changesStream, nil, f.changesErr
}

func (f *fakeCloudantService) NewPostChangesOptions(db string) *cloudantv1.PostChangesOptions {
	return (&cloudantv1.CloudantV1{}).NewPostChangesOptions(db)
}

func (f *fakeCloudantService) NewPostBulkGetOptions(db string, docs []cloudantv1.BulkGetQueryDocument) *cloudantv1.PostBulkGetOptions {
	f.lastBulkDocs = append([]cloudantv1.BulkGetQueryDocument(nil), docs...)
	return (&cloudantv1.CloudantV1{}).NewPostBulkGetOptions(db, docs)
}

func (f *fakeCloudantService) PostBulkGet(*cloudantv1.PostBulkGetOptions) (*cloudantv1.BulkGetResult, *core.DetailedResponse, error) {
	return f.bulkGetResult, nil, f.bulkGetErr
}

type fakeOutputWriter struct {
	headers []string
	results []string
	err     error
}

func (f *fakeOutputWriter) WriteHeader(mode string) error {
	if f.err != nil {
		return f.err
	}
	f.headers = append(f.headers, mode)
	return nil
}

func (f *fakeOutputWriter) WriteResult(result string) error {
	if f.err != nil {
		return f.err
	}
	f.results = append(f.results, result)
	return nil
}

func TestSpoolChangesFeedDispatchesBatches(t *testing.T) {
	service := &fakeCloudantService{
		changesStream: io.NopCloser(strings.NewReader("{\"id\":\"doc1\"}\n{\"id\":\"doc2\"}\n\"last_seq\":\"2-g1AAA\"\n")),
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

	if err := cb.SpoolChangesFeed(); err != nil {
		t.Fatalf("unexpected error spooling changes: %v", err)
	}

	select {
	case batch := <-cb.jobsChan:
		if batch.batchId != 1 {
			t.Fatalf("expected batch id 1, got %d", batch.batchId)
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
	go cb.fetchDocsWorker()

	cb.resultsChan = make(chan ResultSet, 1)

	cb.jobsChan <- *NewBatch(1, []string{"doc1"})
	close(cb.jobsChan)
	cb.wgWorker.Wait()

	select {
	case result := <-cb.resultsChan:
		if result.batchId != 1 {
			t.Fatalf("expected batch id 1, got %d", result.batchId)
		}
		if result.docCount != 1 {
			t.Fatalf("expected doc count 1, got %d", result.docCount)
		}
		if !strings.Contains(result.result, "\"doc1\"") {
			t.Fatalf("expected marshalled result to contain doc1, got %s", result.result)
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
	go cb.statsCollector()

	cb.resultsChan <- ResultSet{
		result:   `[{"_id":"doc1"}]`,
		docCount: 1,
		batchId:  1,
	}
	close(cb.resultsChan)
	cb.wgCollector.Wait()

	if len(output.headers) != 1 || output.headers[0] != ModeShallow {
		t.Fatalf("expected one header for mode %s, got %#v", ModeShallow, output.headers)
	}
	if len(output.results) != 1 || output.results[0] != `[{"_id":"doc1"}]` {
		t.Fatalf("unexpected output results: %#v", output.results)
	}
}
