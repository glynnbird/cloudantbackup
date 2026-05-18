package backup

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

const (
	changesFeedMaxRetries     = 5
	changesFeedInitialBackoff = 1 * time.Second
)

type (
	cloudantService interface {
		PostChangesAsStream(*cloudantv1.PostChangesOptions) (io.ReadCloser, *core.DetailedResponse, error)
		NewPostChangesOptions(string) *cloudantv1.PostChangesOptions
		NewPostBulkGetOptions(string, []cloudantv1.BulkGetQueryDocument) *cloudantv1.PostBulkGetOptions
		PostBulkGet(*cloudantv1.PostBulkGetOptions) (*cloudantv1.BulkGetResult, *core.DetailedResponse, error)
	}

	// ResultSet is the data sent back from the fetchDocsWorker on the resultsChan channel
	ResultSet struct {
		result   []byte
		docCount int
		batchID  int
	}

	// CloudantBackup is the state that represents a backup process.
	CloudantBackup struct {
		appConfig    *AppConfig      // the command-line options
		service      cloudantService // the Cloudant SDK client
		output       outputWriter    // where backup output is written
		buffer       []string        // a batch of document ids to fetch
		bufferLen    int             // the current position in the buffer
		wgWorker     sync.WaitGroup  // WaitGroup to keep track of running worker goroutines
		wgCollector  sync.WaitGroup  // WaitGroup to keep track of the results collector
		resultsChan  chan ResultSet  // channel to carry results of API calls
		jobsChan     chan Batch      // channel to carry jobs, which uses the Batch type
		errorsChan   chan error      // channel to carry errors that occurred when fetching documents from Cloudant
		changesCount int             // the total number of changes fetched from the changes follower
		logFile      *LogFile        // the log file, which is optionally written-to during the backup process
		batchID      int             // the current batch ID
	}
)

// New creates a CloudantBackup with its default dependencies.
// Call Run to execute the backup.
func New() (*CloudantBackup, error) {

	// load the CLI parameters
	appConfig, err := NewAppConfig()
	if err != nil {
		return nil, err
	}

	// set up the Cloudant service
	service, err := cloudantv1.NewCloudantV1UsingExternalConfig(&cloudantv1.CloudantV1Options{})
	if err != nil {
		return nil, err
	}
	service.EnableRetries(3, 5*time.Second)
	header := http.Header{}
	header.Add("user-agent", "couchbackup-cloudant/1.0 (Go)")
	service.SetDefaultHeaders(header)

	return NewWithDeps(appConfig, service, newStdoutOutputWriter())
}

func NewWithDeps(appConfig *AppConfig, service cloudantService, output outputWriter) (*CloudantBackup, error) {
	// create the buffer
	buffer := make([]string, appConfig.BufferSize)

	// create log file
	var logFile *LogFile = nil
	var err error
	if appConfig.LogFilename != "" {
		logFile, err = NewLogFile(appConfig.LogFilename)
		if err != nil {
			return nil, err
		}
	}

	if output == nil {
		output = newStdoutOutputWriter()
	}

	// create struct
	cb := CloudantBackup{
		appConfig:    appConfig,
		service:      service,
		output:       output,
		buffer:       buffer,
		bufferLen:    0,
		wgWorker:     sync.WaitGroup{},
		wgCollector:  sync.WaitGroup{},
		resultsChan:  make(chan ResultSet, appConfig.Parallelism),
		jobsChan:     make(chan Batch, appConfig.Parallelism),
		errorsChan:   make(chan error, appConfig.Parallelism+1),
		changesCount: 0,
		logFile:      logFile,
		batchID:      1,
	}

	return &cb, nil
}

// dispatchBatchToWorker creates a Batch from the buffered document IDs and
// sends it to a worker via jobsChan.
func (cb *CloudantBackup) dispatchBatchToWorker(ctx context.Context) error {
	if cb.bufferLen == 0 {
		return nil
	}
	// clone the batch to avoid data being overwritten
	clone := make([]string, cb.bufferLen)
	copy(clone, cb.buffer[:cb.bufferLen])

	// create a new Batch struct
	batch := NewBatch(cb.batchID, clone)

	// log it
	if cb.logFile != nil {
		if err := cb.logFile.WriteNewBatch(batch); err != nil {
			return err
		}
	}

	// send it to a worker via the jobsChan
	select {
	case <-ctx.Done():
		return ctx.Err()
	case cb.jobsChan <- *batch:
	}

	// update counters
	cb.batchID++
	cb.changesCount += cb.bufferLen
	cb.bufferLen = 0
	return nil
}

// changesFeed models the subset of the Cloudant _changes response used by the backup.
type changesFeed struct {
	Results []cloudantv1.ChangesResultItem `json:"results"`
	LastSeq string                         `json:"last_seq"`
}

// queueChange adds a change ID to the current batch buffer and dispatches a
// full batch when the buffer reaches capacity.
func (cb *CloudantBackup) queueChange(ctx context.Context, change cloudantv1.ChangesResultItem) error {
	if change.ID == nil {
		return nil
	}

	cb.buffer[cb.bufferLen] = *change.ID
	cb.bufferLen++

	if cb.bufferLen == cb.appConfig.BufferSize {
		return cb.dispatchBatchToWorker(ctx)
	}
	return nil
}

// SpoolChangesFeed reads the Cloudant _changes feed, batches document IDs, and
// sends the batches to workers through jobsChan.
func (cb *CloudantBackup) SpoolChangesFeed(ctx context.Context) error {
	since := cb.appConfig.Since
	retries := 0
	backoff := changesFeedInitialBackoff

	for {
		feed, err := cb.loadChangesFeed(since)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if retries >= changesFeedMaxRetries {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}

				retries++
				backoff *= 2
				continue
			}
			return err
		}

		for _, change := range feed.Results {
			if change.Seq != nil {
				since = *change.Seq
			}

			if err := cb.queueChange(ctx, change); err != nil {
				return err
			}
		}

		if feed.LastSeq != "" {
			log.Printf("lastseq %v", feed.LastSeq)
			break
		}
	}

	// if there are still unprocessed changes in the buffer
	if cb.bufferLen > 0 {
		// send them to be processed
		if err := cb.dispatchBatchToWorker(ctx); err != nil {
			return err
		}
	}

	// we're now finished consuming the changes feed
	log.Printf("Changes follower complete. %d changes\n", cb.changesCount)
	if cb.logFile != nil {
		if err := cb.logFile.WriteChangesComplete(); err != nil {
			return err
		}
	}
	return nil
}

func (cb *CloudantBackup) loadChangesFeed(since string) (*changesFeed, error) {
	postChangesOptions := cb.service.NewPostChangesOptions(cb.appConfig.DatabaseName)
	postChangesOptions.SetSince(since)
	postChangesOptions.SetIncludeDocs(false)
	postChangesOptions.SetSeqInterval(500)

	stream, _, err := cb.service.PostChangesAsStream(postChangesOptions)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	decoder := json.NewDecoder(stream)
	var feed changesFeed
	if err := decoder.Decode(&feed); err != nil {
		return nil, err
	}
	return &feed, nil
}

// Run executes the backup.
// If Resume is enabled, pending batches are loaded from the log file.
// Otherwise, batches are created from the _changes feed and processed by workers.
func (cb *CloudantBackup) Run(ctx context.Context) error {
	defer cb.closeResources()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	batchesToResume, err := cb.loadResumeBatches()
	if err != nil {
		return err
	}

	cb.startWorkers(ctx, cancel)
	defer cb.shutdownWorkers()

	if err := cb.produceBatches(ctx, cancel, batchesToResume); err != nil {
		return err
	}

	return cb.finalError()
}

func (cb *CloudantBackup) closeResources() {
	if flusher, ok := cb.output.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			log.Printf("error flushing output: %v", err)
		}
	}

	if cb.logFile != nil {
		if err := cb.logFile.Close(); err != nil {
			log.Printf("error closing log file: %v", err)
		}
	}
}

func (cb *CloudantBackup) loadResumeBatches() ([]Batch, error) {
	if !cb.appConfig.Resume {
		return nil, nil
	}
	return cb.logFile.Load(cb.appConfig.BufferSize)
}

func (cb *CloudantBackup) startWorkers(ctx context.Context, cancel context.CancelFunc) {
	for i := 0; i < cb.appConfig.Parallelism; i++ {
		cb.wgWorker.Add(1)
		go cb.fetchDocsWorker(ctx, cancel)
	}

	cb.wgCollector.Add(1)
	go cb.statsCollector(ctx, cancel)
}

func (cb *CloudantBackup) shutdownWorkers() {
	close(cb.jobsChan)
	cb.wgWorker.Wait()
	close(cb.resultsChan)
	cb.wgCollector.Wait()
}

func (cb *CloudantBackup) produceBatches(ctx context.Context, cancel context.CancelFunc, batchesToResume []Batch) error {
	if cb.appConfig.Resume {
		return cb.resumeBatches(ctx, cancel, batchesToResume)
	}

	if err := cb.SpoolChangesFeed(ctx); err != nil {
		cancel()
		return err
	}
	return nil
}

func (cb *CloudantBackup) resumeBatches(ctx context.Context, cancel context.CancelFunc, batchesToResume []Batch) error {
	log.Printf("Resuming: %v batches", len(batchesToResume))
	for _, batch := range batchesToResume {
		if err := cb.logFile.WriteNewBatch(&batch); err != nil {
			cancel()
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case cb.jobsChan <- batch:
		}

		cb.changesCount += len(batch.docs)
	}
	return nil
}

func (cb *CloudantBackup) finalError() error {
	select {
	case err := <-cb.errorsChan:
		return err
	default:
		return nil
	}
}

func (cb *CloudantBackup) cancelWithError(cancel context.CancelFunc, err error) {
	cancel()
	select {
	case cb.errorsChan <- err:
	default:
	}
}

// fetchDocsWorker reads batches from jobsChan, fetches the documents from
// Cloudant, and sends ResultSet values to resultsChan.
func (cb *CloudantBackup) fetchDocsWorker(ctx context.Context, cancel context.CancelFunc) {
	// make sure we release our slot in the WaitGroup
	defer cb.wgWorker.Done()

	for {
		var job Batch
		var ok bool

		select {
		case <-ctx.Done():
			return
		case job, ok = <-cb.jobsChan:
			if !ok {
				return
			}
		}

		// formulate bulk docs request
		postBulkGetOptions := cb.service.NewPostBulkGetOptions(cb.appConfig.DatabaseName, job.docs)
		if cb.appConfig.Mode == ModeFull {
			postBulkGetOptions.SetRevs(true)
		}
		bulkGetResult, _, err := cb.service.PostBulkGet(postBulkGetOptions)
		if err != nil {
			cb.cancelWithError(cancel, err)
			return
		}
		backupDocs := make([]cloudantv1.Document, 0, len(job.docs))
		docCount := 0
		for _, result := range bulkGetResult.Results {
			for _, doc := range result.Docs {
				if doc.Error == nil {
					backupDocs = append(backupDocs, *doc.Ok)
					docCount++
				}
			}
		}

		// send results back to resultsChan as marshalled JSON bytes together
		// with the number of documents fetched
		b, err := json.Marshal(backupDocs)
		if err != nil {
			cb.cancelWithError(cancel, err)
			return
		}
		rs := ResultSet{
			result:   b,
			docCount: docCount,
			batchID:  job.batchID,
		}
		select {
		case <-ctx.Done():
			return
		case cb.resultsChan <- rs:
		}
	}
}

// statsCollector writes the backup header and result batches, tracks the total
// number of saved documents, and stops on the first error.
func (cb *CloudantBackup) statsCollector(ctx context.Context, cancel context.CancelFunc) {
	defer cb.wgCollector.Done()
	total := 0

	// header line
	if err := cb.output.WriteHeader(cb.appConfig.Mode); err != nil {
		cb.cancelWithError(cancel, err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-cb.resultsChan:
			if !ok {
				return
			}

			// increment docCount
			total += r.docCount

			// write the output batch
			if err := cb.output.WriteResult(r.result); err != nil {
				cb.cancelWithError(cancel, err)
				return
			}

			// update the log file
			if cb.logFile != nil {
				if err := cb.logFile.WriteDoneBatch(r.batchID); err != nil {
					cb.cancelWithError(cancel, err)
					return
				}
			}

			// log the completion of this batch on stderr
			log.Printf("Batch %d: saved %d docs. Total: %d\n", r.batchID, r.docCount, total)
		}
	}
}
