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
	"github.com/IBM/cloudant-go-sdk/features"
	"github.com/IBM/go-sdk-core/v5/core"
)

const changesFeedSeqInterval = 500

type (
	// changesFollower yields one change at a time from the changes feed.
	changesFollower interface {
		Next() (cloudantv1.ChangesResultItem, error)
	}

	// changesFollowerFactory creates a follower positioned at a starting sequence.
	changesFollowerFactory interface {
		New(ctx context.Context, since string) (changesFollower, error)
	}

	// cloudantService defines the subset of the Cloudant client used for bulk fetches.
	cloudantService interface {
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
		appConfig              *AppConfig             // the command-line options
		service                cloudantService        // the Cloudant SDK client
		changesFollowerFactory changesFollowerFactory // creates changes followers
		output                 outputWriter           // where backup output is written
		buffer                 []string               // a batch of document ids to fetch
		bufferLen              int                    // the current position in the buffer
		wgWorker               sync.WaitGroup         // WaitGroup to keep track of running worker goroutines
		wgCollector            sync.WaitGroup         // WaitGroup to keep track of the results collector
		resultsChan            chan ResultSet         // channel to carry results of API calls
		jobsChan               chan Batch             // channel to carry jobs, which uses the Batch type
		errorsChan             chan error             // channel to carry errors that occurred when fetching documents from Cloudant
		changesCount           int                    // the total number of changes fetched from the changes follower
		logFile                *LogFile               // the log file, which is optionally written-to during the backup process
		batchID                int                    // the current batch ID
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

	return NewWithDeps(appConfig, service, newSDKChangesFollowerFactory(service, appConfig.DatabaseName), newStdoutOutputWriter())
}

// NewWithDeps creates a CloudantBackup with injected dependencies for testing
// or custom wiring.
func NewWithDeps(appConfig *AppConfig, service cloudantService, changesFollowerFactory changesFollowerFactory, output outputWriter) (*CloudantBackup, error) {
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

	if changesFollowerFactory == nil {
		return nil, ErrNilChangesFollowerFactory
	}

	if output == nil {
		output = newStdoutOutputWriter()
	}

	// create struct
	cb := CloudantBackup{
		appConfig:              appConfig,
		service:                service,
		changesFollowerFactory: changesFollowerFactory,
		output:                 output,
		buffer:                 buffer,
		bufferLen:              0,
		wgWorker:               sync.WaitGroup{},
		wgCollector:            sync.WaitGroup{},
		resultsChan:            make(chan ResultSet, appConfig.Parallelism),
		jobsChan:               make(chan Batch, appConfig.Parallelism),
		errorsChan:             make(chan error, appConfig.Parallelism+1),
		changesCount:           0,
		logFile:                logFile,
		batchID:                1,
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
	since, err := cb.followChangesFeed(ctx, cb.appConfig.Since)
	if err != nil {
		return err
	}

	log.Printf("lastseq %v", since)

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

// followChangesFeed consumes the one-off changes follower until completion and
// returns the last non-nil sequence observed.
func (cb *CloudantBackup) followChangesFeed(ctx context.Context, since string) (string, error) {
	follower, err := cb.changesFollowerFactory.New(ctx, since)
	if err != nil {
		return since, err
	}

	currentSince := since
	for {
		change, err := follower.Next()
		if err != nil {
			if err == io.EOF {
				return currentSince, nil
			}
			return currentSince, err
		}

		if change.Seq != nil {
			currentSince = *change.Seq
		}

		if err := cb.queueChange(ctx, change); err != nil {
			return currentSince, err
		}
	}
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

// closeResources flushes and closes optional output resources.
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

// loadResumeBatches loads pending batches from the log file when resume mode is enabled.
func (cb *CloudantBackup) loadResumeBatches() ([]Batch, error) {
	if !cb.appConfig.Resume {
		return nil, nil
	}
	return cb.logFile.Load(cb.appConfig.BufferSize)
}

// startWorkers launches fetch workers and the result collector.
func (cb *CloudantBackup) startWorkers(ctx context.Context, cancel context.CancelFunc) {
	for i := 0; i < cb.appConfig.Parallelism; i++ {
		cb.wgWorker.Add(1)
		go cb.fetchDocsWorker(ctx, cancel)
	}

	cb.wgCollector.Add(1)
	go cb.statsCollector(ctx, cancel)
}

// shutdownWorkers closes worker channels and waits for all goroutines to exit.
func (cb *CloudantBackup) shutdownWorkers() {
	close(cb.jobsChan)
	cb.wgWorker.Wait()
	close(cb.resultsChan)
	cb.wgCollector.Wait()
}

// produceBatches either resumes pending work or spools fresh changes.
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

// resumeBatches re-enqueues batches loaded from the resume log.
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

// finalError returns the first asynchronously reported worker or collector error, if any.
func (cb *CloudantBackup) finalError() error {
	select {
	case err := <-cb.errorsChan:
		return err
	default:
		return nil
	}
}

// cancelWithError cancels the pipeline and attempts to publish the triggering error.
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

// ErrNilChangesFollowerFactory indicates that no changes follower factory was provided.
var ErrNilChangesFollowerFactory = io.ErrClosedPipe

// sdkChangesFollowerFactory builds SDK-backed one-off changes followers.
type sdkChangesFollowerFactory struct {
	service *cloudantv1.CloudantV1
	dbName  string
}

// newSDKChangesFollowerFactory creates a changes follower factory backed by the Cloudant SDK.
func newSDKChangesFollowerFactory(service *cloudantv1.CloudantV1, dbName string) changesFollowerFactory {
	return &sdkChangesFollowerFactory{
		service: service,
		dbName:  dbName,
	}
}

func (f *sdkChangesFollowerFactory) New(ctx context.Context, since string) (changesFollower, error) {
	postChangesOptions := f.service.NewPostChangesOptions(f.dbName)
	postChangesOptions.SetSince(since)
	postChangesOptions.SetIncludeDocs(false)
	postChangesOptions.SetSeqInterval(changesFeedSeqInterval)

	follower, err := features.NewChangesFollowerWithContext(ctx, f.service, postChangesOptions)
	if err != nil {
		return nil, err
	}

	changesCh, err := follower.StartOneOff()
	if err != nil {
		return nil, err
	}

	return &sdkChangesFollower{changesCh: changesCh}, nil
}

// sdkChangesFollower adapts the SDK changes channel to the local follower interface.
type sdkChangesFollower struct {
	changesCh <-chan features.ChangesItem
}

func (f *sdkChangesFollower) Next() (cloudantv1.ChangesResultItem, error) {
	changesItem, ok := <-f.changesCh
	if !ok {
		return cloudantv1.ChangesResultItem{}, io.EOF
	}

	return changesItem.Item()
}
