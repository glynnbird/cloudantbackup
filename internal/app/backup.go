package backup

import (
	"context"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

// CloudantBackup is the state that represents a backup process.
type CloudantBackup struct {
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

// ErrNilChangesFollowerFactory indicates that no changes follower factory was provided.
var ErrNilChangesFollowerFactory = io.ErrClosedPipe

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

// SpoolChangesFeed reads the Cloudant _changes feed, batches document IDs, and
// sends the batches to workers through jobsChan.
func (cb *CloudantBackup) SpoolChangesFeed(ctx context.Context) (string, error) {
	since, err := cb.followChangesFeed(ctx, cb.appConfig.Since)
	if err != nil {
		return since, err
	}

	// if there are still unprocessed changes in the buffer
	if cb.bufferLen > 0 {
		// send them to be processed
		if err := cb.dispatchBatchToWorker(ctx); err != nil {
			return since, err
		}
	}

	// we're now finished consuming the changes feed
	log.Printf("Changes follower complete. %d changes\n", cb.changesCount)
	if cb.logFile != nil {
		if err := cb.logFile.WriteChangesComplete(); err != nil {
			return since, err
		}
	}
	return since, nil
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

	lastSeq, err := cb.produceBatches(ctx, cancel, batchesToResume)
	if err != nil {
		cancel()
		cb.shutdownWorkers()
		return err
	}

	cb.shutdownWorkers()

	if err := cb.finalError(); err != nil {
		return err
	}

	if !cb.appConfig.Resume {
		log.Printf("lastseq %v", lastSeq)
	}

	return nil
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
