package backup

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

var lastSeqRE = regexp.MustCompile(`"last_seq":"([^"]*)"`)

type cloudantService interface {
	PostChangesAsStream(*cloudantv1.PostChangesOptions) (io.ReadCloser, *core.DetailedResponse, error)
	NewPostChangesOptions(string) *cloudantv1.PostChangesOptions
	NewPostBulkGetOptions(string, []cloudantv1.BulkGetQueryDocument) *cloudantv1.PostBulkGetOptions
	PostBulkGet(*cloudantv1.PostBulkGetOptions) (*cloudantv1.BulkGetResult, *core.DetailedResponse, error)
}

type outputWriter interface {
	WriteHeader(mode string) error
	WriteResult(result string) error
}

type stdoutOutputWriter struct{}

func (w *stdoutOutputWriter) WriteHeader(mode string) error {
	fmt.Printf(`{"name":"@cloudant/couchbackup","version":"1.0.0","mode":"%v"}`, mode)
	fmt.Println("")
	return nil
}

func (w *stdoutOutputWriter) WriteResult(result string) error {
	fmt.Println(result)
	return nil
}

// ResultSet is the data sent back from the fetchDocsWorker on the resultsChan channel
type ResultSet struct {
	result   string
	docCount int
	batchId  int
}

// CloudantBackup is the state that represents a backup process.
type CloudantBackup struct {
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
	batchId      int             // the current batch id
}

// New creates a new CloudantBackup struct which stores the state of backup, the channels,
// the log file and Cloudant service. A helper function Run actually executes the backup.
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

	return NewWithDeps(appConfig, service, &stdoutOutputWriter{})
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
		output = &stdoutOutputWriter{}
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
		resultsChan:  make(chan ResultSet),
		jobsChan:     make(chan Batch, appConfig.Parallelism),
		errorsChan:   make(chan error, appConfig.Parallelism+1),
		changesCount: 0,
		logFile:      logFile,
		batchId:      1,
	}

	return &cb, nil
}

// DispatchBatchToWorker creates a new Batch struct and sends it to a
// worker goroutine by sending the batch to the jobsChan
func (cb *CloudantBackup) dispatchBatchToWorker() {
	if cb.bufferLen == 0 {
		return
	}
	// clone the batch to avoid data being overwritten
	clone := make([]string, cb.bufferLen)
	copy(clone, cb.buffer[:cb.bufferLen])

	// create a new Batch struct
	batch := NewBatch(cb.batchId, clone)

	// log it
	if cb.logFile != nil {
		if err := cb.logFile.WriteNewBatch(batch); err != nil {
			cb.errorsChan <- err
			return
		}
	}

	// send it to a worker via the jobsChan
	cb.jobsChan <- *batch

	// update counters
	cb.batchId++
	cb.changesCount += cb.bufferLen
	cb.bufferLen = 0
}

// SpoolChangesFeed consumes the Cloudant changes feed, extracting batches of
// document ids that are to be fetched later. These are put into a Batch struct
// and sent to an available worker using the jobsChan.
func (cb *CloudantBackup) SpoolChangesFeed() error {

	// create a changes feed request
	postChangesOptions := cb.service.NewPostChangesOptions(cb.appConfig.DatabaseName)
	postChangesOptions.SetSince(cb.appConfig.Since)
	postChangesOptions.SetIncludeDocs(false)
	postChangesOptions.SetSeqInterval(500)
	stream, _, err := cb.service.PostChangesAsStream(postChangesOptions)
	if err != nil {
		return err
	}

	// scan through the changes feed line by line
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		// fetch a line
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		// changes look like this: { ... }, ignore anything else
		if line[0] == '{' {
			// strip off the ,
			if line[len(line)-1] == ',' {
				line = line[:len(line)-1]
			}

			// parse as a changes result item
			change := cloudantv1.ChangesResultItem{}
			err = json.Unmarshal([]byte(line), &change)
			if err != nil {
				continue
			}
			if change.ID == nil {
				continue
			}

			// add the id to our buffer
			cb.buffer[cb.bufferLen] = *change.ID
			cb.bufferLen++

			// if we have a full buffer
			if cb.bufferLen == cb.appConfig.BufferSize {
				// send the changes to be processed
				cb.dispatchBatchToWorker()
			}
		} else if strings.HasPrefix(line, `"last_seq":"`) {
			lastSequence := cb.extractLastSeq(line)
			log.Printf("lastseq %v", lastSequence)
		}
	}

	// if there are still unprocessed changes in the buffer
	if cb.bufferLen > 0 {
		// send them to be processed
		cb.dispatchBatchToWorker()
	}

	if err := scanner.Err(); err != nil {
		return err
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

func (cb *CloudantBackup) extractLastSeq(str string) string {
	matches := lastSeqRE.FindStringSubmatch(str)
	if matches != nil {
		return matches[1]
	} else {
		return ""
	}
}

// Run executes a Cloudant backup. If a backup is to be resumed, the list of batches
// of document ids to fetch is calculated from a log file, otherwise batches are
// created by spooling through the changes feed. A set of workers handles the document
// fetching.
func (cb *CloudantBackup) Run() error {

	// don't forget to close the log file
	defer func() {
		// close the log file
		if cb.logFile != nil {
			if err := cb.logFile.Close(); err != nil {
				log.Printf("error closing log file: %v", err)
			}
		}
	}()

	// if we are to resume, load the old log file
	var batchesToResume *[]Batch
	var err error
	if cb.appConfig.Resume {
		batchesToResume, err = cb.logFile.Load(cb.appConfig.BufferSize)
		if err != nil {
			return err
		}
	}

	// Start worker pool
	for i := 0; i < cb.appConfig.Parallelism; i++ {
		cb.wgWorker.Add(1)
		go cb.fetchDocsWorker()
	}

	// spin up a goroutine to handle the results and errors
	cb.wgCollector.Add(1)
	go cb.statsCollector()

	// We need to either resume from the batches we found in the log file ...
	if cb.appConfig.Resume {
		log.Printf("Resuming: %v batches", len(*batchesToResume))
		for _, batch := range *batchesToResume {
			// update the log file
			if err := cb.logFile.WriteNewBatch(&batch); err != nil {
				return err
			}

			// send it to a worker via the jobsChan
			cb.jobsChan <- batch

			// update counters
			cb.changesCount += len(batch.docs)
		}
	} else {
		// ... or spool the changes feed ...
		err = cb.SpoolChangesFeed()
		if err != nil {
			close(cb.jobsChan)
			cb.wgWorker.Wait()
			close(cb.resultsChan)
			close(cb.errorsChan)
			cb.wgCollector.Wait()
			return err
		}
	}

	// we can close the jobsChan which will kill the workers in time
	close(cb.jobsChan)

	// wait for the in-flight worker goroutines to complete
	cb.wgWorker.Wait()

	// wait for the collector to finish
	close(cb.resultsChan)
	cb.wgCollector.Wait()

	select {
	case err := <-cb.errorsChan:
		return err
	default:
		return nil
	}
}

// fetchDocsWorker is a goroutine that fetches batches of document ids from the jobsChan. It writes a ResultSet
// back to the resultsChan and any errors to errorsChan
func (cb *CloudantBackup) fetchDocsWorker() {
	// make sure we release our slot in the WaitGroup
	defer cb.wgWorker.Done()

	// wait for a job (a Batch struct) from the jobsChan
	for job := range cb.jobsChan {
		// formulate bulk docs request
		postBulkGetOptions := cb.service.NewPostBulkGetOptions(cb.appConfig.DatabaseName, job.docs)
		if cb.appConfig.Mode == ModeFull {
			postBulkGetOptions.SetRevs(true)
		}
		bulkGetResult, _, err := cb.service.PostBulkGet(postBulkGetOptions)
		if err != nil {
			cb.errorsChan <- err
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

		// send results back to resultsChan as a ResultsSet containing a JSON string
		// and a count of the documents
		b, err := json.Marshal(backupDocs)
		if err != nil {
			cb.errorsChan <- err
			return
		}
		rs := ResultSet{
			result:   string(b),
			docCount: docCount,
			batchId:  job.batchId,
		}
		cb.resultsChan <- rs
	}
}

// statsCollector is a goroutine that waits for data arriving back on resultsChan or
// errorsChan, aggregating results and stopping on the first error.
func (cb *CloudantBackup) statsCollector() {
	defer cb.wgCollector.Done()
	total := 0

	// header line
	if err := cb.output.WriteHeader(cb.appConfig.Mode); err != nil {
		cb.errorsChan <- err
		return
	}

	for {
		select {
		// <- returns the value of the channel and boolean ok,
		// that indicates whether the channel is open or not.
		// If ok == false, we can return - nothing more to do
		case r, ok := <-cb.resultsChan:
			if !ok {
				return
			}

			// increment docCount
			total += r.docCount

			// send the output string to stdout
			if err := cb.output.WriteResult(r.result); err != nil {
				cb.errorsChan <- err
				return
			}

			// update the log file
			if cb.logFile != nil {
				if err := cb.logFile.WriteDoneBatch(r.batchId); err != nil {
					cb.errorsChan <- err
					return
				}
			}

			// log the completion of this batch on stderr
			log.Printf("Batch %d: saved %d docs. Total: %d\n", r.batchId, r.docCount, total)

		case <-cb.errorsChan:
			return
		}
	}
}
