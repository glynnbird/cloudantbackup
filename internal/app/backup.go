package backup

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

// ResultSet is the data sent back from the fetchDocsWorker on the resultsChan channel
type ResultSet struct {
	result   string
	docCount int
	batchId  int
}

// CloudantBackup is the state that represents a backup process.
type CloudantBackup struct {
	appConfig    *AppConfig             // the command-line options
	service      *cloudantv1.CloudantV1 // the Cloudant SDK client
	buffer       []string               // a batch of document ids to fetch
	bufferLen    int                    // the current position in the buffer
	wgWorker     sync.WaitGroup         // WaitGroup to keep track of running worker goroutines
	wgCollector  sync.WaitGroup         // WaitGroup to keep track of the results collector
	resultsChan  chan ResultSet         // channel to carry results of API calls
	jobsChan     chan Batch             // channel to carry jobs, which uses the Batch type
	errorsChan   chan error             // channel to carry errors that occurred when fetching documents from Cloudant
	changesCount int                    // the total number of changes fetched from the changes follower
	logFile      *LogFile               // the log file, which is optionally written-to during the backup process
	batchId      int                    // the current batch id
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

	// create the buffer
	buffer := make([]string, appConfig.BufferSize)

	// create log file
	var logFile *LogFile = nil
	if appConfig.LogFilename != "" {
		logFile, err = NewLogFile(appConfig.LogFilename)
		if err != nil {
			return nil, err
		}
	}

	// create struct
	cb := CloudantBackup{
		appConfig:    appConfig,
		service:      service,
		buffer:       buffer,
		bufferLen:    0,
		wgWorker:     sync.WaitGroup{},
		wgCollector:  sync.WaitGroup{},
		resultsChan:  make(chan ResultSet),
		jobsChan:     make(chan Batch, appConfig.Parallelism),
		errorsChan:   make(chan error),
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
		cb.logFile.WriteNewBatch(batch)
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
	postChangesOptions.SetSince(cb.appConfig.DatabaseName)
	postChangesOptions.SetIncludeDocs(false)
	postChangesOptions.SetSeqInterval(500)
	stream, _, err := cb.service.PostChangesAsStream(postChangesOptions)
	if err != nil {
		return err
	}

	// scan through the changes feed line by line
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		// fetch a line
		line := scanner.Text()

		// changes look like this: { ... }, ignore anything else
		if len(line) > 0 && line[0] == '{' && line[len(line)-1] == ',' {
			// strip off the ,
			line := line[:len(line)-1]

			// parse as a changes result item
			change := cloudantv1.ChangesResultItem{}
			err = json.Unmarshal([]byte(line), &change)
			if err != nil {
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
		}
	}

	// if there are still unprocessed changes in the buffer
	if cb.bufferLen > 0 {
		// send them to be processed
		cb.dispatchBatchToWorker()
	}

	// we're now finished consuming the changes feed
	log.Printf("Changes follower complete. %d changes\n", cb.changesCount)
	if cb.logFile != nil {
		cb.logFile.WriteChangesComplete()
	}
	return nil
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
			cb.logFile.Close()
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
			cb.logFile.WriteNewBatch(&batch)

			// send it to a worker via the jobsChan
			cb.jobsChan <- batch

			// update counters
			cb.changesCount += len(batch.docs)
		}
	} else {
		// ... or spool the changes feed ...
		err = cb.SpoolChangesFeed()
		if err != nil {
			return err
		}
	}

	// we can close the jobsChan which will kill the workers in time
	close(cb.jobsChan)

	// wait for the in-flight worker goroutines to complete
	cb.wgWorker.Wait()

	// wait for the collector to finish
	close(cb.resultsChan)
	close(cb.errorsChan)
	cb.wgCollector.Wait()

	return nil
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
			continue
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
// errorsChan, aggregating results and panicking if an error occurs
func (cb *CloudantBackup) statsCollector() {
	defer cb.wgCollector.Done()
	total := 0

	// header line
	fmt.Printf(`{"name":"@cloudant/couchbackup","version":"1.0.0","mode":"%v"}`, cb.appConfig.Mode)
	fmt.Println("")

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
			fmt.Println(r.result)

			// update the log file
			if cb.logFile != nil {
				cb.logFile.WriteDoneBatch(r.batchId)
			}

			// log the completion of this batch on stderr
			log.Printf("Batch %d: saved %d docs. Total: %d\n", r.batchId, r.docCount, total)

		case err, ok := <-cb.errorsChan:
			if !ok {
				return
			}
			// any error on errorsChan is fatal
			panic(fmt.Sprintf("ERROR: %v", err))
		}
	}
}
