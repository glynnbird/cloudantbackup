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

// ResultSet is the data sent back from the fetchDocsWorker on the resultsChan
type ResultSet struct {
	result   string
	docCount int
	batchId  int
}

// CloudantBackup is the state that represents a backup process.
type CloudantBackup struct {
	appConfig    *AppConfig
	service      *cloudantv1.CloudantV1 // the Cloudant SDK client
	buffer       []string               // a batch of document ids to fetch
	bufferLen    int                    // the current position in the buffer
	wgWorker     sync.WaitGroup         // to keep track of running worker goroutines
	wgCollector  sync.WaitGroup         // to keep track of the results collector
	resultsChan  chan ResultSet         // channel to carry results of API calls
	jobsChan     chan Batch             // channel to carry jobs, which uses the Batch type
	errorsChan   chan error             // channel to carry errors that occurred when fetching documents from Cloudant
	changesCount int                    // the total number of changes fetched from the changes follower
	logFile      *LogFile               // the log file
	batchId      int                    // the current batch id
}

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

func (cb *CloudantBackup) Run() error {

	// don't forget to close the log file
	defer func() {
		// close the log file
		if cb.logFile != nil {
			cb.logFile.Close()
		}
	}()

	// Start worker pool
	for i := 0; i < cb.appConfig.Parallelism; i++ {
		cb.wgWorker.Add(1)
		go cb.fetchDocsWorker()
	}

	// spin up a goroutine to handle the results and errors
	cb.wgCollector.Add(1)
	go cb.statsCollector()

	// create a changes feed request
	postChangesOptions := cb.service.NewPostChangesOptions(cb.appConfig.DatabaseName)
	postChangesOptions.SetSince("0")
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

			// parse as JSON
			//var obj map[string]interface{}
			change := cloudantv1.ChangesResultItem{}
			err = json.Unmarshal([]byte(line), &change)
			if err != nil {
				continue
			}

			// add the id to ur buffer
			cb.buffer[cb.bufferLen] = *change.ID
			cb.bufferLen++

			// if we have a batch
			if cb.bufferLen == cb.appConfig.BufferSize {
				// clone the batch to avoid data being overwritten
				clone := make([]string, cb.bufferLen)
				copy(clone, cb.buffer[:cb.bufferLen])

				// create a neew batch
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
		}
	}

	// process last batch
	if cb.bufferLen > 0 {
		// create last batch
		cb.changesCount += cb.bufferLen
		batch := NewBatch(cb.batchId, cb.buffer[:cb.bufferLen])

		// log it
		if cb.logFile != nil {
			cb.logFile.WriteNewBatch(batch)
		}

		// send it to a worker via the jobsChan
		cb.jobsChan <- *batch
	}

	// we're now finished consuming the changes feed
	log.Printf("Changes follower complete. %d changes\n", cb.changesCount)
	if cb.logFile != nil {
		cb.logFile.ChangesComplete()
	}

	// so we can close the jobsChan which will kill the workers in time
	close(cb.jobsChan)

	// wait for the in-flight worker goroutines to complete
	cb.wgWorker.Wait()

	// wait for the collector to finish
	close(cb.resultsChan)
	close(cb.errorsChan)
	cb.wgCollector.Wait()

	return nil
}

// fetchDocsWorker fetches batches of document ids from the jobsChan. It writes the number of document ids
// fetched back to the resultsChan and any errors to errorsChan
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
		backupDocs := make([]cloudantv1.Document, 0, len(job.buffer))
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

// statsCollector waits for data arriving back on resultsChan and
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
