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

type ResultSet struct {
	result   string
	docCount int
}

type CloudantBackup struct {
	appConfig    *AppConfig
	service      *cloudantv1.CloudantV1 // the Cloudant SDK client
	buffer       []string               // a batch of document ids to fetch
	bufferLen    int                    // the current position in the buffer
	wgWorker     sync.WaitGroup         // to keep track of running worker goroutines
	wgCollector  sync.WaitGroup         // to keep track of the results collector
	resultsChan  chan ResultSet         // channel to carry results of API calls
	jobsChan     chan []string          // channel to carry jobs, slices of Cloudant document ids
	errorsChan   chan error             // channel to carry errors that occurred when fetching documents from Cloudant
	changesCount int                    // the total number of changes fetched from the changes follower
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

	// create struct
	cb := CloudantBackup{
		appConfig:    appConfig,
		service:      service,
		buffer:       buffer,
		bufferLen:    0,
		wgWorker:     sync.WaitGroup{},
		wgCollector:  sync.WaitGroup{},
		resultsChan:  make(chan ResultSet),
		jobsChan:     make(chan []string, appConfig.Parallelism),
		errorsChan:   make(chan error),
		changesCount: 0,
	}

	return &cb, nil
}

func (cb *CloudantBackup) Run() error {

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
			var obj map[string]interface{}
			err = json.Unmarshal([]byte(line), &obj)
			if err != nil {
				continue
			}

			// extract the id
			id := fmt.Sprintf("%v", obj["id"])
			cb.buffer[cb.bufferLen] = id
			cb.bufferLen++

			// if we have a batch
			if cb.bufferLen == cb.appConfig.BufferSize {
				clone := make([]string, cb.bufferLen)
				copy(clone, cb.buffer[:cb.bufferLen])
				cb.jobsChan <- clone
				cb.changesCount += cb.bufferLen
				cb.bufferLen = 0
			}
		}
	}

	// process last batch
	if cb.bufferLen > 0 {
		cb.changesCount += cb.bufferLen
		cb.jobsChan <- cb.buffer[:cb.bufferLen]
	}
	log.Printf("Changes follower complete. %d changes\n", cb.changesCount)
	close(cb.jobsChan)

	// wait for the in-flight goroutines to complete
	cb.wgWorker.Wait()
	close(cb.resultsChan)
	close(cb.errorsChan)
	cb.wgCollector.Wait()

	return nil
}

// fetchDocsWorker fetches batches of document ids from the jobsChan. It writes the number of document ids
// fetched back to the resultsChan and any errors to errorsChan
func (cb *CloudantBackup) fetchDocsWorker() {
	// make sure we release our slot
	defer cb.wgWorker.Done()

	for job := range cb.jobsChan {
		// formulate bulk docs request
		docs := make([]cloudantv1.BulkGetQueryDocument, len(job))
		for i := range job {
			docs[i].ID = &job[i]
		}
		postBulkGetOptions := cb.service.NewPostBulkGetOptions(cb.appConfig.DatabaseName, docs)
		if cb.appConfig.Mode == ModeFull {
			postBulkGetOptions.SetRevs(true)
		}
		bulkGetResult, _, err := cb.service.PostBulkGet(postBulkGetOptions)
		if err != nil {
			cb.errorsChan <- err
			return
		}
		backupDocs := make([]cloudantv1.Document, 0, len(job))
		docCount := 0
		for _, result := range bulkGetResult.Results {
			for _, doc := range result.Docs {
				if doc.Error == nil {
					backupDocs = append(backupDocs, *doc.Ok)
					docCount++
				}
			}
		}

		// send results back to resultsChan as ResultsSet containing a JSON string
		// and a count of the documents
		jsonBytes, err := json.Marshal(backupDocs)
		if err != nil {
			cb.errorsChan <- err
			return
		}
		rs := ResultSet{
			result:   string(jsonBytes),
			docCount: docCount,
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
	fmt.Printf(`{"name":"@cloudant/couchbackup","version":"2.9.10","mode":"%v"}`, cb.appConfig.Mode)
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
			total += r.docCount
			log.Printf("saved %d docs. Total: %d\n", r.docCount, total)
			fmt.Println(r.result)
		case err, ok := <-cb.errorsChan:
			if !ok {
				return
			}
			panic(fmt.Sprintf("ERROR: %v", err))
		}
	}
}
