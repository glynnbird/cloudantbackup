package backup

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

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
		errCount := 0
		for _, result := range bulkGetResult.Results {
			for _, doc := range result.Docs {
				if doc.Error == nil {
					backupDocs = append(backupDocs, *doc.Ok)
					docCount++
				} else {
					errCount++
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
			errCount: errCount,
			batchID:  job.batchID,
		}
		select {
		case <-ctx.Done():
			return
		case cb.resultsChan <- rs:
		}
	}
}

// statsCollector writes the backup header and result batches, tracks total
// saved documents and document errors, and stops on the first error.
func (cb *CloudantBackup) statsCollector(ctx context.Context, cancel context.CancelFunc) {
	defer cb.wgCollector.Done()
	totalDocs := 0
	totalErrors := 0

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
			totalDocs += r.docCount
			totalErrors += r.errCount

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
			log.Printf("batch %d: saved %d docs, %d errors; totals docs=%d errors=%d", r.batchID, r.docCount, r.errCount, totalDocs, totalErrors)
		}
	}
}
