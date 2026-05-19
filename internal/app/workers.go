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
	defer cb.wgWorker.Done()

	for {
		job, ok := cb.receiveJob(ctx)
		if !ok {
			return
		}

		resultSet, err := cb.processBatch(job)
		if err != nil {
			cb.cancelWithError(cancel, err)
			return
		}

		if !cb.sendResult(ctx, resultSet) {
			return
		}
	}
}

// receiveJob waits for the next batch job or context cancellation.
// Returns false if the worker should exit.
func (cb *CloudantBackup) receiveJob(ctx context.Context) (Batch, bool) {
	select {
	case <-ctx.Done():
		return Batch{}, false
	case job, ok := <-cb.jobsChan:
		return job, ok
	}
}

// processBatch fetches documents for a batch and returns the result set.
func (cb *CloudantBackup) processBatch(job Batch) (ResultSet, error) {
	bulkGetResult, err := cb.fetchBulkDocs(job)
	if err != nil {
		return ResultSet{}, err
	}

	backupDocs, docCount, errCount := cb.extractDocuments(bulkGetResult)

	jsonBytes, err := json.Marshal(backupDocs)
	if err != nil {
		return ResultSet{}, err
	}

	return ResultSet{
		result:   jsonBytes,
		docCount: docCount,
		errCount: errCount,
		batchID:  job.batchID,
	}, nil
}

// fetchBulkDocs performs the bulk get API call for a batch of documents.
func (cb *CloudantBackup) fetchBulkDocs(job Batch) (*cloudantv1.BulkGetResult, error) {
	options := cb.service.NewPostBulkGetOptions(cb.appConfig.DatabaseName, job.docs)
	if cb.appConfig.Mode == ModeFull {
		options.SetRevs(true)
	}
	bulkGetResult, _, err := cb.service.PostBulkGet(options)
	return bulkGetResult, err
}

// extractDocuments processes bulk get results and separates successful docs from errors.
func (cb *CloudantBackup) extractDocuments(bulkGetResult *cloudantv1.BulkGetResult) ([]cloudantv1.Document, int, int) {
	backupDocs := make([]cloudantv1.Document, 0, len(bulkGetResult.Results))
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

	return backupDocs, docCount, errCount
}

// sendResult sends a result set to the results channel, respecting context cancellation.
// Returns false if the worker should exit.
func (cb *CloudantBackup) sendResult(ctx context.Context, rs ResultSet) bool {
	select {
	case <-ctx.Done():
		return false
	case cb.resultsChan <- rs:
		return true
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
