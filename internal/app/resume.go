package backup

import (
	"context"
	"log"
)

// loadResumeBatches loads pending batches from the log file when resume mode is enabled.
func (cb *CloudantBackup) loadResumeBatches() ([]Batch, error) {
	if !cb.appConfig.Resume {
		return nil, nil
	}
	return cb.logFile.Load(cb.appConfig.BufferSize)
}

// produceBatches either resumes pending work or spools fresh changes.
func (cb *CloudantBackup) produceBatches(ctx context.Context, cancel context.CancelFunc, batchesToResume []Batch) (string, error) {
	if cb.appConfig.Resume {
		return cb.resumeBatches(ctx, cancel, batchesToResume)
	}

	lastSeq, err := cb.SpoolChangesFeed(ctx)
	if err != nil {
		cancel()
		return "", err
	}
	return lastSeq, nil
}

// resumeBatches re-enqueues batches loaded from the resume log.
func (cb *CloudantBackup) resumeBatches(ctx context.Context, cancel context.CancelFunc, batchesToResume []Batch) (string, error) {
	log.Printf("Resuming: %v batches", len(batchesToResume))
	for _, batch := range batchesToResume {
		if err := cb.logFile.WriteNewBatch(&batch); err != nil {
			cancel()
			return "", err
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case cb.jobsChan <- batch:
		}

		cb.changesCount += len(batch.docs)
	}
	return "", nil
}
