package backup

import (
	"context"
	"io"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/cloudant-go-sdk/features"
)

const changesFeedSeqInterval = 500

// changesFollower yields one change at a time from the changes feed.
type changesFollower interface {
	Next() (cloudantv1.ChangesResultItem, error)
}

// changesFollowerFactory creates a follower positioned at a starting sequence.
type changesFollowerFactory interface {
	New(ctx context.Context, since string) (changesFollower, error)
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
				// Dispatch any remaining buffered changes before returning
				if cb.bufferLen > 0 {
					if err := cb.dispatchBatchToWorker(ctx); err != nil {
						return currentSince, err
					}
				}
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
