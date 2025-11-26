package backup

import (
	"encoding/json"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

// Batch is a batch of document ids and a id that represents documents that need fetching
// from Cloudant. The Batch is what it sent to the fetchDocsWorker on the jobsChan
type Batch struct {
	buffer  []string
	batchId int
	docs    []cloudantv1.BulkGetQueryDocument
}

func NewBatch(batchId int, buffer []string) *Batch {
	batch := Batch{
		batchId: batchId,
		buffer:  buffer,
		docs:    make([]cloudantv1.BulkGetQueryDocument, len(buffer)),
	}
	for i := range batch.buffer {
		batch.docs[i].ID = &batch.buffer[i]
	}
	return &batch
}

func (batch *Batch) ToLogString() string {
	b, err := json.Marshal(batch.docs)
	if err != nil {
		return ""
	}
	return string(b)
}
