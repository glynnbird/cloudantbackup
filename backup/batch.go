package backup

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"

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

// NewBatchFromLogLine creates a new batch given a previously logged log line
// which looks like this:
//
//	:t batch56 [{"id":"a"},{"id":"b"}]
//
// It uses regular expressions to extract the batchId and Unmarshalls the
// array of objects back into arrays of BulkGetQueryDocument
func NewBatchFromLogLine(logLine string, bufferSize int) (*Batch, error) {
	// log lines look like this:
	// :t batch56 [{"id":"a"},{"id":"b"}]
	re := regexp.MustCompile(`^\:t batch([0-9]+) (.*)$`)
	matches := re.FindStringSubmatch(logLine)
	if len(matches) == 3 {

		// extract the batch id
		batchId, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, err
		}

		// parse the json
		docs := make([]cloudantv1.BulkGetQueryDocument, bufferSize)
		err = json.Unmarshal([]byte(matches[2]), &docs)
		if err != nil {
			return nil, err
		}

		// recreate the buffer
		buffer := make([]string, len(docs))
		for i, v := range docs {
			buffer[i] = *v.ID
		}

		// create batch
		batch := Batch{
			batchId: batchId,
			buffer:  buffer,
			docs:    docs,
		}
		return &batch, nil
	} else {
		return nil, errors.New("could not parse log line")
	}
}

// ToLogString turns the slice of BulkGetQueryDocuments into a JSON
// string suitable for the logs
func (batch *Batch) ToLogString() string {
	b, err := json.Marshal(batch.docs)
	if err != nil {
		return ""
	}
	return string(b)
}
