package backup

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

// Batch is a list of document ids which originate from the changes feed. There is also an "id"
// which identifies this batch. These are the documents that need fetching from the database. When a
// new batch is created (either from a slice of ids, or from a log line) the list of ids is converted
// into a slice of BulkGetQueryDocument structs (docs), which is what is needed by the API call.
// The Batch is what it sent to the fetchDocsWorker on the jobsChan
type Batch struct {
	batchId int
	docs    []cloudantv1.BulkGetQueryDocument
}

// NewBatch creates a new batch given its id and an slice of document ids
func NewBatch(batchId int, buffer []string) *Batch {
	batch := Batch{
		batchId: batchId,
		docs:    make([]cloudantv1.BulkGetQueryDocument, len(buffer)),
	}
	for i := range buffer {
		batch.docs[i].ID = &buffer[i]
	}
	return &batch
}

// NewBatchFromLogLine creates a new batch given a previously logged log line
// which looks like this:
//
//	:t batch56 [{"id":"a"},{"id":"b"}]
//
// It uses regular expressions to extract the batchId and Unmarshalls the
// array of objects back into arrays of BulkGetQueryDocument and arrays of
// document ids
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

		// create batch
		batch := Batch{
			batchId: batchId,
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
