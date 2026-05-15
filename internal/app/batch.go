package backup

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

// Batch represents a set of document IDs collected from the changes feed.
// Each batch has an ID and a slice of BulkGetQueryDocument values that can be
// passed directly to the bulk get API.
type Batch struct {
	batchId int
	docs    []cloudantv1.BulkGetQueryDocument
}

// NewBatch creates a batch from its ID and a slice of document IDs.
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

// NewBatchFromLogLine creates a batch from a previously logged line such as:
//
//	:t batch56 [{"id":"a"},{"id":"b"}]
//
// It extracts the batch ID and unmarshals the JSON document list back into
// BulkGetQueryDocument values.
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
		docs := make([]cloudantv1.BulkGetQueryDocument, 0, bufferSize)
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

// ToLogString marshals the batch documents into a JSON string suitable for logging.
func (batch *Batch) ToLogString() string {
	b, err := json.Marshal(batch.docs)
	if err != nil {
		return ""
	}
	return string(b)
}
