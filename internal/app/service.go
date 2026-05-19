package backup

import (
	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

// cloudantService defines the subset of the Cloudant client used for bulk fetches.
type cloudantService interface {
	NewPostBulkGetOptions(string, []cloudantv1.BulkGetQueryDocument) *cloudantv1.PostBulkGetOptions
	PostBulkGet(*cloudantv1.PostBulkGetOptions) (*cloudantv1.BulkGetResult, *core.DetailedResponse, error)
}

// ResultSet is the data sent back from the fetchDocsWorker on the resultsChan channel
type ResultSet struct {
	result   []byte
	docCount int
	errCount int
	batchID  int
}
