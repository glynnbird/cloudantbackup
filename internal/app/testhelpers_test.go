package backup

import (
	"io"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

type fakeCloudantService struct {
	changesStream io.ReadCloser
	changesErr    error
	bulkGetResult *cloudantv1.BulkGetResult
	bulkGetErr    error
	lastBulkDocs  []cloudantv1.BulkGetQueryDocument
}

func (f *fakeCloudantService) PostChangesAsStream(*cloudantv1.PostChangesOptions) (io.ReadCloser, *core.DetailedResponse, error) {
	return f.changesStream, nil, f.changesErr
}

func (f *fakeCloudantService) NewPostChangesOptions(db string) *cloudantv1.PostChangesOptions {
	return (&cloudantv1.CloudantV1{}).NewPostChangesOptions(db)
}

func (f *fakeCloudantService) NewPostBulkGetOptions(db string, docs []cloudantv1.BulkGetQueryDocument) *cloudantv1.PostBulkGetOptions {
	f.lastBulkDocs = append([]cloudantv1.BulkGetQueryDocument(nil), docs...)
	return (&cloudantv1.CloudantV1{}).NewPostBulkGetOptions(db, docs)
}

func (f *fakeCloudantService) PostBulkGet(*cloudantv1.PostBulkGetOptions) (*cloudantv1.BulkGetResult, *core.DetailedResponse, error) {
	return f.bulkGetResult, nil, f.bulkGetErr
}

type fakeOutputWriter struct {
	headers []string
	results [][]byte
	err     error
}

func (f *fakeOutputWriter) WriteHeader(mode string) error {
	if f.err != nil {
		return f.err
	}
	f.headers = append(f.headers, mode)
	return nil
}

func (f *fakeOutputWriter) WriteResult(result []byte) error {
	if f.err != nil {
		return f.err
	}
	f.results = append(f.results, append([]byte(nil), result...))
	return nil
}

// Made with Bob
