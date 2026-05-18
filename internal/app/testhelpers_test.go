package backup

import (
	"context"
	"io"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

type fakeCloudantService struct {
	bulkGetResult *cloudantv1.BulkGetResult
	bulkGetErr    error
	lastBulkDocs  []cloudantv1.BulkGetQueryDocument
}

func (f *fakeCloudantService) NewPostBulkGetOptions(db string, docs []cloudantv1.BulkGetQueryDocument) *cloudantv1.PostBulkGetOptions {
	f.lastBulkDocs = append([]cloudantv1.BulkGetQueryDocument(nil), docs...)
	return (&cloudantv1.CloudantV1{}).NewPostBulkGetOptions(db, docs)
}

func (f *fakeCloudantService) PostBulkGet(*cloudantv1.PostBulkGetOptions) (*cloudantv1.BulkGetResult, *core.DetailedResponse, error) {
	return f.bulkGetResult, nil, f.bulkGetErr
}

type fakeChangesFollowerFactory struct {
	sinceCalls []string
	followers  []fakeChangesFollowerResult
}

type fakeChangesFollowerResult struct {
	follower changesFollower
	err      error
}

func (f *fakeChangesFollowerFactory) New(_ context.Context, since string) (changesFollower, error) {
	f.sinceCalls = append(f.sinceCalls, since)
	callIndex := len(f.sinceCalls) - 1
	if callIndex < len(f.followers) {
		return f.followers[callIndex].follower, f.followers[callIndex].err
	}
	return nil, io.EOF
}

type fakeChangesFollower struct {
	changes []cloudantv1.ChangesResultItem
	err     error
	index   int
}

func (f *fakeChangesFollower) Next() (cloudantv1.ChangesResultItem, error) {
	if f.index < len(f.changes) {
		change := f.changes[f.index]
		f.index++
		return change, nil
	}
	if f.err != nil {
		return cloudantv1.ChangesResultItem{}, f.err
	}
	return cloudantv1.ChangesResultItem{}, io.EOF
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
