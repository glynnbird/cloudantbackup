package backup

import (
	"fmt"
	"os"
)

type LogFile struct {
	handle *os.File
}

func NewLogFile(filename string) (*LogFile, error) {
	// open the log file
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// create LogFile struct
	lf := LogFile{
		handle: f,
	}
	return &lf, nil
}

func (lf *LogFile) WriteNewBatch(batch *Batch) error {
	_, err := fmt.Fprintf(lf.handle, ":t batch%v %v\n", batch.batchId, batch.ToLogString())
	return err
}

func (lf *LogFile) WriteDoneBatch(batchId int) error {
	_, err := fmt.Fprintf(lf.handle, ":d batch%d\n", batchId)
	return err
}

func (lf *LogFile) ChangesComplete() error {
	_, err := fmt.Fprintf(lf.handle, ":changes_complete\n")
	return err
}

func (lf *LogFile) Close() {
	lf.handle.Close()
	lf.handle = nil
}
