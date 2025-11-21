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
	logFileHandle, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// create LogFile struct
	lf := LogFile{
		handle: logFileHandle,
	}
	return &lf, nil
}

func (lf *LogFile) WriteNewBatch(batchId int, batch string) error {
	line := fmt.Sprintf(":t batch%d %s\n", batchId, batch)
	_, err := lf.handle.WriteString(line)
	if err != nil {
		return err
	}
	return nil
}

func (lf *LogFile) WriteDoneBatch(batchId int) error {
	line := fmt.Sprintf(":d batch%d\n", batchId)
	_, err := lf.handle.WriteString(line)
	return err
}

func (lf *LogFile) ChangesComplete() error {
	_, err := lf.handle.WriteString(":changes_complete\n")
	return err
}

func (lf *LogFile) Close() {
	lf.handle.Close()
}
