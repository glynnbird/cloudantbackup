package backup

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type LogFile struct {
	handle   *os.File
	filename string
}

func NewLogFile(filename string) (*LogFile, error) {
	// open the log file
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// create LogFile struct
	lf := LogFile{
		handle:   f,
		filename: filename,
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

func (lf *LogFile) Load(bufferSize int) (*[]Batch, error) {

	rc, err := os.Open(lf.filename)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// scan through the file line by line
	var changesComplete bool = false
	scanner := bufio.NewScanner(rc)
	batches := make([]Batch, 0, 100)
	doneBatchIds := make([]int, 0, 100)
	re := regexp.MustCompile(`^\:d batch([0-9]+)$`)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ":t ") {
			batch, err := NewBatchFromLogLine(line, bufferSize)
			if err != nil {
				return nil, err
			}
			batches = append(batches, *batch)
		} else if strings.HasPrefix(line, ":d ") {
			matches := re.FindStringSubmatch(line)
			if len(matches) == 2 {
				batchId, err := strconv.Atoi(matches[1])
				if err != nil {
					return nil, err
				}
				doneBatchIds = append(doneBatchIds, batchId)
			}
		} else if strings.HasPrefix(line, ":changes_complete") {
			changesComplete = true
		}
	}

	// we cannot resume if we previously didn't complete the changes feed
	if !changesComplete {
		return nil, errors.New("cannot resume - changes feed not complete")
	}

	// log the output
	if len(batches) == len(doneBatchIds) {
		return nil, errors.New("nothing to resume - backup complete")
	}
	if len(batches) < len(doneBatchIds) {
		return nil, errors.New("cannot resume - more batches done than exist")
	}

	// create a slice of Batch that represents the work still to do, that is the batches
	// that are not in the doneBatchIds slice
	batchesToDo := make([]Batch, 0, len(batches)-len(doneBatchIds))
	for _, batch := range batches {

		if !slices.Contains(doneBatchIds, batch.batchId) {
			batchesToDo = append(batchesToDo, batch)
		}
	}
	if len(batchesToDo) == 0 {
		return nil, errors.New("cannot resume - all batches done")
	}

	return &batchesToDo, nil
}
