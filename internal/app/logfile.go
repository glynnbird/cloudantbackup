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

const toDoPrefix = ":t"
const donePrefix = ":d"
const changesCompletePrefix = ":changes_complete"

// LogFile represents a log file including a writable file handle and its filename,
// together with some helper functions that allow the three types of lines to be written
// and a Load function that reads and parses a pre-existing log file.
type LogFile struct {
	handle   *os.File
	filename string
}

// NewLogFile creates a LogFile struct which models a log file. The file is opened
// for append and for writing. The filename is also stored for future reference.
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

// WriteNewBatch writes a ":t batchX [{"id":"y"}...]" line to the log file, recording
// each batch of documents that is to be fetched, including their document ids.
func (lf *LogFile) WriteNewBatch(batch *Batch) error {
	_, err := fmt.Fprintf(lf.handle, "%v batch%v %v\n", toDoPrefix, batch.batchId, batch.ToLogString())
	return err
}

// WriteDoneBatch writes a ":d batchX" line to the log file, indicating that a batch has
// been successfully fetched
func (lf *LogFile) WriteDoneBatch(batchId int) error {
	_, err := fmt.Fprintf(lf.handle, "%v batch%d\n", donePrefix, batchId)
	return err
}

// WriteChangesComplete writes a line to the log file to indicate that the changes feed has been
// completely consumed.
func (lf *LogFile) WriteChangesComplete() error {
	_, err := fmt.Fprintf(lf.handle, "%v\n", changesCompletePrefix)
	return err
}

// Close closes the writable file handle
func (lf *LogFile) Close() {
	lf.handle.Close()
	lf.handle = nil
}

// Load opens a previously saved log file and parses its contents,
// creating a slice of Batch structs, each of which represents a batch
// of document ids that need fetching. Then it creates a final slice
// of Batches, removing batches that have already been fetched - so the
// returned slice are the batches that still need fetching.
func (lf *LogFile) Load(bufferSize int) (*[]Batch, error) {

	// open the log file for reading
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

	// for each line in the log file
	for scanner.Scan() {
		line := scanner.Text()

		// if this is a "to do" line
		if strings.HasPrefix(line, toDoPrefix) {

			// create a new batch
			batch, err := NewBatchFromLogLine(line, bufferSize)
			if err != nil {
				return nil, err
			}

			// add it to our slice of batches
			batches = append(batches, *batch)
		} else if strings.HasPrefix(line, donePrefix) {
			// for "done" lines, look for a batch id
			matches := re.FindStringSubmatch(line)
			if len(matches) == 2 {

				// add the batch id to our list of done batch ids
				batchId, err := strconv.Atoi(matches[1])
				if err != nil {
					return nil, err
				}
				doneBatchIds = append(doneBatchIds, batchId)
			}
		} else if strings.HasPrefix(line, changesCompletePrefix) {
			// make a note if we see a "changes complete" line - without one, the backup cannot be resumed
			changesComplete = true
		}
	}

	// we cannot resume if we previously didn't complete the changes feed
	if !changesComplete {
		return nil, errors.New("cannot resume - changes feed not complete")
	}

	// log the output
	if len(batches) <= len(doneBatchIds) {
		return nil, errors.New("cannot resume - more batches done than exist")
	}

	// create a slice of Batch that represents the work still to do, that is the batches
	// that are not in the doneBatchIds slice
	batchesToDo := make([]Batch, 0, len(batches)-len(doneBatchIds))
	for _, batch := range batches {
		// only include batches whose id doesn't appear in the doneBatchIds slice
		if !slices.Contains(doneBatchIds, batch.batchId) {
			batchesToDo = append(batchesToDo, batch)
		}
	}

	// if there are no batches to do, there's no backup to resume
	if len(batchesToDo) == 0 {
		return nil, errors.New("cannot resume - all batches done")
	}

	// return the slice of batches to do
	return &batchesToDo, nil
}
