package backup

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const toDoPrefix = ":t"
const donePrefix = ":d"
const changesCompletePrefix = ":changes_complete"

// LogFile represents a log file including a writable file handle and its filename,
// together with some helper functions that allow the three types of lines to be written
// and a Load function that reads and parses a pre-existing log file.
type LogFile struct {
	handle   *os.File
	writer   *bufio.Writer
	filename string
	mu       sync.Mutex
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
		writer:   bufio.NewWriterSize(f, 64*1024),
		filename: filename,
	}
	return &lf, nil
}

// WriteNewBatch writes a ":t batchX [{"id":"y"}...]" line to the log file, recording
// each batch of documents that is to be fetched, including their document ids.
func (lf *LogFile) WriteNewBatch(batch *Batch) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	_, err := fmt.Fprintf(lf.writer, "%v batch%v %v\n", toDoPrefix, batch.batchId, batch.ToLogString())
	return err
}

// WriteDoneBatch writes a ":d batchX" line to the log file, indicating that a batch has
// been successfully fetched
func (lf *LogFile) WriteDoneBatch(batchId int) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	_, err := fmt.Fprintf(lf.writer, "%v batch%d\n", donePrefix, batchId)
	return err
}

// WriteChangesComplete writes a line to the log file to indicate that the changes feed has been
// completely consumed.
func (lf *LogFile) WriteChangesComplete() error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	_, err := fmt.Fprintf(lf.writer, "%v\n", changesCompletePrefix)
	return err
}

// Close flushes buffered data and closes the writable file handle.
func (lf *LogFile) Close() error {
	if lf.handle == nil {
		return nil
	}

	var err error
	if lf.writer != nil {
		err = lf.writer.Flush()
		lf.writer = nil
	}
	closeErr := lf.handle.Close()
	lf.handle = nil
	if err != nil {
		return err
	}
	return closeErr
}

// Load opens a previously saved log file and parses its contents,
// creating a slice of Batch structs, each of which represents a batch
// of document ids that need fetching. Then it creates a final slice
// of Batches, removing batches that have already been fetched - so the
// returned slice are the batches that still need fetching.
func (lf *LogFile) Load(bufferSize int) (*[]Batch, error) {
	rc, err := os.Open(lf.filename)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	batches, doneBatchIds, changesComplete, err := lf.parseLogFile(rc, bufferSize)
	if err != nil {
		return nil, err
	}

	if err := lf.validateLogState(changesComplete, batches, doneBatchIds); err != nil {
		return nil, err
	}

	batchesToDo := lf.filterPendingBatches(batches, doneBatchIds)
	if len(batchesToDo) == 0 {
		return nil, errors.New("cannot resume - all batches done")
	}

	return &batchesToDo, nil
}

func (lf *LogFile) parseLogFile(rc *os.File, bufferSize int) ([]Batch, map[int]bool, bool, error) {
	scanner := bufio.NewScanner(rc)
	batches := make([]Batch, 0, 100)
	doneBatchIds := make(map[int]bool)
	changesComplete := false

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, toDoPrefix) {
			batch, err := lf.processTodoLine(line, bufferSize)
			if err != nil {
				return nil, nil, false, err
			}
			batches = append(batches, *batch)
		} else if strings.HasPrefix(line, donePrefix) {
			batchId, err := lf.processDoneLine(line)
			if err != nil {
				return nil, nil, false, err
			}
			if batchId >= 0 {
				doneBatchIds[batchId] = true
			}
		} else if strings.HasPrefix(line, changesCompletePrefix) {
			changesComplete = true
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, false, err
	}

	return batches, doneBatchIds, changesComplete, nil
}

func (lf *LogFile) processTodoLine(line string, bufferSize int) (*Batch, error) {
	batch, err := NewBatchFromLogLine(line, bufferSize)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func (lf *LogFile) processDoneLine(line string) (int, error) {
	re := regexp.MustCompile(`^\:d batch([0-9]+)$`)
	matches := re.FindStringSubmatch(line)
	if len(matches) != 2 {
		return -1, nil
	}

	batchId, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1, err
	}
	return batchId, nil
}

func (lf *LogFile) validateLogState(changesComplete bool, batches []Batch, doneBatchIds map[int]bool) error {
	if !changesComplete {
		return errors.New("cannot resume - changes feed not complete")
	}

	if len(batches) <= len(doneBatchIds) {
		return errors.New("cannot resume - more batches done than exist")
	}

	return nil
}

func (lf *LogFile) filterPendingBatches(batches []Batch, doneBatchIds map[int]bool) []Batch {
	batchesToDo := make([]Batch, 0, len(batches)-len(doneBatchIds))
	for _, batch := range batches {
		if !doneBatchIds[batch.batchId] {
			batchesToDo = append(batchesToDo, batch)
		}
	}
	return batchesToDo
}
