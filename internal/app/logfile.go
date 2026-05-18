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

const (
	toDoPrefix            = ":t"
	donePrefix            = ":d"
	changesCompletePrefix = ":changes_complete"
)

// LogFile appends backup progress to a log file and can reload that state
// to support resume.
type LogFile struct {
	handle   *os.File
	writer   *bufio.Writer
	filename string
	mu       sync.Mutex
}

// NewLogFile creates a LogFile for appending backup progress.
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
	_, err := fmt.Fprintf(lf.writer, "%v batch%v %v\n", toDoPrefix, batch.batchID, batch.ToLogString())
	return err
}

// WriteDoneBatch writes a ":d batchX" line to the log file to record that a
// batch has been fetched successfully.
func (lf *LogFile) WriteDoneBatch(batchID int) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	_, err := fmt.Fprintf(lf.writer, "%v batch%d\n", donePrefix, batchID)
	return err
}

// WriteChangesComplete writes a marker showing that the changes feed has been
// consumed completely.
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

// Load parses a previously saved log file and returns the batches that still
// need to be fetched.
func (lf *LogFile) Load(bufferSize int) ([]Batch, error) {
	rc, err := os.Open(lf.filename)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	batches, doneBatchIDs, changesComplete, err := lf.parseLogFile(rc, bufferSize)
	if err != nil {
		return nil, err
	}

	if err := lf.validateLogState(changesComplete, batches, doneBatchIDs); err != nil {
		return nil, err
	}

	batchesToDo := lf.filterPendingBatches(batches, doneBatchIDs)
	if len(batchesToDo) == 0 {
		return nil, errors.New("cannot resume - all batches done")
	}

	return batchesToDo, nil
}

// parseLogFile reads log lines and returns discovered batches, completed batch
// IDs, and whether the changes feed was marked complete.
func (lf *LogFile) parseLogFile(rc *os.File, bufferSize int) ([]Batch, map[int]bool, bool, error) {
	scanner := bufio.NewScanner(rc)
	batches := make([]Batch, 0, 100)
	doneBatchIDs := make(map[int]bool)
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
			batchID, err := lf.processDoneLine(line)
			if err != nil {
				return nil, nil, false, err
			}
			if batchID >= 0 {
				doneBatchIDs[batchID] = true
			}
		} else if strings.HasPrefix(line, changesCompletePrefix) {
			changesComplete = true
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, false, err
	}

	return batches, doneBatchIDs, changesComplete, nil
}

// processTodoLine parses a todo log line into a Batch.
func (lf *LogFile) processTodoLine(line string, bufferSize int) (*Batch, error) {
	batch, err := NewBatchFromLogLine(line, bufferSize)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// processDoneLine extracts a completed batch ID from a done log line.
func (lf *LogFile) processDoneLine(line string) (int, error) {
	re := regexp.MustCompile(`^\:d batch([0-9]+)$`)
	matches := re.FindStringSubmatch(line)
	if len(matches) != 2 {
		return -1, nil
	}

	batchID, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1, err
	}
	return batchID, nil
}

// validateLogState checks that the log contains enough information to resume safely.
func (lf *LogFile) validateLogState(changesComplete bool, batches []Batch, doneBatchIDs map[int]bool) error {
	if !changesComplete {
		return errors.New("cannot resume - changes feed not complete")
	}

	if len(batches) <= len(doneBatchIDs) {
		return errors.New("cannot resume - more batches done than exist")
	}

	return nil
}

// filterPendingBatches removes batches that have already been marked done.
func (lf *LogFile) filterPendingBatches(batches []Batch, doneBatchIDs map[int]bool) []Batch {
	batchesToDo := make([]Batch, 0, len(batches)-len(doneBatchIDs))
	for _, batch := range batches {
		if !doneBatchIDs[batch.batchID] {
			batchesToDo = append(batchesToDo, batch)
		}
	}
	return batchesToDo
}
