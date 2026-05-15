package backup

import (
	"errors"
	"flag"
	"fmt"
)

// Default and validation values.
const (
	defaultDatabaseName string = ""
	defaultParallelism  int    = 5
	minParallelism      int    = 1
	maxParallelism      int    = 50
	defaultBufferSize   int    = 500
	minBufferSize       int    = 1
	maxBufferSize       int    = 10000
	ModeFull            string = "full"
	ModeShallow         string = "shallow"
	defaultMode         string = ModeFull
	defaultLog          string = ""
	defaultResume       bool   = false
	defaultSince        string = "0"
)

// AppConfig contains the command-line options chosen by the user
type AppConfig struct {
	DatabaseName string
	Parallelism  int
	BufferSize   int
	Mode         string
	LogFilename  string
	Resume       bool
	Since        string
}

// NewAppConfig parses command-line flags and validates the resulting configuration.
func NewAppConfig() (*AppConfig, error) {
	appConfig := AppConfig{}

	// parse command-line options
	flag.StringVar(&appConfig.DatabaseName, "db", defaultDatabaseName, "The Cloudant database name to backup")
	flag.IntVar(&appConfig.Parallelism, "parallelism", defaultParallelism, "The number of HTTP write requests to perform in parallel when performing a backup")
	flag.IntVar(&appConfig.BufferSize, "buffer-size", defaultBufferSize, "The number of documents fetched per bulk read")
	flag.StringVar(&appConfig.Mode, "mode", defaultMode, "The backup mode - full or shallow")
	flag.StringVar(&appConfig.LogFilename, "log", defaultLog, "The name of the log file (optional)")
	flag.BoolVar(&appConfig.Resume, "resume", defaultResume, "Whether to resume a previously incomplete backup")
	flag.StringVar(&appConfig.Since, "since", defaultSince, "Which change to start the backup from (default: '0' - the beginning of time)")
	flag.Parse()

	// if we don't have a database name after parsing
	if appConfig.DatabaseName == "" {
		return nil, errors.New("missing db")
	} else if appConfig.Parallelism < minParallelism || appConfig.Parallelism > maxParallelism {
		return nil, fmt.Errorf("parallelism must be between %v and %v", minParallelism, maxParallelism)
	} else if appConfig.Mode != ModeFull && appConfig.Mode != ModeShallow {
		return nil, fmt.Errorf("mode must be one of %v or %v", ModeFull, ModeShallow)
	} else if appConfig.BufferSize < minBufferSize || appConfig.BufferSize > maxBufferSize {
		return nil, fmt.Errorf("buffer-size must be between %v and %v", minBufferSize, maxBufferSize)
	} else if appConfig.Resume && appConfig.LogFilename == "" {
		return nil, fmt.Errorf("--resume must be paired with --log")
	} else {
		return &appConfig, nil
	}
}
