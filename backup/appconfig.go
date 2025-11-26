package backup

import (
	"errors"
	"flag"
	"fmt"
)

// defaults
const defaultDatabaseName string = ""
const defaultParallelism int = 5
const minParallelism int = 1
const maxParallelism int = 50
const defaultBufferSize int = 500
const minBufferSize int = 1
const maxBuferSize int = 10000
const ModeFull string = "full"
const ModeShallow string = "shallow"
const defaultMode string = ModeFull
const defaultLog string = ""
const defaultResume bool = false

// AppConfig contains the command-line options chosen by the user
type AppConfig struct {
	DatabaseName string
	Parallelism  int
	BufferSize   int
	Mode         string
	LogFilename  string
	Resume       bool
}

// NewAppConfig creates a new AppConfig struct, parsing any command-line parameters
func NewAppConfig() (*AppConfig, error) {
	appConfig := AppConfig{}

	// parse command-line options
	flag.StringVar(&appConfig.DatabaseName, "db", defaultDatabaseName, "The Cloudant database name to backup")
	flag.IntVar(&appConfig.Parallelism, "parallelism", defaultParallelism, "The number of HTTP write requests to perform in parallel when performing a backup")
	flag.IntVar(&appConfig.BufferSize, "buffer-size", defaultBufferSize, "The number of documents fetched per bulk read")
	flag.StringVar(&appConfig.Mode, "mode", defaultMode, "The backup mode - full or shallow")
	flag.StringVar(&appConfig.LogFilename, "log", defaultLog, "The name of the log file (optional)")
	flag.BoolVar(&appConfig.Resume, "resume", defaultResume, "Whether to resume a previously incomplete backup")
	flag.Parse()

	// if we don't have a database name after parsing
	if appConfig.DatabaseName == "" {
		return nil, errors.New("missing db")
	} else if appConfig.Parallelism < minParallelism || appConfig.Parallelism > maxParallelism {
		return nil, fmt.Errorf("parallelism must be between %v and %v", minParallelism, maxParallelism)
	} else if appConfig.Mode != ModeFull && appConfig.Mode != ModeShallow {
		return nil, fmt.Errorf("mode must one of %v and %v", ModeFull, ModeShallow)
	} else if appConfig.BufferSize < minBufferSize || appConfig.BufferSize > maxBuferSize {
		return nil, fmt.Errorf("buffer-size must be between %v and %v", minBufferSize, maxBuferSize)
	} else if appConfig.Resume && appConfig.LogFilename == "" {
		return nil, fmt.Errorf("--resume must be paired with --log")
	} else {
		return &appConfig, nil
	}
}
