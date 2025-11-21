package backup

import (
	"errors"
	"flag"
)

// AppConfig contains the command-line options chosen by the user
type AppConfig struct {
	DatabaseName string
	LogFilename  string
	Parallelism  int
}

func NewAppConfig() (*AppConfig, error) {
	appConfig := AppConfig{}

	// parse command-line options
	flag.StringVar(&appConfig.DatabaseName, "db", "", "The Cloudant database name to backup")
	flag.StringVar(&appConfig.DatabaseName, "d", "", "The Cloudant database name to backup")
	flag.IntVar(&appConfig.Parallelism, "parallelism", 5, "The number of HTTP requests to perform in parallel when performing a backup")
	flag.IntVar(&appConfig.Parallelism, "p", 1, "The number of HTTP requests to perform in parallel when performing a backup")
	flag.Parse()

	// if we don't have a database name after parsing
	if appConfig.DatabaseName == "" {
		return nil, errors.New("missing d/db")
	} else if appConfig.Parallelism < 1 || appConfig.Parallelism > 50 {
		return nil, errors.New("parallelism must be between 1 and 50")
	} else {
		return &appConfig, nil
	}
}
