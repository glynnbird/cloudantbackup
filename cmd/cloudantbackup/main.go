package main

import (
	"fmt"
	"os"

	backup "github.com/glynnbird/cloudantbackup/internal/app"
)

func main() {

	// create cloudant backup
	cloudantBackup, err := backup.New()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// run it
	err = cloudantBackup.Run()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
