package main

import (
	"github.com/glynnbird/cloudantbackup/backup"
)

func main() {

	// create cloudant snap
	cloudantBackup, err := backup.New()
	if err != nil {
		panic(err)
	}

	// run it
	err = cloudantBackup.Run()
	if err != nil {
		panic(err)
	}
}
