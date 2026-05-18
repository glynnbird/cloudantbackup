package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	backup "github.com/glynnbird/cloudantbackup/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// create cloudant backup
	cloudantBackup, err := backup.New()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// run it
	err = cloudantBackup.Run(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
