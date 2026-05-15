package backup

import (
	"flag"
	"os"
	"strings"
	"testing"
)

func runNewAppConfigForTest(t *testing.T, args []string) (*AppConfig, error) {
	t.Helper()

	oldArgs := os.Args
	oldCommandLine := flag.CommandLine

	os.Args = append([]string{"cloudantbackup"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	})

	return NewAppConfig()
}

func TestNewAppConfigValid(t *testing.T) {
	cfg, err := runNewAppConfigForTest(t, []string{
		"--db", "mydb",
		"--parallelism", "10",
		"--buffer-size", "1000",
		"--mode", "shallow",
		"--log", "backup.log",
		"--resume",
		"--since", "123-g1AAA",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.DatabaseName != "mydb" {
		t.Fatalf("expected db mydb, got %s", cfg.DatabaseName)
	}
	if cfg.Parallelism != 10 {
		t.Fatalf("expected parallelism 10, got %d", cfg.Parallelism)
	}
	if cfg.BufferSize != 1000 {
		t.Fatalf("expected buffer size 1000, got %d", cfg.BufferSize)
	}
	if cfg.Mode != ModeShallow {
		t.Fatalf("expected mode %s, got %s", ModeShallow, cfg.Mode)
	}
	if cfg.LogFilename != "backup.log" {
		t.Fatalf("expected log filename backup.log, got %s", cfg.LogFilename)
	}
	if !cfg.Resume {
		t.Fatal("expected resume to be true")
	}
	if cfg.Since != "123-g1AAA" {
		t.Fatalf("expected since 123-g1AAA, got %s", cfg.Since)
	}
}

func TestNewAppConfigMissingDB(t *testing.T) {
	_, err := runNewAppConfigForTest(t, nil)
	if err == nil {
		t.Fatal("expected error for missing db")
	}
	if err.Error() != "missing db" {
		t.Fatalf("expected missing db error, got %v", err)
	}
}

func TestNewAppConfigInvalidParallelism(t *testing.T) {
	_, err := runNewAppConfigForTest(t, []string{"--db", "mydb", "--parallelism", "0"})
	if err == nil {
		t.Fatal("expected error for invalid parallelism")
	}
	if !strings.Contains(err.Error(), "parallelism must be between") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAppConfigInvalidMode(t *testing.T) {
	_, err := runNewAppConfigForTest(t, []string{"--db", "mydb", "--mode", "bad"})
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "mode must be one of") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAppConfigInvalidBufferSize(t *testing.T) {
	_, err := runNewAppConfigForTest(t, []string{"--db", "mydb", "--buffer-size", "0"})
	if err == nil {
		t.Fatal("expected error for invalid buffer size")
	}
	if !strings.Contains(err.Error(), "buffer-size must be between") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAppConfigResumeRequiresLog(t *testing.T) {
	_, err := runNewAppConfigForTest(t, []string{"--db", "mydb", "--resume"})
	if err == nil {
		t.Fatal("expected error when resume is used without log")
	}
	if err.Error() != "--resume must be paired with --log" {
		t.Fatalf("unexpected error: %v", err)
	}
}
