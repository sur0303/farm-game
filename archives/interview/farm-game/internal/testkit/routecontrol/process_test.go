//go:build linux

package routecontrol

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/signal"
	"testing"
	"time"
)

func TestRouteControlProcessHelper(t *testing.T) {
	if os.Getenv("ROUTECONTROL_PROCESS_HELPER") != "1" {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	defer signal.Stop(signals)
	if os.Getenv("ROUTECONTROL_IGNORE_INTERRUPT") == "1" {
		if path := os.Getenv("ROUTECONTROL_READY_FILE"); path != "" {
			_ = os.WriteFile(path, []byte("ready\n"), 0o600)
		}
		for {
			select {
			case <-signals:
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	if path := os.Getenv("ROUTECONTROL_READY_FILE"); path != "" {
		_ = os.WriteFile(path, []byte("ready\n"), 0o600)
	}
	if os.Getenv("ROUTECONTROL_EXIT_NATURALLY") == "1" {
		return
	}
	<-signals
}

func TestProcessRejectsUnboundedOutputWriter(t *testing.T) {
	_, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Stdout:  bytes.NewBuffer(nil),
	})
	if !errors.Is(err, ErrInvalidProcessWriter) {
		t.Fatalf("writer error=%v, want ErrInvalidProcessWriter", err)
	}
}

func TestProcessRejectsTypedNilFileWriter(t *testing.T) {
	var file *os.File
	_, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Stdout:  file,
	})
	if !errors.Is(err, ErrInvalidProcessWriter) {
		t.Fatalf("typed nil writer error=%v, want ErrInvalidProcessWriter", err)
	}
}

func TestProcessShutdownGracefulIsBounded(t *testing.T) {
	ready := t.TempDir() + "/ready"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_HELPER=1",
			"ROUTECONTROL_READY_FILE=" + ready,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForProcessHelper(t, ready)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := process.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.PID == 0 || result.ExitCode != 0 {
		t.Fatalf("unexpected graceful result: %+v", result)
	}
	if result.FinishedAt.Before(result.StartedAt) {
		t.Fatalf("finish time before start: %+v", result)
	}
}

func TestProcessShutdownKillsIgnoringChildWithinBound(t *testing.T) {
	ready := t.TempDir() + "/ready"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_HELPER=1",
			"ROUTECONTROL_IGNORE_INTERRUPT=1",
			"ROUTECONTROL_READY_FILE=" + ready,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForProcessHelper(t, ready)
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result, err := process.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("shutdown exceeded bound: %s", elapsed)
	}
	if result.PID == 0 || result.ExitCode == 0 {
		t.Fatalf("child was not killed: %+v", result)
	}
}

func TestProcessWaitAutomaticallyClosesCapability(t *testing.T) {
	before := countOpenDescriptors(t)
	ready := t.TempDir() + "/ready"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_HELPER=1",
			"ROUTECONTROL_EXIT_NATURALLY=1",
			"ROUTECONTROL_READY_FILE=" + ready,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForProcessHelper(t, ready)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := process.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	after := countOpenDescriptors(t)
	if after > before {
		t.Fatalf("process finalization leaked descriptors: before=%d after=%d", before, after)
	}
}

func TestProcessConcurrentWaitAndShutdownAreIdempotent(t *testing.T) {
	ready := t.TempDir() + "/ready"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_HELPER=1",
			"ROUTECONTROL_IGNORE_INTERRUPT=1",
			"ROUTECONTROL_READY_FILE=" + ready,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForProcessHelper(t, ready)

	results := make(chan error, 4)
	for i := 0; i < 2; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := process.Wait(ctx)
			results <- err
		}()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := process.Shutdown(ctx)
			results <- err
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("concurrent process lifecycle operation failed: %v", err)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("concurrent Wait/Shutdown did not complete")
		}
	}
}

func waitForProcessHelper(t *testing.T, path string) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("process helper did not become ready: %s", path)
		case <-ticker.C:
		}
	}
}

func countOpenDescriptors(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
