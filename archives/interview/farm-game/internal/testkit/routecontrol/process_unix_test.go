//go:build linux

package routecontrol

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestRouteControlProcessTreeHelper(t *testing.T) {
	if os.Getenv("ROUTECONTROL_PROCESS_TREE_HELPER") != "1" {
		return
	}
	child := exec.Command("sh", "-c", "trap '' INT TERM; sleep 30")
	if err := child.Start(); err != nil {
		return
	}
	pidPath := os.Getenv("ROUTECONTROL_CHILD_PID_FILE")
	readyPath := os.Getenv("ROUTECONTROL_READY_FILE")
	_ = os.WriteFile(pidPath, []byte(strconv.Itoa(child.Process.Pid)), 0o600)
	_ = os.WriteFile(readyPath, []byte("ready\n"), 0o600)
	if os.Getenv("ROUTECONTROL_EXIT_LEADER_NATURALLY") == "1" {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	for {
		select {
		case <-signals:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRouteControlProcessLeaderExitsFirstHelper(t *testing.T) {
	if os.Getenv("ROUTECONTROL_PROCESS_LEADER_EXITS_FIRST_HELPER") != "1" {
		return
	}
	child := exec.Command("sh", "-c", "trap '' INT TERM; sleep 30")
	if err := child.Start(); err != nil {
		return
	}
	pidPath := os.Getenv("ROUTECONTROL_CHILD_PID_FILE")
	readyPath := os.Getenv("ROUTECONTROL_READY_FILE")
	_ = os.WriteFile(pidPath, []byte(strconv.Itoa(child.Process.Pid)), 0o600)
	_ = os.WriteFile(readyPath, []byte("ready\n"), 0o600)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signalCh)
	<-signalCh
}

func TestProcessShutdownKillsProcessGroupAndGrandchild(t *testing.T) {
	ready := t.TempDir() + "/ready"
	childPIDPath := t.TempDir() + "/child.pid"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessTreeHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_TREE_HELPER=1",
			"ROUTECONTROL_READY_FILE=" + ready,
			"ROUTECONTROL_CHILD_PID_FILE=" + childPIDPath,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = process.Shutdown(ctx)
	})
	waitForProcessHelper(t, ready)
	rawPID, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(rawPID))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := process.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	waitForProcessGone(t, childPID)
}

func TestProcessWaitAutomaticallyCleansProcessGroupAndGrandchild(t *testing.T) {
	ready := t.TempDir() + "/ready"
	childPIDPath := t.TempDir() + "/child.pid"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessTreeHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_TREE_HELPER=1",
			"ROUTECONTROL_EXIT_LEADER_NATURALLY=1",
			"ROUTECONTROL_READY_FILE=" + ready,
			"ROUTECONTROL_CHILD_PID_FILE=" + childPIDPath,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForProcessHelper(t, ready)
	rawPID, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(rawPID))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := process.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	waitForProcessGone(t, childPID)
}

func TestProcessShutdownCleansGrandchildWhenLeaderExitsFirst(t *testing.T) {
	ready := t.TempDir() + "/ready"
	childPIDPath := t.TempDir() + "/child.pid"
	process, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessLeaderExitsFirstHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_LEADER_EXITS_FIRST_HELPER=1",
			"ROUTECONTROL_READY_FILE=" + ready,
			"ROUTECONTROL_CHILD_PID_FILE=" + childPIDPath,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = process.Shutdown(ctx)
	})
	waitForProcessHelper(t, ready)
	rawPID, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(rawPID))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := process.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	waitForProcessGone(t, childPID)
}

func waitForProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("process %d remained alive", pid)
		case <-ticker.C:
		}
	}
}
