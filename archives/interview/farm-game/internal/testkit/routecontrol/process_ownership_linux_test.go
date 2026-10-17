//go:build linux

package routecontrol

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestAfterStartFailureTimeoutRetainsSupervisedReaperUntilDrain(t *testing.T) {
	oldReadFile := linuxReadFile
	oldWait := waitAfterStartFailure
	oldTimeout := processStartFailureWaitTimeout
	t.Cleanup(func() {
		linuxReadFile = oldReadFile
		waitAfterStartFailure = oldWait
		processStartFailureWaitTimeout = oldTimeout
	})

	ready := t.TempDir() + "/ready"
	injectedRead := errors.New("injected process identity read failure")
	linuxReadFile = func(string) ([]byte, error) {
		waitForProcessHelper(t, ready)
		return nil, injectedRead
	}
	waitStarted := make(chan struct{})
	releaseWait := make(chan struct{})
	waitAfterStartFailure = func(cmd *exec.Cmd) error {
		close(waitStarted)
		<-releaseWait
		return cmd.Wait()
	}
	processStartFailureWaitTimeout = 20 * time.Millisecond

	_, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_HELPER=1",
			"ROUTECONTROL_READY_FILE=" + ready,
		},
	})
	if err == nil || !errors.Is(err, ErrProcessShutdownTimed) || !errors.Is(err, injectedRead) {
		t.Fatalf("StartProcess error=%v, want bounded timeout and identity error", err)
	}
	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("after-start wait seam did not start")
	}
	waitForSupervisedReaperCount(t, 1)

	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := DrainSupervisedReapers(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("early reaper drain error=%v, want context deadline", err)
	}
	if got := SupervisedReaperCount(); got != 1 {
		t.Fatalf("supervised reaper count after blocked drain=%d, want 1", got)
	}

	close(releaseWait)
	drainCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := DrainSupervisedReapers(drainCtx); err == nil {
		t.Fatal("final supervised reaper drain lost the real Wait error")
	}
	if got := SupervisedReaperCount(); got != 0 {
		t.Fatalf("supervised reaper count after drain=%d, want 0", got)
	}
}

func TestLinuxAfterStartFailureWithoutRawPIDFDKillsPinnedChild(t *testing.T) {
	ready := t.TempDir() + "/ready"
	cmd := exec.Command(os.Args[0], "-test.run=^TestRouteControlProcessHelper$")
	cmd.Env = append(os.Environ(),
		"ROUTECONTROL_PROCESS_HELPER=1",
		"ROUTECONTROL_READY_FILE="+ready,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitForProcessHelper(t, ready)

	rawPIDFD := -1
	starter := &linuxProcessGroupStarter{pidfd: &rawPIDFD}
	if err := starter.cleanupAfterStartFailure(cmd); err != nil {
		t.Fatalf("cleanup without raw pidfd error=%v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("pinned child was not reaped after raw pidfd failure")
	case err := <-waitDone:
		if err == nil {
			t.Fatal("killed child unexpectedly exited successfully")
		}
	}
}

func TestProcessWaitRetriesLeaderCloseFailure(t *testing.T) {
	oldClose := linuxCloseFD
	t.Cleanup(func() { linuxCloseFD = oldClose })
	injected := errors.New("injected leader close failure")
	var mu sync.Mutex
	closeCalls := 0
	linuxCloseFD = func(fd int) error {
		mu.Lock()
		closeCalls++
		call := closeCalls
		mu.Unlock()
		if call == 1 {
			return injected
		}
		return oldClose(fd)
	}

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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := process.Wait(ctx); err != nil {
		t.Fatalf("Wait did not retry leader close: %v", err)
	}
	mu.Lock()
	gotCalls := closeCalls
	mu.Unlock()
	if gotCalls < 2 {
		t.Fatalf("leader close calls=%d, want retry", gotCalls)
	}
	group, ok := process.group.(*linuxProcessGroup)
	if !ok {
		t.Fatalf("group type=%T, want *linuxProcessGroup", process.group)
	}
	group.mu.Lock()
	leaderFD := group.leaderPIDFD
	pending := len(group.pendingMembers)
	group.mu.Unlock()
	if leaderFD >= 0 || pending != 0 {
		t.Fatalf("capability ownership after Wait: leaderFD=%d pending=%d", leaderFD, pending)
	}
	if after := countOpenDescriptors(t); after > before {
		t.Fatalf("descriptor leak after leader close retry: before=%d after=%d", before, after)
	}
}

func TestProcessShutdownRetriesLeaderCloseFailure(t *testing.T) {
	oldClose := linuxCloseFD
	t.Cleanup(func() { linuxCloseFD = oldClose })
	injected := errors.New("injected shutdown leader close failure")
	var mu sync.Mutex
	closeCalls := 0
	linuxCloseFD = func(fd int) error {
		mu.Lock()
		closeCalls++
		call := closeCalls
		mu.Unlock()
		if call == 1 {
			return injected
		}
		return oldClose(fd)
	}

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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := process.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown did not retry leader close: %v", err)
	}
	mu.Lock()
	gotCalls := closeCalls
	mu.Unlock()
	if gotCalls < 2 {
		t.Fatalf("shutdown leader close calls=%d, want retry", gotCalls)
	}
}

func TestProcessShutdownRetriesMemberCloseFailure(t *testing.T) {
	oldClose := linuxCloseFD
	t.Cleanup(func() { linuxCloseFD = oldClose })
	injected := errors.New("injected member close failure")
	var mu sync.Mutex
	failedFD := -1
	closeCalls := make(map[int]int)
	linuxCloseFD = func(fd int) error {
		mu.Lock()
		closeCalls[fd]++
		if failedFD < 0 {
			failedFD = fd
			mu.Unlock()
			return injected
		}
		mu.Unlock()
		return oldClose(fd)
	}

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
	_, firstErr := process.Shutdown(ctx)
	if firstErr == nil || !errors.Is(firstErr, injected) {
		t.Fatalf("first Shutdown error=%v, want injected member close error", firstErr)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := process.Shutdown(ctx); err != nil {
		t.Fatalf("retry Shutdown error=%v", err)
	}
	waitForProcessGone(t, childPID)

	mu.Lock()
	fd := failedFD
	failedCalls := closeCalls[failedFD]
	mu.Unlock()
	if fd < 0 || failedCalls != 2 {
		t.Fatalf("failed member fd=%d close calls=%d, want exactly one retry", fd, failedCalls)
	}
}

func TestLinuxCloseEINTRDoesNotRetryDescriptorNumber(t *testing.T) {
	oldClose := linuxCloseFD
	t.Cleanup(func() { linuxCloseFD = oldClose })
	var mu sync.Mutex
	closeCalls := 0
	linuxCloseFD = func(int) error {
		mu.Lock()
		closeCalls++
		mu.Unlock()
		return unix.EINTR
	}
	group := &linuxProcessGroup{leaderPIDFD: 99}
	if err := group.close(); !errors.Is(err, unix.EINTR) {
		t.Fatalf("first close error=%v, want EINTR", err)
	}
	if err := group.close(); err != nil {
		t.Fatalf("second close error=%v, want no retry after EINTR", err)
	}
	mu.Lock()
	gotCalls := closeCalls
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("EINTR close calls=%d, want 1", gotCalls)
	}
}

func waitForSupervisedReaperCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := SupervisedReaperCount(); got == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("supervised reaper count did not reach %d; got %d", want, SupervisedReaperCount())
		case <-ticker.C:
		}
	}
}
