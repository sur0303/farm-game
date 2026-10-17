//go:build linux

package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLinuxProcessGroupFailsClosedWhenLeaderPIDIsReused(t *testing.T) {
	oldReadDir := linuxReadDir
	oldReadFile := linuxReadFile
	defer func() {
		linuxReadDir = oldReadDir
		linuxReadFile = oldReadFile
	}()

	linuxReadDir = func(string) ([]os.DirEntry, error) {
		info := fakeLinuxFileInfo{name: "1234"}
		return []os.DirEntry{fs.FileInfoToDirEntry(info)}, nil
	}
	linuxReadFile = func(string) ([]byte, error) {
		fields := []string{
			"S", "1", "1234", "2", "0", "0", "0", "0", "0", "0",
			"0", "0", "0", "0", "0", "0", "0", "0", "0", "999",
		}
		return []byte("1234 (replacement) " + strings.Join(fields, " ")), nil
	}

	group := &linuxProcessGroup{
		pgid:        1234,
		session:     2,
		leaderStart: 111,
		leaderPIDFD: 99,
	}
	if err := group.kill(); !errors.Is(err, ErrProcessGroupIdentityReused) {
		t.Fatalf("PID reuse error=%v, want ErrProcessGroupIdentityReused", err)
	}
}

func TestLinuxProcessGroupMemberStatErrorFailsClosed(t *testing.T) {
	oldReadDir := linuxReadDir
	oldReadFile := linuxReadFile
	oldSend := linuxPidfdSendSignal
	defer func() {
		linuxReadDir = oldReadDir
		linuxReadFile = oldReadFile
		linuxPidfdSendSignal = oldSend
	}()

	linuxReadDir = func(string) ([]os.DirEntry, error) {
		return []os.DirEntry{fs.FileInfoToDirEntry(fakeLinuxFileInfo{name: "1234"})}, nil
	}
	linuxReadFile = func(string) ([]byte, error) {
		return nil, errors.New("injected /proc stat failure")
	}
	var signals int
	linuxPidfdSendSignal = func(int, unix.Signal, *unix.Siginfo, int) error {
		signals++
		return nil
	}

	group := &linuxProcessGroup{
		pgid:        1234,
		session:     2,
		leaderStart: 111,
		leaderPIDFD: 99,
	}
	err := group.kill()
	if err == nil || errors.Is(err, os.ErrProcessDone) ||
		!errors.Is(err, ErrProcessGroupUnavailable) {
		t.Fatalf("member stat error=%v, want fail-closed ErrProcessGroupUnavailable", err)
	}
	if signals != 0 {
		t.Fatalf("fail-closed /proc error signaled %d members", signals)
	}
}

func TestLinuxProcessGroupReadDirErrorFailsClosedWithoutGroupSignal(t *testing.T) {
	oldReadDir := linuxReadDir
	oldSend := linuxPidfdSendSignal
	defer func() {
		linuxReadDir = oldReadDir
		linuxPidfdSendSignal = oldSend
	}()
	injected := errors.New("injected /proc directory failure")
	linuxReadDir = func(string) ([]os.DirEntry, error) {
		return nil, injected
	}
	var signals int
	linuxPidfdSendSignal = func(int, unix.Signal, *unix.Siginfo, int) error {
		signals++
		return nil
	}
	group := &linuxProcessGroup{
		pgid:        1234,
		session:     2,
		leaderStart: 111,
		leaderPIDFD: 99,
	}
	err := group.kill()
	if !errors.Is(err, injected) || !errors.Is(err, ErrProcessGroupUnavailable) {
		t.Fatalf("read /proc error=%v, want injected capability error", err)
	}
	if signals != 0 {
		t.Fatalf("read /proc error signaled %d members", signals)
	}
}

func TestLinuxAfterStartTransfersOriginalRawPIDFD(t *testing.T) {
	oldReadFile := linuxReadFile
	defer func() { linuxReadFile = oldReadFile }()
	linuxReadFile = func(string) ([]byte, error) {
		return linuxTestProcessStat(1234, 1234, 2, 111), nil
	}
	rawFD := 77
	starter := &linuxProcessGroupStarter{pidfd: &rawFD}
	groupCapability, err := starter.afterStart(&exec.Cmd{
		Process: &os.Process{Pid: 1234},
	})
	if err != nil {
		t.Fatal(err)
	}
	group, ok := groupCapability.(*linuxProcessGroup)
	if !ok {
		t.Fatalf("capability type=%T, want *linuxProcessGroup", groupCapability)
	}
	if rawFD != -1 || group.leaderPIDFD != 77 {
		t.Fatalf("raw fd ownership: starter=%d group=%d", rawFD, group.leaderPIDFD)
	}
}

func TestLinuxOwnedMemberCloseErrorsPropagate(t *testing.T) {
	scanErr := errors.New("injected /proc scan failure")
	closeErr := errors.New("injected member pidfd close failure")
	install := func(entries []string, read func(int) ([]byte, error)) func() {
		oldReadDir := linuxReadDir
		oldReadFile := linuxReadFile
		oldPidfdOpen := linuxPidfdOpen
		oldSend := linuxPidfdSendSignal
		oldClose := linuxCloseFD
		linuxReadDir = func(string) ([]os.DirEntry, error) {
			result := make([]os.DirEntry, 0, len(entries))
			for _, name := range entries {
				result = append(result, fs.FileInfoToDirEntry(fakeLinuxFileInfo{name: name}))
			}
			return result, nil
		}
		linuxReadFile = func(path string) ([]byte, error) {
			parts := strings.Split(path, "/")
			pid, err := strconv.Atoi(parts[len(parts)-2])
			if err != nil {
				return nil, err
			}
			return read(pid)
		}
		linuxPidfdOpen = func(pid int, _ int) (int, error) {
			return pid - 1193, nil
		}
		linuxPidfdSendSignal = func(int, unix.Signal, *unix.Siginfo, int) error {
			return nil
		}
		linuxCloseFD = func(fd int) error {
			if fd >= 42 {
				return closeErr
			}
			return nil
		}
		return func() {
			linuxReadDir = oldReadDir
			linuxReadFile = oldReadFile
			linuxPidfdOpen = oldPidfdOpen
			linuxPidfdSendSignal = oldSend
			linuxCloseFD = oldClose
		}
	}

	t.Run("membersLocked error cleanup", func(t *testing.T) {
		restore := install([]string{"1234", "1235", "1236"}, func(pid int) ([]byte, error) {
			if pid == 1236 {
				return nil, scanErr
			}
			return linuxTestProcessStat(pid, 1234, 2, map[int]uint64{1234: 111, 1235: 222}[pid]), nil
		})
		defer restore()
		group := &linuxProcessGroup{pgid: 1234, session: 2, leaderStart: 111, leaderPIDFD: 99}
		_, err := group.membersLocked()
		if !errors.Is(err, scanErr) || !errors.Is(err, closeErr) {
			t.Fatalf("membersLocked error=%v, want scan and close errors", err)
		}
	})

	t.Run("signal traversal", func(t *testing.T) {
		restore := install([]string{"1234", "1235"}, func(pid int) ([]byte, error) {
			return linuxTestProcessStat(pid, 1234, 2, map[int]uint64{1234: 111, 1235: 222}[pid]), nil
		})
		defer restore()
		group := &linuxProcessGroup{pgid: 1234, session: 2, leaderStart: 111, leaderPIDFD: 99}
		if err := group.kill(); !errors.Is(err, closeErr) {
			t.Fatalf("signal close error=%v, want member close error", err)
		}
	})

	t.Run("wait traversal", func(t *testing.T) {
		restore := install([]string{"1234", "1235"}, func(pid int) ([]byte, error) {
			return linuxTestProcessStat(pid, 1234, 2, map[int]uint64{1234: 111, 1235: 222}[pid]), nil
		})
		defer restore()
		group := &linuxProcessGroup{pgid: 1234, session: 2, leaderStart: 111, leaderPIDFD: 99}
		if err := group.waitForExit(context.Background()); !errors.Is(err, closeErr) {
			t.Fatalf("wait close error=%v, want member close error", err)
		}
	})
}

func TestProcessFirstInterruptFailureStillCleansFullGroup(t *testing.T) {
	oldSend := linuxPidfdSendSignal
	defer func() { linuxPidfdSendSignal = oldSend }()
	linuxPidfdSendSignal = func(fd int, signal unix.Signal, info *unix.Siginfo, flags int) error {
		if signal == unix.SIGINT {
			return errors.New("injected interrupt failure")
		}
		return oldSend(fd, signal, info, flags)
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
	if _, err := process.Shutdown(ctx); err == nil || !strings.Contains(err.Error(), "injected interrupt failure") {
		t.Fatalf("Shutdown error=%v, want injected interrupt failure after hard cleanup", err)
	}
	waitForProcessGone(t, childPID)
}

func TestProcessAfterStartFailureUsesRawPIDFDAndPropagatesWaitError(t *testing.T) {
	oldReadFile := linuxReadFile
	oldSend := linuxPidfdSendSignal
	oldClose := linuxCloseFD
	oldWait := waitAfterStartFailure
	defer func() {
		linuxReadFile = oldReadFile
		linuxPidfdSendSignal = oldSend
		linuxCloseFD = oldClose
		waitAfterStartFailure = oldWait
	}()
	ready := t.TempDir() + "/ready"
	injectedRead := errors.New("injected process identity read failure")
	injectedWait := errors.New("injected wait error")
	linuxReadFile = func(path string) ([]byte, error) {
		waitForProcessHelper(t, ready)
		return nil, injectedRead
	}
	waitAfterStartFailure = func(cmd *exec.Cmd) error {
		return errors.Join(oldWait(cmd), injectedWait)
	}
	var mu sync.Mutex
	var events []string
	linuxPidfdSendSignal = func(fd int, signal unix.Signal, info *unix.Siginfo, flags int) error {
		mu.Lock()
		events = append(events, fmt.Sprintf("signal:%d:%d", fd, signal))
		mu.Unlock()
		return oldSend(fd, signal, info, flags)
	}
	linuxCloseFD = func(fd int) error {
		mu.Lock()
		events = append(events, fmt.Sprintf("close:%d", fd))
		mu.Unlock()
		return oldClose(fd)
	}

	_, err := StartProcess(ProcessSpec{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestRouteControlProcessHelper$"},
		Env: []string{
			"ROUTECONTROL_PROCESS_HELPER=1",
			"ROUTECONTROL_READY_FILE=" + ready,
		},
	})
	if err == nil || !errors.Is(err, injectedRead) || !errors.Is(err, injectedWait) {
		t.Fatalf("StartProcess error=%v, want injected afterStart failure", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) < 2 || !strings.HasPrefix(events[0], "signal:") ||
		!strings.HasPrefix(events[1], "close:") {
		t.Fatalf("raw pidfd cleanup events=%v, want signal before close", events)
	}
}

func TestProcessAfterStartFailureWaitIsBoundedAndPropagatesTimeout(t *testing.T) {
	oldReadFile := linuxReadFile
	oldWait := waitAfterStartFailure
	oldTimeout := processStartFailureWaitTimeout
	defer func() {
		linuxReadFile = oldReadFile
		waitAfterStartFailure = oldWait
		processStartFailureWaitTimeout = oldTimeout
	}()
	ready := t.TempDir() + "/ready"
	injectedRead := errors.New("injected process identity read failure")
	linuxReadFile = func(string) ([]byte, error) {
		waitForProcessHelper(t, ready)
		return nil, injectedRead
	}
	releaseWait := make(chan struct{})
	waitFinished := make(chan struct{})
	waitAfterStartFailure = func(cmd *exec.Cmd) error {
		defer close(waitFinished)
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
		t.Fatalf("StartProcess error=%v, want injected read and bounded wait errors", err)
	}
	close(releaseWait)
	select {
	case <-waitFinished:
	case <-time.After(time.Second):
		t.Fatal("stuck wait seam did not unwind after release")
	}
}

type fakeLinuxFileInfo struct {
	name string
}

func linuxTestProcessStat(pid, pgid, session int, start uint64) []byte {
	fields := make([]string, 20)
	for index := range fields {
		fields[index] = "0"
	}
	fields[0] = "S"
	fields[2] = strconv.Itoa(pgid)
	fields[3] = strconv.Itoa(session)
	fields[19] = strconv.FormatUint(start, 10)
	return []byte(fmt.Sprintf("%d (test) %s", pid, strings.Join(fields, " ")))
}

func (f fakeLinuxFileInfo) Name() string     { return f.name }
func (fakeLinuxFileInfo) Size() int64        { return 0 }
func (fakeLinuxFileInfo) Mode() os.FileMode  { return 0 }
func (fakeLinuxFileInfo) ModTime() time.Time { return time.Time{} }
func (fakeLinuxFileInfo) IsDir() bool        { return false }
func (fakeLinuxFileInfo) Sys() any           { return nil }
