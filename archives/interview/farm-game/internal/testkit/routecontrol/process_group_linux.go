//go:build linux

package routecontrol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type processGroupCapability interface {
	interrupt() error
	kill() error
	cleanup(context.Context) error
	close() error
	ownsCapability() bool
}

type processGroupStarter interface {
	configure(*exec.Cmd)
	afterStart(*exec.Cmd) (processGroupCapability, error)
	cleanupAfterStartFailure(*exec.Cmd) error
}

type linuxProcessGroupStarter struct {
	pidfd        *int
	verifiedInfo *linuxProcessInfo
}

type linuxProcessGroup struct {
	mu             sync.Mutex
	pgid           int
	session        int
	leaderStart    uint64
	leaderPIDFD    int
	pendingMembers map[int]linuxProcessMember
	closed         bool
}

type linuxProcessInfo struct {
	pid       int
	pgid      int
	session   int
	startTime uint64
	state     byte
}

type linuxProcessMember struct {
	pid   int
	pidfd int
	owned bool
}

var (
	linuxReadDir         = os.ReadDir
	linuxReadFile        = os.ReadFile
	linuxPidfdOpen       = unix.PidfdOpen
	linuxPidfdSendSignal = unix.PidfdSendSignal
	linuxCloseFD         = unix.Close
)

func newProcessGroupStarter() (processGroupStarter, error) {
	pidfd := -1
	return &linuxProcessGroupStarter{pidfd: &pidfd}, nil
}

func (s *linuxProcessGroupStarter) configure(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
		PidFD:   s.pidfd,
	}
}

func (s *linuxProcessGroupStarter) afterStart(cmd *exec.Cmd) (processGroupCapability, error) {
	if s == nil || s.pidfd == nil || *s.pidfd < 0 {
		return nil, fmt.Errorf("%w: Linux pidfd was not returned", ErrProcessGroupUnavailable)
	}
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 1 {
		return nil, fmt.Errorf("%w: started command has no valid process", ErrProcessGroupUnavailable)
	}
	info, err := readLinuxProcessInfo(cmd.Process.Pid)
	if err != nil {
		return nil, fmt.Errorf("%w: read original process-group identity: %w", ErrProcessGroupUnavailable, err)
	}
	if info.pid != cmd.Process.Pid || info.pgid != cmd.Process.Pid {
		return nil, fmt.Errorf("%w: child did not become its own process-group leader", ErrProcessGroupUnavailable)
	}
	s.verifiedInfo = &info
	fd := *s.pidfd
	*s.pidfd = -1
	return &linuxProcessGroup{
		pgid:        info.pgid,
		session:     info.session,
		leaderStart: info.startTime,
		leaderPIDFD: fd,
	}, nil
}

func (s *linuxProcessGroupStarter) cleanupAfterStartFailure(cmd *exec.Cmd) error {
	if s == nil || s.pidfd == nil || *s.pidfd < 0 {
		return killStartedProcess(cmd)
	}

	fd := *s.pidfd
	var joined error
	if err := linuxPidfdSendSignal(fd, unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
		joined = errors.Join(joined, fmt.Errorf("signal failed-start leader through pidfd: %w", err))
		joined = errors.Join(joined, killStartedProcess(cmd))
	}
	if s.verifiedInfo != nil {
		group := &linuxProcessGroup{
			pgid:        s.verifiedInfo.pgid,
			session:     s.verifiedInfo.session,
			leaderStart: s.verifiedInfo.startTime,
			leaderPIDFD: fd,
		}
		*s.pidfd = -1
		joined = errors.Join(joined, group.cleanup(context.Background()))
		joined = errors.Join(joined, group.close())
		return joined
	}
	if err := s.closeRawPIDFD(); err != nil {
		joined = errors.Join(joined, fmt.Errorf("close raw leader pidfd: %w", err))
	}
	return joined
}

func (s *linuxProcessGroupStarter) closeRawPIDFD() error {
	if s == nil || s.pidfd == nil || *s.pidfd < 0 {
		return nil
	}
	fd := *s.pidfd
	if err := linuxCloseFD(fd); err != nil {
		return err
	}
	*s.pidfd = -1
	return nil
}

func (g *linuxProcessGroup) interrupt() error {
	return g.signalMembers(unix.SIGINT)
}

func (g *linuxProcessGroup) kill() error {
	return g.signalMembers(unix.SIGKILL)
}

func (g *linuxProcessGroup) cleanup(ctx context.Context) error {
	if ctx == nil {
		return errors.New("process group cleanup context is nil")
	}
	var joined error
	if err := g.signalMembers(unix.SIGINT); err != nil && !errors.Is(err, os.ErrProcessDone) {
		joined = errors.Join(joined, err)
	}

	graceCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := g.waitForExit(graceCtx); err == nil {
		return joined
	} else {
		if !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) {
			joined = errors.Join(joined, err)
		}
	}

	if err := g.signalMembers(unix.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		joined = errors.Join(joined, err)
	}
	killCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.waitForExit(killCtx); err != nil {
		joined = errors.Join(joined, fmt.Errorf("process group %d remained alive: %w", g.pgid, err))
	}
	return joined
}

func (g *linuxProcessGroup) close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	var joined error
	if err := g.closePendingMembersLocked(); err != nil {
		joined = errors.Join(joined, err)
	}
	if g.leaderPIDFD >= 0 {
		fd := g.leaderPIDFD
		err := linuxCloseFD(fd)
		if err == nil || linuxCloseReleasesFD(err) {
			g.leaderPIDFD = -1
		}
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("close leader pidfd: %w", err))
		}
	}
	if g.leaderPIDFD < 0 && len(g.pendingMembers) == 0 {
		g.closed = true
	}
	return joined
}

func (g *linuxProcessGroup) ownsCapability() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.hasOpenCapabilityLocked()
}

func (g *linuxProcessGroup) signalMembers(signal unix.Signal) error {
	if g == nil {
		return os.ErrProcessDone
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || !g.hasOpenCapabilityLocked() {
		return os.ErrProcessDone
	}

	members, scanErr := g.membersLocked()
	if scanErr != nil {
		return scanErr
	}
	if len(members) == 0 {
		return os.ErrProcessDone
	}
	var joined error
	for _, member := range members {
		err := linuxPidfdSendSignal(member.pidfd, signal, nil, 0)
		if err != nil && !errors.Is(err, unix.ESRCH) {
			joined = errors.Join(joined, fmt.Errorf("signal pid %d: %w", member.pid, err))
		}
		if err := g.closeMemberLocked(member); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	if joined != nil {
		return joined
	}
	return nil
}

func (g *linuxProcessGroup) waitForExit(ctx context.Context) error {
	if ctx == nil {
		return errors.New("process group wait context is nil")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		g.mu.Lock()
		if g.closed || !g.hasOpenCapabilityLocked() {
			g.mu.Unlock()
			return nil
		}
		members, err := g.membersLocked()
		closeErr := g.closeMembersLocked(members)
		g.mu.Unlock()
		if err != nil {
			return errors.Join(err, closeErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if len(members) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (g *linuxProcessGroup) members() ([]linuxProcessMember, error) {
	if g == nil {
		return nil, os.ErrProcessDone
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || !g.hasOpenCapabilityLocked() {
		return nil, os.ErrProcessDone
	}
	return g.membersLocked()
}

func (g *linuxProcessGroup) membersLocked() ([]linuxProcessMember, error) {
	if g.closed {
		return nil, os.ErrProcessDone
	}
	members := g.pendingMembersLocked()
	pendingPIDs := make(map[int]struct{}, len(members))
	for _, member := range members {
		pendingPIDs[member.pid] = struct{}{}
	}
	if g.leaderPIDFD < 0 {
		return members, nil
	}
	entries, err := linuxReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("%w: read /proc: %w", ErrProcessGroupUnavailable, err)
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		if _, ok := pendingPIDs[pid]; ok {
			continue
		}
		info, err := readLinuxProcessInfo(pid)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			closeErr := g.closeMembersLocked(members)
			return nil, errors.Join(
				fmt.Errorf("%w: stat process %d: %w", ErrProcessGroupUnavailable, pid, err),
				closeErr,
			)
		}
		if pid == g.pgid && info.startTime != g.leaderStart {
			closeErr := g.closeMembersLocked(members)
			return nil, errors.Join(
				fmt.Errorf("%w: process-group leader pid %d was reused", ErrProcessGroupIdentityReused, g.pgid),
				closeErr,
			)
		}
		if info.pgid != g.pgid || info.session != g.session {
			continue
		}
		if info.state == 'Z' || info.state == 'X' {
			continue
		}
		if pid == g.pgid {
			members = append(members, linuxProcessMember{pid: pid, pidfd: g.leaderPIDFD})
			continue
		}
		pidfd, err := linuxPidfdOpen(pid, 0)
		if err != nil {
			if errors.Is(err, unix.ESRCH) {
				continue
			}
			closeErr := g.closeMembersLocked(members)
			return nil, errors.Join(
				fmt.Errorf("%w: open pidfd for process %d: %w", ErrProcessGroupUnavailable, pid, err),
				closeErr,
			)
		}
		confirm, err := readLinuxProcessInfo(pid)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				closeErr := g.closeMemberLocked(linuxProcessMember{pid: pid, pidfd: pidfd, owned: true})
				if closeErr != nil {
					previousCloseErr := g.closeMembersLocked(members)
					return nil, errors.Join(
						closeErr,
						previousCloseErr,
					)
				}
				continue
			}
			closeErr := g.closeMemberLocked(linuxProcessMember{pid: pid, pidfd: pidfd, owned: true})
			previousCloseErr := g.closeMembersLocked(members)
			return nil, errors.Join(
				fmt.Errorf("%w: confirm stat process %d: %w", ErrProcessGroupUnavailable, pid, err),
				closeErr,
				previousCloseErr,
			)
		}
		if confirm.startTime != info.startTime ||
			confirm.pgid != g.pgid || confirm.session != g.session {
			closeErr := g.closeMemberLocked(linuxProcessMember{pid: pid, pidfd: pidfd, owned: true})
			if closeErr != nil {
				previousCloseErr := g.closeMembersLocked(members)
				return nil, errors.Join(
					fmt.Errorf("close identity-changed member pid %d pidfd: %w", pid, closeErr),
					previousCloseErr,
				)
			}
			continue
		}
		members = append(members, linuxProcessMember{pid: pid, pidfd: pidfd, owned: true})
	}
	return members, nil
}

func (g *linuxProcessGroup) hasOpenCapabilityLocked() bool {
	return g.leaderPIDFD >= 0 || len(g.pendingMembers) != 0
}

func (g *linuxProcessGroup) pendingMembersLocked() []linuxProcessMember {
	if len(g.pendingMembers) == 0 {
		return nil
	}
	members := make([]linuxProcessMember, 0, len(g.pendingMembers))
	for _, member := range g.pendingMembers {
		members = append(members, member)
	}
	return members
}

func (g *linuxProcessGroup) closeMembersLocked(members []linuxProcessMember) error {
	var joined error
	for _, member := range members {
		if err := g.closeMemberLocked(member); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func (g *linuxProcessGroup) closePendingMembersLocked() error {
	return g.closeMembersLocked(g.pendingMembersLocked())
}

func (g *linuxProcessGroup) closeMemberLocked(member linuxProcessMember) error {
	if !member.owned {
		return nil
	}
	err := linuxCloseFD(member.pidfd)
	if err == nil || linuxCloseReleasesFD(err) {
		delete(g.pendingMembers, member.pidfd)
		return wrapLinuxMemberCloseError(member.pid, err)
	}
	if g.pendingMembers == nil {
		g.pendingMembers = make(map[int]linuxProcessMember)
	}
	g.pendingMembers[member.pidfd] = member
	return wrapLinuxMemberCloseError(member.pid, err)
}

// Linux releases the descriptor early even when close reports EINTR. Retrying
// such a close can close an unrelated descriptor that reused the number. The
// test seam uses a non-EINTR fail-once error when it wants to model a safely
// retryable close failure.
func linuxCloseReleasesFD(err error) bool {
	return errors.Is(err, unix.EINTR) || errors.Is(err, unix.EBADF)
}

func closeLinuxMember(member linuxProcessMember) error {
	if !member.owned {
		return nil
	}
	return wrapLinuxMemberCloseError(member.pid, linuxCloseFD(member.pidfd))
}

func closeLinuxMembers(members []linuxProcessMember) error {
	var joined error
	for _, member := range members {
		if err := closeLinuxMember(member); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func wrapLinuxMemberCloseError(pid int, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close member pid %d pidfd: %w", pid, err)
}

func readLinuxProcessInfo(pid int) (linuxProcessInfo, error) {
	raw, err := linuxReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		// A process can disappear between /proc enumeration and opening its
		// stat file. Linux may report that race as ESRCH rather than ENOENT;
		// both mean there is no member to signal, not an identity failure.
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			return linuxProcessInfo{}, os.ErrNotExist
		}
		return linuxProcessInfo{}, err
	}
	closeParen := bytes.LastIndexByte(raw, ')')
	if closeParen < 0 || closeParen+2 >= len(raw) {
		return linuxProcessInfo{}, errors.New("malformed /proc stat")
	}
	fields := strings.Fields(string(raw[closeParen+2:]))
	if len(fields) <= 19 {
		return linuxProcessInfo{}, errors.New("short /proc stat")
	}
	if len(fields[0]) != 1 {
		return linuxProcessInfo{}, errors.New("malformed /proc stat state")
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return linuxProcessInfo{}, err
	}
	session, err := strconv.Atoi(fields[3])
	if err != nil {
		return linuxProcessInfo{}, err
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return linuxProcessInfo{}, err
	}
	return linuxProcessInfo{
		pid:       pid,
		pgid:      pgid,
		session:   session,
		startTime: start,
		state:     fields[0][0],
	}, nil
}
