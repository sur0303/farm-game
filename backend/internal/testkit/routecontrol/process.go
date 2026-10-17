package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// ProcessSpec describes a generic test process. It has no knowledge of
// production binaries or route-control semantics.
type ProcessSpec struct {
	Command string
	Args    []string
	Dir     string
	Env     []string
	Stdout  io.Writer
	Stderr  io.Writer
}

// ProcessResult is the observed process termination record.
type ProcessResult struct {
	PID        int
	ExitCode   int
	StartedAt  time.Time
	FinishedAt time.Time
	Err        error
}

// Process is a bounded-shutdown wrapper around os/exec.
type Process struct {
	cmd               *exec.Cmd
	group             processGroupCapability
	done              chan struct{}
	mu                sync.Mutex
	groupCloseMu      sync.Mutex
	finalizeMu        sync.Mutex
	signalOnce        sync.Once
	signalErr         error
	finalizeOnce      sync.Once
	finalErr          error
	cleanupErr        error
	cleanupAttempted  bool
	groupCloseErr     error
	groupCloseRetry   bool
	finalizationRetry bool
	result            ProcessResult
	signaled          bool
	groupClosed       bool
}

var (
	// waitAfterStartFailure is a seam for proving the bounded cleanup contract.
	// Production uses exec.Cmd.Wait directly.
	waitAfterStartFailure          = func(cmd *exec.Cmd) error { return cmd.Wait() }
	processStartFailureWaitTimeout = time.Second
)

// supervisedReaperRegistry owns wait goroutines that outlive the bounded
// start-failure cleanup window. Entries remain visible until the real Wait
// call returns, so callers and tests can prove that no child was abandoned.
type supervisedReaperRegistry struct {
	mu        sync.Mutex
	nextID    uint64
	entries   map[uint64]struct{}
	changed   chan struct{}
	completed []error
}

var processStartFailureReapers = newSupervisedReaperRegistry()

func newSupervisedReaperRegistry() *supervisedReaperRegistry {
	return &supervisedReaperRegistry{
		entries: make(map[uint64]struct{}),
		changed: make(chan struct{}),
	}
}

func (r *supervisedReaperRegistry) register(done <-chan error) {
	if r == nil || done == nil {
		return
	}
	r.mu.Lock()
	r.nextID++
	id := r.nextID
	r.entries[id] = struct{}{}
	r.signalChangeLocked()
	r.mu.Unlock()

	go func() {
		err, ok := <-done
		if !ok {
			err = nil
		}
		r.finish(id, err)
	}()
}

func (r *supervisedReaperRegistry) finish(id uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[id]; !ok {
		return
	}
	delete(r.entries, id)
	if err != nil {
		r.completed = append(r.completed, err)
	}
	r.signalChangeLocked()
}

func (r *supervisedReaperRegistry) signalChangeLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *supervisedReaperRegistry) count() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

func (r *supervisedReaperRegistry) drain(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("supervised reaper drain context is nil")
	}
	for {
		r.mu.Lock()
		if len(r.entries) == 0 {
			err := errors.Join(r.completed...)
			r.completed = nil
			r.mu.Unlock()
			return err
		}
		changed := r.changed
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// SupervisedReaperCount reports start-failure wait goroutines that are still
// owned by the package.
func SupervisedReaperCount() int {
	return processStartFailureReapers.count()
}

// DrainSupervisedReapers waits until all supervised start-failure waits have
// completed and returns any wait errors observed by the registry.
func DrainSupervisedReapers(ctx context.Context) error {
	return processStartFailureReapers.drain(ctx)
}

// StartProcess starts a generic test process and begins collecting its result.
func StartProcess(spec ProcessSpec) (*Process, error) {
	if spec.Command == "" {
		return nil, errors.New("process command is required")
	}
	if err := validateProcessWriter(spec.Stdout); err != nil {
		return nil, err
	}
	if err := validateProcessWriter(spec.Stderr); err != nil {
		return nil, err
	}
	groupStarter, err := newProcessGroupStarter()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(spec.Command, spec.Args...)
	cmd.Dir = spec.Dir
	if spec.Env != nil {
		cmd.Env = append(os.Environ(), spec.Env...)
	}
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	groupStarter.configure(cmd)
	if err := cmd.Start(); err != nil {
		cleanupErr := groupStarter.cleanupAfterStartFailure(cmd)
		return nil, errors.Join(fmt.Errorf("start process: %w", err), cleanupErr)
	}
	group, err := groupStarter.afterStart(cmd)
	if err != nil {
		cleanupErr := groupStarter.cleanupAfterStartFailure(cmd)
		if group != nil {
			cleanupErr = errors.Join(cleanupErr, cleanupProcessGroupAfterStartFailure(group))
		}
		waitErr := waitAfterStartFailureBounded(cmd)
		return nil, errors.Join(fmt.Errorf("after start: %w", err), cleanupErr, waitErr)
	}
	process := &Process{
		cmd:   cmd,
		group: group,
		done:  make(chan struct{}),
		result: ProcessResult{
			PID:       cmd.Process.Pid,
			StartedAt: time.Now().UTC(),
		},
	}
	go process.wait()
	return process, nil
}

// PID returns the child PID.
func (p *Process) PID() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.result.PID
}

// Wait waits for process termination without extending the caller's context.
func (p *Process) Wait(ctx context.Context) (ProcessResult, error) {
	if p == nil {
		return ProcessResult{}, errors.New("process is nil")
	}
	if ctx == nil {
		return ProcessResult{}, errors.New("wait context is nil")
	}
	select {
	case <-p.done:
		return p.completed()
	case <-ctx.Done():
		return ProcessResult{}, ctx.Err()
	}
}

// Shutdown asks the process to stop, then kills it if the caller deadline
// expires. The post-kill wait is itself bounded so a stuck child cannot hold a
// test forever.
func (p *Process) Shutdown(ctx context.Context) (ProcessResult, error) {
	if p == nil {
		return ProcessResult{}, errors.New("process is nil")
	}
	if ctx == nil {
		return ProcessResult{}, errors.New("shutdown context is nil")
	}

	select {
	case <-p.done:
		return p.completed()
	default:
	}

	interruptErr := p.requestInterrupt()
	var terminateErr error
	if interruptErr != nil && !errors.Is(interruptErr, os.ErrProcessDone) {
		// A failed first interrupt must not close the capability. Retry with a
		// hard group signal so the background finalizer can still observe and
		// clean every original member. Finalization owns the eventual close.
		terminateErr = p.forceKill()
	}

	select {
	case <-p.done:
		result, err := p.completed()
		return result, errors.Join(terminateErr, interruptFailure(interruptErr), err)
	case <-ctx.Done():
		if err := p.forceKill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			terminateErr = errors.Join(terminateErr, fmt.Errorf("kill process group: %w", err))
		}
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
			result, err := p.completed()
			return result, errors.Join(terminateErr, interruptFailure(interruptErr), err)
		case <-timer.C:
			return ProcessResult{}, errors.Join(
				terminateErr,
				interruptFailure(interruptErr),
				fmt.Errorf("%w: %v", ErrProcessShutdownTimed, ctx.Err()),
			)
		}
	}
}

func (p *Process) wait() {
	err := p.cmd.Wait()
	p.mu.Lock()
	p.result.Err = err
	p.result.FinishedAt = time.Now().UTC()
	if p.cmd.ProcessState != nil {
		p.result.ExitCode = p.cmd.ProcessState.ExitCode()
	}
	p.mu.Unlock()
	p.finalize()
}

func (p *Process) requestInterrupt() error {
	p.signalOnce.Do(func() {
		err := p.group.interrupt()
		p.mu.Lock()
		p.signaled = true
		p.signalErr = err
		p.mu.Unlock()
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.signalErr
}

func (p *Process) forceKill() error {
	return p.group.kill()
}

func (p *Process) snapshot() ProcessResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.result
}

func (p *Process) completed() (ProcessResult, error) {
	result := p.snapshot()
	p.retryFinalization()
	return result, p.finalizationError()
}

func (p *Process) finalizationError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.finalErr
}

func interruptFailure(err error) error {
	if err == nil || errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return fmt.Errorf("signal process group: %w", err)
}

func validateProcessWriter(writer io.Writer) error {
	if writer == nil {
		return nil
	}
	file, ok := writer.(*os.File)
	if !ok || file == nil {
		return ErrInvalidProcessWriter
	}
	return nil
}

func (p *Process) finalize() {
	p.finalizeOnce.Do(func() {
		p.finalizeMu.Lock()
		p.finalizationPassLocked()
		p.finalizeMu.Unlock()
		close(p.done)
	})
}

func (p *Process) retryFinalization() {
	p.finalizeMu.Lock()
	defer p.finalizeMu.Unlock()

	p.mu.Lock()
	retry := p.finalizationRetry
	p.mu.Unlock()
	if retry {
		p.finalizationPassLocked()
	}
}

func (p *Process) finalizationPassLocked() {
	p.mu.Lock()
	runCleanup := !p.cleanupAttempted
	p.cleanupAttempted = true
	p.mu.Unlock()

	if runCleanup {
		if cleanupErr := p.group.cleanup(context.Background()); cleanupErr != nil {
			p.mu.Lock()
			p.cleanupErr = fmt.Errorf("cleanup process group: %w", cleanupErr)
			p.mu.Unlock()
		}
	}

	_ = p.closeGroup()
	p.mu.Lock()
	var closeErr error
	if p.groupCloseErr != nil {
		closeErr = fmt.Errorf("close process-group capability: %w", p.groupCloseErr)
	}
	p.finalErr = errors.Join(p.cleanupErr, closeErr)
	p.finalizationRetry = p.groupCloseRetry
	p.mu.Unlock()
}

func cleanupProcessGroupAfterStartFailure(group processGroupCapability) error {
	if group == nil {
		return nil
	}
	cleanupErr := group.cleanup(context.Background())
	closeErr := group.close()
	if cleanupErr != nil {
		cleanupErr = fmt.Errorf("cleanup process group after start failure: %w", cleanupErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close process-group capability after start failure: %w", closeErr)
	}
	return errors.Join(cleanupErr, closeErr)
}

func waitAfterStartFailureBounded(cmd *exec.Cmd) error {
	if cmd == nil {
		return errors.New("wait after start failure: command is nil")
	}
	done := make(chan error, 1)
	go func() {
		done <- waitAfterStartFailure(cmd)
	}()
	timer := time.NewTimer(processStartFailureWaitTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err == nil {
			return nil
		}
		return fmt.Errorf("wait after start failure: %w", err)
	case <-timer.C:
		processStartFailureReapers.register(done)
		return fmt.Errorf("%w: wait after start failure exceeded %s",
			ErrProcessShutdownTimed, processStartFailureWaitTimeout)
	}
}

func killStartedProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill started process: %w", err)
	}
	return nil
}

func (p *Process) closeGroup() error {
	p.groupCloseMu.Lock()
	defer p.groupCloseMu.Unlock()

	p.mu.Lock()
	if p.groupClosed {
		err := p.groupCloseErr
		p.mu.Unlock()
		return err
	}
	group := p.group
	p.mu.Unlock()

	err := group.close()
	ownsCapability := group.ownsCapability()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.groupCloseErr = err
		p.groupCloseRetry = ownsCapability
		p.groupClosed = !ownsCapability
		return err
	}
	p.groupCloseErr = nil
	p.groupCloseRetry = false
	p.groupClosed = true
	return nil
}
