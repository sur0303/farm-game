package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var (
	gitLookPath          = exec.LookPath
	gitCommandTimeout    = 5 * time.Second
	gitCommandExec       = executeGitCommand
	gitEnvironmentSource = os.Environ
)

// CurrentGitHead returns the verified commit object at worktree HEAD. The
// command is invoked directly with a bounded context, a trusted absolute
// executable path, and an environment without ambient Git repository/config
// overrides.
func CurrentGitHead(worktree string) (string, error) {
	if strings.TrimSpace(worktree) == "" {
		return "", errors.New("git worktree is required")
	}
	output, err := runGitCommand(worktree, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve git HEAD: %w", err)
	}
	head := strings.TrimSpace(string(output))
	if !isCommitToken(head) {
		return "", fmt.Errorf("%w: git HEAD is not a non-zero 7-64 hex commit", ErrInvalidEvidence)
	}
	return strings.ToLower(head), nil
}

// CurrentGitBranch returns the short branch name or "detached" for a detached
// HEAD. It uses fixed exec arguments and treats command output as data.
func CurrentGitBranch(worktree string) (string, error) {
	if strings.TrimSpace(worktree) == "" {
		return "", errors.New("git worktree is required")
	}
	output, err := runGitCommand(worktree, "symbolic-ref", "--short", "-q", "HEAD")
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return "detached", nil
		}
		if !errors.As(err, &exitErr) {
			return "", fmt.Errorf("resolve git branch: %w", err)
		}
		return "", fmt.Errorf("%w: resolve git branch: %v", ErrGitCommandFailed, err)
	}
	branch := strings.TrimSpace(string(output))
	if branch == "" {
		return "detached", nil
	}
	if !safeToken(branch) {
		return "", fmt.Errorf("%w: git branch is not a safe token", ErrInvalidEvidence)
	}
	return branch, nil
}

// RealProvenance binds a non-synthetic record to the current Git HEAD.
// BaseCommit is conservatively set to the same verified HEAD when no external
// base revision is supplied by the caller.
func RealProvenance(taskID, runID, worktree, collector string, level EvidenceLevel, source SourceKind, now time.Time) (Provenance, error) {
	head, err := CurrentGitHead(worktree)
	if err != nil {
		return Provenance{}, err
	}
	branch, err := CurrentGitBranch(worktree)
	if err != nil {
		return Provenance{}, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	return Provenance{
		TaskID:        taskID,
		RunID:         runID,
		BaseCommit:    head,
		HeadCommit:    head,
		Branch:        branch,
		Worktree:      worktree,
		Collector:     collector,
		SourceKind:    source,
		DeclaredLevel: level,
		RecordedAt:    now.UTC().Format(time.RFC3339Nano),
		Synthetic:     false,
	}, nil
}

// validateBoundGitHead binds real provenance to a repository when the
// declared worktree is available locally. Synthetic and fixture records may
// remain portable test data when no repository is present at their worktree.
func validateBoundGitHead(p Provenance) error {
	if p.Synthetic || p.SourceKind == SourceSelfCheck || p.SourceKind == SourceFixture {
		return nil
	}
	if _, err := os.Stat(p.Worktree); err != nil {
		return fmt.Errorf("%w: real provenance worktree is unavailable: %v", ErrInvalidEvidence, err)
	}
	head, err := CurrentGitHead(p.Worktree)
	if err != nil {
		return fmt.Errorf("%w: real provenance worktree is not a Git repository: %v", ErrInvalidEvidence, err)
	}
	if p.HeadCommit != head {
		return fmt.Errorf("%w: provenance head_commit %s does not match Git HEAD %s", ErrInvalidEvidence, p.HeadCommit, head)
	}
	if err := gitCommitExists(p.Worktree, p.BaseCommit); err != nil {
		return fmt.Errorf("%w: provenance base_commit is not a Git commit: %v", ErrInvalidEvidence, err)
	}
	return nil
}

func gitCommitExists(worktree, commit string) error {
	if !isCommitToken(commit) {
		return fmt.Errorf("invalid commit token %q", commit)
	}
	if _, err := runGitCommand(worktree, "cat-file", "-e", commit+"^{commit}"); err != nil {
		return err
	}
	return nil
}

func runGitCommand(worktree string, args ...string) ([]byte, error) {
	gitPath, err := resolveGitPath()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), gitCommandTimeout)
	defer cancel()
	commandArgs := make([]string, 0, len(args)+3)
	commandArgs = append(commandArgs, "--no-optional-locks", "-C", worktree)
	commandArgs = append(commandArgs, args...)
	output, err := gitCommandExec(ctx, gitPath, commandArgs, safeGitEnvironment(gitPath, gitEnvironmentSource()))
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%w: %v", ErrGitCommandTimeout, ctx.Err())
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrGitCommandFailed, err)
	}
	return output, nil
}

func resolveGitPath() (string, error) {
	path, err := gitLookPath("git")
	if err != nil {
		return "", fmt.Errorf("%w: locate git: %v", ErrGitCommandFailed, err)
	}
	absolute, err := filepath.Abs(path)
	if err != nil || !filepath.IsAbs(absolute) {
		return "", fmt.Errorf("%w: git path is not absolute: %q", ErrGitCommandFailed, path)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("%w: stat git executable: %v", ErrGitCommandFailed, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: git executable is not regular", ErrGitCommandFailed)
	}
	return absolute, nil
}

func safeGitEnvironment(gitPath string, source []string) []string {
	// Git repository, object, index, and config variables are deliberately
	// excluded. Keep only a narrow executable/runtime environment and point
	// global config at the platform null device.
	environment := []string{
		"PATH=" + filepath.Dir(gitPath),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_OPTIONAL_LOCKS=0",
	}
	for _, value := range source {
		key, _, ok := strings.Cut(value, "=")
		if !ok {
			continue
		}
		switch key {
		case "SystemRoot", "WINDIR", "ComSpec", "TMP", "TEMP":
			environment = append(environment, value)
		}
	}
	return environment
}

func executeGitCommand(ctx context.Context, path string, args []string, environment []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = environment
	return cmd.Output()
}
