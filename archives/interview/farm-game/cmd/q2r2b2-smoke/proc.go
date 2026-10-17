package main

// Process lifecycle, readiness/Redis observation and verdict collection.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

type verdicts struct {
	items []map[string]any
	path  string
}

func newVerdicts(path string) *verdicts {
	return &verdicts{path: path}
}

func (v *verdicts) add(name string, pass bool, detail string) {
	v.items = append(v.items, map[string]any{
		"check": name, "pass": pass, "detail": detail, "ts": time.Now().UnixMilli(),
	})
}

func (v *verdicts) flush(final bool) error {
	overall := "PASS"
	for _, it := range v.items {
		if p, _ := it["pass"].(bool); !p {
			overall = "FAIL"
			break
		}
	}
	f, err := os.Create(v.path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	_ = enc.Encode(map[string]any{"final": final, "overall": overall, "checks": v.items})
	return nil
}

type proc struct {
	cmd        *exec.Cmd
	logPath    string
	healthPort int
}

func startProc(binary string, env []string, healthPort int, logPath string) (*proc, error) {
	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary)
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	_ = f.Close()
	return &proc{cmd: cmd, logPath: logPath, healthPort: healthPort}, nil
}

func (p *proc) logText() string {
	raw, _ := os.ReadFile(p.logPath)
	return string(raw)
}

func (p *proc) exited() bool {
	return p.cmd.ProcessState != nil
}

func (p *proc) readyz(timeout time.Duration) (int, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", p.healthPort)
	resp, err := http.Get(url)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (p *proc) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.exited() {
			return fmt.Errorf("process exited before ready: %s", strings.TrimSpace(p.logText()))
		}
		code, err := p.readyz(time.Second)
		if err == nil && code == http.StatusOK {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("readyz never 200: %s", strings.TrimSpace(p.logText()))
}

func (p *proc) waitHealthz(timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", p.healthPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.exited() {
			return fmt.Errorf("process exited before healthz: %s", strings.TrimSpace(p.logText()))
		}
		resp, err := http.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("healthz never 200: %s", strings.TrimSpace(p.logText()))
}

// terminate sends SIGTERM and waits for a bounded graceful exit.
func (p *proc) terminate(timeout time.Duration) (int, error) {
	if p.exited() {
		return p.cmd.ProcessState.ExitCode(), nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return 0, err
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
		return p.cmd.ProcessState.ExitCode(), nil
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		return 0, fmt.Errorf("process did not exit within %s: %s", timeout, strings.TrimSpace(p.logText()))
	}
}

func killProc(p *proc) {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
}

// timelineWriter appends one JSON event per line to an artifact file.
type timelineWriter struct {
	f *os.File
}

func newTimelineWriter(path string) (*timelineWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &timelineWriter{f: f}, nil
}

func (w *timelineWriter) write(ev map[string]any) {
	line, _ := json.Marshal(ev)
	_, _ = w.f.Write(append(line, '\n'))
}

func (w *timelineWriter) close() { _ = w.f.Close() }

// observeLoop samples readiness, Redis TIME and the authoritative lease
// deadline on an interval until stop is closed.
func observeLoop(ctx context.Context, client *redis.Client, key rediskey.RedisKey, p *proc, tw *timelineWriter, interval time.Duration, stop chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		ev := map[string]any{"ts": time.Now().UnixMilli()}
		if code, err := p.readyz(time.Second); err == nil {
			ev["readyz"] = code
		} else {
			ev["readyz_err"] = err.Error()
		}
		ev["redis_time_ms"] = redisTimeMS(ctx, client)
		if d, err := leaseDeadline(client, key); err == nil {
			ev["lease_expire_at_ms"] = d
		}
		ev["exited"] = p.exited()
		tw.write(ev)
	}
}

// readLeaseWindow returns a sample of (redis_time, lease_deadline) for a
// deterministic N3/N4 fault window.
func readLeaseWindow(ctx context.Context, client *redis.Client, key rediskey.RedisKey) (int64, int64, error) {
	d, err := leaseDeadline(client, key)
	if err != nil {
		return 0, 0, err
	}
	return redisTimeMS(ctx, client), d, nil
}

func execCommand(out, pkg, root string) *exec.Cmd {
	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", out, pkg)
	cmd.Dir = root
	return cmd
}

var _ = context.Background
var _ = filepath.Join
