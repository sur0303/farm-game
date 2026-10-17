//go:build linux

package routecontrol

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestComputeArtifactRejectsFIFOWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "artifact.fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ComputeArtifact(root, "artifact.fifo", "fifo")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrArtifactMismatch) {
			t.Fatalf("FIFO error=%v, want ErrArtifactMismatch", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("FIFO hashing blocked")
	}
	_ = os.Remove(fifo)
}

func TestLinuxSecureResolutionRejectsCrossDeviceTraversal(t *testing.T) {
	if linuxSecureResolve&unix.RESOLVE_NO_XDEV == 0 {
		t.Fatal("Linux secure resolution omitted RESOLVE_NO_XDEV")
	}
}

func TestLinuxRollbackNeverRemovesRenamedAndRecreatedRunDirectory(t *testing.T) {
	root := t.TempDir()
	runPath := filepath.Join(root, "run")
	capability, err := createRunDirectory(runPath)
	if err != nil {
		t.Fatal(err)
	}
	runDir, ok := capability.(*linuxSecureRunDir)
	if !ok {
		t.Fatalf("run directory type=%T, want *linuxSecureRunDir", capability)
	}
	file, err := runDir.createFile("partial.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	originalPath := filepath.Join(root, "run-original")
	if err := os.Rename(runPath, originalPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(runPath, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(runPath, "replacement")
	if err := os.WriteFile(marker, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runDir.cleanup([]string{"partial.ndjson"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("rollback removed replacement run directory: %v", err)
	}
	entries, err := os.ReadDir(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("original abandoned run retained files: %v", entries)
	}
}
