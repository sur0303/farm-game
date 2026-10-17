//go:build windows

package routecontrol

import (
	"errors"
	"testing"
)

func TestWindowsStrongFilesystemCapabilityFailsClosed(t *testing.T) {
	if _, err := openSecureRoot(t.TempDir()); !errors.Is(err, ErrCapability) {
		t.Fatalf("openSecureRoot error=%v, want ErrCapability", err)
	}
	if _, err := createRunDirectory(t.TempDir() + "/run"); !errors.Is(err, ErrCapability) {
		t.Fatalf("createRunDirectory error=%v, want ErrCapability", err)
	}
}
