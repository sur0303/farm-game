//go:build !linux && !windows

package routecontrol

import (
	"errors"
	"testing"
)

func TestUnsupportedFilesystemCapabilityFailsClosed(t *testing.T) {
	if _, err := openSecureRoot(t.TempDir()); !errors.Is(err, ErrDurableEvidenceUnavailable) {
		t.Fatalf("openSecureRoot error=%v, want ErrDurableEvidenceUnavailable", err)
	}
	if _, err := createRunDirectory(t.TempDir() + "/run"); !errors.Is(err, ErrDurableEvidenceUnavailable) {
		t.Fatalf("createRunDirectory error=%v, want ErrDurableEvidenceUnavailable", err)
	}
}
