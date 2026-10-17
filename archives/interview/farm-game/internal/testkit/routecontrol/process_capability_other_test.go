//go:build !linux

package routecontrol

import (
	"errors"
	"os"
	"testing"
)

func TestUnsupportedProcessGroupCapabilityFailsClosed(t *testing.T) {
	_, err := StartProcess(ProcessSpec{Command: os.Args[0]})
	if !errors.Is(err, ErrProcessGroupUnavailable) || !errors.Is(err, ErrCapability) {
		t.Fatalf("StartProcess error=%v, want process-group and generic capability errors", err)
	}
}
