//go:build unix

package selfprobe

import (
	"syscall"
	"time"
)

// processCPUTime is kernel-accounted rather than runtime-accounted, so time
// this process spent inside network syscalls is included. For a driver that
// is mostly socket work, leaving that out would understate its cost.
func processCPUTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano()) + time.Duration(usage.Stime.Nano())
}
