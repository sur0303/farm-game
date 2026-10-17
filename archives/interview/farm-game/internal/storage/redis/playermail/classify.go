package playermail

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
)

// sendFate answers the only question that matters after a transport
// failure: can the script have entered execution on Redis? The policy is
// identical to the R1 authority adapter (31 §6.7): only errors that
// provably happened before the command reached the Redis command processor
// count as not-sent; everything else is conservatively unknown.
type sendFate uint8

const (
	fateUnknown sendFate = iota // may have executed: TIMEOUT_UNKNOWN
	fateNotSent                 // provably never executed: UNAVAILABLE_NOT_SENT
)

func classifyWriteError(err error) sendFate {
	if err == nil {
		return fateUnknown
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return fateNotSent
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "NOSCRIPT"):
		return fateNotSent
	case strings.HasPrefix(msg, "MOVED") || strings.HasPrefix(msg, "ASK") ||
		strings.Contains(msg, "CLUSTERDOWN"):
		return fateNotSent
	case strings.Contains(msg, "redis: client is closed") ||
		strings.Contains(msg, "connection pool is closed"):
		return fateNotSent
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fateUnknown
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fateUnknown
	}
	return fateUnknown
}

// isContractViolation reports Redis replies that prove an adapter bug
// (cross-slot script invocation). These fail closed as Go errors, never as
// typed store statuses.
func isContractViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "CROSSSLOT")
}
