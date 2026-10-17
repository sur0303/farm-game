package authority

import (
	"context"
	"errors"
	"net"
	"strings"
)

// sendFate answers the only question that matters after a transport failure:
// can the script have entered execution on Redis?
type sendFate uint8

const (
	fateUnknown sendFate = iota // may have executed: TIMEOUT_UNKNOWN
	fateNotSent                 // provably never executed: UNAVAILABLE_NOT_SENT
)

// classifyWriteError maps a failed script call to a send fate, following the
// same conservative policy as blobstore: only errors that provably happened
// before the command reached the Redis command processor count as not-sent.
func classifyWriteError(err error) sendFate {
	if err == nil {
		return fateUnknown
	}
	var operationErr *net.OpError
	if errors.As(err, &operationErr) && strings.EqualFold(operationErr.Op, "dial") {
		// A dial-phase failure has no established connection on which the Redis
		// command could have been written. DNS, network-unreachable, and
		// host-unreachable/refused dial errors are therefore definitive
		// not-sent fates. The same errno during write/read is ambiguous.
		return fateNotSent
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "NOSCRIPT"):
		// NOSCRIPT is a definitive pre-execution reply.
		return fateNotSent
	case strings.HasPrefix(msg, "MOVED") || strings.HasPrefix(msg, "ASK") ||
		strings.Contains(msg, "CLUSTERDOWN"):
		// Routing-level rejections happen before script execution.
		return fateNotSent
	case strings.Contains(msg, "redis: client is closed") ||
		strings.Contains(msg, "connection pool is closed"):
		// Failed before checkout of a connection.
		return fateNotSent
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fateUnknown
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fateUnknown
	}
	// Any other transport failure on a write is conservatively unknown.
	return fateUnknown
}

// isContractViolation reports Redis replies that prove an adapter bug
// (cross-slot script invocation). These fail closed as Go errors, never as
// typed store statuses.
func isContractViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "CROSSSLOT")
}
