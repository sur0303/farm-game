package compendium

import "fmt"

// ErrorCode is the public/business error vocabulary for compendium commands.
// Codes are taken from the registered table of docs/design/28; do not invent
// new wire codes here.
type ErrorCode string

const (
	ErrNone ErrorCode = ""
	// ErrSeenAlreadyAdvanced: a repeated or older ACK_COMPENDIUM_SEEN did
	// not advance the seen watermark (docs/design/28 §6.1, NO_CHANGE).
	ErrSeenAlreadyAdvanced ErrorCode = "COMPENDIUM_SEEN_ALREADY_ADVANCED"
	// ErrWorkCapacityExceeded: a versioned CompendiumLimitProfile hard cap
	// would be exceeded. This is a temporary REJECTED with
	// RETRY_SAME_OPERATION semantics (docs/design/28 §6.1); the integrator
	// must keep the surrounding Harvest workflow in
	// PROCESSING/RECONCILE_REQUIRED instead of silently skipping the
	// permanent unlock (docs/design/37 §5.4).
	ErrWorkCapacityExceeded ErrorCode = "WORK_CAPACITY_EXCEEDED"
	// ErrInvalidArgument: malformed domain input (zero IDs, mismatched
	// snapshot identity). Never persisted.
	ErrInvalidArgument ErrorCode = "INVALID_ARGUMENT"
)

// Error is the deterministic rejection of a compendium domain decision.
type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func reject(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
