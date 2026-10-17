package pet

// ActionStatus is the deterministic terminal status of a pet domain
// decision, mirroring docs/design/36 §6.6. Recoverable execution states
// (capacity, timeout, redirect) never appear here; they are handled by the
// Actor/Authority layer before or around the domain call.
type ActionStatus uint8

const (
	StatusSucceeded ActionStatus = iota + 1
	StatusNoChange
	StatusRejected
)

func (s ActionStatus) String() string {
	switch s {
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusNoChange:
		return "NO_CHANGE"
	case StatusRejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}

// ErrorCode is the registered public/business error vocabulary for pet
// commands. The values are frozen by docs/design/28 §error table and
// docs/design/36 §8.2; do not invent new codes here.
type ErrorCode string

const (
	ErrNone                    ErrorCode = ""
	ErrPetAlreadyUnlocked      ErrorCode = "PET_ALREADY_UNLOCKED"       // NO_CHANGE
	ErrPetAlreadyActive        ErrorCode = "PET_ALREADY_ACTIVE"         // NO_CHANGE
	ErrPetNotFound             ErrorCode = "PET_NOT_FOUND"              // REJECTED
	ErrPetNotUnlocked          ErrorCode = "PET_NOT_UNLOCKED"           // REJECTED
	ErrPetNotAvailable         ErrorCode = "PET_NOT_AVAILABLE"          // REJECTED
	ErrPetFeedLimitExceeded    ErrorCode = "PET_FEED_LIMIT_EXCEEDED"    // REJECTED
	ErrPetPriceChanged         ErrorCode = "PET_PRICE_CHANGED"          // REJECTED
	ErrPetConfigVersionChanged ErrorCode = "PET_CONFIG_VERSION_CHANGED" // REJECTED
	ErrPetFeedRuleChanged      ErrorCode = "PET_FEED_RULE_CHANGED"      // REJECTED
	ErrPetGuardRuleChanged     ErrorCode = "PET_GUARD_RULE_CHANGED"     // REJECTED
	ErrInsufficientCoin        ErrorCode = "INSUFFICIENT_COIN"          // REJECTED
	ErrInsufficientDogFood     ErrorCode = "INSUFFICIENT_DOG_FOOD"      // REJECTED
)
