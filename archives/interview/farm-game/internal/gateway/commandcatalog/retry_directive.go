package commandcatalog

import (
	commonpb "farmgame/api/gen/common"
)

// RetryDirective tells the client (once Batch 3 wires the envelope field) how
// to recover from a command outcome. Mutually exclusive alternatives — never
// a bitset. Wire format has no field yet; DecideRetryDirective is Go-side only
// until Batch 3 / contract 32 hang it on CommandResultEnvelope.
//
// Full ErrorCode domain tables wait for contract 32. Mappings below cover codes
// whose recovery semantics are already clear from enums.proto comments; unknown
// codes fall through to a conservative default (DO_NOT_RETRY).
type RetryDirective uint8

const (
	RetryDirectiveUnspecified RetryDirective = iota
	RetryDirectiveNone
	RetryDirectiveFreshJoin             // subscription / farm view stale → re-Join
	RetryDirectiveReconnect             // transport / downlink broken → reconnect
	RetryDirectiveReauthenticate        // session / auth invalid → re-login
	RetryDirectivePollOperationResult   // result unknown → query by operation_id
	RetryDirectiveRetrySameOperation    // safe bounded retry with same operation_id
	RetryDirectiveRefreshAuthorityState // stale list/period: pull current authority state
	RetryDirectiveDoNotRetry            // business / protocol reject — do not auto-retry
)

func (d RetryDirective) String() string {
	switch d {
	case RetryDirectiveNone:
		return "NONE"
	case RetryDirectiveFreshJoin:
		return "FRESH_JOIN"
	case RetryDirectiveReconnect:
		return "RECONNECT"
	case RetryDirectiveReauthenticate:
		return "REAUTHENTICATE"
	case RetryDirectivePollOperationResult:
		return "POLL_OPERATION_RESULT"
	case RetryDirectiveRetrySameOperation:
		return "RETRY_SAME_OPERATION"
	case RetryDirectiveRefreshAuthorityState:
		return "REFRESH_AUTHORITY_STATE"
	case RetryDirectiveDoNotRetry:
		return "DO_NOT_RETRY"
	default:
		return "UNSPECIFIED"
	}
}

// RetryDecisionInput is the Gate/result-path view used to pick one directive.
// Optional fields (CommandType, HasOperationID) are reserved for contract 32
// refinements; DecideRetryDirective ignores them today.
type RetryDecisionInput struct {
	Status    commonpb.CommandStatus
	ErrorCode commonpb.ErrorCode
	// CommandType   commonpb.CommandType // reserved TODO(32)
	// HasOperationID bool                // reserved TODO(32)
}

// DecideRetryDirective maps status/error to exactly one RetryDirective.
//
// Success statuses with no error → NONE.
// Known ErrorCodes → the locked table in retry_directive_test.go.
// Anything else → DO_NOT_RETRY (never silent RETRY / POLL).
func DecideRetryDirective(in RetryDecisionInput) RetryDirective {
	if isSuccessNoError(in) {
		return RetryDirectiveNone
	}
	switch in.ErrorCode {
	// --- result unknown: must poll, never blind same-op retry ---
	case commonpb.ErrorCode_ERR_TIMEOUT:
		return RetryDirectivePollOperationResult

	// --- transient infra: bounded same operation_id retry ---
	case commonpb.ErrorCode_ERR_SERVER_BUSY,
		commonpb.ErrorCode_ERR_STORE_CAS_CONFLICT,
		commonpb.ErrorCode_ERR_ACTOR_REDIRECT:
		return RetryDirectiveRetrySameOperation

	// ERR_STORE_UNAVAILABLE: storage blip, transport still fine. Prefer
	// RETRY_SAME_OPERATION over RECONNECT so the client keeps the socket and
	// retries the same operation_id after backoff. Reconnect would drop
	// session state without fixing Redis/Tcaplus.
	case commonpb.ErrorCode_ERR_STORE_UNAVAILABLE:
		return RetryDirectiveRetrySameOperation

	// --- transport / downlink ---
	case commonpb.ErrorCode_ERR_DOWNLINK_OVERFLOW:
		return RetryDirectiveReconnect

	// --- session / auth ---
	case commonpb.ErrorCode_ERR_UNAUTHENTICATED,
		commonpb.ErrorCode_ERR_SESSION_EXPIRED,
		commonpb.ErrorCode_ERR_LOGIN_ELSEWHERE,
		commonpb.ErrorCode_ERR_CONNECTION_TICKET_INVALID,
		commonpb.ErrorCode_ERR_SESSION_PERMIT_INVALID:
		return RetryDirectiveReauthenticate

	// --- presence / view: must Fresh Join ---
	case commonpb.ErrorCode_ERR_NOT_IN_FARM:
		return RetryDirectiveFreshJoin

	// ERR_FARM_FULL: capacity won't clear by auto-retry of the same Join;
	// client should surface the limit (or wait and send a new intent later).
	case commonpb.ErrorCode_ERR_FARM_FULL:
		return RetryDirectiveDoNotRetry

	// --- version / idempotency: never POLL or RETRY_SAME ---
	case commonpb.ErrorCode_ERR_VERSION_CONFLICT,
		commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT:
		return RetryDirectiveDoNotRetry

	case commonpb.ErrorCode_ERR_TASK_INSTANCE_EXPIRED:
		return RetryDirectiveRefreshAuthorityState

	// --- protocol / frame rejects ---
	case commonpb.ErrorCode_ERR_UNKNOWN_COMMAND,
		commonpb.ErrorCode_ERR_DECODE,
		commonpb.ErrorCode_ERR_BAD_FRAME,
		commonpb.ErrorCode_ERR_MESSAGE_TOO_LARGE:
		return RetryDirectiveDoNotRetry

	// --- business rejects (economy / plot / crop / friend / steal / permission) ---
	case commonpb.ErrorCode_ERR_INSUFFICIENT_COIN,
		commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM,
		commonpb.ErrorCode_ERR_OFFER_NOT_FOUND,
		commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH,
		commonpb.ErrorCode_ERR_TASK_NOT_CLAIMABLE,
		commonpb.ErrorCode_ERR_TASK_ALREADY_CLAIMED,
		commonpb.ErrorCode_ERR_TASK_INSTANCE_NOT_FOUND,
		commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY,
		commonpb.ErrorCode_ERR_PLOT_EMPTY,
		commonpb.ErrorCode_ERR_CROP_NOT_MATURE,
		commonpb.ErrorCode_ERR_PLOT_NOT_FOUND,
		commonpb.ErrorCode_ERR_CROP_ALREADY_MATURE,
		commonpb.ErrorCode_ERR_NOT_FRIEND,
		commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID,
		commonpb.ErrorCode_ERR_FRIEND_LIMIT,
		commonpb.ErrorCode_ERR_SHARE_LINK_INVALID,
		commonpb.ErrorCode_ERR_PERMISSION_DENIED,
		commonpb.ErrorCode_ERR_STEAL_ALREADY_DONE,
		commonpb.ErrorCode_ERR_STEAL_QUOTA_EXHAUSTED,
		commonpb.ErrorCode_ERR_STEAL_BLOCKED_BY_PET,
		commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND:
		return RetryDirectiveDoNotRetry

	// --- conservative default for unlisted / unknown codes ---
	// (ERR_INTERNAL, ERR_RATE_LIMITED, ERR_UNSUPPORTED_VERSION, OK-with-reject, …)
	default:
		return RetryDirectiveDoNotRetry
	}
}

func isSuccessNoError(in RetryDecisionInput) bool {
	switch in.Status {
	case commonpb.CommandStatus_SUCCEEDED,
		commonpb.CommandStatus_ACCEPTED,
		commonpb.CommandStatus_NO_CHANGE:
		// treat OK and UNSPECIFIED as "no error"
		return in.ErrorCode == commonpb.ErrorCode_OK ||
			in.ErrorCode == commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED
	default:
		return false
	}
}
