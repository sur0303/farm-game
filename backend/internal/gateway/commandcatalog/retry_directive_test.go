package commandcatalog

import (
	"testing"

	commonpb "farmgame/api/gen/common"
)

func TestDecideRetryDirective_ScenarioTable(t *testing.T) {
	cases := []struct {
		name string
		in   RetryDecisionInput
		want RetryDirective
	}{
		{
			name: "ERR_TIMEOUT→POLL_OPERATION_RESULT",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT},
			want: RetryDirectivePollOperationResult,
		},
		{
			name: "ERR_SERVER_BUSY→RETRY_SAME_OPERATION",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_SERVER_BUSY},
			want: RetryDirectiveRetrySameOperation,
		},
		{
			name: "ERR_STORE_CAS_CONFLICT→RETRY_SAME_OPERATION",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_STORE_CAS_CONFLICT},
			want: RetryDirectiveRetrySameOperation,
		},
		{
			name: "ERR_ACTOR_REDIRECT→RETRY_SAME_OPERATION",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_ACTOR_REDIRECT},
			want: RetryDirectiveRetrySameOperation,
		},
		{
			name: "ERR_STORE_UNAVAILABLE→RETRY_SAME_OPERATION",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_STORE_UNAVAILABLE},
			want: RetryDirectiveRetrySameOperation,
		},
		{
			name: "ERR_DOWNLINK_OVERFLOW→RECONNECT",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_DOWNLINK_OVERFLOW},
			want: RetryDirectiveReconnect,
		},
		{
			name: "ERR_UNAUTHENTICATED→REAUTHENTICATE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_UNAUTHENTICATED},
			want: RetryDirectiveReauthenticate,
		},
		{
			name: "ERR_SESSION_EXPIRED→REAUTHENTICATE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_SESSION_EXPIRED},
			want: RetryDirectiveReauthenticate,
		},
		{
			name: "ERR_LOGIN_ELSEWHERE→REAUTHENTICATE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_LOGIN_ELSEWHERE},
			want: RetryDirectiveReauthenticate,
		},
		{
			name: "ERR_CONNECTION_TICKET_INVALID→REAUTHENTICATE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_CONNECTION_TICKET_INVALID},
			want: RetryDirectiveReauthenticate,
		},
		{
			name: "ERR_SESSION_PERMIT_INVALID→REAUTHENTICATE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_SESSION_PERMIT_INVALID},
			want: RetryDirectiveReauthenticate,
		},
		{
			name: "ERR_NOT_IN_FARM→FRESH_JOIN",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_NOT_IN_FARM},
			want: RetryDirectiveFreshJoin,
		},
		{
			name: "ERR_FARM_FULL→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_FARM_FULL},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_VERSION_CONFLICT→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_VERSION_CONFLICT},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_IDEMPOTENCY_CONFLICT→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_INSUFFICIENT_COIN→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_INSUFFICIENT_COIN},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_INSUFFICIENT_ITEM→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_PLOT_NOT_EMPTY→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_PLOT_EMPTY→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_PLOT_EMPTY},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_CROP_NOT_MATURE→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_CROP_NOT_MATURE},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_NOT_FRIEND→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_NOT_FRIEND},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_PERMISSION_DENIED→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_PERMISSION_DENIED},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_STEAL_ALREADY_DONE→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_STEAL_ALREADY_DONE},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_STEAL_QUOTA_EXHAUSTED→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_STEAL_QUOTA_EXHAUSTED},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_STEAL_BLOCKED_BY_PET→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_STEAL_BLOCKED_BY_PET},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_UNKNOWN_COMMAND→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_UNKNOWN_COMMAND},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_DECODE→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_DECODE},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_BAD_FRAME→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_BAD_FRAME},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_MESSAGE_TOO_LARGE→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_MESSAGE_TOO_LARGE},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "SUCCEEDED+OK→NONE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK},
			want: RetryDirectiveNone,
		},
		{
			name: "ACCEPTED+OK→NONE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_ACCEPTED, ErrorCode: commonpb.ErrorCode_OK},
			want: RetryDirectiveNone,
		},
		{
			name: "NO_CHANGE+OK→NONE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_NO_CHANGE, ErrorCode: commonpb.ErrorCode_OK},
			want: RetryDirectiveNone,
		},
		{
			name: "SUCCEEDED+UNSPECIFIED_error→NONE",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED},
			want: RetryDirectiveNone,
		},
		{
			name: "unknown_ErrorCode→DO_NOT_RETRY",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode(99999)},
			want: RetryDirectiveDoNotRetry,
		},
		{
			name: "ERR_INTERNAL→DO_NOT_RETRY_default",
			in:   RetryDecisionInput{Status: commonpb.CommandStatus_UNAVAILABLE, ErrorCode: commonpb.ErrorCode_ERR_INTERNAL},
			want: RetryDirectiveDoNotRetry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecideRetryDirective(tc.in)
			if got != tc.want {
				t.Fatalf("DecideRetryDirective(%+v)=%s, want %s", tc.in, got, tc.want)
			}
			// Single mutually exclusive directive (not a bitset / zero-as-unspecified for known rows).
			if got == RetryDirectiveUnspecified {
				t.Fatalf("known scenario must not return UNSPECIFIED: %+v", tc.in)
			}
		})
	}
}

// TestDecideRetryDirective_TimeoutMustNotBlindRetry locks the correctness core:
// unknown-result TIMEOUT must never become RETRY_SAME_OPERATION.
func TestDecideRetryDirective_TimeoutMustNotBlindRetry(t *testing.T) {
	got := DecideRetryDirective(RetryDecisionInput{
		Status:    commonpb.CommandStatus_UNAVAILABLE,
		ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT,
	})
	if got == RetryDirectiveRetrySameOperation {
		t.Fatal("ERR_TIMEOUT must not return RETRY_SAME_OPERATION (blind retry of unknown result)")
	}
	if got != RetryDirectivePollOperationResult {
		t.Fatalf("ERR_TIMEOUT got %s, want POLL_OPERATION_RESULT", got)
	}
}

// TestDecideRetryDirective_ConflictMustNotPollOrRetrySame: version/idempotency
// conflicts require a new client intent after refresh, not POLL / RETRY_SAME.
func TestDecideRetryDirective_ConflictMustNotPollOrRetrySame(t *testing.T) {
	for _, code := range []commonpb.ErrorCode{
		commonpb.ErrorCode_ERR_VERSION_CONFLICT,
		commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT,
	} {
		got := DecideRetryDirective(RetryDecisionInput{
			Status:    commonpb.CommandStatus_REJECTED,
			ErrorCode: code,
		})
		if got == RetryDirectivePollOperationResult || got == RetryDirectiveRetrySameOperation {
			t.Fatalf("%s must not return POLL or RETRY_SAME, got %s", code, got)
		}
		if got != RetryDirectiveDoNotRetry {
			t.Fatalf("%s got %s, want DO_NOT_RETRY", code, got)
		}
	}
}

// TestDecideRetryDirective_LoginElsewhereNotFreshJoin: being kicked by another
// login is a session problem, not a missing farm subscription.
func TestDecideRetryDirective_LoginElsewhereNotFreshJoin(t *testing.T) {
	got := DecideRetryDirective(RetryDecisionInput{
		Status:    commonpb.CommandStatus_REJECTED,
		ErrorCode: commonpb.ErrorCode_ERR_LOGIN_ELSEWHERE,
	})
	if got == RetryDirectiveFreshJoin {
		t.Fatal("ERR_LOGIN_ELSEWHERE must not return FRESH_JOIN")
	}
	if got != RetryDirectiveReauthenticate {
		t.Fatalf("ERR_LOGIN_ELSEWHERE got %s, want REAUTHENTICATE", got)
	}
}

func TestDecideRetryDirective_SuccessPathsNone(t *testing.T) {
	for _, status := range []commonpb.CommandStatus{
		commonpb.CommandStatus_SUCCEEDED,
		commonpb.CommandStatus_ACCEPTED,
		commonpb.CommandStatus_NO_CHANGE,
	} {
		got := DecideRetryDirective(RetryDecisionInput{
			Status:    status,
			ErrorCode: commonpb.ErrorCode_OK,
		})
		if got != RetryDirectiveNone {
			t.Fatalf("status=%s got %s, want NONE", status, got)
		}
	}
}

func TestRetryDirective_String(t *testing.T) {
	cases := map[RetryDirective]string{
		RetryDirectiveUnspecified:         "UNSPECIFIED",
		RetryDirectiveNone:                "NONE",
		RetryDirectiveFreshJoin:           "FRESH_JOIN",
		RetryDirectiveReconnect:           "RECONNECT",
		RetryDirectiveReauthenticate:      "REAUTHENTICATE",
		RetryDirectivePollOperationResult: "POLL_OPERATION_RESULT",
		RetryDirectiveRetrySameOperation:  "RETRY_SAME_OPERATION",
		RetryDirectiveDoNotRetry:          "DO_NOT_RETRY",
		RetryDirective(255):               "UNSPECIFIED",
	}
	for d, want := range cases {
		if got := d.String(); got != want {
			t.Fatalf("%d.String()=%q, want %q", d, got, want)
		}
	}
}
