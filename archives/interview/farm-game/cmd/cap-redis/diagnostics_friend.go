package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"farmgame/internal/friend"

	"github.com/redis/go-redis/v9"
)

const maxFailureDiagnosticsPerCell = 1024

func diagnosticHash(domain, value string) string {
	sum := sha256.Sum256([]byte(domain + "\x00" + value))
	return hex.EncodeToString(sum[:])
}

func diagnosticErrorClass(err error) string {
	switch {
	case errors.Is(err, friend.ErrFriendReconcile):
		return "ERR_FRIEND_RECONCILE"
	case errors.Is(err, friend.ErrFriendAttemptConflict):
		return "ERR_FRIEND_ATTEMPT_CONFLICT"
	case err != nil:
		return "ERR_TECHNICAL"
	default:
		return "NONE"
	}
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 256 {
		value = value[:256]
	}
	return value
}

// sanitizedAPIError is an allowlist, not a best-effort string replacement.
// Product errors may eventually include authority or operation material; raw
// error strings therefore never enter persisted per-operation evidence.
func sanitizedAPIError(err error) string {
	if err == nil {
		return "NONE"
	}
	if errors.Is(err, friend.ErrFriendReconcile) {
		for _, reason := range []struct{ fragment, code string }{
			{"bucket receipt count", "BUCKET_RECEIPT_COUNT"},
			{"bucket attempt count", "BUCKET_ATTEMPT_COUNT"},
			{"bucket pending work count", "BUCKET_PENDING_WORK_COUNT"},
			{"bucket meta manifest", "BUCKET_META_MANIFEST"},
			{"bucket meta identity", "BUCKET_META_IDENTITY"},
			{"canonical record", "CANONICAL_RECORD"},
		} {
			if strings.Contains(err.Error(), reason.fragment) {
				return "ERR_FRIEND_RECONCILE:" + reason.code
			}
		}
		return "ERR_FRIEND_RECONCILE:REDACTED"
	}
	if errors.Is(err, friend.ErrFriendAttemptConflict) {
		return "ERR_FRIEND_ATTEMPT_CONFLICT"
	}
	return "ERR_TECHNICAL:REDACTED"
}

func markDiagnosticError(diagnostic *failureDiagnostic, code string, err error) {
	if err != nil && diagnostic.DiagnosticError == "" {
		diagnostic.DiagnosticError = code
	}
}

func relationStatus(status friend.RelationStatus) string {
	switch status {
	case friend.RelationStatusActive:
		return "ACTIVE"
	case friend.RelationStatusDeleted:
		return "DELETED"
	case friend.RelationStatusNone:
		return "NONE"
	default:
		return "UNKNOWN"
	}
}

func (e *benchmarkEnvironment) diagnoseFriendAuthorityFailures(ctx context.Context, failures []failedOperation) ([]failureDiagnostic, error) {
	if len(failures) > maxFailureDiagnosticsPerCell {
		return nil, fmt.Errorf("failure diagnostics bound exceeded: %d > %d", len(failures), maxFailureDiagnosticsPerCell)
	}
	out := make([]failureDiagnostic, 0, len(failures))
	collectionFailures := 0
	for _, failure := range failures {
		diagnostic := failureDiagnostic{OperationIndex: failure.index, ErrorClass: diagnosticErrorClass(failure.err),
			APIError: sanitizedAPIError(failure.err), FinalClassification: "UNKNOWN", ClassificationBasis: "DIAGNOSTIC_INCOMPLETE",
			AttemptStates: []string{}}
		input := failure.sample.Diagnostic
		if input == nil || input.OperationID == "" || input.AuthorityID == "" || input.AuthorityKind == "" {
			diagnostic.DiagnosticError = "OPERATION_DIAGNOSTIC_IDENTITY_UNAVAILABLE"
			collectionFailures++
			out = append(out, finalizeFailureDiagnostic(diagnostic))
			continue
		}
		diagnostic.OperationIDHash = diagnosticHash("cap-redis-operation-id-v1", input.OperationID)
		diagnostic.AuthorityKind = string(input.AuthorityKind)
		diagnostic.AuthorityIDHash = diagnosticHash("cap-redis-authority-id-v1:"+string(input.AuthorityKind), input.AuthorityID)
		diagnostic.BucketID = input.AuthorityBucketID
		diagnostic.AuthorityRecordPresentBefore = input.AuthorityRecordPresentBefore
		diagnostic.AuthorityRecordVersionBefore = input.AuthorityRecordVersionBefore
		if input.AuthorityKind == friend.FriendPairAuthority {
			diagnostic.PairIDHash = diagnosticHash("cap-redis-pair-id-v1", input.AuthorityID)
			diagnostic.PairRecordPresentBefore = input.AuthorityRecordPresentBefore
			diagnostic.PairRecordVersionBefore = input.AuthorityRecordVersionBefore
		}
		ref := friend.FriendshipBucketRef{Kind: input.AuthorityKind,
			LayoutVersion: e.friendKeys.Profile().LayoutVersion, BucketID: input.AuthorityBucketID}
		if input.recordKey == "" || input.receiptKey == "" || input.receiptField == "" || input.attemptKey == "" || input.attemptStateField == "" {
			diagnostic.DiagnosticError = "EXACT_BOUNDED_OPERATION_LOCATORS_UNAVAILABLE"
			collectionFailures++
			out = append(out, finalizeFailureDiagnostic(diagnostic))
			continue
		}
		recordType, recordErr := e.client.Type(ctx, input.recordKey).Result()
		if recordErr != nil {
			markDiagnosticError(&diagnostic, "PAIR_TYPE_READ_FAILED", recordErr)
		} else {
			diagnostic.AuthorityRecordPresent = recordType == "hash"
			if recordType != "none" && recordType != "hash" {
				diagnostic.DiagnosticError = "AUTHORITY_KEY_TYPE_INVALID"
			}
		}
		if input.AuthorityKind == friend.FriendPairAuthority {
			diagnostic.PairRecordPresent = diagnostic.AuthorityRecordPresent
			if diagnostic.PairRecordPresent {
				snapshot, snapshotErr := e.friends.Pair.GetPair(ctx, input.PairLowPlayerID, input.PairHighPlayerID)
				if snapshotErr != nil {
					markDiagnosticError(&diagnostic, "PAIR_SNAPSHOT_READ_FAILED", snapshotErr)
				} else {
					diagnostic.PairStatus = relationStatus(snapshot.Relation.Status)
					diagnostic.PairRelationEpoch = snapshot.Relation.RelationEpoch
					diagnostic.PairRecordVersion = snapshot.RecordVersion
					diagnostic.AuthorityRecordVersion = snapshot.RecordVersion
				}
			}
		} else if input.AuthorityKind == friend.FriendQuotaAuthority && diagnostic.AuthorityRecordPresent {
			playerID, parseErr := strconv.ParseUint(input.AuthorityID, 10, 64)
			if parseErr != nil {
				diagnostic.DiagnosticError = "QUOTA_AUTHORITY_ID_INVALID"
			} else if snapshot, snapshotErr := e.friends.Quota.GetQuota(ctx, playerID); snapshotErr != nil {
				markDiagnosticError(&diagnostic, "QUOTA_SNAPSHOT_READ_FAILED", snapshotErr)
			} else {
				diagnostic.AuthorityRecordVersion = snapshot.RecordVersion
			}
		} else if input.AuthorityKind == friend.FriendShareIssuerAuthority && diagnostic.AuthorityRecordPresent {
			inviterID, parseErr := strconv.ParseUint(input.AuthorityID, 10, 64)
			if parseErr != nil {
				diagnostic.DiagnosticError = "SHARE_AUTHORITY_ID_INVALID"
			} else if snapshot, snapshotErr := e.friends.Share.GetIssuer(ctx, inviterID); snapshotErr != nil {
				markDiagnosticError(&diagnostic, "SHARE_SNAPSHOT_READ_FAILED", snapshotErr)
			} else {
				diagnostic.AuthorityRecordVersion = snapshot.RecordVersion
			}
		}
		receiptPresent, receiptErr := e.client.HExists(ctx, input.receiptKey, input.receiptField).Result()
		if receiptErr != nil {
			markDiagnosticError(&diagnostic, "RECEIPT_FIELD_READ_FAILED", receiptErr)
		} else {
			diagnostic.ReceiptPresent = receiptPresent
		}
		diagnostic.ReceiptFieldCount, receiptErr = e.client.HLen(ctx, input.receiptKey).Result()
		markDiagnosticError(&diagnostic, "RECEIPT_PARTITION_READ_FAILED", receiptErr)
		diagnostic.AttemptFieldCount, receiptErr = e.client.HLen(ctx, input.attemptKey).Result()
		markDiagnosticError(&diagnostic, "ATTEMPT_PARTITION_READ_FAILED", receiptErr)
		diagnostic.AttemptPresent = diagnostic.AttemptFieldCount > 0
		attemptState, attemptErr := e.client.HGet(ctx, input.attemptKey, input.attemptStateField).Result()
		if attemptErr != nil && !errors.Is(attemptErr, redis.Nil) {
			markDiagnosticError(&diagnostic, "ATTEMPT_STATE_READ_FAILED", attemptErr)
		} else if attemptErr == nil {
			diagnostic.AttemptStates = append(diagnostic.AttemptStates, attemptState)
		}
		metaKey, keyErr := e.friendKeys.BucketMeta(ref)
		if keyErr != nil {
			markDiagnosticError(&diagnostic, "BUCKET_META_KEY_CONTRACT_FAILED", keyErr)
		} else {
			values, metaErr := e.client.HMGet(ctx, metaKey.String(), "receipt_count", "pending_work_count", "attempt_count").Result()
			if metaErr != nil {
				markDiagnosticError(&diagnostic, "BUCKET_META_READ_FAILED", metaErr)
			} else if len(values) == 3 {
				var parseErr error
				diagnostic.BucketMeta.ReceiptCount, parseErr = diagnosticInt64(values[0])
				markDiagnosticError(&diagnostic, "BUCKET_META_COUNTER_INVALID", parseErr)
				diagnostic.BucketMeta.PendingWorkCount, parseErr = diagnosticInt64(values[1])
				markDiagnosticError(&diagnostic, "BUCKET_META_COUNTER_INVALID", parseErr)
				diagnostic.BucketMeta.AttemptCount, parseErr = diagnosticInt64(values[2])
				markDiagnosticError(&diagnostic, "BUCKET_META_COUNTER_INVALID", parseErr)
			} else {
				diagnostic.DiagnosticError = "BUCKET_META_REPLY_INVALID"
			}
			diagnostic.BucketMeta.ManifestHLEN, metaErr = e.client.HLen(ctx, metaKey.String()).Result()
			markDiagnosticError(&diagnostic, "BUCKET_META_MANIFEST_READ_FAILED", metaErr)
			if metaErr == nil && diagnostic.DiagnosticError == "" && diagnostic.BucketMeta.ManifestHLEN != 0 && diagnostic.BucketMeta.ManifestHLEN != 11 {
				diagnostic.DiagnosticError = "BUCKET_META_MANIFEST_INVALID"
			}
		}
		receiptDue, receiptDueErr := e.friendKeys.ReceiptDueIndex(ref)
		attemptDue, attemptDueErr := e.friendKeys.CommitAttemptDueIndex(ref)
		markDiagnosticError(&diagnostic, "RECEIPT_DUE_KEY_CONTRACT_FAILED", receiptDueErr)
		markDiagnosticError(&diagnostic, "ATTEMPT_DUE_KEY_CONTRACT_FAILED", attemptDueErr)
		if receiptDueErr == nil {
			diagnostic.DueCounts.ReceiptDue, receiptDueErr = e.client.ZCard(ctx, receiptDue.String()).Result()
			markDiagnosticError(&diagnostic, "RECEIPT_DUE_READ_FAILED", receiptDueErr)
		}
		if input.AuthorityKind == friend.FriendPairAuthority {
			sourceDue, sourceDueErr := e.friendKeys.SourceDueIndex(ref)
			markDiagnosticError(&diagnostic, "SOURCE_DUE_KEY_CONTRACT_FAILED", sourceDueErr)
			if sourceDueErr == nil {
				diagnostic.DueCounts.SourceDue, sourceDueErr = e.client.ZCard(ctx, sourceDue.String()).Result()
				markDiagnosticError(&diagnostic, "SOURCE_DUE_READ_FAILED", sourceDueErr)
			}
		}
		if attemptDueErr == nil {
			diagnostic.DueCounts.AttemptDue, attemptDueErr = e.client.ZCard(ctx, attemptDue.String()).Result()
			markDiagnosticError(&diagnostic, "ATTEMPT_DUE_READ_FAILED", attemptDueErr)
		}
		if diagnostic.DiagnosticError == "" {
			switch {
			case diagnostic.ReceiptPresent && diagnostic.AuthorityRecordPresent &&
				(input.AuthorityKind != friend.FriendPairAuthority || diagnostic.PairStatus == "ACTIVE"):
				diagnostic.FinalClassification = "COMMITTED"
				diagnostic.ClassificationBasis = "EXACT_RECEIPT_AND_CANONICAL_AUTHORITY_RECORD_PRESENT"
			case !input.AuthorityRecordPresentBefore && !diagnostic.AuthorityRecordPresent && !diagnostic.ReceiptPresent && !diagnostic.AttemptPresent:
				diagnostic.FinalClassification = "NOT_COMMITTED"
				diagnostic.ClassificationBasis = "NO_AUTHORITY_RECORD_RECEIPT_OR_ATTEMPT"
			case !diagnostic.ReceiptPresent && containsString(diagnostic.AttemptStates, "NOT_COMMITTED"):
				diagnostic.FinalClassification = "NOT_COMMITTED"
				diagnostic.ClassificationBasis = "ATTEMPT_FENCED_NOT_COMMITTED_WITHOUT_RECEIPT"
			default:
				diagnostic.ClassificationBasis = "DURABLE_EVIDENCE_DOES_NOT_PROVE_ONE_TERMINAL_CLASS"
			}
		}
		if diagnostic.DiagnosticError != "" {
			collectionFailures++
		}
		out = append(out, finalizeFailureDiagnostic(diagnostic))
	}
	if collectionFailures != 0 {
		return out, fmt.Errorf("%d per-operation diagnostics incomplete", collectionFailures)
	}
	return out, nil
}

func diagnosticInt64(value interface{}) (int64, error) {
	if value == nil {
		return 0, nil
	}
	var raw string
	switch typed := value.(type) {
	case string:
		raw = typed
	case []byte:
		raw = string(typed)
	case int64:
		if typed < 0 {
			return 0, errors.New("negative diagnostic counter")
		}
		return typed, nil
	default:
		return 0, errors.New("unsupported diagnostic counter type")
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != raw {
		return 0, errors.New("invalid diagnostic counter")
	}
	return parsed, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func finalizeFailureDiagnostic(value failureDiagnostic) failureDiagnostic {
	value.EvidenceHash = ""
	encoded, _ := json.Marshal(value)
	value.EvidenceHash = diagnosticHash("cap-redis-failure-evidence-v1", string(encoded))
	return value
}

func aggregateFailureEvidence(values []failureDiagnostic) (index []failureHashIndex, aggregate string) {
	if len(values) == 0 {
		return nil, ""
	}
	index = make([]failureHashIndex, len(values))
	var builder strings.Builder
	for position, value := range values {
		index[position] = failureHashIndex{OperationIndex: value.OperationIndex, EvidenceHash: value.EvidenceHash}
		builder.WriteString(strconv.Itoa(value.OperationIndex))
		builder.WriteByte(':')
		builder.WriteString(value.EvidenceHash)
		builder.WriteByte('\n')
	}
	return index, diagnosticHash("cap-redis-failure-index-v1", builder.String())
}
