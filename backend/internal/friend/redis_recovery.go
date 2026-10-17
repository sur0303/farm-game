package friend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
	"github.com/redis/go-redis/v9"
)

var completePairActionResultScript = redis.NewScript(`
local count = redis.call('HLEN', KEYS[1])
if count ~= 22 and count ~= 23 then return {'WORK_RECONCILE'} end
local values = redis.call('HMGET', KEYS[1], 'schema_version', 'work_id', 'work_type', 'placement_kind',
  'source_authority_kind', 'canonical_source_authority_id', 'work_ref', 'payload_hash', 'typed_payload',
  'next_due_at_ms', 'work_version', 'retry_count', 'lease_epoch', 'lease_holder_id', 'lease_token_hash',
  'lease_expire_at_ms', 'completed_at_ms')
if values[1] ~= 'friend-source-order-v1' or values[2] ~= ARGV[1] or values[3] ~= 'RESULT_PUBLISH_ORDER' or
   values[4] ~= 'SOURCE_PERSISTENCE_BUCKET' or values[5] ~= 'FRIEND_PAIR' or values[6] ~= ARGV[3] or
   values[7] ~= ARGV[2] or values[8] ~= ARGV[4] or values[9] ~= ARGV[7] then
  return {'WORK_RECONCILE'}
end
local state = redis.call('HGET', KEYS[1], 'state') or ''
if state == ARGV[6] then return {'ALREADY_COMPLETED'} end
if state ~= 'PENDING' or values[11] ~= ARGV[8] or values[12] ~= ARGV[9] or values[10] ~= ARGV[10] or values[13] ~= '0' or
   values[14] ~= '' or values[15] ~= '' or values[16] ~= '0' or values[17] ~= '0' then
  return {'WORK_RECONCILE'}
end
local score = redis.call('ZSCORE', KEYS[2], ARGV[2])
if not score or tonumber(score) ~= tonumber(values[10]) then return {'WORK_RECONCILE'} end
local pendingCount = tonumber(redis.call('HGET', KEYS[3], 'pending_work_count') or '')
local pendingBytes = tonumber(redis.call('HGET', KEYS[3], 'pending_work_bytes') or '')
local charged = 512 + 16 + #values[2] + 16 + #values[3] + 16 + #values[9] + 16 + #values[7] +
  16 + #values[8] + 16 + 1
if not pendingCount or not pendingBytes or pendingCount < 1 or pendingBytes < charged then
  return {'COUNTER_RECONCILE'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', KEYS[1], 'state', ARGV[6], 'completed_at_ms', tostring(now), 'updated_at_ms', tostring(now),
  'work_version', tostring(tonumber(values[11]) + 1))
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
redis.call('HINCRBY', KEYS[3], 'pending_work_bytes', -charged)
redis.call('HSET', KEYS[4], 'state', ARGV[6], 'result_proof_hash', ARGV[5], 'completed_at_ms', tostring(now))
return {'COMPLETED', tostring(charged)}
`)

var deferPairActionResultScript = redis.NewScript(`
local count = redis.call('HLEN', KEYS[1])
if count ~= 22 and count ~= 23 then return {'WORK_RECONCILE'} end
local values = redis.call('HMGET', KEYS[1], 'schema_version', 'work_id', 'work_type', 'placement_kind',
  'source_authority_kind', 'canonical_source_authority_id', 'work_ref', 'payload_hash', 'typed_payload',
  'state', 'next_due_at_ms', 'work_version', 'retry_count', 'lease_epoch', 'lease_holder_id',
  'lease_token_hash', 'lease_expire_at_ms', 'completed_at_ms')
if values[1] ~= 'friend-source-order-v1' or values[2] ~= ARGV[1] or values[3] ~= 'RESULT_PUBLISH_ORDER' or
   values[4] ~= 'SOURCE_PERSISTENCE_BUCKET' or values[5] ~= 'FRIEND_PAIR' or values[6] ~= ARGV[3] or
   values[7] ~= ARGV[2] or values[8] ~= ARGV[4] or values[9] ~= ARGV[8] then
  return {'WORK_RECONCILE'}
end
if values[10] ~= 'PENDING' then return {'ALREADY_TERMINAL'} end
if values[11] ~= ARGV[7] or values[12] ~= ARGV[5] or values[13] ~= ARGV[6] or values[14] ~= '0' or
   values[15] ~= '' or values[16] ~= '' or values[17] ~= '0' or values[18] ~= '0' then
  return {'VERSION_CONFLICT'}
end
local score = redis.call('ZSCORE', KEYS[2], ARGV[2])
if not score or tonumber(score) ~= tonumber(values[11]) then return {'WORK_RECONCILE'} end
local delay = tonumber(ARGV[9])
if not delay or delay < 1 or delay > 60000 then return {'INVALID_DELAY'} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local next_due = now + delay
local next_version = tonumber(values[12]) + 1
local next_retry = tonumber(values[13]) + 1
redis.call('HSET', KEYS[1], 'next_due_at_ms', tostring(next_due), 'updated_at_ms', tostring(now),
  'work_version', tostring(next_version), 'retry_count', tostring(next_retry))
redis.call('ZADD', KEYS[2], next_due, ARGV[2])
return {'DEFERRED', tostring(next_due), tostring(next_version), tostring(next_retry)}
`)

// PairRecoverySourceOrder is a validated, bounded read of one Pair
// FRIEND_ACTION_DISPATCH_ORDER. Fields are copied out of Redis; callers must
// still validate Theft-specific payload semantics before advancing work.
type PairRecoverySourceOrder struct {
	BucketID uint32
	WorkID   string
	WorkType string
	Fields   map[string]string
}

// PairActionResultBinding is the exact Pair admission root used by result
// production and GET_OPERATION_RESULT. It covers both legal manifests:
// rejected-before-Work and accepted with a dispatch anchor. Pair remains the
// authority; the binding itself carries no projected terminal payload.
type PairActionResultBinding struct {
	Scope                ActionScope
	SubjectPlayerID      uint64
	OwnerPlayerID        uint64
	OperationID          string
	OperationDigest      string
	PairID               string
	WorkflowID           string
	ResultOrderID        string
	DispatchOrderID      string
	PayloadHash          []byte
	TargetBindingHash    []byte
	WorkflowDeadlineAtMs int64
	AdmissionStatus      StatusClass
	AdmissionReason      ReasonCode
	BucketID             uint32
	ResultWorkRef        string
	ResultPayloadHash    []byte
	ResultState          string
	ResultWorkVersion    uint64
	ResultRetryCount     uint64
	ResultNextDueAtMs    int64
	resultTypedPayload   string
	resultDispatchID     string
}

// PairBucketIDForRelation exposes only the deterministic physical locator
// required by the online dispatch accelerator.  It does not expose Redis key
// construction and it does not change FRIEND_PAIR authority semantics.
func (r *RedisFriendshipRepositories) PairBucketIDForRelation(pairID string) (uint32, error) {
	if r == nil || r.authority == nil || strings.TrimSpace(pairID) == "" || workidentity.ValidateStableOpaqueID(pairID) != nil {
		return 0, ErrInvalidCommand
	}
	ref, err := r.authority.keys.BucketFor(FriendPairAuthority, pairID)
	if err != nil {
		return 0, err
	}
	return ref.BucketID, nil
}

// LookupPairActionResult performs a precise Pair-root lookup. The caller
// supplies the authenticated subject, target owner and operation id; this
// method derives the operation digest, bucket and stable ResultPublishOrder
// locator, then validates that order and its dispatch anchor.
func (r *RedisFriendshipRepositories) LookupPairActionResult(ctx context.Context, visitor, owner uint64,
	operationID string) (PairActionResultBinding, bool, error) {
	return r.lookupPairActionResult(ctx, visitor, owner, operationID, ActionScopeTheft)
}

// lookupPairActionResult resolves the exact Pair-root result for one admitted
// action family. The scope is part of the operation digest and is never
// inferred from optional order fields.
func (r *RedisFriendshipRepositories) lookupPairActionResult(ctx context.Context, visitor, owner uint64,
	operationID string, scope ActionScope) (PairActionResultBinding, bool, error) {
	if r == nil || r.authority == nil || !ValidPair(visitor, owner) || strings.TrimSpace(operationID) == "" {
		return PairActionResultBinding{}, false, ErrInvalidCommand
	}
	commandType, err := pairActionCommandType(scope)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	pairID, _, _, err := pairIDForPlayers(visitor, owner)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	identityValue := CommitIdentity{SubjectPlayerID: visitor, CommandType: commandType, OperationID: operationID}
	operationDigest, err := workOperationDigest(ActionAdmissionCommand{Identity: identityValue}, pairID)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	digestBytes, err := hex.DecodeString(operationDigest)
	if err != nil || len(digestBytes) != 32 {
		return PairActionResultBinding{}, false, ErrFriendReconcile
	}
	var digest [32]byte
	copy(digest[:], digestBytes)
	group, err := r.authority.group(FriendPairAuthority, pairID, digest)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	resultKey, err := r.authority.sourceOrderKey(group, "RESULT_PUBLISH_ORDER", resultID)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	if exists, existsErr := r.authority.client.Exists(ctx, resultKey).Result(); existsErr != nil {
		return PairActionResultBinding{}, false, existsErr
	} else if exists == 0 {
		return PairActionResultBinding{}, false, nil
	}
	resultFields, accepted, err := r.readAndValidatePairResultOrder(ctx, group, pairID, resultID)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	typed, err := decodeFriendTypedPayload(resultFields["typed_payload"])
	if err != nil || len(typed) != 11 || typed[0] != "ACTION_RESULT" || typed[1] != pairID ||
		typed[2] != operationDigest || typed[3] != "1" || typed[4] != operationID ||
		typed[6] != strconv.FormatUint(visitor, 10) || typed[7] != strconv.FormatUint(owner, 10) ||
		typed[8] != strconv.FormatUint(owner, 10) {
		return PairActionResultBinding{}, false, fmt.Errorf("%w: action result typed identity", ErrFriendReconcile)
	}
	payloadHash, payloadErr := hex.DecodeString(typed[9])
	targetHash, targetErr := hex.DecodeString(typed[10])
	orderPayloadHash, orderPayloadErr := hex.DecodeString(resultFields["payload_hash"])
	if payloadErr != nil || targetErr != nil || orderPayloadErr != nil || len(payloadHash) != 32 ||
		len(targetHash) != 32 || len(orderPayloadHash) != 32 {
		return PairActionResultBinding{}, false, fmt.Errorf("%w: action result payload hashes", ErrFriendReconcile)
	}
	dispatchID := typed[5]
	if !accepted {
		// A base Result manifest is legal only for rejected-before-Work. If the
		// exact immutable Dispatch anchor exists, a missing dispatch_order_id
		// field is corruption of an accepted result and must fail closed rather
		// than being reclassified as a business rejection.
		dispatchKey, keyErr := r.authority.sourceOrderKey(group, "FRIEND_ACTION_DISPATCH_ORDER", dispatchID)
		if keyErr != nil {
			return PairActionResultBinding{}, false, keyErr
		}
		dispatchExists, existsErr := r.authority.client.Exists(ctx, dispatchKey).Result()
		if existsErr != nil {
			return PairActionResultBinding{}, false, existsErr
		}
		if dispatchExists != 0 {
			return PairActionResultBinding{}, false, fmt.Errorf("%w: accepted result lost dispatch_order_id", ErrFriendReconcile)
		}
	}
	resultOrder, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "ACTION_RESULT",
		pairID, operationDigest, "1", operationID, dispatchID,
		strconv.FormatUint(visitor, 10), strconv.FormatUint(owner, 10), strconv.FormatUint(owner, 10),
		typed[9], typed[10])
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	if resultFields["typed_payload"] != resultOrder.blob || resultFields["work_ref"] != resultOrder.workRef ||
		resultFields["payload_hash"] != resultOrder.payloadHash {
		return PairActionResultBinding{}, false, fmt.Errorf("%w: action result rebuilt order mismatch", ErrFriendReconcile)
	}
	workflowID, workflowErr := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID: visitor, StableCommandTypeName: commandType, OperationID: operationID,
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
	if workflowErr != nil {
		return PairActionResultBinding{}, false, fmt.Errorf("%w: action result workflow identity: %v", ErrFriendReconcile, workflowErr)
	}
	resultVersion, versionErr := strconv.ParseUint(resultFields["work_version"], 10, 64)
	resultRetry, retryErr := strconv.ParseUint(resultFields["retry_count"], 10, 64)
	resultNextDue, dueErr := strconv.ParseInt(resultFields["next_due_at_ms"], 10, 64)
	if versionErr != nil || retryErr != nil || dueErr != nil {
		return PairActionResultBinding{}, false, fmt.Errorf("%w: action result mutable numbers", ErrFriendReconcile)
	}
	binding := PairActionResultBinding{Scope: scope, SubjectPlayerID: visitor, OwnerPlayerID: owner, OperationID: operationID,
		OperationDigest: operationDigest, PairID: pairID, WorkflowID: workflowID, ResultOrderID: resultID,
		PayloadHash: payloadHash, TargetBindingHash: targetHash, BucketID: group.ref.BucketID,
		ResultWorkRef: resultFields["work_ref"], ResultPayloadHash: orderPayloadHash, ResultState: resultFields["state"],
		ResultWorkVersion: resultVersion, ResultRetryCount: resultRetry, ResultNextDueAtMs: resultNextDue,
		resultTypedPayload: resultFields["typed_payload"], resultDispatchID: dispatchID}
	if !accepted {
		binding.AdmissionStatus = StatusRejected
		binding.AdmissionReason = ReasonVisitNotPermitted
		return binding, true, nil
	}
	if resultFields["dispatch_order_id"] != dispatchID || workidentity.ValidateStableOpaqueID(dispatchID) != nil {
		return PairActionResultBinding{}, false, ErrFriendReconcile
	}
	dispatch, err := r.ReadPairDispatchOrder(ctx, group.ref.BucketID, "FRIEND_ACTION_DISPATCH_ORDER", dispatchID)
	if err != nil {
		return PairActionResultBinding{}, false, err
	}
	fields := dispatch.Fields
	permit, permitErr := actionPermitFromOrder(fields)
	if permitErr != nil || permit.Scope != scope || permit.RelationID != pairID || permit.OperationID != operationID ||
		permit.VisitorPlayerID != visitor || permit.OwnerPlayerID != owner || permit.TargetFarmID != owner ||
		fields["canonical_source_authority_id"] != pairID || fields["visitor_player_id"] != strconv.FormatUint(visitor, 10) ||
		fields["owner_player_id"] != strconv.FormatUint(owner, 10) || fields["operation_id"] != operationID {
		return PairActionResultBinding{}, false, ErrFriendReconcile
	}
	deadline, deadlineErr := strconv.ParseInt(fields["workflow_deadline_at_ms"], 10, 64)
	if deadlineErr != nil || fields["permit_payload_hash"] != typed[9] || fields["target_binding_hash"] != typed[10] {
		return PairActionResultBinding{}, false, ErrFriendReconcile
	}
	binding.AdmissionStatus = StatusSucceeded
	binding.DispatchOrderID = dispatchID
	binding.WorkflowDeadlineAtMs = deadline
	return binding, true, nil
}

func (r *RedisFriendshipRepositories) readAndValidatePairResultOrder(ctx context.Context, group friendRedisGroup,
	pairID, resultID string) (map[string]string, bool, error) {
	key, err := r.authority.sourceOrderKey(group, "RESULT_PUBLISH_ORDER", resultID)
	if err != nil {
		return nil, false, err
	}
	count, err := r.authority.client.HLen(ctx, key).Result()
	if err != nil {
		return nil, false, err
	}
	accepted := count == int64(len(friendSourceOrderManifest(friendSourceOrderActionResult)))
	if count != int64(len(friendSourceOrderManifest(friendSourceOrderBase))) && !accepted {
		return nil, false, fmt.Errorf("%w: action result manifest", ErrFriendReconcile)
	}
	manifest := friendSourceOrderManifest(friendSourceOrderBase)
	if accepted {
		manifest = friendSourceOrderManifest(friendSourceOrderActionResult)
	}
	fields, err := r.authority.readFields(ctx, key, manifest...)
	if err != nil || len(fields) != len(manifest) {
		return nil, false, err
	}
	expected := map[string]string{
		"schema_version": "friend-source-order-v1", "work_id": resultID, "work_type": "RESULT_PUBLISH_ORDER",
		"placement_kind": "SOURCE_PERSISTENCE_BUCKET", "bucket_version": group.ref.LayoutVersion,
		"bucket_id": strconv.FormatUint(uint64(group.ref.BucketID), 10), "source_authority_kind": "FRIEND_PAIR",
		"canonical_source_authority_id": pairID,
	}
	for field, want := range expected {
		if fields[field] != want {
			return nil, false, fmt.Errorf("%w: action result field %s", ErrFriendReconcile, field)
		}
	}
	ref, err := workidentity.DecodeWorkRefV1(fields["work_ref"])
	if err != nil || ref.WorkType != "RESULT_PUBLISH_ORDER" || ref.WorkID != resultID ||
		fields["payload_hash"] != hexHash([]byte(fields["typed_payload"])) {
		return nil, false, fmt.Errorf("%w: action result identity", ErrFriendReconcile)
	}
	for _, field := range []string{"work_version", "retry_count", "lease_epoch", "lease_expire_at_ms", "created_at_ms", "updated_at_ms", "completed_at_ms", "next_due_at_ms"} {
		value := fields[field]
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != value {
			return nil, false, fmt.Errorf("%w: action result numeric field %s", ErrFriendReconcile, field)
		}
	}
	version, versionErr := strconv.ParseUint(fields["work_version"], 10, 64)
	retry, retryErr := strconv.ParseUint(fields["retry_count"], 10, 64)
	if versionErr != nil || retryErr != nil || version == 0 || fields["lease_epoch"] != "0" ||
		fields["lease_holder_id"] != "" || fields["lease_token_hash"] != "" || fields["lease_expire_at_ms"] != "0" {
		return nil, false, fmt.Errorf("%w: action result mutable fields", ErrFriendReconcile)
	}
	dueKey, err := r.PairDueIndexKey(group.ref.BucketID)
	if err != nil {
		return nil, false, err
	}
	score, scoreErr := r.authority.client.ZScore(ctx, dueKey, fields["work_ref"]).Result()
	switch fields["state"] {
	case "PENDING":
		due, parseErr := strconv.ParseInt(fields["next_due_at_ms"], 10, 64)
		if version != retry+1 || fields["completed_at_ms"] != "0" || parseErr != nil || scoreErr != nil || int64(score) != due {
			return nil, false, fmt.Errorf("%w: pending action result due binding", ErrFriendReconcile)
		}
	case "SUCCEEDED", "REJECTED", "NO_CHANGE", "RECONCILE_REQUIRED":
		completed, _ := strconv.ParseInt(fields["completed_at_ms"], 10, 64)
		if (version != retry+1 && version != retry+2) || completed <= 0 || !errors.Is(scoreErr, redis.Nil) {
			return nil, false, fmt.Errorf("%w: terminal action result due binding", ErrFriendReconcile)
		}
	default:
		return nil, false, fmt.Errorf("%w: action result state", ErrFriendReconcile)
	}
	return fields, accepted, nil
}

// ReadPairActionResultOrder resolves one DueIndex result member into the same
// precise Pair binding used by the public fallback. The typed order carries
// enough immutable identity to avoid SCAN or guessing either player.
func (r *RedisFriendshipRepositories) ReadPairActionResultOrder(ctx context.Context, bucketID uint32,
	workType, workID string) (PairActionResultBinding, error) {
	if workType != "RESULT_PUBLISH_ORDER" || workidentity.ValidateStableOpaqueID(workID) != nil {
		return PairActionResultBinding{}, fmt.Errorf("%w: result locator", ErrInvalidCommand)
	}
	key, err := r.PairSourceOrderKey(bucketID, workType, workID)
	if err != nil {
		return PairActionResultBinding{}, err
	}
	values, err := r.authority.readFields(ctx, key, "typed_payload", "canonical_source_authority_id")
	if err != nil {
		return PairActionResultBinding{}, err
	}
	typed, err := decodeFriendTypedPayload(values["typed_payload"])
	if err != nil || len(typed) != 11 || typed[0] != "ACTION_RESULT" || typed[1] != values["canonical_source_authority_id"] {
		return PairActionResultBinding{}, fmt.Errorf("%w: due result typed locator", ErrFriendReconcile)
	}
	visitor, visitorErr := strconv.ParseUint(typed[6], 10, 64)
	owner, ownerErr := strconv.ParseUint(typed[7], 10, 64)
	if visitorErr != nil || ownerErr != nil || typed[8] != typed[7] || !ValidPair(visitor, owner) {
		return PairActionResultBinding{}, fmt.Errorf("%w: due result players", ErrFriendReconcile)
	}
	scope, err := pairActionScopeForDigest(visitor, typed[4], typed[1], typed[2])
	if err != nil {
		return PairActionResultBinding{}, err
	}
	binding, found, err := r.lookupPairActionResult(ctx, visitor, owner, typed[4], scope)
	if err != nil {
		return PairActionResultBinding{}, err
	}
	if !found {
		return PairActionResultBinding{}, redis.Nil
	}
	if binding.BucketID != bucketID || binding.ResultOrderID != workID || binding.ResultWorkRef == "" {
		return PairActionResultBinding{}, fmt.Errorf("%w: due result physical locator", ErrFriendReconcile)
	}
	return binding, nil
}

// ReadPairActionWorkScope validates the exact Pair source order and returns
// its immutable admitted scope. Callers use this before invoking a family
// handler; a non-owned scope is not an invalid order and must not be leased or
// mutated by that handler.
func (r *RedisFriendshipRepositories) ReadPairActionWorkScope(ctx context.Context, bucketID uint32,
	workType, workID string) (ActionScope, error) {
	switch workType {
	case "FRIEND_ACTION_DISPATCH_ORDER":
		source, err := r.ReadPairDispatchOrder(ctx, bucketID, workType, workID)
		if err != nil {
			return ActionScopeUnspecified, err
		}
		permit, err := actionPermitFromOrder(source.Fields)
		if err != nil {
			return ActionScopeUnspecified, errors.Join(ErrFriendReconcile, err)
		}
		return permit.Scope, nil
	case "RESULT_PUBLISH_ORDER":
		key, err := r.PairSourceOrderKey(bucketID, workType, workID)
		if err != nil {
			return ActionScopeUnspecified, err
		}
		value, err := r.authority.client.HGet(ctx, key, "typed_payload").Result()
		if err != nil {
			return ActionScopeUnspecified, err
		}
		typed, err := decodeFriendTypedPayload(value)
		if err != nil {
			return ActionScopeUnspecified, errors.Join(ErrFriendReconcile, err)
		}
		if len(typed) == 0 || typed[0] != "ACTION_RESULT" {
			return ActionScopeUnspecified, ErrPairActionScopeNotOwned
		}
		binding, err := r.ReadPairActionResultOrder(ctx, bucketID, workType, workID)
		if err != nil {
			return ActionScopeUnspecified, err
		}
		return binding.Scope, nil
	default:
		return ActionScopeUnspecified, ErrInvalidCommand
	}
}

func pairActionCommandType(scope ActionScope) (string, error) {
	switch scope {
	case ActionScopeCare:
		return "CARE", nil
	case ActionScopeTheft:
		return "THEFT", nil
	default:
		return "", ErrInvalidCommand
	}
}

func pairActionScopeForDigest(visitor uint64, operationID, pairID, operationDigest string) (ActionScope, error) {
	if visitor == 0 || strings.TrimSpace(operationID) == "" || workidentity.ValidateStableOpaqueID(pairID) != nil {
		return ActionScopeUnspecified, fmt.Errorf("%w: action scope digest inputs", ErrFriendReconcile)
	}
	matched := ActionScopeUnspecified
	for _, scope := range []ActionScope{ActionScopeCare, ActionScopeTheft} {
		commandType, _ := pairActionCommandType(scope)
		digest, err := workOperationDigest(ActionAdmissionCommand{Identity: CommitIdentity{
			SubjectPlayerID: visitor, CommandType: commandType, OperationID: operationID,
		}}, pairID)
		if err != nil {
			return ActionScopeUnspecified, errors.Join(ErrFriendReconcile, err)
		}
		if digest == operationDigest {
			if matched != ActionScopeUnspecified {
				return ActionScopeUnspecified, fmt.Errorf("%w: action scope digest collision", ErrFriendReconcile)
			}
			matched = scope
		}
	}
	if matched == ActionScopeUnspecified {
		return ActionScopeUnspecified, fmt.Errorf("%w: action scope digest is neither CARE nor THEFT", ErrFriendReconcile)
	}
	return matched, nil
}

// CompletePairActionResultOrder durably terminalizes one validated result
// order and releases the exact logical bytes charged by Pair admission. The
// script is idempotent; counter inconsistencies fail closed with the order
// still due and therefore observable to reconciliation.
func (r *RedisFriendshipRepositories) CompletePairActionResultOrder(ctx context.Context,
	binding PairActionResultBinding, terminal string, resultProof []byte) error {
	if r == nil || r.authority == nil || binding.ResultOrderID == "" || binding.ResultWorkRef == "" ||
		binding.PairID == "" || len(binding.ResultPayloadHash) != sha256.Size || len(resultProof) != sha256.Size {
		return ErrInvalidCommand
	}
	switch terminal {
	case "SUCCEEDED", "REJECTED", "NO_CHANGE", "RECONCILE_REQUIRED":
	default:
		return ErrInvalidCommand
	}
	expected, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", binding.ResultOrderID, "ACTION_RESULT",
		binding.PairID, binding.OperationDigest, "1", binding.OperationID, binding.resultDispatchID,
		strconv.FormatUint(binding.SubjectPlayerID, 10), strconv.FormatUint(binding.OwnerPlayerID, 10),
		strconv.FormatUint(binding.OwnerPlayerID, 10), hex.EncodeToString(binding.PayloadHash),
		hex.EncodeToString(binding.TargetBindingHash))
	if err != nil {
		return errors.Join(ErrFriendReconcile, err)
	}
	if expected.workRef != binding.ResultWorkRef {
		return fmt.Errorf("%w: result completion WorkRef", ErrFriendReconcile)
	}
	if expected.blob != binding.resultTypedPayload {
		return fmt.Errorf("%w: result completion typed payload", ErrFriendReconcile)
	}
	if expected.payloadHash != hex.EncodeToString(binding.ResultPayloadHash) {
		return fmt.Errorf("%w: result completion payload", ErrFriendReconcile)
	}
	workKey, err := r.PairSourceOrderKey(binding.BucketID, "RESULT_PUBLISH_ORDER", binding.ResultOrderID)
	if err != nil {
		return err
	}
	dueKey, err := r.PairDueIndexKey(binding.BucketID)
	if err != nil {
		return err
	}
	metaKey, err := r.PairBucketMetaKey(binding.BucketID)
	if err != nil {
		return err
	}
	recoveryKey, err := r.PairRecoveryKey(binding.BucketID, binding.ResultOrderID)
	if err != nil {
		return err
	}
	reply, err := completePairActionResultScript.Run(ctx, r.authority.client,
		[]string{workKey, dueKey, metaKey, recoveryKey}, binding.ResultOrderID, binding.ResultWorkRef,
		binding.PairID, hex.EncodeToString(binding.ResultPayloadHash), hex.EncodeToString(resultProof), terminal,
		expected.blob, strconv.FormatUint(binding.ResultWorkVersion, 10), strconv.FormatUint(binding.ResultRetryCount, 10),
		strconv.FormatInt(binding.ResultNextDueAtMs, 10)).Result()
	if err != nil {
		return err
	}
	values, ok := reply.([]interface{})
	if !ok || len(values) == 0 {
		return ErrFriendReconcile
	}
	status := fmt.Sprint(values[0])
	if status == "COMPLETED" || status == "ALREADY_COMPLETED" {
		return nil
	}
	return fmt.Errorf("%w: result completion %s", ErrFriendReconcile, status)
}

// DeferPairActionResultOrder atomically moves a still-processing result order
// out of the due head. It updates WorkRecord version/retry and the exact ZSET
// score in one same-slot Lua operation, preventing every worker round from
// re-reading a result whose independent Theft Workflow is not terminal yet.
func (r *RedisFriendshipRepositories) DeferPairActionResultOrder(ctx context.Context,
	binding PairActionResultBinding, delay time.Duration) error {
	if r == nil || r.authority == nil || binding.ResultOrderID == "" || binding.ResultWorkRef == "" ||
		binding.PairID == "" || len(binding.ResultPayloadHash) != sha256.Size || delay <= 0 || delay > time.Minute {
		return ErrInvalidCommand
	}
	expected, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", binding.ResultOrderID, "ACTION_RESULT",
		binding.PairID, binding.OperationDigest, "1", binding.OperationID, binding.resultDispatchID,
		strconv.FormatUint(binding.SubjectPlayerID, 10), strconv.FormatUint(binding.OwnerPlayerID, 10),
		strconv.FormatUint(binding.OwnerPlayerID, 10), hex.EncodeToString(binding.PayloadHash),
		hex.EncodeToString(binding.TargetBindingHash))
	if err != nil || expected.workRef != binding.ResultWorkRef || expected.blob != binding.resultTypedPayload ||
		expected.payloadHash != hex.EncodeToString(binding.ResultPayloadHash) {
		return errors.Join(ErrFriendReconcile, err)
	}
	workKey, err := r.PairSourceOrderKey(binding.BucketID, "RESULT_PUBLISH_ORDER", binding.ResultOrderID)
	if err != nil {
		return err
	}
	dueKey, err := r.PairDueIndexKey(binding.BucketID)
	if err != nil {
		return err
	}
	reply, err := deferPairActionResultScript.Run(ctx, r.authority.client, []string{workKey, dueKey},
		binding.ResultOrderID, binding.ResultWorkRef, binding.PairID, hex.EncodeToString(binding.ResultPayloadHash),
		strconv.FormatUint(binding.ResultWorkVersion, 10), strconv.FormatUint(binding.ResultRetryCount, 10),
		strconv.FormatInt(binding.ResultNextDueAtMs, 10), expected.blob, strconv.FormatInt(delay.Milliseconds(), 10)).Result()
	if err != nil {
		return err
	}
	values, ok := reply.([]interface{})
	if !ok || len(values) == 0 {
		return ErrFriendReconcile
	}
	switch fmt.Sprint(values[0]) {
	case "DEFERRED", "ALREADY_TERMINAL":
		return nil
	case "VERSION_CONFLICT":
		return ErrFriendRecordVersionConflict
	default:
		return fmt.Errorf("%w: result defer %s", ErrFriendReconcile, fmt.Sprint(values[0]))
	}
}

// Client exposes the Redis client for recovery-only adapters. It does not
// expose the private authority key builder or permit mutation methods.
func (r *RedisFriendshipRepositories) Client() redis.UniversalClient {
	if r == nil || r.authority == nil {
		return nil
	}
	return r.authority.client
}

func (r *RedisFriendshipRepositories) PairBucketCount() uint32 {
	if r == nil || r.authority == nil {
		return 0
	}
	return r.authority.keys.Profile().PairBucketCount
}

func (r *RedisFriendshipRepositories) pairBucket(bucketID uint32) (FriendshipBucketRef, error) {
	if r == nil || r.authority == nil {
		return FriendshipBucketRef{}, ErrInvalidCommand
	}
	profile := r.authority.keys.Profile()
	if bucketID >= profile.PairBucketCount {
		return FriendshipBucketRef{}, ErrInvalidFriendBucket
	}
	return FriendshipBucketRef{Kind: FriendPairAuthority, LayoutVersion: profile.LayoutVersion, BucketID: bucketID}, nil
}

func (r *RedisFriendshipRepositories) PairDueIndexKey(bucketID uint32) (string, error) {
	ref, err := r.pairBucket(bucketID)
	if err != nil {
		return "", err
	}
	key, err := r.authority.keys.SourceDueIndex(ref)
	return string(key), err
}

func (r *RedisFriendshipRepositories) PairSourceOrderKey(bucketID uint32, workType, workID string) (string, error) {
	ref, err := r.pairBucket(bucketID)
	if err != nil {
		return "", err
	}
	key, err := r.authority.keys.SourceOrder(ref, workType, workID)
	return string(key), err
}

func (r *RedisFriendshipRepositories) PairBucketMetaKey(bucketID uint32) (string, error) {
	ref, err := r.pairBucket(bucketID)
	if err != nil {
		return "", err
	}
	key, err := r.authority.keys.BucketMeta(ref)
	return string(key), err
}

// PairRecoveryKey is a same-slot sidecar for boot/deadline/proof metadata.
// The exact Pair source-order manifest remains untouched.
func (r *RedisFriendshipRepositories) PairRecoveryKey(bucketID uint32, workID string) (string, error) {
	ref, err := r.pairBucket(bucketID)
	if err != nil {
		return "", err
	}
	if workidentity.ValidateStableOpaqueID(workID) != nil {
		return "", ErrInvalidFriendBucket
	}
	tag, err := r.authority.keys.HashTag(ref)
	if err != nil {
		return "", err
	}
	return "{" + tag + "}:recovery:" + workID, nil
}

// ReadPairDispatchOrder validates immutable address/payload fields and the
// current mutable lease tuple without assuming the order is still at its
// initial retry/lease values. A missing/corrupt manifest fails closed.
func (r *RedisFriendshipRepositories) ReadPairDispatchOrder(ctx context.Context, bucketID uint32, workType, workID string) (PairRecoverySourceOrder, error) {
	if workType != "FRIEND_ACTION_DISPATCH_ORDER" || workidentity.ValidateStableOpaqueID(workID) != nil {
		return PairRecoverySourceOrder{}, fmt.Errorf("%w: dispatch locator", ErrInvalidCommand)
	}
	key, err := r.PairSourceOrderKey(bucketID, workType, workID)
	if err != nil {
		return PairRecoverySourceOrder{}, err
	}
	if exists, existsErr := r.authority.client.Exists(ctx, key).Result(); existsErr != nil {
		return PairRecoverySourceOrder{}, existsErr
	} else if exists == 0 {
		return PairRecoverySourceOrder{}, redis.Nil
	}
	manifest := friendSourceOrderManifest(friendSourceOrderDispatch)
	fields, err := r.authority.readFields(ctx, key, manifest...)
	if err != nil {
		return PairRecoverySourceOrder{}, err
	}
	count, err := r.authority.client.HLen(ctx, key).Result()
	if err != nil {
		return PairRecoverySourceOrder{}, err
	}
	if count != int64(len(manifest)) || len(fields) != len(manifest) {
		return PairRecoverySourceOrder{}, fmt.Errorf("%w: dispatch manifest", ErrFriendReconcile)
	}
	if err := validateRecoverySourceFields(ctx, r, bucketID, workType, workID, fields, manifest); err != nil {
		return PairRecoverySourceOrder{}, err
	}
	return PairRecoverySourceOrder{BucketID: bucketID, WorkID: workID, WorkType: workType, Fields: fields}, nil
}

func validateRecoverySourceFields(ctx context.Context, repos *RedisFriendshipRepositories, bucketID uint32,
	workType, workID string, fields map[string]string, manifest []string) error {
	profile := repos.authority.keys.Profile()
	expected := map[string]string{
		"schema_version":                "friend-source-order-v1",
		"work_id":                       workID,
		"work_type":                     workType,
		"placement_kind":                "SOURCE_PERSISTENCE_BUCKET",
		"bucket_version":                profile.LayoutVersion,
		"bucket_id":                     strconv.FormatUint(uint64(bucketID), 10),
		"source_authority_kind":         string(FriendPairAuthority),
		"state":                         fields["state"],
		"work_ref":                      fields["work_ref"],
		"canonical_source_authority_id": fields["canonical_source_authority_id"],
	}
	for field, want := range expected {
		if fields[field] != want || strings.TrimSpace(want) == "" {
			return fmt.Errorf("%w: source field %s", ErrFriendReconcile, field)
		}
	}
	ref, err := workidentity.DecodeWorkRefV1(fields["work_ref"])
	if err != nil || ref.WorkType != workType || ref.WorkID != workID {
		return fmt.Errorf("%w: source work ref", ErrFriendReconcile)
	}
	if workidentity.ValidateStableOpaqueID(fields["canonical_source_authority_id"]) != nil {
		return fmt.Errorf("%w: source pair id", ErrFriendReconcile)
	}
	if _, err := decodeFriendTypedPayload(fields["typed_payload"]); err != nil {
		return fmt.Errorf("%w: typed payload: %v", ErrFriendReconcile, err)
	}
	if fields["payload_hash"] != hexHash([]byte(fields["typed_payload"])) {
		return fmt.Errorf("%w: typed payload hash", ErrFriendReconcile)
	}
	due, err := strconv.ParseInt(fields["next_due_at_ms"], 10, 64)
	if err != nil || due < 0 || strconv.FormatInt(due, 10) != fields["next_due_at_ms"] {
		return fmt.Errorf("%w: next due", ErrFriendReconcile)
	}
	dueKey, err := repos.PairDueIndexKey(bucketID)
	if err != nil {
		return err
	}
	score, err := repos.authority.client.ZScore(ctx, dueKey, fields["work_ref"]).Result()
	state := fields["state"]
	if state == "PENDING" {
		if err != nil || int64(score) != due || strconv.FormatInt(int64(score), 10) != fields["next_due_at_ms"] {
			return fmt.Errorf("%w: DueIndex binding", ErrFriendReconcile)
		}
	} else if !errors.Is(err, redis.Nil) {
		return fmt.Errorf("%w: terminal DueIndex binding", ErrFriendReconcile)
	}
	for _, field := range []string{"work_version", "retry_count", "lease_epoch", "lease_expire_at_ms", "created_at_ms", "updated_at_ms", "completed_at_ms"} {
		value := fields[field]
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 || value != strconv.FormatInt(parsed, 10) {
			return fmt.Errorf("%w: mutable field %s", ErrFriendReconcile, field)
		}
	}
	if fields["state"] != "PENDING" && fields["state"] != "COMPLETED" && fields["state"] != "RECONCILE_REQUIRED" {
		return fmt.Errorf("%w: state", ErrFriendReconcile)
	}
	if completed, _ := strconv.ParseInt(fields["completed_at_ms"], 10, 64); (state == "PENDING" && completed != 0) || (state != "PENDING" && completed <= 0) {
		return fmt.Errorf("%w: completed state", ErrFriendReconcile)
	}
	permit, err := actionPermitFromOrder(fields)
	if err != nil {
		return fmt.Errorf("%w: action permit: %v", ErrFriendReconcile, err)
	}
	parseDue, parseErr := strconv.ParseInt(fields["next_due_at_ms"], 10, 64)
	workflowDeadline, deadlineErr := strconv.ParseInt(fields["workflow_deadline_at_ms"], 10, 64)
	if fields["dispatch_order_id"] != workID || fields["relation_id"] != fields["canonical_source_authority_id"] ||
		parseErr != nil || deadlineErr != nil || workflowDeadline <= permit.ExpireAtAuthorityMs || parseDue > workflowDeadline {
		return fmt.Errorf("%w: dispatch identity/deadline binding", ErrFriendReconcile)
	}
	for _, field := range manifest {
		if _, ok := fields[field]; !ok {
			return fmt.Errorf("%w: missing field %s", ErrFriendReconcile, field)
		}
	}
	return nil
}
