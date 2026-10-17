package ingress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/contracts"
	"farmgame/internal/friend"
	"farmgame/internal/harvestonline"
	"farmgame/internal/identity"
	harvestscenario "farmgame/internal/scenario/harvest"
	theftscenario "farmgame/internal/scenario/theft"
	unlockscenario "farmgame/internal/scenario/unlockplot"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const (
	operationResultMinRetryAfterMs uint32 = 50
	operationResultMaxRetryAfterMs uint32 = 5000
	operationResultDefaultRetry           = 50 * time.Millisecond
)

var (
	// These errors are classification sentinels for the public OperationReadAPI
	// adapter. They do not expose storage details to the public response.
	ErrOperationResultReaderUnavailable     = errors.New("operation result: reader unavailable")
	ErrOperationResultInvalidRequest        = errors.New("operation result: invalid request")
	ErrOperationResultMalformedOperationID  = errors.New("operation result: malformed operation id")
	ErrOperationResultInvalidLocator        = errors.New("operation result: invalid target locator")
	ErrOperationResultUnsupportedCommand    = errors.New("operation result: original command is not supported")
	ErrOperationResultRepositoryUnavailable = errors.New("operation result: repository unavailable")
	ErrOperationResultInvalidRecord         = errors.New("operation result: invalid WorkRecord")
	ErrOperationResultTerminalUnavailable   = errors.New("operation result: terminal state has no public outcome")
)

// OperationResultLookup is the read-only port used by the public
// GET_OPERATION_RESULT FirstHop. Implementations must not claim a lease,
// advance a WorkRecord, or invoke a workflow engine.
type OperationResultLookup interface {
	Lookup(context.Context, uint64, *publicpb.GetOperationResultRequest) (*publicpb.GetOperationResultResult, error)
}

// OperationResultReaderConfig is the smallest operation-result composition
// input for the generic WorkRecord lookup. The resolver is the existing W31
// placement implementation; it is not a second result store.
type OperationResultReaderConfig struct {
	Repository           workflow.WorkRepository
	BucketResolver       workidentity.BucketResolver
	BucketVersion        uint32
	TheftAdmissionLookup TheftAdmissionLookup
	ResultDirectory      ResultDirectoryLookup
	RetryAfter           time.Duration
	Now                  func() time.Time
}

// TheftAdmissionLookup is the precise FRIEND_PAIR fallback used only before
// the independently placed WorkRecord exists.
type TheftAdmissionLookup interface {
	LookupPairActionResult(context.Context, uint64, uint64, string) (friend.PairActionResultBinding, bool, error)
}

// ResultDirectoryLookup is a rebuildable terminal projection. A miss, read
// error, or invalid entry always falls through to Work/Pair authority.
type ResultDirectoryLookup interface {
	LookupResult(context.Context, uint64, string) (redisauthority.ResultEntry, bool, error)
}

// OperationResultReader derives the frozen operation workflow identity and
// reads one generic WorkRecord. It has no workflow Definition or Engine.
type OperationResultReader struct {
	repository      workflow.WorkRepository
	bucketResolver  workidentity.BucketResolver
	bucketVersion   uint32
	theftAdmission  TheftAdmissionLookup
	resultDirectory ResultDirectoryLookup
	retryAfterMs    uint32
	now             func() time.Time
}

var _ OperationResultLookup = (*OperationResultReader)(nil)

// NewOperationResultReader constructs the generic read path. A missing
// repository or placement resolver is a composition error, not NOT_FOUND.
func NewOperationResultReader(config OperationResultReaderConfig) (*OperationResultReader, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 {
		return nil, fmt.Errorf("%w: repository, bucket resolver and bucket version are required", ErrOperationResultReaderUnavailable)
	}
	retryAfter := config.RetryAfter
	if retryAfter <= 0 {
		retryAfter = operationResultDefaultRetry
	}
	retryAfterMs := uint32(retryAfter / time.Millisecond)
	if retryAfterMs < operationResultMinRetryAfterMs {
		retryAfterMs = operationResultMinRetryAfterMs
	}
	if retryAfterMs > operationResultMaxRetryAfterMs {
		retryAfterMs = operationResultMaxRetryAfterMs
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &OperationResultReader{
		repository:      config.Repository,
		bucketResolver:  config.BucketResolver,
		bucketVersion:   config.BucketVersion,
		theftAdmission:  config.TheftAdmissionLookup,
		resultDirectory: config.ResultDirectory,
		retryAfterMs:    retryAfterMs,
		now:             now,
	}, nil
}

// Lookup resolves a public operation query using only the trusted subject
// supplied by FirstHop and the original command's root material. In
// particular, the request has no subject field and no current Farm fence.
func (r *OperationResultReader) Lookup(ctx context.Context, subjectPlayerID uint64, request *publicpb.GetOperationResultRequest) (*publicpb.GetOperationResultResult, error) {
	if r == nil || r.repository == nil || r.bucketResolver == nil || r.bucketVersion == 0 {
		return nil, ErrOperationResultReaderUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if subjectPlayerID == 0 || request == nil {
		return nil, ErrOperationResultInvalidRequest
	}
	if err := validateOperationID(request.GetOriginalOperationId()); err != nil {
		return nil, err
	}
	commandType := request.GetOriginalCommandType()
	if commandType != commonpb.CommandType_PLANT && commandType != commonpb.CommandType_HARVEST && commandType != commonpb.CommandType_UNLOCK_PLOT &&
		commandType != commonpb.CommandType_CARE && commandType != commonpb.CommandType_THEFT && commandType != commonpb.CommandType_CLAIM_TASK {
		return nil, fmt.Errorf("%w: %s", ErrOperationResultUnsupportedCommand, request.GetOriginalCommandType())
	}
	// A locator may be carried as an opaque hint. It is never parsed or used
	// as a key here; the authenticated subject, original command and target
	// still derive and validate the exact SC1 identity below.
	if locator := request.GetServerOperationLocator(); len(locator) > 4096 {
		return nil, fmt.Errorf("%w: server locator too long", ErrOperationResultInvalidLocator)
	}
	if commandType == commonpb.CommandType_CLAIM_TASK {
		// CLAIM_TASK commits its terminal receipt in the PlayerActor record, not
		// the generic WorkRepository. Until that authority receipt has a direct
		// read projection, an honest NOT_FOUND instructs the frozen recovery
		// policy to replay the same operation_id. The PlayerActor receipt then
		// returns the exact terminal payload without granting twice.
		return lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND), nil
	}
	targetFarmID, err := operationTargetFarmID(request.GetOriginalTarget(), commandType)
	if err != nil {
		return nil, err
	}
	if commandType == commonpb.CommandType_CARE {
		// CARE commits its Farm mutation, terminal receipt and reward order in
		// the FarmActor authority rather than in the generic WorkRepository.
		// A projection miss therefore returns honest NOT_FOUND (which never
		// proves the original request was not accepted). The frozen client
		// recovery matrix Fresh-Joins and replays the same operation_id; the
		// FarmActor receipt then returns the exact terminal CareResult without
		// duplicating the mutation or reward obligation.
		return lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND), nil
	}

	workType := operationWorkType(commandType)
	workflowID, address, err := r.operationAddress(subjectPlayerID, request.GetOriginalOperationId(), targetFarmID, workType)
	if err != nil {
		return nil, err
	}
	operationDigest, rootKind, rootID, err := operationResultDigest(subjectPlayerID, request.GetOriginalOperationId(), targetFarmID, workType)
	if err != nil {
		return nil, err
	}
	if projected, ok := r.lookupProjectedTerminal(ctx, subjectPlayerID, targetFarmID, request, operationDigest,
		rootKind, rootID, address); ok {
		return projected, nil
	}
	record, err := r.repository.Get(ctx, workType, workflowID)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			if commandType == commonpb.CommandType_THEFT {
				return r.lookupTheftAdmission(ctx, subjectPlayerID, targetFarmID, request, workflowID, address)
			}
			return lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND), nil
		}
		return nil, errors.Join(ErrOperationResultRepositoryUnavailable, err)
	}
	if record == nil {
		return nil, errors.Join(ErrOperationResultRepositoryUnavailable, ErrOperationResultInvalidRecord)
	}
	if err := r.validateRecordIdentity(record, address, subjectPlayerID, request.GetOriginalOperationId(), targetFarmID, commandType); err != nil {
		// Identity mismatch is deliberately indistinguishable from absence. A
		// query must not turn a bad locator or a cross-subject record into an
		// information oracle.
		if errors.Is(err, errOperationResultIdentityMismatch) {
			return lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND), nil
		}
		return nil, errors.Join(ErrOperationResultInvalidRecord, err)
	}
	if _, err := decodeOperationRoot(record, subjectPlayerID, targetFarmID, commandType); err != nil {
		return nil, errors.Join(ErrOperationResultInvalidRecord, err)
	}

	result := lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND)
	switch record.State {
	case workflow.WorkStateCreated, workflow.WorkStateRunning, workflow.WorkStateStepUnknown:
		result.LookupStatus = publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING
		result.RetryAfterMs = r.retryAfterMs
		return result, nil
	case workflow.WorkStateCompleted:
		var outcome *publicpb.StoredOperationOutcome
		if commandType == commonpb.CommandType_PLANT {
			outcome, err = r.completedPlantOutcome(record, subjectPlayerID, targetFarmID)
		} else if commandType == commonpb.CommandType_UNLOCK_PLOT {
			outcome, err = r.completedUnlockPlotOutcome(record, subjectPlayerID, targetFarmID)
		} else {
			switch commandType {
			case commonpb.CommandType_HARVEST:
				outcome, err = r.completedHarvestOutcome(record, subjectPlayerID, targetFarmID)
			case commonpb.CommandType_THEFT:
				outcome, err = r.completedTheftOutcome(record, subjectPlayerID, targetFarmID)
			}
		}
		if err != nil {
			return nil, errors.Join(ErrOperationResultInvalidRecord, err)
		}
		result.LookupStatus = publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED
		result.CompletedOutcome = outcome
		result.CompletedRetryDirective = commonpb.RetryDirective_NONE
		return result, nil
	case workflow.WorkStateRejected, workflow.WorkStateCompensated:
		result.LookupStatus = publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED
		if commandType == commonpb.CommandType_PLANT {
			result.CompletedOutcome = rejectedPlantOutcome(record, request.GetOriginalOperationId(), targetFarmID)
		} else if commandType == commonpb.CommandType_UNLOCK_PLOT {
			if record.State == workflow.WorkStateCompensated {
				if outcome, noChange, noChangeErr := r.compensatedUnlockPlotOutcome(record, subjectPlayerID, targetFarmID); noChangeErr != nil {
					return nil, errors.Join(ErrOperationResultInvalidRecord, noChangeErr)
				} else if noChange {
					result.CompletedOutcome = outcome
				} else {
					result.CompletedOutcome = rejectedUnlockPlotOutcome(record, request.GetOriginalOperationId(), targetFarmID)
				}
			} else {
				result.CompletedOutcome = rejectedUnlockPlotOutcome(record, request.GetOriginalOperationId(), targetFarmID)
			}
		} else if commandType == commonpb.CommandType_HARVEST {
			result.CompletedOutcome = rejectedHarvestOutcome(record, request.GetOriginalOperationId(), targetFarmID)
		} else {
			result.CompletedOutcome, _, err = theftscenario.PublicOutcome(record)
			if err != nil {
				return nil, errors.Join(ErrOperationResultInvalidRecord, err)
			}
		}
		result.CompletedRetryDirective = commonpb.RetryDirective_NONE
		return result, nil
	case workflow.WorkStateFailed:
		if r.expiredEvidence(record) {
			result.LookupStatus = publicpb.OperationLookupStatus_OPERATION_LOOKUP_EXPIRED
			return result, nil
		}
		// FAILED is an immutable control terminal even when a participant's
		// business outcome remained unknown. Returning an adapter ERR_INTERNAL
		// leaves the client polling an operation that can never advance. Expose
		// an honest UNAVAILABLE terminal and require an authority refresh; never
		// invent SUCCEEDED/REJECTED or replay under a new operation id.
		result.LookupStatus = publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED
		result.CompletedOutcome = failedOperationOutcome(commandType, request.GetOriginalOperationId(), targetFarmID)
		result.CompletedRetryDirective = commonpb.RetryDirective_REFRESH_AUTHORITY_STATE
		return result, nil
	case workflow.WorkStateSuperseded:
		return nil, ErrOperationResultTerminalUnavailable
	default:
		return nil, errors.Join(ErrOperationResultInvalidRecord, fmt.Errorf("unknown WorkRecord state %s", record.State))
	}
}

func (r *OperationResultReader) lookupTheftAdmission(ctx context.Context, subjectPlayerID, targetFarmID uint64,
	request *publicpb.GetOperationResultRequest, workflowID string, address workidentity.WorkAddress) (*publicpb.GetOperationResultResult, error) {
	if r.theftAdmission == nil {
		return nil, fmt.Errorf("%w: Theft Pair fallback is required", ErrOperationResultReaderUnavailable)
	}
	binding, found, err := r.theftAdmission.LookupPairActionResult(ctx, subjectPlayerID, targetFarmID,
		request.GetOriginalOperationId())
	if err != nil {
		return nil, errors.Join(ErrOperationResultRepositoryUnavailable, err)
	}
	if !found {
		return lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND), nil
	}
	pairID, err := identity.CanonicalFriendRelationID(subjectPlayerID, targetFarmID)
	if err != nil {
		return nil, errors.Join(ErrOperationResultInvalidLocator, err)
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: subjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: request.GetOriginalOperationId(),
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID}}
	digest, err := key.OperationDigest()
	if err != nil {
		return nil, errors.Join(ErrOperationResultInvalidRequest, err)
	}
	if binding.SubjectPlayerID != subjectPlayerID || binding.OwnerPlayerID != targetFarmID ||
		binding.OperationID != request.GetOriginalOperationId() || binding.OperationDigest != digest ||
		binding.PairID != pairID || binding.WorkflowID != workflowID || address.WorkID != workflowID ||
		binding.ResultOrderID == "" || len(binding.PayloadHash) != 32 || len(binding.TargetBindingHash) != 32 {
		return nil, errors.Join(ErrOperationResultInvalidRecord, errors.New("Theft Pair result binding mismatch"))
	}
	if binding.AdmissionStatus == friend.StatusRejected {
		if binding.AdmissionReason != friend.ReasonVisitNotPermitted || binding.DispatchOrderID != "" || binding.WorkflowDeadlineAtMs != 0 {
			return nil, errors.Join(ErrOperationResultInvalidRecord, errors.New("Theft Pair rejection binding mismatch"))
		}
		result := lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED)
		result.CompletedOutcome = &publicpb.StoredOperationOutcome{
			OriginalCommandType: commonpb.CommandType_THEFT, OriginalOperationId: request.GetOriginalOperationId(),
			Status:      commonpb.CommandStatus_REJECTED,
			Error:       &commonpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, ErrorMessage: "Theft admission was rejected"},
			FarmContext: &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
		}
		result.CompletedRetryDirective = commonpb.RetryDirective_NONE
		return result, nil
	}
	if binding.AdmissionStatus != friend.StatusSucceeded || binding.DispatchOrderID == "" || binding.WorkflowDeadlineAtMs <= 0 {
		return nil, errors.Join(ErrOperationResultInvalidRecord, errors.New("Theft Pair processing binding mismatch"))
	}
	result := lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING)
	result.RetryAfterMs = r.retryAfterMs
	return result, nil
}

// Read is an explicit alias for composition code that names the port by its
// read operation rather than the public Lookup verb.
func (r *OperationResultReader) Read(ctx context.Context, subjectPlayerID uint64, request *publicpb.GetOperationResultRequest) (*publicpb.GetOperationResultResult, error) {
	return r.Lookup(ctx, subjectPlayerID, request)
}

var errOperationResultIdentityMismatch = errors.New("operation result: record identity mismatch")

func operationWorkType(commandType commonpb.CommandType) string {
	if commandType == commonpb.CommandType_UNLOCK_PLOT {
		return string(identity.WorkTypeUnlockPlot)
	}
	if commandType == commonpb.CommandType_HARVEST {
		return string(identity.WorkTypeHarvest)
	}
	if commandType == commonpb.CommandType_THEFT {
		return string(identity.WorkTypeTheft)
	}
	return string(identity.WorkTypePlant)
}

func operationResultDigest(subjectPlayerID uint64, operationID string, targetFarmID uint64, workType string) (string, identity.RootAuthorityKind, string, error) {
	rootKind := identity.RootKindFarmActor
	rootID := strconv.FormatUint(targetFarmID, 10)
	if workType == string(identity.WorkTypeTheft) {
		pairID, err := identity.CanonicalFriendRelationID(subjectPlayerID, targetFarmID)
		if err != nil {
			return "", "", "", errors.Join(ErrOperationResultInvalidLocator, err)
		}
		rootKind, rootID = identity.RootKindFriendPair, pairID
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: subjectPlayerID, StableCommandTypeName: workType,
		OperationID: operationID, Root: identity.OperationRootRef{Kind: rootKind, CanonicalID: rootID}}
	digest, err := key.OperationDigest()
	if err != nil {
		return "", "", "", errors.Join(ErrOperationResultInvalidRequest, err)
	}
	return digest, rootKind, rootID, nil
}

func (r *OperationResultReader) lookupProjectedTerminal(ctx context.Context, subjectPlayerID, targetFarmID uint64,
	request *publicpb.GetOperationResultRequest, operationDigest string, rootKind identity.RootAuthorityKind, rootID string,
	address workidentity.WorkAddress) (*publicpb.GetOperationResultResult, bool) {
	if r == nil || r.resultDirectory == nil {
		return nil, false
	}
	entry, found, err := r.resultDirectory.LookupResult(ctx, subjectPlayerID, operationDigest)
	if err != nil || !found || entry.CommandType != operationWorkType(request.GetOriginalCommandType()) ||
		entry.OperationDigest != operationDigest || entry.RootKind != string(rootKind) || entry.RootID != rootID {
		return nil, false
	}
	binding, err := redisauthority.DecodeResultBinding(entry.MinimalOutcome)
	if err != nil || binding.SubjectPlayerID != subjectPlayerID || binding.CommandType != entry.CommandType ||
		binding.OperationID != request.GetOriginalOperationId() || len(binding.PayloadHash) != 32 {
		return nil, false
	}
	outcome := &publicpb.StoredOperationOutcome{}
	if err := proto.Unmarshal(binding.Outcome, outcome); err != nil {
		return nil, false
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(outcome)
	if err != nil || !bytes.Equal(deterministic, binding.Outcome) ||
		outcome.GetOriginalCommandType() != request.GetOriginalCommandType() ||
		outcome.GetOriginalOperationId() != request.GetOriginalOperationId() ||
		outcome.GetFarmContext().GetTargetFarmId() != targetFarmID || entry.FinalStatusClass != outcome.GetStatus().String() {
		return nil, false
	}
	switch outcome.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_NO_CHANGE, commonpb.CommandStatus_REJECTED:
	default:
		return nil, false
	}
	wantWorkSource := operationResultWorkSource(address)
	wantPairSource := "pair-result-v1:" + operationDigest
	if entry.SourceRef != wantWorkSource && !(request.GetOriginalCommandType() == commonpb.CommandType_THEFT &&
		entry.SourceRef == wantPairSource) {
		return nil, false
	}
	result := lookupResult(request, publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED)
	result.CompletedOutcome = outcome
	result.CompletedRetryDirective = commonpb.RetryDirective_NONE
	return result, true
}

func operationResultWorkSource(address workidentity.WorkAddress) string {
	return strings.Join([]string{"work-address-v1", address.WorkType, address.WorkID,
		strconv.FormatUint(uint64(address.PlacementKind), 10), strconv.FormatUint(uint64(address.BucketVersion), 10),
		strconv.FormatUint(uint64(address.BucketID), 10), strconv.FormatUint(uint64(address.SourceRouteDomain), 10),
		strconv.FormatUint(address.SourceActorID, 10)}, ":")
}

func (r *OperationResultReader) operationAddress(subjectPlayerID uint64, operationID string, targetFarmID uint64, workType string) (string, workidentity.WorkAddress, error) {
	rootKind := identity.RootKindFarmActor
	rootID := strconv.FormatUint(targetFarmID, 10)
	if workType == string(identity.WorkTypeTheft) {
		pairID, pairErr := identity.CanonicalFriendRelationID(subjectPlayerID, targetFarmID)
		if pairErr != nil {
			return "", workidentity.WorkAddress{}, errors.Join(ErrOperationResultInvalidLocator, pairErr)
		}
		rootKind = identity.RootKindFriendPair
		rootID = pairID
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       subjectPlayerID,
		StableCommandTypeName: workType,
		OperationID:           operationID,
		Root:                  identity.OperationRootRef{Kind: rootKind, CanonicalID: rootID},
	}
	workflowID, err := identity.WorkflowID(key)
	if err != nil {
		return "", workidentity.WorkAddress{}, errors.Join(ErrOperationResultInvalidRequest, err)
	}
	bucketID, err := r.bucketResolver.IndependentWorkBucket(workflowID, r.bucketVersion)
	if err != nil {
		return "", workidentity.WorkAddress{}, errors.Join(ErrOperationResultInvalidLocator, err)
	}
	address := workidentity.WorkAddress{
		WorkID:        workflowID,
		WorkType:      workType,
		PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: r.bucketVersion,
		BucketID:      bucketID,
	}
	if err := address.Validate(r.bucketResolver); err != nil {
		return "", workidentity.WorkAddress{}, errors.Join(ErrOperationResultInvalidLocator, err)
	}
	return workflowID, address, nil
}

func (r *OperationResultReader) plantAddress(subjectPlayerID uint64, operationID string, targetFarmID uint64) (string, workidentity.WorkAddress, error) {
	return r.operationAddress(subjectPlayerID, operationID, targetFarmID, string(identity.WorkTypePlant))
}

func (r *OperationResultReader) validateRecordIdentity(record *workflow.WorkRecord, expected workidentity.WorkAddress, subjectPlayerID uint64, operationID string, targetFarmID uint64, commandType commonpb.CommandType) error {
	if record.Address != expected || record.SubjectPlayerID != subjectPlayerID || record.OperationID != operationID ||
		record.Address.WorkType != operationWorkType(commandType) || record.Address.WorkID != expected.WorkID {
		return errOperationResultIdentityMismatch
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if err := record.Address.Validate(r.bucketResolver); err != nil {
		return err
	}
	root, err := decodeOperationRoot(record, subjectPlayerID, targetFarmID, commandType)
	if err != nil {
		return err
	}
	if !operationRootIdentityMatches(root, subjectPlayerID, targetFarmID) {
		return errOperationResultIdentityMismatch
	}
	return nil
}

func operationRootIdentityMatches(root proto.Message, subjectPlayerID, targetFarmID uint64) bool {
	switch root := root.(type) {
	case *contractspb.CommitPlotUnlockPayload:
		return root.GetOwnerPlayerId() == subjectPlayerID && root.GetFarmId() == targetFarmID
	case *contractspb.CommitPlantPayload:
		return root.GetFarmId() == targetFarmID && root.GetOwnerPlayerId() == subjectPlayerID
	case *contractspb.CommitHarvestPayload:
		return root.GetFarmId() == targetFarmID && root.GetOwnerPlayerId() == subjectPlayerID
	case *contractspb.DecideTheftPayload:
		return root.GetFarmId() == targetFarmID && root.GetOwnerPlayerId() == targetFarmID && root.GetVisitorPlayerId() == subjectPlayerID
	default:
		return false
	}
}

func decodeOperationRoot(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64, commandType commonpb.CommandType) (proto.Message, error) {
	if commandType == commonpb.CommandType_THEFT {
		root, err := theftscenario.DecodeRoot(record)
		if err != nil {
			return nil, fmt.Errorf("Theft root: %w", err)
		}
		if root.GetVisitorPlayerId() != subjectPlayerID || root.GetFarmId() != targetFarmID {
			return nil, errOperationResultIdentityMismatch
		}
		return root, nil
	}
	if commandType == commonpb.CommandType_HARVEST {
		return decodeHarvestRoot(record, subjectPlayerID, targetFarmID)
	}
	if commandType == commonpb.CommandType_UNLOCK_PLOT {
		root, err := unlockscenario.DecodeRoot(record)
		if err != nil {
			return nil, fmt.Errorf("UnlockPlot root: %w", err)
		}
		if root.GetOwnerPlayerId() != subjectPlayerID || root.GetFarmId() != targetFarmID {
			return nil, errOperationResultIdentityMismatch
		}
		return root, nil
	}
	return decodePlantRoot(record, subjectPlayerID, targetFarmID)
}

func decodeHarvestRoot(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*contractspb.CommitHarvestPayload, error) {
	root, err := harvestscenario.DecodeRoot(record)
	if err != nil {
		return nil, fmt.Errorf("Harvest root: %w", err)
	}
	if root.GetOwnerPlayerId() != subjectPlayerID || root.GetFarmId() != targetFarmID {
		return nil, errOperationResultIdentityMismatch
	}
	return root, nil
}

func decodePlantRoot(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*contractspb.CommitPlantPayload, error) {
	if record == nil || len(record.Payload) == 0 {
		return nil, errors.New("Plant root payload missing")
	}
	root := &contractspb.CommitPlantPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return nil, fmt.Errorf("Plant root protobuf: %w", err)
	}
	canonical, err := contracts.CanonicalHash(root)
	if err != nil || canonical != record.PayloadHash {
		return nil, errors.New("Plant root payload hash mismatch")
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return nil, errors.New("Plant root payload is not deterministic")
	}
	if root.GetOwnerPlayerId() != subjectPlayerID || root.GetFarmId() != targetFarmID || root.GetPlotId() == 0 ||
		root.GetReservationId() == "" || root.GetCropCycleId() == "" {
		return nil, errOperationResultIdentityMismatch
	}
	wantReservation, err := identity.SeedReservationID(record.Address.WorkID)
	if err != nil || root.GetReservationId() != wantReservation {
		return nil, errOperationResultIdentityMismatch
	}
	wantCropCycle, err := identity.CropCycleID(record.Address.WorkID)
	if err != nil || root.GetCropCycleId() != wantCropCycle {
		return nil, errOperationResultIdentityMismatch
	}
	return root, nil
}

func (r *OperationResultReader) completedPlantOutcome(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*publicpb.StoredOperationOutcome, error) {
	step := plantStep(record, identity.StepCommitPlant)
	if step == nil || (step.Status != workflow.StepApplied && step.Status != workflow.StepNoChange) ||
		len(step.Result) == 0 || step.ResultHash == ([32]byte{}) {
		return nil, errors.New("COMMIT_PLANT result is missing")
	}
	farmResult := &contractspb.CommitPlantResult{}
	if err := proto.Unmarshal(step.Result, farmResult); err != nil {
		return nil, fmt.Errorf("COMMIT_PLANT result protobuf: %w", err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(farmResult)
	if err != nil || !bytes.Equal(deterministic, step.Result) {
		return nil, errors.New("COMMIT_PLANT result is not deterministic")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		return nil, err
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, farmResult)
	if err != nil || resultHash != step.ResultHash {
		return nil, errors.New("COMMIT_PLANT result hash mismatch")
	}
	root, err := decodePlantRoot(record, subjectPlayerID, targetFarmID)
	if err != nil {
		return nil, err
	}
	if farmResult.GetFarmId() != root.GetFarmId() || farmResult.GetPlotId() != root.GetPlotId() ||
		farmResult.GetCropCycleId() != root.GetCropCycleId() || farmResult.GetPlotId() == 0 {
		return nil, errors.New("COMMIT_PLANT result identity mismatch")
	}
	resultPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.PlantResult{
		TargetFarmId: farmResult.GetFarmId(),
		// The Participant contract stores one-based PlotID; the frozen public
		// PlantResult uses the same zero-based plot index as PlantRequest.
		PlotId:               farmResult.GetPlotId() - 1,
		CropCycleId:          farmResult.GetCropCycleId(),
		CommittedFarmVersion: farmResult.GetCommittedFarmVersion(),
	})
	if err != nil {
		return nil, err
	}
	status := commonpb.CommandStatus_SUCCEEDED
	if final := plantStep(record, identity.StepCommitSeedReservation); final != nil && final.Status == workflow.StepNoChange {
		status = commonpb.CommandStatus_NO_CHANGE
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_PLANT,
		OriginalOperationId: record.OperationID,
		Status:              status,
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
		ResultPayload:       resultPayload,
	}, nil
}

func (r *OperationResultReader) completedUnlockPlotOutcome(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*publicpb.StoredOperationOutcome, error) {
	terminal, err := unlockscenario.ReadTerminalResults(record)
	if err != nil {
		return nil, err
	}
	if terminal.Root == nil || terminal.Reserve == nil || terminal.Farm == nil || terminal.Commit == nil {
		return nil, errors.New("UnlockPlot terminal typed receipts are missing")
	}
	if terminal.Root.GetOwnerPlayerId() != subjectPlayerID || terminal.Root.GetFarmId() != targetFarmID || terminal.Farm.GetFarmId() != targetFarmID || terminal.Farm.GetPlotId() != terminal.Root.GetPlotId() {
		return nil, errOperationResultIdentityMismatch
	}
	if terminal.Root.GetPlotId() == 0 || terminal.Farm.GetPlotId() == 0 {
		return nil, errors.New("UnlockPlot terminal plot id is invalid")
	}
	resultPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.UnlockPlotResult{
		FarmId: targetFarmID, PlotId: terminal.Root.GetPlotId() - 1,
		AppliedUnlockRuleVersion: terminal.Farm.GetAppliedUnlockRuleVersion(),
		PaidNormalCoin:           terminal.Root.GetAmountNormalCoin(), NormalCoinBalance: terminal.Reserve.GetAvailableNormalCoinAfter(),
		CommittedPlotVersion: terminal.Farm.GetCommittedPlotVersion(), CommittedFarmVersion: terminal.Farm.GetCommittedFarmVersion(),
	})
	if err != nil {
		return nil, err
	}
	status := commonpb.CommandStatus_SUCCEEDED
	if terminal.FarmStatus == workflow.StepNoChange {
		status = commonpb.CommandStatus_NO_CHANGE
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_UNLOCK_PLOT,
		OriginalOperationId: record.OperationID,
		Status:              status,
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
		ResultPayload:       resultPayload,
	}, nil
}

func (r *OperationResultReader) compensatedUnlockPlotOutcome(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*publicpb.StoredOperationOutcome, bool, error) {
	terminal, err := unlockscenario.ReadTerminalResults(record)
	if err != nil {
		return nil, false, err
	}
	if terminal.FarmStatus != workflow.StepNoChange {
		return nil, false, nil
	}
	if terminal.Root == nil || terminal.Reserve == nil || terminal.Farm == nil || terminal.Release == nil {
		return nil, false, errors.New("UnlockPlot no-change terminal receipts are missing")
	}
	if terminal.Root.GetOwnerPlayerId() != subjectPlayerID || terminal.Root.GetFarmId() != targetFarmID || terminal.Farm.GetFarmId() != targetFarmID || terminal.Farm.GetPlotId() != terminal.Root.GetPlotId() {
		return nil, false, errOperationResultIdentityMismatch
	}
	resultPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.UnlockPlotResult{
		FarmId: targetFarmID, PlotId: terminal.Root.GetPlotId() - 1,
		AppliedUnlockRuleVersion: terminal.Farm.GetAppliedUnlockRuleVersion(),
		PaidNormalCoin:           terminal.Root.GetAmountNormalCoin(), NormalCoinBalance: terminal.Reserve.GetAvailableNormalCoinAfter(),
		CommittedPlotVersion: terminal.Farm.GetCommittedPlotVersion(), CommittedFarmVersion: terminal.Farm.GetCommittedFarmVersion(),
	})
	if err != nil {
		return nil, false, err
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_UNLOCK_PLOT,
		OriginalOperationId: record.OperationID,
		Status:              commonpb.CommandStatus_NO_CHANGE,
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
		ResultPayload:       resultPayload,
	}, true, nil
}

func (r *OperationResultReader) completedHarvestOutcome(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*publicpb.StoredOperationOutcome, error) {
	if record == nil || len(record.Steps) < 2 || record.Steps[0].Name != string(identity.StepCommitHarvest) ||
		record.Steps[0].Status != workflow.StepApplied || record.Steps[1].Name != string(identity.StepGrantHarvestReward) ||
		(record.Steps[1].Status != workflow.StepApplied && record.Steps[1].Status != workflow.StepNoChange) {
		return nil, errors.New("Harvest terminal participant receipts are missing")
	}
	if _, err := decodeHarvestRoot(record, subjectPlayerID, targetFarmID); err != nil {
		return nil, err
	}
	result, err := harvestonline.PublicResult(record)
	if err != nil {
		return nil, fmt.Errorf("Harvest terminal result: %w", err)
	}
	resultPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil {
		return nil, err
	}
	status := commonpb.CommandStatus_SUCCEEDED
	if record.Steps[1].Status == workflow.StepNoChange {
		status = commonpb.CommandStatus_NO_CHANGE
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_HARVEST,
		OriginalOperationId: record.OperationID,
		Status:              status,
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
		ResultPayload:       resultPayload,
	}, nil
}

func (r *OperationResultReader) completedTheftOutcome(record *workflow.WorkRecord, subjectPlayerID, targetFarmID uint64) (*publicpb.StoredOperationOutcome, error) {
	if _, err := decodeOperationRoot(record, subjectPlayerID, targetFarmID, commonpb.CommandType_THEFT); err != nil {
		return nil, err
	}
	outcome, _, err := theftscenario.PublicOutcome(record)
	if err != nil {
		return nil, fmt.Errorf("Theft terminal result: %w", err)
	}
	if outcome.GetFarmContext().GetTargetFarmId() != targetFarmID {
		return nil, errors.New("Theft terminal result target mismatch")
	}
	return outcome, nil
}

func failedOperationOutcome(commandType commonpb.CommandType, operationID string, targetFarmID uint64) *publicpb.StoredOperationOutcome {
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commandType,
		OriginalOperationId: operationID,
		Status:              commonpb.CommandStatus_UNAVAILABLE,
		Error: &commonpb.ErrorDetail{
			ErrorCode:    commonpb.ErrorCode_ERR_INTERNAL,
			ErrorMessage: "Operation reached a failed terminal without a public business outcome; refresh authority state",
		},
		FarmContext: &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
	}
}

func rejectedPlantOutcome(record *workflow.WorkRecord, operationID string, targetFarmID uint64) *publicpb.StoredOperationOutcome {
	code := commonpb.ErrorCode_ERR_INTERNAL
	if class := operationErrorClass(record); class != "" {
		if value, ok := commonpb.ErrorCode_value[class]; ok && value != int32(commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED) {
			code = commonpb.ErrorCode(value)
		}
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_PLANT,
		OriginalOperationId: operationID,
		Status:              commonpb.CommandStatus_REJECTED,
		Error:               &commonpb.ErrorDetail{ErrorCode: code, ErrorMessage: "Plant was rejected"},
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
	}
}

func rejectedUnlockPlotOutcome(record *workflow.WorkRecord, operationID string, targetFarmID uint64) *publicpb.StoredOperationOutcome {
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_UNLOCK_PLOT,
		OriginalOperationId: operationID,
		Status:              commonpb.CommandStatus_REJECTED,
		Error:               &commonpb.ErrorDetail{ErrorCode: operationErrorCode(record), ErrorMessage: "UnlockPlot was rejected"},
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
	}
}

func operationErrorCode(record *workflow.WorkRecord) commonpb.ErrorCode {
	code := commonpb.ErrorCode_ERR_INTERNAL
	class := strings.ToUpper(strings.TrimSpace(operationErrorClass(record)))
	if value, ok := commonpb.ErrorCode_value[class]; ok && value != int32(commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED) {
		return commonpb.ErrorCode(value)
	}
	switch class {
	case "PARTICIPANT_IDEMPOTENCY_CONFLICT", "ERR_IDEMPOTENCY_CONFLICT", "IDEMPOTENCY_CONFLICT":
		return commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	}
	return code
}

func rejectedHarvestOutcome(record *workflow.WorkRecord, operationID string, targetFarmID uint64) *publicpb.StoredOperationOutcome {
	code := commonpb.ErrorCode_ERR_INTERNAL
	if class := operationErrorClass(record); class != "" {
		switch strings.ToUpper(strings.TrimSpace(class)) {
		case "PARTICIPANT_IDEMPOTENCY_CONFLICT", "ERR_IDEMPOTENCY_CONFLICT", "IDEMPOTENCY_CONFLICT":
			code = commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
		default:
			if value, ok := commonpb.ErrorCode_value[class]; ok && value != int32(commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED) {
				code = commonpb.ErrorCode(value)
			}
		}
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_HARVEST,
		OriginalOperationId: operationID,
		Status:              commonpb.CommandStatus_REJECTED,
		Error:               &commonpb.ErrorDetail{ErrorCode: code, ErrorMessage: "Harvest was rejected"},
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
	}
}

func rejectedTheftOutcome(record *workflow.WorkRecord, operationID string, targetFarmID uint64) *publicpb.StoredOperationOutcome {
	code := commonpb.ErrorCode_ERR_INTERNAL
	if class := operationErrorClass(record); class != "" {
		switch strings.ToUpper(strings.TrimSpace(class)) {
		case "PARTICIPANT_IDEMPOTENCY_CONFLICT", "ERR_IDEMPOTENCY_CONFLICT", "IDEMPOTENCY_CONFLICT":
			code = commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
		default:
			if value, ok := commonpb.ErrorCode_value[class]; ok && value != int32(commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED) {
				code = commonpb.ErrorCode(value)
			}
		}
	}
	return &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: operationID,
		Status:              commonpb.CommandStatus_REJECTED,
		Error:               &commonpb.ErrorDetail{ErrorCode: code, ErrorMessage: "Theft was rejected"},
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: targetFarmID},
	}
}

func operationErrorClass(record *workflow.WorkRecord) string {
	if record == nil {
		return ""
	}
	if strings.TrimSpace(record.LastErrorClass) != "" {
		return record.LastErrorClass
	}
	for _, step := range record.Steps {
		if step.Status == workflow.StepRejected && strings.TrimSpace(step.ErrorClass) != "" {
			return step.ErrorClass
		}
	}
	return ""
}

func (r *OperationResultReader) expiredEvidence(record *workflow.WorkRecord) bool {
	if record == nil || record.State != workflow.WorkStateFailed {
		return false
	}
	if operationErrorClass(record) == "DEADLINE_EXCEEDED" {
		return true
	}
	return record.DeadlineAtMs > 0 && r.now != nil && r.now().UnixMilli() >= record.DeadlineAtMs
}

func plantStep(record *workflow.WorkRecord, name identity.ParticipantStep) *workflow.StepRecord {
	if record == nil {
		return nil
	}
	for i := range record.Steps {
		if record.Steps[i].Name == string(name) {
			return &record.Steps[i]
		}
	}
	return nil
}

func lookupResult(request *publicpb.GetOperationResultRequest, status publicpb.OperationLookupStatus) *publicpb.GetOperationResultResult {
	return &publicpb.GetOperationResultResult{
		OriginalCommandType: request.GetOriginalCommandType(),
		OriginalOperationId: request.GetOriginalOperationId(),
		LookupStatus:        status,
	}
}

func plantTargetFarmID(locator *publicpb.OperationTargetLocator) (uint64, error) {
	return operationTargetFarmID(locator, commonpb.CommandType_PLANT)
}

func operationTargetFarmID(locator *publicpb.OperationTargetLocator, commandType commonpb.CommandType) (uint64, error) {
	commandName := "PLANT"
	if commandType == commonpb.CommandType_HARVEST {
		commandName = "HARVEST"
	} else if commandType == commonpb.CommandType_UNLOCK_PLOT {
		commandName = "UNLOCK_PLOT"
	} else if commandType == commonpb.CommandType_CARE {
		commandName = "CARE"
	} else if commandType == commonpb.CommandType_THEFT {
		commandName = "THEFT"
	}
	if locator == nil {
		return 0, fmt.Errorf("%w: original target is required for %s", ErrOperationResultInvalidLocator, commandName)
	}
	target, ok := locator.GetTarget().(*publicpb.OperationTargetLocator_TargetFarmId)
	if !ok || target == nil || target.TargetFarmId == 0 {
		return 0, fmt.Errorf("%w: %s requires target_farm_id", ErrOperationResultInvalidLocator, commandName)
	}
	return target.TargetFarmId, nil
}

func validateOperationID(operationID string) error {
	if len(operationID) == 0 || len(operationID) > 64 {
		return fmt.Errorf("%w: operation_id must be 1..64 printable ASCII bytes", ErrOperationResultMalformedOperationID)
	}
	for i := 0; i < len(operationID); i++ {
		if operationID[i] < 0x21 || operationID[i] > 0x7e {
			return fmt.Errorf("%w: operation_id contains non-printable ASCII at byte %d", ErrOperationResultMalformedOperationID, i)
		}
	}
	return nil
}
