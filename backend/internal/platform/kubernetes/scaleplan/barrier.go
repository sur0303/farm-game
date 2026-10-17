package scaleplan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	core "farmgame/internal/scaleplan"
)

type BarrierStatus string

const (
	BarrierPending          BarrierStatus = "PENDING"
	BarrierCommitted        BarrierStatus = "COMMITTED"
	BarrierNotCommitted     BarrierStatus = "NOT_COMMITTED"
	BarrierAdoptedCommitted BarrierStatus = "ADOPTED_COMMITTED"
	BarrierSuperseded       BarrierStatus = "SUPERSEDED"
	BarrierConflict         BarrierStatus = "CONFLICT"
	BarrierUnknown          BarrierStatus = "UNKNOWN"
)

type PendingScaleOperation struct {
	Ref                     core.WorkloadRef
	WorkloadUID             string
	ExpectedResourceVersion string
	// ResourceVersion is accepted as a readable alias for callers that persist
	// the opaque precondition under the shorter Kubernetes field name.
	ResourceVersion  string
	ExpectedReplicas int32
	DesiredReplicas  int32
	OperationID      string
	FenceEpoch       string
	AttemptNonce     string
	// Nonce is an alias for AttemptNonce when decoding an external operation
	// record.
	Nonce              string
	ExpectedGeneration int64
}

type BarrierResult struct {
	// Status is the scale operation disposition. BarrierOperationStatus is the
	// disposition of the annotation write itself.
	Status                 BarrierStatus
	BarrierOperationStatus BarrierStatus
	OwnershipRetained      bool
	Operation              PendingScaleOperation
	State                  core.WorkloadState
	NextAttemptNonce       string
	NextResourceVersion    string
}

func (a *Adapter) CancelPendingScale(
	ctx context.Context,
	ref core.WorkloadRef,
	precondition core.ScalePrecondition,
	desired int32,
	operationID string,
	fenceEpoch string,
) (BarrierResult, error) {
	return a.ResolvePendingScale(ctx, PendingScaleOperation{
		Ref:                     ref,
		WorkloadUID:             precondition.WorkloadUID,
		ExpectedResourceVersion: precondition.ResourceVersion,
		ResourceVersion:         precondition.ResourceVersion,
		ExpectedReplicas:        precondition.ExpectedReplicas,
		DesiredReplicas:         desired,
		OperationID:             operationID,
		FenceEpoch:              fenceEpoch,
	})
}

// ResolvePendingScale is a bounded convenience loop. A durable caller should
// persist result.Operation plus NextAttemptNonce/NextResourceVersion before
// issuing the next attempt. It never returns a terminal disposition when a
// barrier proof is unavailable.
func (a *Adapter) ResolvePendingScale(ctx context.Context, operation PendingScaleOperation) (BarrierResult, error) {
	operation, err := normalizeOperation(operation)
	if err != nil {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}
	attempts := defaultBarrierAttempts
	if a != nil && a.barrierMaxAttempts > 0 {
		attempts = a.barrierMaxAttempts
	}
	var result BarrierResult
	for i := 0; i < attempts; i++ {
		result, err = a.ApplyCancellationBarrier(ctx, operation)
		if result.Operation.ExpectedResourceVersion != "" {
			operation = result.Operation
		}
		if result.NextResourceVersion != "" {
			operation.ExpectedResourceVersion = result.NextResourceVersion
			operation.ResourceVersion = result.NextResourceVersion
		}
		if result.NextAttemptNonce != "" {
			operation.AttemptNonce = result.NextAttemptNonce
			operation.Nonce = result.NextAttemptNonce
		}
		if result.Status != BarrierPending || ctx == nil || ctx.Err() != nil {
			return result, err
		}
	}
	if result.Status == "" {
		result = pendingBarrier(operation)
	}
	result.Status = BarrierPending
	result.BarrierOperationStatus = BarrierPending
	result.OwnershipRetained = true
	result.Operation = operation
	return result, unknownOutcome(ErrBarrierNotEstablished)
}

// ApplyCancellationBarrier performs one exact-RV annotation barrier attempt.
// The patch is deliberately merge-patch metadata only; it never includes spec
// or spec.template fields.
func (a *Adapter) ApplyCancellationBarrier(ctx context.Context, operation PendingScaleOperation) (BarrierResult, error) {
	operation, err := normalizeOperation(operation)
	if err != nil {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}
	result := pendingBarrier(operation)
	if err := a.validate(ctx, operation.Ref); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return result, unknownOutcome(ctx.Err())
		}
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}

	start, err := a.getStatefulSet(ctx, operation.Ref)
	if err != nil {
		if isUnknownReadError(err) {
			return result, unknownOutcome(err)
		}
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}
	if string(start.UID) != operation.WorkloadUID {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, fmt.Errorf("%w: barrier workload UID changed", core.ErrConflict)
	}

	currentRV := operation.expectedRV()
	if start.ResourceVersion != currentRV {
		if replicasOf(start) == operation.DesiredReplicas && start.ResourceVersion != currentRV {
			return completedBarrier(operation, start, BarrierAdoptedCommitted, BarrierSuperseded), nil
		}
		if replicasOf(start) == operation.ExpectedReplicas {
			result.BarrierOperationStatus = BarrierSuperseded
			result.NextResourceVersion = start.ResourceVersion
			result.NextAttemptNonce, err = a.newNonce(operation.attemptNonce())
			if err != nil {
				return result, unknownOutcome(err)
			}
			return result, unknownOutcome(ErrBarrierNotEstablished)
		}
		return unknownBarrier(operation, start, BarrierSuperseded), unknownOutcome(ErrBarrierNotEstablished)
	}

	nonce := operation.attemptNonce()
	if nonce == "" {
		nonce, err = a.newNonce()
		if err != nil {
			return result, unknownOutcome(err)
		}
		operation.AttemptNonce = nonce
		operation.Nonce = nonce
		result.Operation = operation
	}

	payload, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"resourceVersion": currentRV,
			"annotations": map[string]string{
				BarrierOperationAnnotation: operation.OperationID,
				BarrierFenceAnnotation:     operation.FenceEpoch,
				BarrierNonceAnnotation:     nonce,
			},
		},
	})
	if err != nil {
		return result, unknownOutcome(err)
	}

	patched, patchErr := a.client.AppsV1().StatefulSets(operation.Ref.Namespace).Patch(
		ctx,
		operation.Ref.Name,
		types.MergePatchType,
		payload,
		metav1.PatchOptions{},
	)
	if patchErr != nil {
		if apierrors.IsConflict(patchErr) {
			return a.resolveBarrierConflict(ctx, operation)
		}
		if apierrors.IsNotFound(patchErr) {
			return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, patchErr
		}
		return result, unknownOutcome(patchErr)
	}
	if patched == nil {
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
	if patched.UID != start.UID {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, fmt.Errorf("%w: barrier response UID changed", core.ErrConflict)
	}
	if patched.Generation != start.Generation {
		return BarrierResult{Status: BarrierUnknown, BarrierOperationStatus: BarrierUnknown, OwnershipRetained: true, Operation: operation}, unknownOutcome(ErrBarrierNotEstablished)
	}
	if patched.ResourceVersion == "" {
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
	if patched.ResourceVersion == currentRV {
		result.BarrierOperationStatus = BarrierNotCommitted
		result.Operation = operation
		result.NextAttemptNonce, err = a.newNonce(nonce)
		if err != nil {
			return result, unknownOutcome(err)
		}
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}

	barrierStatus := BarrierCommitted
	if patched.Annotations[BarrierNonceAnnotation] != nonce {
		barrierStatus = BarrierSuperseded
	}
	return a.classifyBarrierObject(operation, patched, barrierStatus)
}

func (a *Adapter) ResolveBarrierOperation(ctx context.Context, operation PendingScaleOperation) (BarrierResult, error) {
	operation, err := normalizeOperation(operation)
	if err != nil {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}
	result := pendingBarrier(operation)
	if err := a.validate(ctx, operation.Ref); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return result, unknownOutcome(ctx.Err())
		}
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}
	current, err := a.getStatefulSet(ctx, operation.Ref)
	if err != nil {
		if isUnknownReadError(err) {
			return result, unknownOutcome(err)
		}
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, err
	}
	if string(current.UID) != operation.WorkloadUID {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, fmt.Errorf("%w: barrier workload UID changed", core.ErrConflict)
	}

	if current.ResourceVersion == operation.expectedRV() {
		result.BarrierOperationStatus = BarrierNotCommitted
		result.NextAttemptNonce, err = a.newNonce(operation.attemptNonce())
		if err != nil {
			return result, unknownOutcome(err)
		}
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
	if operation.attemptNonce() != "" && current.Annotations[BarrierNonceAnnotation] == operation.attemptNonce() {
		return a.classifyBarrierObject(operation, current, BarrierCommitted)
	}
	return a.classifyBarrierObject(operation, current, BarrierSuperseded)
}

func (a *Adapter) resolveBarrierConflict(ctx context.Context, operation PendingScaleOperation) (BarrierResult, error) {
	current, err := a.getStatefulSet(ctx, operation.Ref)
	if err != nil {
		return pendingBarrier(operation), unknownOutcome(preferError(err, ErrBarrierNotEstablished))
	}
	if string(current.UID) != operation.WorkloadUID {
		return BarrierResult{Status: BarrierConflict, BarrierOperationStatus: BarrierConflict, OwnershipRetained: true, Operation: operation}, fmt.Errorf("%w: barrier workload UID changed", core.ErrConflict)
	}
	if replicasOf(current) == operation.DesiredReplicas {
		return completedBarrier(operation, current, BarrierAdoptedCommitted, BarrierSuperseded), nil
	}
	if replicasOf(current) == operation.ExpectedReplicas {
		result := pendingBarrier(operation)
		result.BarrierOperationStatus = BarrierSuperseded
		result.NextResourceVersion = current.ResourceVersion
		result.NextAttemptNonce, err = a.newNonce(operation.attemptNonce())
		if err != nil {
			return result, unknownOutcome(err)
		}
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
	return unknownBarrier(operation, current, BarrierSuperseded), unknownOutcome(ErrBarrierNotEstablished)
}

func (a *Adapter) classifyBarrierObject(operation PendingScaleOperation, object *appsv1.StatefulSet, barrierStatus BarrierStatus) (BarrierResult, error) {
	result := pendingBarrier(operation)
	result.BarrierOperationStatus = barrierStatus
	result.State = workloadState(operation.Ref, object, nil)
	if object.ResourceVersion == operation.expectedRV() {
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
	if barrierStatus == BarrierCommitted && (operation.attemptNonce() == "" || object.Annotations[BarrierNonceAnnotation] != operation.attemptNonce()) {
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
	replicas := result.State.Replicas
	switch replicas {
	case operation.ExpectedReplicas:
		if barrierStatus == BarrierCommitted {
			result.Status = BarrierNotCommitted
			result.OwnershipRetained = false
			return result, nil
		}
		result.Status = BarrierPending
		result.NextResourceVersion = object.ResourceVersion
		result.NextAttemptNonce, _ = a.newNonce(operation.attemptNonce())
		return result, unknownOutcome(ErrBarrierNotEstablished)
	case operation.DesiredReplicas:
		result.Status = BarrierAdoptedCommitted
		result.OwnershipRetained = false
		return result, nil
	default:
		result.Status = BarrierUnknown
		return result, unknownOutcome(ErrBarrierNotEstablished)
	}
}

func completedBarrier(operation PendingScaleOperation, object *appsv1.StatefulSet, status, barrierStatus BarrierStatus) BarrierResult {
	result := pendingBarrier(operation)
	result.Status = status
	result.BarrierOperationStatus = barrierStatus
	result.OwnershipRetained = false
	result.State = workloadState(operation.Ref, object, nil)
	return result
}

func unknownBarrier(operation PendingScaleOperation, object *appsv1.StatefulSet, barrierStatus BarrierStatus) BarrierResult {
	result := pendingBarrier(operation)
	result.Status = BarrierUnknown
	result.BarrierOperationStatus = barrierStatus
	result.State = workloadState(operation.Ref, object, nil)
	return result
}

func pendingBarrier(operation PendingScaleOperation) BarrierResult {
	return BarrierResult{
		Status:                 BarrierPending,
		BarrierOperationStatus: BarrierPending,
		OwnershipRetained:      true,
		Operation:              operation,
	}
}

func (a *Adapter) newNonce(previous ...string) (string, error) {
	if a == nil || a.nonce == nil {
		return "", errors.New("barrier nonce generator is nil")
	}
	nonce := a.nonce()
	if nonce == "" {
		return "", errors.New("barrier nonce generator returned empty nonce")
	}
	if len(previous) > 0 && nonce == previous[0] {
		nonce += "-next"
	}
	return nonce, nil
}

func normalizeOperation(operation PendingScaleOperation) (PendingScaleOperation, error) {
	if operation.ExpectedResourceVersion == "" {
		operation.ExpectedResourceVersion = operation.ResourceVersion
	}
	if operation.ResourceVersion == "" {
		operation.ResourceVersion = operation.ExpectedResourceVersion
	}
	if operation.AttemptNonce == "" {
		operation.AttemptNonce = operation.Nonce
	}
	if operation.Nonce == "" {
		operation.Nonce = operation.AttemptNonce
	}
	if err := operation.Ref.Validate(); err != nil {
		return operation, err
	}
	if operation.WorkloadUID == "" || operation.ExpectedResourceVersion == "" ||
		operation.OperationID == "" || operation.FenceEpoch == "" ||
		operation.ExpectedReplicas < 0 || operation.DesiredReplicas < 0 ||
		operation.ExpectedReplicas == operation.DesiredReplicas {
		return operation, fmt.Errorf("%w: invalid pending scale operation", core.ErrInvalidPlan)
	}
	return operation, nil
}

func (operation PendingScaleOperation) expectedRV() string {
	if operation.ExpectedResourceVersion != "" {
		return operation.ExpectedResourceVersion
	}
	return operation.ResourceVersion
}

func (operation PendingScaleOperation) attemptNonce() string {
	if operation.AttemptNonce != "" {
		return operation.AttemptNonce
	}
	return operation.Nonce
}
