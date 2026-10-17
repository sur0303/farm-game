package scaleplan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/autorebalance"
)

const (
	ManagedRolloutAnnotation         = "scaleplan.farm/managed-rollout-state"
	ManagedRolloutIntentAnnotation   = "scaleplan.farm/rollout-intent"
	ManagedRolloutRecoveryAnnotation = "scaleplan.farm/rollout-recovery"
	RolloutIntentSchemaVersion       = "statefulset-holder-rollout-intent-v1"
	RolloutRecoverySchemaVersion     = "statefulset-holder-rollout-recovery-v1"
)

type RolloutIntent struct {
	SchemaVersion     string `json:"schema_version"`
	ReleaseID         string `json:"release_id"`
	TargetImageDigest string `json:"target_image_digest"`
	ExpectedReplicas  int32  `json:"expected_replicas"`
	DeadlineAtMs      int64  `json:"deadline_at_ms"`
}

type RolloutRecovery struct {
	SchemaVersion string                              `json:"schema_version"`
	CommandID     string                              `json:"command_id"`
	RolloutID     string                              `json:"rollout_id"`
	IntentHash    string                              `json:"intent_hash"`
	Action        autorebalance.RolloutRecoveryAction `json:"action"`
	DeadlineAtMs  int64                               `json:"deadline_at_ms"`
}

type RolloutManager struct {
	client            kubernetes.Interface
	rolloutAnnotation string
	intentAnnotation  string
	desiredAnnotation string
}

func NewRolloutManager(client kubernetes.Interface, rolloutAnnotation, intentAnnotation, desiredAnnotation string) (*RolloutManager, error) {
	rolloutAnnotation = strings.TrimSpace(rolloutAnnotation)
	intentAnnotation = strings.TrimSpace(intentAnnotation)
	desiredAnnotation = strings.TrimSpace(desiredAnnotation)
	if client == nil || rolloutAnnotation == "" || intentAnnotation == "" || desiredAnnotation == "" ||
		rolloutAnnotation == desiredAnnotation || rolloutAnnotation == intentAnnotation || intentAnnotation == desiredAnnotation ||
		rolloutAnnotation == ManagedRolloutRecoveryAnnotation || intentAnnotation == ManagedRolloutRecoveryAnnotation ||
		desiredAnnotation == ManagedRolloutRecoveryAnnotation {
		return nil, fmt.Errorf("%w: rollout manager", core.ErrInvalidPlan)
	}
	return &RolloutManager{client: client, rolloutAnnotation: rolloutAnnotation,
		intentAnnotation: intentAnnotation, desiredAnnotation: desiredAnnotation}, nil
}

var _ autorebalance.RolloutControl = (*RolloutManager)(nil)

func (m *RolloutManager) Observe(ctx context.Context, ref core.WorkloadRef) (autorebalance.RolloutState, bool, error) {
	set, err := m.get(ctx, ref)
	if err != nil {
		return autorebalance.RolloutState{}, false, err
	}
	raw, found := set.Annotations[m.rolloutAnnotation]
	if !found {
		return autorebalance.RolloutState{}, false, nil
	}
	state, err := decodeRolloutState(raw)
	return state, true, err
}

func (m *RolloutManager) ObserveRecovery(ctx context.Context, ref core.WorkloadRef) (autorebalance.RolloutRecoveryRequest, bool, error) {
	set, err := m.get(ctx, ref)
	if err != nil {
		return autorebalance.RolloutRecoveryRequest{}, false, err
	}
	raw, found := set.Annotations[ManagedRolloutRecoveryAnnotation]
	if !found {
		return autorebalance.RolloutRecoveryRequest{}, false, nil
	}
	recovery, err := decodeRolloutRecovery(raw)
	if err != nil {
		return autorebalance.RolloutRecoveryRequest{}, true, err
	}
	return recovery.request(), true, nil
}

func (m *RolloutManager) Begin(ctx context.Context, ref core.WorkloadRef, observed core.WorkloadState, targetRevision string) (autorebalance.RolloutState, error) {
	targetRevision = strings.TrimSpace(targetRevision)
	if observed.Ref != ref || observed.UID == "" || observed.ResourceVersion == "" || observed.Replicas < 1 ||
		targetRevision == "" || observed.UpdateStrategy != string(appsv1.OnDeleteStatefulSetStrategyType) {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout begin observation", core.ErrConflict)
	}
	set, err := m.get(ctx, ref)
	if err != nil {
		return autorebalance.RolloutState{}, err
	}
	if string(set.UID) != observed.UID || set.ResourceVersion != observed.ResourceVersion ||
		replicasOf(set) != observed.Replicas || set.Status.UpdateRevision != targetRevision ||
		set.Status.CurrentRevision == "" || set.Status.CurrentRevision == targetRevision ||
		set.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout begin changed", core.ErrConflict)
	}
	if _, exists := set.Annotations[m.rolloutAnnotation]; exists {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout already exists", core.ErrConflict)
	}
	if _, exists := set.Annotations[ManagedRolloutRecoveryAnnotation]; exists {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: stale rollout recovery command", core.ErrConflict)
	}
	intentRaw, exists := set.Annotations[m.intentAnnotation]
	if !exists {
		// A revision mismatch alone is not release authorization. The caller
		// treats ErrNoPlan as a deliberate no-op until a publisher writes an
		// explicit, immutable rollout intent.
		return autorebalance.RolloutState{}, autorebalance.ErrNoPlan
	}
	intent, intentHash, err := decodeRolloutIntent(intentRaw)
	if err != nil {
		return autorebalance.RolloutState{}, err
	}
	if intent.ExpectedReplicas != observed.Replicas || intent.DeadlineAtMs <= time.Now().UnixMilli() ||
		!statefulSetTargetsDigest(set, ref, intent.TargetImageDigest) {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout intent does not match workload", core.ErrConflict)
	}
	nowMs := time.Now().UnixMilli()
	state := autorebalance.RolloutState{
		SchemaVersion: autorebalance.RolloutSchemaVersion,
		RolloutID:     deterministicRolloutID(observed.UID, targetRevision, observed.Replicas, intentHash),
		ReleaseID:     intent.ReleaseID, WorkloadUID: observed.UID,
		SourceRevision: set.Status.CurrentRevision, TargetRevision: targetRevision,
		TargetImageDigest: intent.TargetImageDigest, ExpectedReplicas: intent.ExpectedReplicas,
		OriginalReplicas: observed.Replicas, StartedAtMs: nowMs, DeadlineAtMs: intent.DeadlineAtMs,
		IntentHash: intentHash, Phase: autorebalance.RolloutPhaseExpanding, VictimOrdinal: -1,
	}
	if err := state.Validate(); err != nil {
		return autorebalance.RolloutState{}, err
	}
	raw, _ := json.Marshal(state)
	if set.Annotations == nil {
		set.Annotations = make(map[string]string)
	}
	set.Annotations[m.rolloutAnnotation] = string(raw)
	if _, err := m.client.AppsV1().StatefulSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{}); err != nil {
		return autorebalance.RolloutState{}, err
	}
	return state, nil
}

func (m *RolloutManager) Recover(ctx context.Context, ref core.WorkloadRef, expected autorebalance.RolloutState,
	request autorebalance.RolloutRecoveryRequest, next autorebalance.RolloutState) (autorebalance.RolloutState, error) {
	if err := request.Validate(); err != nil || request.RolloutID != expected.RolloutID || request.IntentHash != expected.IntentHash ||
		request.DeadlineAtMs <= time.Now().UnixMilli() {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout recovery request", core.ErrConflict)
	}
	if request.Action == autorebalance.RolloutRecoveryResume {
		resumed, resumeErr := autorebalance.ResumeBlockedRolloutWithDeadline(expected, request.DeadlineAtMs)
		if resumeErr != nil || !reflect.DeepEqual(resumed, next) {
			if resumeErr != nil {
				return autorebalance.RolloutState{}, resumeErr
			}
			return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout recovery checkpoint", core.ErrConflict)
		}
	} else if err := autorebalance.ValidateRolloutRecoveryUpdate(expected, next, request.Action); err != nil {
		return autorebalance.RolloutState{}, err
	}
	set, err := m.get(ctx, ref)
	if err != nil {
		return autorebalance.RolloutState{}, err
	}
	if request.Action == autorebalance.RolloutRecoveryAbort {
		err = m.validateAbortSet(set, ref, expected)
	} else {
		err = m.validateManagedSet(set, ref, expected)
	}
	if err != nil {
		return autorebalance.RolloutState{}, err
	}
	expectedRaw, _ := json.Marshal(expected)
	if set.Annotations[m.rolloutAnnotation] != string(expectedRaw) {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout annotation changed", core.ErrRevisionConflict)
	}
	recoveryRaw, found := set.Annotations[ManagedRolloutRecoveryAnnotation]
	if !found {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout recovery command missing", core.ErrConflict)
	}
	recovery, err := decodeRolloutRecovery(recoveryRaw)
	if err != nil || recovery.request() != request {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout recovery command changed", core.ErrRevisionConflict)
	}
	nextRaw, _ := json.Marshal(next)
	set.Annotations[m.rolloutAnnotation] = string(nextRaw)
	delete(set.Annotations, ManagedRolloutRecoveryAnnotation)
	if _, err := m.client.AppsV1().StatefulSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{}); err != nil {
		return autorebalance.RolloutState{}, err
	}
	return next, nil
}

func (m *RolloutManager) Update(ctx context.Context, ref core.WorkloadRef, expected, next autorebalance.RolloutState) (autorebalance.RolloutState, error) {
	if err := autorebalance.ValidateRolloutUpdate(expected, next); err != nil {
		return autorebalance.RolloutState{}, err
	}
	set, err := m.get(ctx, ref)
	if err != nil {
		return autorebalance.RolloutState{}, err
	}
	if err := m.validateManagedSet(set, ref, expected); err != nil {
		return autorebalance.RolloutState{}, err
	}
	current, found := set.Annotations[m.rolloutAnnotation]
	expectedRaw, _ := json.Marshal(expected)
	if !found || current != string(expectedRaw) {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout annotation changed", core.ErrRevisionConflict)
	}
	nextRaw, _ := json.Marshal(next)
	set.Annotations[m.rolloutAnnotation] = string(nextRaw)
	if _, err := m.client.AppsV1().StatefulSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{}); err != nil {
		return autorebalance.RolloutState{}, err
	}
	return next, nil
}

func (m *RolloutManager) EnsureDesiredReplicas(ctx context.Context, ref core.WorkloadRef, rollout autorebalance.RolloutState, desired int32) error {
	if err := rollout.Validate(); err != nil || desired < 1 {
		return core.ErrInvalidPlan
	}
	set, err := m.get(ctx, ref)
	if err != nil {
		return err
	}
	if rollout.Phase == autorebalance.RolloutPhaseAborting {
		err = m.validateAbortSet(set, ref, rollout)
	} else {
		err = m.validateManagedSet(set, ref, rollout)
	}
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(rollout)
	if set.Annotations[m.rolloutAnnotation] != string(raw) {
		return fmt.Errorf("%w: rollout annotation changed", core.ErrRevisionConflict)
	}
	// EXPANDING is a managed-rollout surge, not an ordinary capacity scale-out.
	// The extra Pod must remain READY_ZERO_SHARD so the rollout can pin one old
	// Holder's complete Shard set to that exact replacement generation. Routing
	// the request through SafeDesiredReplicas would start the ordinary ScalePlan
	// rebalance and could assign a Shard to the surge Pod before DRAIN_HOLDER.
	// Adding one Pod is non-destructive, so the rollout checkpoint itself owns a
	// UID/resourceVersion-fenced StatefulSet update here. A retry observes the
	// desired replica count and is idempotent.
	if rollout.Phase == autorebalance.RolloutPhaseExpanding {
		if desired != rollout.OriginalReplicas+1 {
			return fmt.Errorf("%w: rollout surge must add exactly one replica", core.ErrConflict)
		}
		if _, exists := set.Annotations[m.desiredAnnotation]; exists {
			return fmt.Errorf("%w: ordinary scale intent conflicts with rollout surge", core.ErrConflict)
		}
		current := replicasOf(set)
		if current == desired {
			return nil
		}
		if current != rollout.OriginalReplicas {
			return fmt.Errorf("%w: rollout surge replica drift", core.ErrConflict)
		}
		updated := set.DeepCopy()
		updated.Spec.Replicas = new(int32)
		*updated.Spec.Replicas = desired
		_, err := m.client.AppsV1().StatefulSets(ref.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	}
	want := strconv.FormatInt(int64(desired), 10)
	if current, exists := set.Annotations[m.desiredAnnotation]; exists {
		if current == want {
			return nil
		}
		return fmt.Errorf("%w: safe desired replicas owned by another intent", core.ErrConflict)
	}
	set.Annotations[m.desiredAnnotation] = want
	_, err = m.client.AppsV1().StatefulSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{})
	return err
}

func (m *RolloutManager) DeleteVictimPod(ctx context.Context, ref core.WorkloadRef, rollout autorebalance.RolloutState) (bool, error) {
	if err := rollout.Validate(); err != nil || rollout.Phase != autorebalance.RolloutPhaseDeleting {
		return false, core.ErrInvalidPlan
	}
	set, err := m.get(ctx, ref)
	if err != nil {
		return false, err
	}
	if err := m.validateManagedSet(set, ref, rollout); err != nil {
		return false, err
	}
	if replicasOf(set) != rollout.OriginalReplicas+1 || set.Status.UpdateRevision != rollout.TargetRevision {
		return false, fmt.Errorf("%w: rollout delete workload drift", core.ErrConflict)
	}
	raw, _ := json.Marshal(rollout)
	if set.Annotations[m.rolloutAnnotation] != string(raw) {
		return false, fmt.Errorf("%w: rollout delete checkpoint changed", core.ErrRevisionConflict)
	}
	name := fmt.Sprintf("%s-%d", ref.Name, rollout.VictimOrdinal)
	pod, err := m.client.CoreV1().Pods(ref.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if string(pod.UID) != rollout.VictimPodUID {
		return true, nil
	}
	if !podOwnedByWorkload(pod, set) || pod.Name != fmt.Sprintf("%s-%d", set.Name, rollout.VictimOrdinal) {
		return false, fmt.Errorf("%w: rollout victim owner identity", core.ErrConflict)
	}
	uid := types.UID(rollout.VictimPodUID)
	err = m.client.CoreV1().Pods(ref.Namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return false, nil
	}
	return false, err
}

func (m *RolloutManager) Complete(ctx context.Context, ref core.WorkloadRef, rollout autorebalance.RolloutState) error {
	if err := rollout.Validate(); err != nil ||
		(rollout.Phase != autorebalance.RolloutPhaseContracting && rollout.Phase != autorebalance.RolloutPhaseAborting) {
		return core.ErrInvalidPlan
	}
	set, err := m.get(ctx, ref)
	if err != nil {
		return err
	}
	if rollout.Phase == autorebalance.RolloutPhaseAborting {
		err = m.validateAbortSet(set, ref, rollout)
	} else {
		err = m.validateManagedSet(set, ref, rollout)
	}
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(rollout)
	if set.Annotations[m.rolloutAnnotation] != string(raw) {
		return fmt.Errorf("%w: rollout completion checkpoint changed", core.ErrRevisionConflict)
	}
	delete(set.Annotations, m.rolloutAnnotation)
	delete(set.Annotations, m.intentAnnotation)
	delete(set.Annotations, ManagedRolloutRecoveryAnnotation)
	_, err = m.client.AppsV1().StatefulSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{})
	return err
}

func (m *RolloutManager) validateAbortSet(set *appsv1.StatefulSet, ref core.WorkloadRef, rollout autorebalance.RolloutState) error {
	if set == nil || string(set.UID) != rollout.WorkloadUID || set.Name != ref.Name || set.Namespace != ref.Namespace ||
		set.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		return fmt.Errorf("%w: abort workload identity changed", core.ErrConflict)
	}
	raw, found := set.Annotations[m.intentAnnotation]
	if !found {
		return fmt.Errorf("%w: rollout intent missing", core.ErrConflict)
	}
	intent, hash, err := decodeRolloutIntent(raw)
	if err != nil {
		return err
	}
	if hash != rollout.IntentHash || intent.ReleaseID != rollout.ReleaseID ||
		intent.TargetImageDigest != rollout.TargetImageDigest || intent.ExpectedReplicas != rollout.ExpectedReplicas ||
		intent.DeadlineAtMs != rollout.DeadlineAtMs {
		return fmt.Errorf("%w: rollout intent changed", core.ErrConflict)
	}
	return nil
}

func decodeRolloutRecovery(raw string) (RolloutRecovery, error) {
	if raw == "" || len(raw) > 4*1024 {
		return RolloutRecovery{}, fmt.Errorf("%w: rollout recovery size", core.ErrConflict)
	}
	var recovery RolloutRecovery
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&recovery); err != nil {
		return RolloutRecovery{}, fmt.Errorf("%w: rollout recovery: %v", core.ErrConflict, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return RolloutRecovery{}, fmt.Errorf("%w: trailing rollout recovery", core.ErrConflict)
	}
	if recovery.SchemaVersion != RolloutRecoverySchemaVersion {
		return RolloutRecovery{}, fmt.Errorf("%w: rollout recovery schema", core.ErrConflict)
	}
	if err := recovery.request().Validate(); err != nil {
		return RolloutRecovery{}, err
	}
	canonical, _ := json.Marshal(recovery)
	if string(canonical) != raw {
		return RolloutRecovery{}, fmt.Errorf("%w: non-canonical rollout recovery", core.ErrConflict)
	}
	return recovery, nil
}

func (r RolloutRecovery) request() autorebalance.RolloutRecoveryRequest {
	return autorebalance.RolloutRecoveryRequest{
		CommandID: r.CommandID, RolloutID: r.RolloutID, IntentHash: r.IntentHash,
		Action: r.Action, DeadlineAtMs: r.DeadlineAtMs,
	}
}

func (m *RolloutManager) get(ctx context.Context, ref core.WorkloadRef) (*appsv1.StatefulSet, error) {
	if m == nil || m.client == nil || ctx == nil {
		return nil, core.ErrInvalidPlan
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	set, err := m.client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if set == nil || set.UID == "" || set.ResourceVersion == "" || set.Spec.Replicas == nil {
		return nil, fmt.Errorf("%w: incomplete StatefulSet", core.ErrConflict)
	}
	return set.DeepCopy(), nil
}

func decodeRolloutState(raw string) (autorebalance.RolloutState, error) {
	if raw == "" || len(raw) > 16*1024 {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout annotation size", core.ErrConflict)
	}
	var state autorebalance.RolloutState
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: rollout annotation: %v", core.ErrConflict, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: trailing rollout data", core.ErrConflict)
	}
	if err := state.Validate(); err != nil {
		return autorebalance.RolloutState{}, err
	}
	canonical, _ := json.Marshal(state)
	if string(canonical) != raw {
		return autorebalance.RolloutState{}, fmt.Errorf("%w: non-canonical rollout annotation", core.ErrConflict)
	}
	return state, nil
}

func deterministicRolloutID(workloadUID, targetRevision string, replicas int32, intentHash string) string {
	sum := sha256.Sum256([]byte("statefulset-holder-rollout-v2\x00" + workloadUID + "\x00" + targetRevision + "\x00" + strconv.FormatInt(int64(replicas), 10) + "\x00" + intentHash))
	return "rollout-v2-" + hex.EncodeToString(sum[:16])
}

func decodeRolloutIntent(raw string) (RolloutIntent, string, error) {
	if raw == "" || len(raw) > 4*1024 {
		return RolloutIntent{}, "", fmt.Errorf("%w: rollout intent size", core.ErrConflict)
	}
	var intent RolloutIntent
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&intent); err != nil {
		return RolloutIntent{}, "", fmt.Errorf("%w: rollout intent: %v", core.ErrConflict, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return RolloutIntent{}, "", fmt.Errorf("%w: trailing rollout intent", core.ErrConflict)
	}
	if intent.SchemaVersion != RolloutIntentSchemaVersion || strings.TrimSpace(intent.ReleaseID) == "" ||
		len(intent.ReleaseID) > 128 || !validSHA256Digest(intent.TargetImageDigest) ||
		intent.ExpectedReplicas < 1 || intent.DeadlineAtMs <= 0 {
		return RolloutIntent{}, "", fmt.Errorf("%w: rollout intent fields", core.ErrConflict)
	}
	canonical, _ := json.Marshal(intent)
	sum := sha256.Sum256(canonical)
	return intent, hex.EncodeToString(sum[:]), nil
}

func (m *RolloutManager) validateManagedSet(set *appsv1.StatefulSet, ref core.WorkloadRef, rollout autorebalance.RolloutState) error {
	if set == nil || string(set.UID) != rollout.WorkloadUID || set.Name != ref.Name || set.Namespace != ref.Namespace ||
		set.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType ||
		set.Status.UpdateRevision != rollout.TargetRevision || !statefulSetTargetsDigest(set, ref, rollout.TargetImageDigest) {
		return fmt.Errorf("%w: managed rollout workload identity changed", core.ErrConflict)
	}
	raw, found := set.Annotations[m.intentAnnotation]
	if !found {
		return fmt.Errorf("%w: rollout intent missing", core.ErrConflict)
	}
	intent, hash, err := decodeRolloutIntent(raw)
	if err != nil {
		return err
	}
	// A RESUME recovery may explicitly extend the durable rollout deadline.
	// The rollout intent itself remains immutable (the hash still has to match),
	// so its original deadline may be earlier than the resumed state deadline.
	// Any other intent mutation changes the hash and remains fail-closed.
	if hash != rollout.IntentHash || intent.ReleaseID != rollout.ReleaseID ||
		intent.TargetImageDigest != rollout.TargetImageDigest || intent.ExpectedReplicas != rollout.ExpectedReplicas ||
		intent.DeadlineAtMs > rollout.DeadlineAtMs {
		return fmt.Errorf("%w: rollout intent changed", core.ErrConflict)
	}
	return nil
}

func statefulSetTargetsDigest(set *appsv1.StatefulSet, ref core.WorkloadRef, digest string) bool {
	if set == nil || !validSHA256Digest(digest) {
		return false
	}
	for _, container := range set.Spec.Template.Spec.Containers {
		if container.Name == ref.Name {
			return strings.HasSuffix(strings.TrimSpace(container.Image), "@"+digest)
		}
	}
	return false
}

func podOwnedByWorkload(pod *corev1.Pod, set *appsv1.StatefulSet) bool {
	if pod == nil || set == nil || pod.Namespace != set.Namespace {
		return false
	}
	controllers := 0
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller {
			controllers++
			if owner.APIVersion != "apps/v1" || owner.Kind != "StatefulSet" || owner.Name != set.Name || owner.UID != set.UID {
				return false
			}
		}
	}
	return controllers == 1
}

func validSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, ch := range strings.TrimPrefix(value, "sha256:") {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
