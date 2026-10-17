// Package autorebalance turns one exact workload/placement observation into
// the explicit move manifest consumed by scaleplan. It deliberately owns no
// Kubernetes or RouteControl mutations; the existing durable ScalePlan and
// RoutePlan state machines remain the only mutation path.
package autorebalance

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

var (
	ErrInvalidTopology       = errors.New("scaleplan autorebalance: invalid topology")
	ErrNoPlan                = errors.New("scaleplan autorebalance: no plan required")
	ErrUnsafeExternalScaleIn = errors.New("scaleplan autorebalance: StatefulSet was reduced before shard drain")
)

// Policy is intentionally small for A6. Shard heat is not guessed: the first
// automatic policy minimizes the number of moved shards and uses stable
// ordering for reproducibility.
type Policy struct {
	RoutePlanTTL      time.Duration
	PlanTTL           time.Duration
	RecoveryRetention time.Duration
	Limits            core.FunctionalLimits
}

func DefaultPolicy() Policy {
	return Policy{
		RoutePlanTTL:      time.Hour,
		PlanTTL:           24 * time.Hour,
		RecoveryRetention: 7 * 24 * time.Hour,
		Limits:            core.LegacyParallelFunctionalLimits(),
	}
}

func (p Policy) validate() error {
	if p.RoutePlanTTL <= 0 || p.RoutePlanTTL > core.MaxRoutePlanTTL ||
		p.PlanTTL <= 0 || p.PlanTTL > core.MaxPlanTTL ||
		p.RecoveryRetention <= 0 || p.RecoveryRetention > core.MaxRecoveryRetention ||
		p.PlanTTL+p.RecoveryRetention > core.MaxRecoveryHorizon {
		return fmt.Errorf("%w: policy durations", ErrInvalidTopology)
	}
	if err := p.Limits.Validate(); err != nil {
		return fmt.Errorf("%w: policy limits: %v", ErrInvalidTopology, err)
	}
	return nil
}

// Topology is one bracketed RouteControl snapshot joined with the latest
// workload and verified-holder observations. Revalidate must be called by the
// controller immediately before submission.
type Topology struct {
	Workload   core.WorkloadState
	Token      core.PlacementCaptureToken
	Placements []core.ShardPlacement
	Holders    []core.HolderState
}

// PlannedScaleOut creates a normal +1 plan before Kubernetes creates the new
// StatefulSet ordinal. targetHolderID is deterministic (normally
// <statefulset-name>-<ordinal>); BootID/PodUID are resolved only after the Pod
// is discovered and verified.
func PlannedScaleOut(topology Topology, targetHolderID string, policy Policy) (core.Submission, error) {
	if topology.Workload.Replicas < 1 {
		return core.Submission{}, fmt.Errorf("%w: scale-out requires one existing replica", ErrInvalidTopology)
	}
	return buildScaleOut(topology, topology.Workload.Replicas, topology.Workload.Replicas+1,
		targetHolderID, topology.Workload.Replicas, policy, "planned")
}

// AdoptedScaleOut recognizes a Pod that Kubernetes has already added. It
// produces the same durable SCALE_OUT plan but with expected=N-1, desired=N;
// Executor records the K8s mutation as ADOPTED_COMMITTED before moving shards.
func AdoptedScaleOut(topology Topology, policy Policy) (core.Submission, error) {
	if topology.Workload.Replicas < 2 {
		return core.Submission{}, ErrNoPlan
	}
	targetOrdinal := topology.Workload.Replicas - 1
	counts, err := placementCounts(topology)
	if err != nil {
		return core.Submission{}, err
	}
	var target *core.HolderState
	for i := range topology.Holders {
		holder := topology.Holders[i]
		if holder.Ref.Domain != topology.Workload.Ref.Domain || holder.Ref.Ordinal != targetOrdinal {
			continue
		}
		if target != nil {
			return core.Submission{}, fmt.Errorf("%w: ambiguous newest ordinal", ErrInvalidTopology)
		}
		copy := holder
		target = &copy
	}
	if target == nil || !zeroShardTarget(*target, topology.Workload) || counts[target.Ref.HolderID] != 0 {
		return core.Submission{}, ErrNoPlan
	}
	return buildScaleOut(topology, topology.Workload.Replicas-1, topology.Workload.Replicas,
		target.Ref.HolderID, targetOrdinal, policy, "adopted")
}

// ScaleIn drains exactly the highest StatefulSet ordinal and only then lets
// the existing ScalePlan state machine reduce replicas. Moving every shard on
// that victim is the theoretical minimum for a safe -1 operation.
func ScaleIn(topology Topology, desired int32, policy Policy) (core.Submission, error) {
	if err := validateTopology(topology, policy); err != nil {
		return core.Submission{}, err
	}
	if topology.Workload.Replicas < 2 || desired != topology.Workload.Replicas-1 {
		return core.Submission{}, fmt.Errorf("%w: A6 scale-in supports one replica at a time", ErrInvalidTopology)
	}
	victimOrdinal := topology.Workload.Replicas - 1
	counts, err := placementCounts(topology)
	if err != nil {
		return core.Submission{}, err
	}
	holdersByOrdinal, err := holderOrdinals(topology)
	if err != nil {
		return core.Submission{}, err
	}
	victim, found := holdersByOrdinal[victimOrdinal]
	if !found || !scaleInVictim(victim, topology.Workload) {
		return core.Submission{}, fmt.Errorf("%w: highest ordinal is not a verified source", ErrInvalidTopology)
	}
	recipients := make([]core.HolderState, 0, desired)
	for ordinal := int32(0); ordinal < desired; ordinal++ {
		holder, ok := holdersByOrdinal[ordinal]
		if !ok || !placementTarget(holder, topology.Workload) {
			return core.Submission{}, fmt.Errorf("%w: remaining ordinal %d is not eligible", ErrInvalidTopology, ordinal)
		}
		recipients = append(recipients, holder)
	}
	victimShards := make([]core.ShardPlacement, 0, counts[victim.Ref.HolderID])
	for _, placement := range topology.Placements {
		if placement.Holder.HolderID == victim.Ref.HolderID {
			victimShards = append(victimShards, placement)
		}
	}
	sort.Slice(victimShards, func(i, j int) bool { return victimShards[i].VirtualShard < victimShards[j].VirtualShard })

	moves := make([]core.ShardMoveIntent, 0, len(victimShards))
	for _, shard := range victimShards {
		sort.Slice(recipients, func(i, j int) bool {
			left, right := counts[recipients[i].Ref.HolderID], counts[recipients[j].Ref.HolderID]
			if left != right {
				return left < right
			}
			if recipients[i].Ref.Ordinal != recipients[j].Ref.Ordinal {
				return recipients[i].Ref.Ordinal < recipients[j].Ref.Ordinal
			}
			return recipients[i].Ref.HolderID < recipients[j].Ref.HolderID
		})
		target := recipients[0]
		moves = append(moves, core.ShardMoveIntent{
			Domain: topology.Workload.Ref.Domain, VirtualShard: shard.VirtualShard,
			SourceHolderID: victim.Ref.HolderID, TargetHolderID: target.Ref.HolderID,
			TargetOrdinal: target.Ref.Ordinal,
		})
		counts[target.Ref.HolderID]++
	}
	return submission(topology, core.OperationScaleIn, topology.Workload.Replicas, desired, "", moves, policy, "safe-in")
}

// DrainHolder preserves the StatefulSet replica count while moving every
// Shard away from one exact, verified holder. It is the durable primitive used
// before an OnDelete rolling replacement removes an old Pod generation.
func DrainHolder(topology Topology, victimHolderID string, policy Policy) (core.Submission, error) {
	return drainHolder(topology, victimHolderID, core.HolderRef{}, policy)
}

// DrainHolderTo is the managed-rollout primitive: every Shard owned by the
// old Holder is moved to one exact, freshly verified replacement Pod. The
// complete target generation is persisted in the ScalePlan submission so an
// executor restart cannot fall back to ordinary least-loaded placement.
func DrainHolderTo(topology Topology, victimHolderID string, replacement core.HolderRef, policy Policy) (core.Submission, error) {
	if !replacement.Exact() {
		return core.Submission{}, fmt.Errorf("%w: exact replacement required", ErrInvalidTopology)
	}
	return drainHolder(topology, victimHolderID, replacement, policy)
}

func drainHolder(topology Topology, victimHolderID string, replacement core.HolderRef, policy Policy) (core.Submission, error) {
	if err := validateTopology(topology, policy); err != nil {
		return core.Submission{}, err
	}
	if strings.TrimSpace(victimHolderID) == "" || topology.Workload.Replicas < 2 {
		return core.Submission{}, fmt.Errorf("%w: holder drain requires a victim and a recipient", ErrInvalidTopology)
	}
	counts, err := placementCounts(topology)
	if err != nil {
		return core.Submission{}, err
	}
	var victim *core.HolderState
	recipients := make([]core.HolderState, 0, len(topology.Holders)-1)
	for i := range topology.Holders {
		holder := topology.Holders[i]
		if holder.Ref.HolderID == victimHolderID {
			if victim != nil || !scaleInVictim(holder, topology.Workload) {
				return core.Submission{}, fmt.Errorf("%w: drain victim is not unique and eligible", ErrInvalidTopology)
			}
			copy := holder
			victim = &copy
			continue
		}
		if placementTarget(holder, topology.Workload) {
			recipients = append(recipients, holder)
		}
	}
	if victim == nil || len(recipients) == 0 {
		return core.Submission{}, fmt.Errorf("%w: drain topology has no victim or recipient", ErrInvalidTopology)
	}
	var pinned *core.HolderState
	if replacement != (core.HolderRef{}) {
		for i := range recipients {
			if recipients[i].Ref == replacement {
				candidate := recipients[i]
				pinned = &candidate
				break
			}
		}
		if pinned == nil || !zeroShardTarget(*pinned, topology.Workload) || counts[replacement.HolderID] != 0 {
			return core.Submission{}, fmt.Errorf("%w: replacement is not verified zero-shard target", ErrInvalidTopology)
		}
	}
	victimShards := make([]core.ShardPlacement, 0, counts[victimHolderID])
	for _, placement := range topology.Placements {
		if placement.Holder.HolderID == victimHolderID {
			victimShards = append(victimShards, placement)
		}
	}
	sort.Slice(victimShards, func(i, j int) bool { return victimShards[i].VirtualShard < victimShards[j].VirtualShard })
	moves := make([]core.ShardMoveIntent, 0, len(victimShards))
	for _, shard := range victimShards {
		target := recipients[0]
		if pinned != nil {
			target = *pinned
		} else {
			sort.Slice(recipients, func(i, j int) bool {
				left, right := counts[recipients[i].Ref.HolderID], counts[recipients[j].Ref.HolderID]
				if left != right {
					return left < right
				}
				if recipients[i].Ref.Ordinal != recipients[j].Ref.Ordinal {
					return recipients[i].Ref.Ordinal < recipients[j].Ref.Ordinal
				}
				return recipients[i].Ref.HolderID < recipients[j].Ref.HolderID
			})
			target = recipients[0]
		}
		moves = append(moves, core.ShardMoveIntent{Domain: topology.Workload.Ref.Domain,
			VirtualShard: shard.VirtualShard, SourceHolderID: victimHolderID,
			TargetHolderID: target.Ref.HolderID, TargetOrdinal: target.Ref.Ordinal})
		counts[target.Ref.HolderID]++
	}
	result, err := submission(topology, core.OperationDrainHolder, topology.Workload.Replicas,
		topology.Workload.Replicas, victimHolderID, moves, policy, "drain-holder")
	if err != nil {
		return core.Submission{}, err
	}
	if pinned != nil {
		result.PinnedTarget = replacement
		// PinnedTarget participates in the durable identity, so normalize again
		// after attaching it rather than returning the generic drain identity.
		return result.Normalize()
	}
	return result, nil
}

func buildScaleOut(topology Topology, expected, desired int32, targetHolderID string, targetOrdinal int32,
	policy Policy, flavor string) (core.Submission, error) {
	if err := validateTopology(topology, policy); err != nil {
		return core.Submission{}, err
	}
	if strings.TrimSpace(targetHolderID) == "" || desired != expected+1 || targetOrdinal != desired-1 {
		return core.Submission{}, fmt.Errorf("%w: scale-out target", ErrInvalidTopology)
	}
	counts, err := placementCounts(topology)
	if err != nil {
		return core.Submission{}, err
	}
	if err := validateBalancedScaleOutSources(topology, expected, counts); err != nil {
		return core.Submission{}, err
	}
	if counts[targetHolderID] != 0 {
		return core.Submission{}, ErrNoPlan
	}
	total := len(topology.Placements)
	moveCount := total / int(desired)
	if moveCount == 0 {
		return core.Submission{}, fmt.Errorf("%w: fewer shards than desired holders", ErrInvalidTopology)
	}

	shardsByHolder := make(map[string][]core.ShardPlacement)
	refs := make(map[string]core.HolderRef)
	for _, placement := range topology.Placements {
		shardsByHolder[placement.Holder.HolderID] = append(shardsByHolder[placement.Holder.HolderID], placement)
		refs[placement.Holder.HolderID] = placement.Holder
	}
	for holderID := range shardsByHolder {
		sort.Slice(shardsByHolder[holderID], func(i, j int) bool {
			return shardsByHolder[holderID][i].VirtualShard > shardsByHolder[holderID][j].VirtualShard
		})
	}
	moves := make([]core.ShardMoveIntent, 0, moveCount)
	for len(moves) < moveCount {
		donor := ""
		for holderID, count := range counts {
			if holderID == targetHolderID || len(shardsByHolder[holderID]) == 0 {
				continue
			}
			if donor == "" || count > counts[donor] ||
				(count == counts[donor] && lessHolder(refs[holderID], refs[donor])) {
				donor = holderID
			}
		}
		if donor == "" {
			return core.Submission{}, fmt.Errorf("%w: insufficient donor shards", ErrInvalidTopology)
		}
		shards := shardsByHolder[donor]
		selected := shards[0]
		shardsByHolder[donor] = shards[1:]
		moves = append(moves, core.ShardMoveIntent{
			Domain: topology.Workload.Ref.Domain, VirtualShard: selected.VirtualShard,
			SourceHolderID: donor, TargetHolderID: targetHolderID, TargetOrdinal: targetOrdinal,
		})
		counts[donor]--
		counts[targetHolderID]++
	}
	if !balancedCounts(counts) {
		return core.Submission{}, fmt.Errorf("%w: scale-out result is not balanced", ErrInvalidTopology)
	}
	return submission(topology, core.OperationScaleOut, expected, desired, "", moves, policy, flavor)
}

func validateBalancedScaleOutSources(topology Topology, expected int32, counts map[string]int) error {
	holders, err := holderOrdinals(topology)
	if err != nil {
		return err
	}
	sourceCounts := make(map[string]int, expected)
	for ordinal := int32(0); ordinal < expected; ordinal++ {
		holder, found := holders[ordinal]
		if !found || !servingSource(holder, topology.Workload) {
			return fmt.Errorf("%w: scale-out source ordinal %d is not serving", ErrInvalidTopology, ordinal)
		}
		count := counts[holder.Ref.HolderID]
		if count == 0 || uint32(count) != holder.ServingShardCount {
			return fmt.Errorf("%w: scale-out source count mismatch", ErrInvalidTopology)
		}
		sourceCounts[holder.Ref.HolderID] = count
	}
	if len(sourceCounts) != int(expected) || !balancedCounts(sourceCounts) {
		return fmt.Errorf("%w: existing scale-out sources are not count-balanced", ErrInvalidTopology)
	}
	return nil
}

func balancedCounts(counts map[string]int) bool {
	if len(counts) == 0 {
		return false
	}
	minimum, maximum := int(^uint(0)>>1), 0
	for _, count := range counts {
		if count < minimum {
			minimum = count
		}
		if count > maximum {
			maximum = count
		}
	}
	return maximum-minimum <= 1
}

func submission(topology Topology, operation core.Operation, expected, desired int32, victimHolderID string, moves []core.ShardMoveIntent,
	policy Policy, flavor string) (core.Submission, error) {
	key := idempotencyKey(topology, operation, expected, desired, victimHolderID, moves, flavor)
	value := core.Submission{
		IdempotencyKey: key, Realm: topology.Workload.Ref.Realm, Domain: topology.Workload.Ref.Domain,
		Operation: operation, Workload: topology.Workload.Ref, VictimHolderID: victimHolderID,
		ExpectedReplicas: expected, DesiredReplicas: desired, Moves: moves,
		RoutePlanTTLms: policy.RoutePlanTTL.Milliseconds(), PlanTTLms: policy.PlanTTL.Milliseconds(),
		RecoveryRetentionMs: policy.RecoveryRetention.Milliseconds(),
		Limits:              policy.Limits,
	}
	return value.Normalize()
}

func validateTopology(topology Topology, policy Policy) error {
	if err := policy.validate(); err != nil {
		return err
	}
	state := topology.Workload
	if err := state.Ref.Validate(); err != nil || state.UID == "" || state.ResourceVersion == "" || state.Replicas < 1 {
		return fmt.Errorf("%w: workload", ErrInvalidTopology)
	}
	if topology.Token.ID == "" || topology.Token.ActiveCardinality != uint64(len(topology.Placements)) {
		return fmt.Errorf("%w: incomplete placement capture", ErrInvalidTopology)
	}
	seen := make(map[uint32]struct{}, len(topology.Placements))
	for _, placement := range topology.Placements {
		if placement.Domain != state.Ref.Domain || !placement.Active || !placement.KnownFresh || !placement.Holder.Exact() ||
			placement.Holder.WorkloadUID != state.UID {
			return fmt.Errorf("%w: placement %d", ErrInvalidTopology, placement.VirtualShard)
		}
		if _, duplicate := seen[placement.VirtualShard]; duplicate {
			return fmt.Errorf("%w: duplicate shard", ErrInvalidTopology)
		}
		seen[placement.VirtualShard] = struct{}{}
	}
	return nil
}

func placementCounts(topology Topology) (map[string]int, error) {
	if err := validateTopology(topology, DefaultPolicy()); err != nil {
		// Policy duration validation is irrelevant here; DefaultPolicy is valid.
		return nil, err
	}
	counts := make(map[string]int)
	for _, placement := range topology.Placements {
		counts[placement.Holder.HolderID]++
	}
	return counts, nil
}

func holderOrdinals(topology Topology) (map[int32]core.HolderState, error) {
	result := make(map[int32]core.HolderState)
	for _, holder := range topology.Holders {
		if holder.Ref.Domain != topology.Workload.Ref.Domain || holder.Ref.WorkloadUID != topology.Workload.UID || !holder.Ref.Exact() {
			continue
		}
		if _, duplicate := result[holder.Ref.Ordinal]; duplicate {
			return nil, fmt.Errorf("%w: duplicate holder ordinal", ErrInvalidTopology)
		}
		result[holder.Ref.Ordinal] = holder
	}
	return result, nil
}

func zeroShardTarget(holder core.HolderState, workload core.WorkloadState) bool {
	return holder.Ref.Exact() && holder.Ref.Domain == workload.Ref.Domain && holder.Ref.WorkloadUID == workload.UID &&
		holder.State == core.HolderReadyZeroShard && holder.ProcessReady && !holder.Draining &&
		holder.ServingShardCount == 0 && holder.PlacementEligible
}

func servingSource(holder core.HolderState, workload core.WorkloadState) bool {
	return holder.Ref.Exact() && holder.Ref.Domain == workload.Ref.Domain && holder.Ref.WorkloadUID == workload.UID &&
		holder.State == core.HolderServing && holder.ProcessReady && !holder.Draining && holder.ServingShardCount > 0
}

func scaleInVictim(holder core.HolderState, workload core.WorkloadState) bool {
	if !holder.Ref.Exact() || holder.Ref.Domain != workload.Ref.Domain || holder.Ref.WorkloadUID != workload.UID ||
		!holder.ProcessReady || holder.Draining || !holder.PlacementEligible {
		return false
	}
	return (holder.State == core.HolderServing && holder.ServingShardCount > 0) ||
		(holder.State == core.HolderReadyZeroShard && holder.ServingShardCount == 0)
}

// placementTarget accepts both an already-serving Holder and a freshly
// replaced READY_ZERO_SHARD Holder. During an OnDelete rollout, replacement
// Pods intentionally start with zero Shards; excluding them here would push
// all Shards onto the temporary spare and make the final contraction
// impossible.
func placementTarget(holder core.HolderState, workload core.WorkloadState) bool {
	if !holder.Ref.Exact() || holder.Ref.Domain != workload.Ref.Domain || holder.Ref.WorkloadUID != workload.UID ||
		!holder.ProcessReady || holder.Draining || !holder.PlacementEligible || !holder.AssignmentKnownFresh {
		return false
	}
	switch holder.State {
	case core.HolderServing:
		return holder.ServingShardCount > 0
	case core.HolderReadyZeroShard:
		return holder.ServingShardCount == 0
	default:
		return false
	}
}

func lessHolder(left, right core.HolderRef) bool {
	if left.Ordinal != right.Ordinal {
		return left.Ordinal < right.Ordinal
	}
	return left.HolderID < right.HolderID
}

func idempotencyKey(topology Topology, operation core.Operation, expected, desired int32,
	victimHolderID string, moves []core.ShardMoveIntent, flavor string) string {
	parts := []string{"autorebalance-v1", flavor, topology.Workload.Ref.Key(), topology.Workload.UID,
		topology.Workload.ResourceVersion, topology.Token.ID, string(operation), strconv.FormatInt(int64(expected), 10),
		strconv.FormatInt(int64(desired), 10), victimHolderID}
	canonical := append([]core.ShardMoveIntent(nil), moves...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].VirtualShard < canonical[j].VirtualShard })
	for _, move := range canonical {
		parts = append(parts, string(move.Domain), strconv.FormatUint(uint64(move.VirtualShard), 10),
			move.SourceHolderID, move.TargetHolderID, strconv.FormatInt(int64(move.TargetOrdinal), 10))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "auto-" + strings.ToLower(string(operation)) + "-" + hex.EncodeToString(sum[:16])
}

// Compile-time guard against accidentally broadening this planner to a third
// route domain without a separate contract.
var _ = []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm}
