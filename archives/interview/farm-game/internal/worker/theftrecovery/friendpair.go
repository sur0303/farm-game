package theftrecovery

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

// FriendPairTheftTarget is the domain-neutral portion needed to derive the
// stable Theft attempt id. The worker does not implement Participant logic;
// the composition root supplies this decoder from the already admitted
// target binding/payload source.
type FriendPairTheftTarget struct {
	PlotID      uint32
	CropCycleID string
}

type FriendPairTheftTargetDecoder func(context.Context, friend.PairRecoverySourceOrder, friend.ActionPermitSnapshot) (FriendPairTheftTarget, error)

type FriendPairDispatchRepositoryConfig struct {
	Repository   *friend.RedisFriendshipRepositories
	TargetDecode FriendPairTheftTargetDecoder
	MaxBuckets   int
}

// FriendPairDispatchRepository binds the bounded generic Redis discovery to
// the frozen FRIEND_PAIR key builder and manifest reader.
type FriendPairDispatchRepository struct {
	repository   *friend.RedisFriendshipRepositories
	targetDecode FriendPairTheftTargetDecoder
	discovery    *RedisPairDispatchRepository
}

type PairDueFamilyObservation struct {
	Depth         int64
	OldestDueAtMs int64
}

// PairDueObservation is a complete bounded view of the shared Pair DueIndex.
// Theft and Care include both dispatch and result orders for their scope.
// RESULT_PUBLISH_ORDER is terminal operation-result publication, not the
// independent FriendIndex repair/projection Family. Available=false means the
// shared view exceeded the bound or contained an unclassifiable member, never
// a zero-depth assertion.
type PairDueObservation struct {
	Theft     PairDueFamilyObservation
	Care      PairDueFamilyObservation
	Available bool
}

func NewFriendPairDispatchRepository(config FriendPairDispatchRepositoryConfig) (*FriendPairDispatchRepository, error) {
	if config.Repository == nil || config.TargetDecode == nil {
		return nil, fmt.Errorf("%w: Pair repository and Theft target decoder are required", ErrInvalidConfig)
	}
	adapter := &FriendPairDispatchRepository{repository: config.Repository, targetDecode: config.TargetDecode}
	discovery, err := NewRedisPairDispatchRepository(RedisPairDiscoveryConfig{
		Client: config.Repository.Client(), BucketCount: config.Repository.PairBucketCount(), MaxBuckets: config.MaxBuckets,
		DueIndexKey: config.Repository.PairDueIndexKey,
		LoadDispatch: func(ctx context.Context, bucketID uint32, workType, workID string) (DispatchOrder, error) {
			source, err := config.Repository.ReadPairDispatchOrder(ctx, bucketID, workType, workID)
			if err != nil {
				return DispatchOrder{}, err
			}
			return decodeFriendPairDispatch(ctx, source, config.TargetDecode)
		},
	})
	if err != nil {
		return nil, err
	}
	adapter.discovery = discovery
	return adapter, nil
}

func (r *FriendPairDispatchRepository) Discover(ctx context.Context, request DiscoveryRequest) (DiscoveryPage, error) {
	if r == nil || r.discovery == nil {
		return DiscoveryPage{}, fmt.Errorf("%w: nil Friend Pair discovery", ErrInvalidConfig)
	}
	return r.discovery.Discover(ctx, request)
}

func (r *FriendPairDispatchRepository) ObserveDue(ctx context.Context, limit int) (PairDueObservation, error) {
	if r == nil || r.discovery == nil || r.discovery.client == nil || limit <= 0 || limit > 4096 {
		return PairDueObservation{}, fmt.Errorf("%w: Pair DueIndex observer", ErrInvalidConfig)
	}
	now, err := r.discovery.client.Time(ctx).Result()
	if err != nil {
		return PairDueObservation{}, err
	}
	observation := PairDueObservation{Available: true}
	type bucketCount struct {
		bucket uint32
		key    string
		count  *redis.IntCmd
	}
	counts := make([]bucketCount, 0, r.discovery.bucketCount)
	pipe := r.discovery.client.Pipeline()
	for bucket := uint32(0); bucket < r.discovery.bucketCount; bucket++ {
		key, keyErr := r.discovery.dueKey(bucket)
		if keyErr != nil {
			return PairDueObservation{}, keyErr
		}
		counts = append(counts, bucketCount{bucket: bucket, key: key,
			count: pipe.ZCount(ctx, key, "-inf", strconv.FormatInt(now.UnixMilli(), 10))})
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return PairDueObservation{}, err
	}
	total := int64(0)
	for _, item := range counts {
		count, countErr := item.count.Result()
		if countErr != nil {
			return PairDueObservation{}, countErr
		}
		total += count
		if total > int64(limit) {
			return PairDueObservation{}, nil
		}
	}
	type bucketEntries struct {
		bucket  uint32
		count   int64
		entries *redis.ZSliceCmd
	}
	entryCommands := make([]bucketEntries, 0)
	pipe = r.discovery.client.Pipeline()
	for _, item := range counts {
		count, _ := item.count.Result()
		if count == 0 {
			continue
		}
		entryCommands = append(entryCommands, bucketEntries{bucket: item.bucket, count: count,
			entries: pipe.ZRangeByScoreWithScores(ctx, item.key, &redis.ZRangeBy{
				Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: count + 1,
			})})
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return PairDueObservation{}, err
	}
	for _, command := range entryCommands {
		entries, readErr := command.entries.Result()
		if readErr != nil {
			return PairDueObservation{}, readErr
		}
		if int64(len(entries)) != command.count {
			return PairDueObservation{}, nil
		}
		for _, entry := range entries {
			candidate := DispatchCandidate{BucketID: command.bucket, Member: redisString(entry.Member), ScoreMs: int64(entry.Score)}
			ref, decodeErr := workidentity.DecodeWorkRefV1(candidate.Member)
			if decodeErr != nil {
				return PairDueObservation{}, nil
			}
			if ref.WorkType != FriendActionDispatchOrder && ref.WorkType != ResultPublishOrder {
				continue
			}
			scope, scopeErr := r.ActionScope(ctx, candidate)
			if errors.Is(scopeErr, ErrNotTheft) {
				continue
			}
			if scopeErr != nil {
				return PairDueObservation{}, scopeErr
			}
			switch scope {
			case friend.ActionScopeTheft:
				addPairDue(&observation.Theft, candidate.ScoreMs)
			case friend.ActionScopeCare:
				addPairDue(&observation.Care, candidate.ScoreMs)
			default:
				return PairDueObservation{}, nil
			}
		}
	}
	return observation, nil
}

func addPairDue(observation *PairDueFamilyObservation, dueAtMs int64) {
	observation.Depth++
	if observation.OldestDueAtMs == 0 || dueAtMs < observation.OldestDueAtMs {
		observation.OldestDueAtMs = dueAtMs
	}
}

func (r *FriendPairDispatchRepository) ActionScope(ctx context.Context, candidate DispatchCandidate) (friend.ActionScope, error) {
	if r == nil || r.repository == nil {
		return friend.ActionScopeUnspecified, fmt.Errorf("%w: nil Friend Pair scope reader", ErrInvalidConfig)
	}
	ref, err := workidentity.DecodeWorkRefV1(candidate.Member)
	if err != nil || (ref.WorkType != FriendActionDispatchOrder && ref.WorkType != ResultPublishOrder) {
		return friend.ActionScopeUnspecified, ErrMalformedMember
	}
	scope, err := r.repository.ReadPairActionWorkScope(ctx, candidate.BucketID, ref.WorkType, ref.WorkID)
	if errors.Is(err, friend.ErrPairActionScopeNotOwned) {
		return friend.ActionScopeUnspecified, ErrNotTheft
	}
	return scope, err
}

func (r *FriendPairDispatchRepository) LoadDispatch(ctx context.Context, candidate DispatchCandidate) (DispatchOrder, error) {
	if r == nil || r.discovery == nil {
		return DispatchOrder{}, fmt.Errorf("%w: nil Friend Pair loader", ErrInvalidConfig)
	}
	return r.discovery.LoadDispatch(ctx, candidate)
}

// LoadActionDispatch performs the same exact manifest read as the Theft
// loader but stops before family-specific target decoding. It is used by the
// correctly scoped CARE handler after the shared router selected CARE.
func (r *FriendPairDispatchRepository) LoadActionDispatch(ctx context.Context, candidate DispatchCandidate) (DispatchOrder, error) {
	if r == nil || r.repository == nil {
		return DispatchOrder{}, fmt.Errorf("%w: nil Friend Pair action loader", ErrInvalidConfig)
	}
	ref, err := workidentity.DecodeWorkRefV1(candidate.Member)
	if err != nil || ref.WorkType != FriendActionDispatchOrder {
		return DispatchOrder{}, ErrMalformedMember
	}
	source, err := r.repository.ReadPairDispatchOrder(ctx, candidate.BucketID, ref.WorkType, ref.WorkID)
	if err != nil {
		return DispatchOrder{}, err
	}
	return decodeFriendPairActionDispatch(source)
}

// LoadPairTheftDispatchByOperation resolves the exact accepted Pair dispatch
// anchor without requiring the Theft target sidecar. It is used by Farm
// Participant recovery admission before the target mutation; target facts
// remain bound by the admitted target hash.
func LoadPairTheftDispatchByOperation(ctx context.Context, repository *friend.RedisFriendshipRepositories,
	visitor, owner uint64, operationID string) (DispatchOrder, error) {
	if repository == nil || visitor == 0 || owner == 0 || strings.TrimSpace(operationID) == "" {
		return DispatchOrder{}, fmt.Errorf("%w: Pair operation lookup", ErrInvalidConfig)
	}
	binding, found, err := repository.LookupPairActionResult(ctx, visitor, owner, operationID)
	if err != nil {
		return DispatchOrder{}, err
	}
	if !found || binding.Scope != friend.ActionScopeTheft || binding.DispatchOrderID == "" || binding.BucketID >= repository.PairBucketCount() {
		return DispatchOrder{}, fmt.Errorf("%w: Pair dispatch anchor", ErrInvalidDispatch)
	}
	source, err := repository.ReadPairDispatchOrder(ctx, binding.BucketID, FriendActionDispatchOrder, binding.DispatchOrderID)
	if err != nil {
		return DispatchOrder{}, err
	}
	order, err := decodeFriendPairActionDispatch(source)
	if err != nil {
		return DispatchOrder{}, err
	}
	order, err = NormalizePairActionDispatchOrder(order, friend.ActionScopeTheft)
	if err != nil {
		return DispatchOrder{}, err
	}
	if order.VisitorPlayerID != visitor || order.OwnerPlayerID != owner || order.OperationID != operationID ||
		order.Work.WorkID != binding.DispatchOrderID || order.Work.BucketID != binding.BucketID ||
		order.PairID != binding.PairID || !bytesEqual(order.CanonicalPayloadHash, binding.PayloadHash) ||
		!bytesEqual(order.TargetBindingHash, binding.TargetBindingHash) {
		return DispatchOrder{}, fmt.Errorf("%w: Pair result/dispatch binding", ErrInvalidDispatch)
	}
	return order, nil
}

// LoadByID is the bounded fast-path lookup used after public Pair
// ActionAdmission. The dispatch port receives only the durable order ID, so
// it probes the authority-owned bounded bucket set; it never scans keys or
// manufactures an in-memory order.
func (r *FriendPairDispatchRepository) LoadByID(ctx context.Context, workID string) (DispatchOrder, error) {
	if r == nil || r.repository == nil || r.targetDecode == nil || workidentity.ValidateStableOpaqueID(workID) != nil {
		return DispatchOrder{}, fmt.Errorf("%w: Pair dispatch lookup", ErrInvalidConfig)
	}
	for bucket := uint32(0); bucket < r.repository.PairBucketCount(); bucket++ {
		source, err := r.repository.ReadPairDispatchOrder(ctx, bucket, FriendActionDispatchOrder, workID)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return DispatchOrder{}, err
		}
		return decodeFriendPairDispatch(ctx, source, r.targetDecode)
	}
	return DispatchOrder{}, fmt.Errorf("%w: dispatch order %s", ErrInvalidDispatch, workID)
}

// LoadByPairID is the O(1) production fast-path lookup. Public admission has
// already persisted the canonical Pair id in the target-binding sidecar, so
// the dispatcher can derive the one physical bucket instead of probing the
// complete Pair bucket space. Recovery discovery still comes from DueIndex.
func (r *FriendPairDispatchRepository) LoadByPairID(ctx context.Context, pairID, workID string) (DispatchOrder, error) {
	if r == nil || r.repository == nil || r.targetDecode == nil ||
		workidentity.ValidateStableOpaqueID(pairID) != nil || workidentity.ValidateStableOpaqueID(workID) != nil {
		return DispatchOrder{}, fmt.Errorf("%w: Pair dispatch lookup", ErrInvalidConfig)
	}
	bucketID, err := r.repository.PairBucketIDForRelation(pairID)
	if err != nil {
		return DispatchOrder{}, err
	}
	source, err := r.repository.ReadPairDispatchOrder(ctx, bucketID, FriendActionDispatchOrder, workID)
	if err != nil {
		return DispatchOrder{}, err
	}
	order, err := decodeFriendPairDispatch(ctx, source, r.targetDecode)
	if err != nil {
		return DispatchOrder{}, err
	}
	if order.PairID != pairID {
		return DispatchOrder{}, fmt.Errorf("%w: Pair dispatch relation binding", ErrInvalidDispatch)
	}
	return order, nil
}

// ResultCandidateByOperation returns the exact Pair RESULT_PUBLISH_ORDER
// locator for an admitted Theft. It is used only as a low-latency hint by the
// online accelerator; ResultOrderProcessor still reloads and validates the
// complete immutable binding before publishing or terminalizing it.
func (r *FriendPairDispatchRepository) ResultCandidateByOperation(ctx context.Context, visitor, owner uint64, operationID string) (DispatchCandidate, error) {
	if r == nil || r.repository == nil || visitor == 0 || owner == 0 || strings.TrimSpace(operationID) == "" {
		return DispatchCandidate{}, fmt.Errorf("%w: Pair result locator", ErrInvalidConfig)
	}
	binding, found, err := r.repository.LookupPairActionResult(ctx, visitor, owner, operationID)
	if err != nil {
		return DispatchCandidate{}, err
	}
	if !found || binding.ResultWorkRef == "" || binding.ResultOrderID == "" {
		return DispatchCandidate{}, fmt.Errorf("%w: Pair result order not found", ErrInvalidDispatch)
	}
	return DispatchCandidate{BucketID: binding.BucketID, Member: binding.ResultWorkRef, ScoreMs: binding.ResultNextDueAtMs}, nil
}

// FriendPairWorkLeaseKeys maps the recovery sidecar to the same Pair bucket
// hash tag as the source order and DueIndex.
type FriendPairWorkLeaseKeys struct {
	Repository *friend.RedisFriendshipRepositories
}

func NewFriendPairWorkLeaseRepository(repository *friend.RedisFriendshipRepositories, minTTL, maxTTL time.Duration) (*RedisWorkLeaseRepository, error) {
	if repository == nil {
		return nil, fmt.Errorf("%w: Pair repository is required", ErrInvalidConfig)
	}
	return NewRedisWorkLeaseRepository(RedisWorkLeaseConfig{Client: repository.Client(), Keys: FriendPairWorkLeaseKeys{Repository: repository}, MinTTL: minTTL, MaxTTL: maxTTL})
}

func (k FriendPairWorkLeaseKeys) WorkKey(address WorkAddress) (string, error) {
	if k.Repository == nil {
		return "", ErrInvalidConfig
	}
	return k.Repository.PairSourceOrderKey(address.BucketID, address.WorkType, address.WorkID)
}

func (k FriendPairWorkLeaseKeys) DueIndexKey(address WorkAddress) (string, error) {
	if k.Repository == nil {
		return "", ErrInvalidConfig
	}
	return k.Repository.PairDueIndexKey(address.BucketID)
}

func (k FriendPairWorkLeaseKeys) BucketMetaKey(address WorkAddress) (string, error) {
	if k.Repository == nil {
		return "", ErrInvalidConfig
	}
	return k.Repository.PairBucketMetaKey(address.BucketID)
}

func (k FriendPairWorkLeaseKeys) RecoveryKey(address WorkAddress) (string, error) {
	if k.Repository == nil {
		return "", ErrInvalidConfig
	}
	return k.Repository.PairRecoveryKey(address.BucketID, address.WorkID)
}

func decodeFriendPairDispatch(ctx context.Context, source friend.PairRecoverySourceOrder, targetDecode FriendPairTheftTargetDecoder) (DispatchOrder, error) {
	order, err := decodeFriendPairActionDispatch(source)
	if err != nil {
		return DispatchOrder{}, err
	}
	// Scope ownership is decided before any Theft-only sidecar lookup. A CARE
	// order has no Theft target binding and is a normal non-owned result, not a
	// malformed Theft order.
	if order.Permit.Scope != friend.ActionScopeTheft {
		return DispatchOrder{}, ErrNotTheft
	}
	target, err := targetDecode(ctx, source, order.Permit)
	if err != nil {
		return DispatchOrder{}, err
	}
	order.PlotID, order.CropCycleID = target.PlotID, target.CropCycleID
	return order, nil
}

func decodeFriendPairActionDispatch(source friend.PairRecoverySourceOrder) (DispatchOrder, error) {
	fields := source.Fields
	parseU := func(name string) (uint64, error) {
		value, err := strconv.ParseUint(fields[name], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s", ErrInvalidDispatch, name)
		}
		return value, nil
	}
	parseI := func(name string) (int64, error) {
		value, err := strconv.ParseInt(fields[name], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s", ErrInvalidDispatch, name)
		}
		return value, nil
	}
	visitor, err := parseU("visitor_player_id")
	if err != nil {
		return DispatchOrder{}, err
	}
	owner, err := parseU("owner_player_id")
	if err != nil {
		return DispatchOrder{}, err
	}
	targetFarm, err := parseU("target_farm_id")
	if err != nil {
		return DispatchOrder{}, err
	}
	epoch, err := parseU("relation_epoch")
	if err != nil {
		return DispatchOrder{}, err
	}
	scope, err := parseU("scope")
	if err != nil {
		return DispatchOrder{}, err
	}
	issued, err := parseI("issued_at_authority_ms")
	if err != nil {
		return DispatchOrder{}, err
	}
	expires, err := parseI("expire_at_authority_ms")
	if err != nil {
		return DispatchOrder{}, err
	}
	accepted, err := parseI("execution_accepted_at_authority_ms")
	if err != nil {
		return DispatchOrder{}, err
	}
	workflowDeadline, err := parseI("workflow_deadline_at_ms")
	if err != nil || workflowDeadline <= expires {
		return DispatchOrder{}, fmt.Errorf("%w: workflow_deadline_at_ms", ErrInvalidDispatch)
	}
	payloadHash, err := hex.DecodeString(fields["permit_payload_hash"])
	if err != nil {
		return DispatchOrder{}, err
	}
	targetBindingHash, err := hex.DecodeString(fields["target_binding_hash"])
	if err != nil {
		return DispatchOrder{}, err
	}
	permit := friend.ActionPermitSnapshot{PermitID: fields["permit_id"], RelationID: fields["relation_id"],
		RelationEpoch: epoch, VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: targetFarm,
		OperationID: fields["operation_id"], Scope: friend.ActionScope(scope), PayloadHash: payloadHash,
		TargetBindingHash: targetBindingHash, IssuedAtAuthorityMs: issued, ExpireAtAuthorityMs: expires,
		ExecutionAcceptedAtAuthorityMs: accepted}
	if err := friend.VerifyAdmittedPermit(permit); err != nil {
		return DispatchOrder{}, fmt.Errorf("%w: permit: %v", ErrInvalidDispatch, err)
	}
	nextDue, err := parseI("next_due_at_ms")
	if err != nil {
		return DispatchOrder{}, err
	}
	retry, err := parseU("retry_count")
	if err != nil {
		return DispatchOrder{}, err
	}
	version, err := parseU("work_version")
	if err != nil {
		return DispatchOrder{}, err
	}
	payload := []byte(fields["typed_payload"])
	physicalPayloadHash, err := hex.DecodeString(fields["payload_hash"])
	if err != nil {
		return DispatchOrder{}, err
	}
	return DispatchOrder{Work: WorkAddress{WorkID: source.WorkID, WorkType: source.WorkType,
		PlacementKind: fields["placement_kind"], BucketVersion: fields["bucket_version"], BucketID: source.BucketID,
		SourceAuthorityKind: fields["source_authority_kind"], CanonicalSourceAuthority: fields["canonical_source_authority_id"],
		SourceLayoutVersion: fields["bucket_version"]}, WorkRef: fields["work_ref"], WorkVersion: version,
		Payload: payload, PayloadHash: physicalPayloadHash, CanonicalPayloadHash: payloadHash, TargetBindingHash: targetBindingHash,
		PairID: fields["canonical_source_authority_id"], OperationID: permit.OperationID, VisitorPlayerID: visitor,
		OwnerPlayerID: owner, TargetFarmID: targetFarm,
		Permit: permit, Admission: AdmissionProof{ReceiptID: fields["dispatch_order_id"], PayloadHash: payloadHash,
			PermitHash: PermitHash(permit), AcceptedAtMs: accepted}, NextDueAtMs: nextDue, RetryCount: retry,
		DeadlineAtMs: workflowDeadline}, nil
}
