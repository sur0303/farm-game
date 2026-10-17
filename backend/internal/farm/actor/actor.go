package actor

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/subscription"
	"farmgame/internal/gateway/session"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

var (
	ErrJoinNotFound = errors.New("farm actor: subscription not found")
	ErrJoinOverflow = errors.New("farm actor: joining buffer overflow")
	ErrJoinStale    = errors.New("farm actor: stale presence fence")
	presenceCounter atomic.Uint64
)

const (
	operationReceiptRetention = 7 * 24 * time.Hour
	maxRecentReceipts         = 1024
	maxActiveSettlements      = 1024
	defaultMaxSubscriptions   = 200
	defaultMaxJoinBufferBytes = 1 << 20
	defaultMaxJoinDuration    = 10 * time.Second
	maxJoinIdempotencyEntries = 1024
	joinIdempotencyTTL        = 2 * time.Minute
)

type Error struct {
	Code    commonpb.ErrorCode
	Message string
}

func (e *Error) Error() string { return e.Message }

// UnknownContext is the minimum durable-write evidence required to resolve an
// ambiguous CAS without letting a later user turn cross the uncertainty point.
type UnknownContext struct {
	OperationID         string
	BeforeRecordVersion uint64
	ExpectedWriterEpoch uint64
	ActivationEpoch     uint64
	CommandType         commonpb.CommandType
}

type ActionResult struct {
	Status     commonpb.CommandStatus
	Result     *farmpb.FarmCommandResult
	CareResult *CareActionResult
	Change     *farmpb.FarmChange
	// FriendOpportunityInvalidated marks a committed mutation that can revoke
	// or change an already-visible WATER/THEFT recommendation. Future-only
	// mutations such as PLANT leave it false; their due-time wakeup publishes
	// the first actionable Hint instead.
	FriendOpportunityInvalidated bool
	Recipients                   []Presence
	Settlement                   *farmpb.FarmSettlement
	ActivationEpoch              uint64
	Replayed                     bool
	Resyncs                      []ResyncNotice
	HarvestOrderID               string
	// CommittedSourceOrder is present only on the post-turn, non-replayed
	// source-actor result. In durable mode it mirrors the AttachedOrder written
	// atomically with Farm state. In dirty best-effort mode it is an immutable
	// process-local envelope and must never be described as durable.
	CommittedSourceOrder *authoritystore.AttachedOrder
	// CommittedTheftSourceOrders carries the visitor and optional guard
	// envelopes produced by one successful Theft turn. Durable mode also writes
	// them to SourceOrder/DueIndex; dirty best-effort mode dispatches these
	// copies directly and intentionally has no worker recovery fallback.
	CommittedTheftSourceOrders []authoritystore.AttachedOrder
	// SourceCommitEvidence carries the exact sealed first-COMMITTED output for
	// CommittedSourceOrder. It is process-local and never persisted. A nil
	// value means Direct must leave delivery to the durable Worker path.
	SourceCommitEvidence *authoritystore.ExactCommitEvidence
	HarvestPublic        *publicpb.HarvestResult
	TheftVisitorOrderID  string
	TheftGuardOrderID    string
}

type FarmActor struct {
	farmID            uint64
	store             blobstore.Store
	record            *farmpb.FarmRecord
	recordVersion     uint64
	activationEpoch   uint64
	unknownContext    *UnknownContext
	subscriptionHub   *SubscriptionHub
	presence          map[string]*Presence
	presenceByPlayer  map[uint64]string
	presenceByAttempt map[string]string
	// Join idempotency is deliberately runtime-only. Presence is runtime-only
	// too: after actor reactivation all old subscriptions are gone and clients
	// must Join again, so a new subscription ID is the correct outcome.
	joinIdempotency      map[string]joinIdempotencyEntry
	joinIdempotencyOrder []string
	// realtimeState 是 Lane 事件时由 ActorBinding 刷新的 Farm 实时状态视图
	// (27 §5.17),Lane 内访问一致。
	realtimeState subscription.FarmRealtimeState
	// pendingGuard 是 27 §5.17 的 PostCommitDeliveryGuard,强提交前安装,
	// 实时交接有定论后清除。
	guardMu          sync.Mutex
	pendingGuard     *commitGuard
	maxSubscriptions int
	maxBufferItems   int
	maxBufferBytes   int
	maxJoinDuration  time.Duration
	writerEpoch      uint64
	// participantState/participantCycles are the Farm domain view used by the
	// COMMIT_PLANT Participant.  The existing farmpb record remains the public
	// projection for legacy commands; the participant proof below is the
	// durable source for the extra frozen rule/care/compendium fields and is
	// rebuilt after passivation.
	participantState  *participantFarmState
	participantCycles map[string]*participantCropState
	// theftPRF and guardSnapshot are Farm-side inputs.  The former is injected
	// by composition and the latter is installed by the existing GuardSync
	// boundary; neither is accepted from the Theft request payload.
	theftPRF           TheftPRF
	theftGuardSnapshot domain.GuardSnapshot
	// opportunityHeaderV2/opportunityFactsV1 are compact authority read fields
	// loaded and saved atomically beside record. Missing fields on a legacy
	// record are UNKNOWN/fallback, never an authoritative empty opportunity.
	opportunityHeaderV2        domain.FarmOpportunityHeaderV2
	opportunityFactsV1         domain.FarmOpportunityFactsV1
	opportunityModelsAvailable bool
	// opportunityWakeupEnabled is frozen at activation from RegistryOptions.
	// The store capability check below remains a second fail-closed fence.
	opportunityWakeupEnabled bool
}

func (a *FarmActor) UnknownContext() (UnknownContext, bool) {
	if a == nil || a.unknownContext == nil {
		return UnknownContext{}, false
	}
	return *a.unknownContext, true
}

func (a *FarmActor) FarmID() uint64          { return a.farmID }
func (a *FarmActor) ActivationEpoch() uint64 { return a.activationEpoch }

func (a *FarmActor) Snapshot(now time.Time) *farmpb.FarmSnapshot {
	return snapshot(a.record, now)
}

func (a *FarmActor) Operation(operationID string) (*ActionResult, error) {
	if result := a.replay(operationID); result != nil {
		result.Settlement = a.findSettlement(operationID)
		result.ActivationEpoch = a.activationEpoch
		return result, nil
	}
	return nil, farmError(commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND, "operation not found")
}

func (a *FarmActor) Join(req JoinRequest, now time.Time) (*JoinResult, error) {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.Execute(context.Background(), a.farmID, a, func() (any, error) {
			return a.join(req, now)
		})
		if err != nil {
			return nil, err
		}
		result, _ := value.(*JoinResult)
		return result, nil
	}
	return a.join(req, now)
}

func (a *FarmActor) join(req JoinRequest, now time.Time) (*JoinResult, error) {
	return a.joinWithMode(req, now, false)
}

// CreateOrReuseJoining 创建未 Arm 的 JOINING。它只分配名额和服务端
// subscription_id，不读取 Snapshot，也不建立 FarmChange 捕获游标。
func (a *FarmActor) CreateOrReuseJoining(req JoinRequest, now time.Time) (*JoinResult, error) {
	if strings.TrimSpace(req.JoinAttemptID) == "" {
		return nil, errors.New("farm actor: join_attempt_id required")
	}
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteCreateOrReuseJoining(
			context.Background(), a.farmID, a, req.JoinAttemptID, now,
			func() (any, error) { return a.joinWithMode(req, now, true) },
		)
		if err != nil {
			return nil, err
		}
		result, _ := value.(*JoinResult)
		return result, nil
	}
	return a.joinWithMode(req, now, true)
}

func (a *FarmActor) joinWithMode(req JoinRequest, now time.Time, unarmed bool) (*JoinResult, error) {
	if req.ViewerPlayerID == 0 || req.SessionEpoch == 0 ||
		req.GatewayID == "" || req.GatewayBootID == "" || req.ConnectionID == "" ||
		req.TransportGeneration == 0 || req.RouteRevision == 0 {
		return nil, errors.New("farm actor: invalid join")
	}
	if a.maxJoinDuration <= 0 {
		a.maxJoinDuration = defaultMaxJoinDuration
	}
	a.pruneExpiredJoins(now)
	a.pruneExpiredSubscriptionLeases(now)
	snapshotForResult := func(p *Presence) *farmpb.FarmSnapshot {
		if unarmed {
			return p.BaselineSnapshot
		}
		return a.Snapshot(now)
	}
	joinHash := joinPayloadHash(a.farmID, req.ViewerPlayerID)
	if req.OperationID != "" {
		a.pruneJoinIdempotency(now)
		if entry, ok := a.joinIdempotency[req.OperationID]; ok {
			if !bytes.Equal(entry.IntentHash, joinHash) {
				return nil, farmError(commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, "operation_id 已绑定到不同 Join")
			}
			req.SubscriptionID = entry.SubscriptionID
			old := a.presence[req.SubscriptionID]
			if old != nil && sameJoinRequest(old, req) {
				return a.joinResult(old, snapshotForResult(old), true, now), nil
			}
			if unarmed {
				// The production W2 Join path must not recreate or revive a removed
				// lease with the old intent. The client needs a Fresh Join intent.
				return nil, ErrJoinStale
			}
		}
	}
	if oldID := a.presenceByPlayer[req.ViewerPlayerID]; oldID != "" {
		old := a.presence[oldID]
		if old != nil {
			if req.SessionEpoch < old.SessionEpoch {
				return nil, ErrJoinStale
			}
			if sameJoinRequest(old, req) {
				return a.joinResult(old, snapshotForResult(old), true, now), nil
			}
			if req.ForceFresh {
				// Gate has already revoked the old subscription locally, but its
				// asynchronous remote cleanup may still be in flight. Do not let
				// that transient Presence reuse the old JoinAttemptID; the Gate
				// has already installed a new pending Join fence.
				a.removePresence(oldID)
			} else {
				// A navigation duplicate from the same authenticated transport
				// does not own a client-chosen subscription_id. Reuse the
				// established Presence and bind the new operation_id to its
				// server-issued subscription instead of creating a second Presence.
				if req.SessionEpoch == old.SessionEpoch && req.SubscriptionID == "" &&
					old.Phase == PresenceInFarm && sameConnectionFence(old, req) {
					if req.OperationID != "" {
						a.rememberJoinIdempotency(req.OperationID, old.SubscriptionID, joinHash, now)
					}
					return a.joinResult(old, snapshotForResult(old), true, now), nil
				}
				// connection_generation/route_revision are scoped identifiers,
				// not a global ordering. Within one session_epoch only an exact
				// fence is idempotent; any different fence is a conflict.
				if req.SessionEpoch == old.SessionEpoch {
					return nil, ErrJoinStale
				}
				a.removePresence(oldID)
			}
		}
	}
	if req.SubscriptionID == "" {
		req.SubscriptionID = issueSubscriptionID()
	}
	if collision := a.presence[req.SubscriptionID]; collision != nil {
		if collision.ViewerPlayerID != req.ViewerPlayerID {
			return nil, ErrJoinStale
		}
		a.removePresence(req.SubscriptionID)
	}
	if a.maxSubscriptions <= 0 {
		a.maxSubscriptions = defaultMaxSubscriptions
	}
	if len(a.presence) >= a.maxSubscriptions {
		return nil, farmError(commonpb.ErrorCode_ERR_FARM_FULL, "农场在线人数已满")
	}
	epoch := presenceCounter.Add(1)
	var snapshotValue *farmpb.FarmSnapshot
	snapshotVersion := uint64(0)
	if !unarmed {
		snapshotValue = a.Snapshot(now)
		snapshotVersion = snapshotValue.FarmVersion
	}
	fallbackExpiresAt := time.Time{}
	if a.subscriptionHub != nil {
		fallbackExpiresAt = now.Add(a.subscriptionHub.FallbackTTL())
	}
	leaseTTL := a.subscriptionHub.SubscriptionLeaseTTL()
	leaseExpiresAt := now.Add(leaseTTL)
	a.presence[req.SubscriptionID] = &Presence{
		JoinOperationID: req.OperationID,
		JoinAttemptID:   req.JoinAttemptID,
		SubscriptionID:  req.SubscriptionID, ViewerPlayerID: req.ViewerPlayerID,
		SessionEpoch: req.SessionEpoch, GatewayID: req.GatewayID, GatewayBootID: req.GatewayBootID,
		ConnectionID: req.ConnectionID, TransportGeneration: req.TransportGeneration,
		RouteRevision: req.RouteRevision, PresenceEpoch: epoch,
		SnapshotVersion: snapshotVersion, LastPushedVersion: snapshotVersion,
		BaselineSnapshot: snapshotValue,
		// 基线安装代次(27 §5.17):失效后首批 Join 为旧代,Fresh Baseline 完成后
		// 由 InstallBaseline 按代次条件清除 invalid。
		DeliveryGeneration: a.realtimeState.DeliveryGeneration,
		Phase:              PresenceJoining,
		// 当前双锁存路径在创建时即视为 armed(W2 的 CreateOrReuseJoining 才会
		// 创建 unarmed Presence 并由 ArmSnapshot 显式授权)。
		Armed:             !unarmed,
		JoiningStartedAt:  now,
		FallbackExpiresAt: fallbackExpiresAt,
		LeaseExpiresAt:    leaseExpiresAt,
	}
	a.presenceByPlayer[req.ViewerPlayerID] = req.SubscriptionID
	if req.JoinAttemptID != "" {
		if a.presenceByAttempt == nil {
			a.presenceByAttempt = make(map[string]string)
		}
		a.presenceByAttempt[req.JoinAttemptID] = req.SubscriptionID
	}
	if req.OperationID != "" {
		a.rememberJoinIdempotency(req.OperationID, req.SubscriptionID, joinHash, now)
	}
	return a.joinResult(a.presence[req.SubscriptionID], snapshotValue, false, now), nil
}

// ArmSnapshot 在同一 Farm Runtime Lane 中原子安装 Snapshot(V) 与 V+1 捕获
// 游标。重复调用复用原基线；旧 attempt 或连接 Fence 不得重新 Arm。
func (a *FarmActor) ArmSnapshot(req ArmSnapshotRequest, now time.Time) (*JoinResult, bool, error) {
	arm := func() (any, error) {
		presence := a.presence[req.SubscriptionID]
		if presence == nil || presence.JoinAttemptID != req.JoinAttemptID ||
			!sameLeaveFence(presence, req.Fence) || presence.Phase != PresenceJoining {
			return nil, ErrJoinNotFound
		}
		if presence.Armed {
			if presence.BaselineSnapshot == nil {
				return nil, ErrJoinNotFound
			}
			return a.joinResult(presence, presence.BaselineSnapshot, true, now), nil
		}
		snapshot := a.Snapshot(now)
		if err := publicprofile.Validate(req.OwnerPublicProfile, a.record.GetFarmId()); err != nil {
			return nil, ErrJoinNotFound
		}
		snapshot.OwnerPublicProfile = proto.Clone(req.OwnerPublicProfile).(*publicpb.PublicProfileView)
		presence.BaselineSnapshot = snapshot
		presence.SnapshotVersion = snapshot.FarmVersion
		presence.LastPushedVersion = snapshot.FarmVersion
		presence.Armed = true
		return a.joinResult(presence, snapshot, false, now), nil
	}
	var value any
	var err error
	if a.subscriptionHub != nil {
		value, err = a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, arm)
	} else {
		value, err = arm()
	}
	if err != nil {
		return nil, false, err
	}
	result, _ := value.(*JoinResult)
	return result, result != nil && result.Replayed, nil
}

func (a *FarmActor) SnapshotQueueAccepted(subscriptionID, joinAttemptID string, snapshotVersion uint64,
	fence LeaveRequest) ([]*farmpb.FarmChange, uint64, bool, uint64, error) {
	if a.subscriptionHub == nil {
		return nil, 0, false, 0, ErrJoinNotFound
	}
	return a.subscriptionHub.SnapshotQueueAccepted(context.Background(), a.farmID,
		joinControlFence(subscriptionID, joinAttemptID, fence), snapshotVersion)
}

func (a *FarmActor) CatchupBatchQueueAccepted(subscriptionID, joinAttemptID string,
	fromVersion, throughVersion uint64, fence LeaveRequest) (bool, uint64, error) {
	if a.subscriptionHub == nil {
		return false, 0, ErrJoinNotFound
	}
	return a.subscriptionHub.CatchupBatchQueueAccepted(context.Background(), a.farmID,
		joinControlFence(subscriptionID, joinAttemptID, fence), fromVersion, throughVersion)
}

func (a *FarmActor) SubscriptionReadyQueueAccepted(subscriptionID, joinAttemptID string,
	readyThrough uint64, fence LeaveRequest) ([]*farmpb.FarmChange, uint64, error) {
	if a.subscriptionHub == nil {
		return nil, 0, ErrJoinNotFound
	}
	postReady, deliveryGeneration, presenceEpoch, err := a.subscriptionHub.ReadyQueueAccepted(context.Background(),
		a.farmID, joinControlFence(subscriptionID, joinAttemptID, fence), readyThrough)
	if err == nil {
		_, _ = a.subscriptionHub.InstallBaseline(context.Background(), a.farmID,
			deliveryGeneration)
	}
	return postReady, presenceEpoch, err
}

func joinControlFence(subscriptionID, joinAttemptID string, fence LeaveRequest) subscription.JoinControlFence {
	return subscription.JoinControlFence{
		SubscriptionID: subscriptionID, JoinAttemptID: joinAttemptID,
		ViewerPlayerID: fence.ViewerPlayerID, SessionEpoch: fence.SessionEpoch,
		GatewayID: fence.GatewayID, GatewayBootID: fence.GatewayBootID,
		ConnectionID: fence.ConnectionID, TransportGeneration: fence.TransportGeneration,
		RouteRevision: fence.RouteRevision,
	}
}

func joinPayloadHash(farmID, viewerPlayerID uint64) []byte {
	// Join's business intent is the target farm plus viewer. request_id,
	// session fences, and operation_id are transport/retry metadata.
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d", farmID, viewerPlayerID)))
	return sum[:]
}

func issueSubscriptionID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d", presenceCounter.Add(1))))
		return "sub-" + hex.EncodeToString(sum[:16])
	}
	return "sub-" + hex.EncodeToString(raw[:])
}

func (a *FarmActor) pruneJoinIdempotency(now time.Time) {
	if a.joinIdempotency == nil {
		a.joinIdempotency = make(map[string]joinIdempotencyEntry)
	}
	kept := a.joinIdempotencyOrder[:0]
	for _, operationID := range a.joinIdempotencyOrder {
		entry, ok := a.joinIdempotency[operationID]
		if !ok || !entry.ExpiresAt.After(now) {
			delete(a.joinIdempotency, operationID)
			continue
		}
		kept = append(kept, operationID)
	}
	a.joinIdempotencyOrder = kept
}

func (a *FarmActor) rememberJoinIdempotency(operationID, subscriptionID string, intentHash []byte, now time.Time) {
	a.pruneJoinIdempotency(now)
	if _, exists := a.joinIdempotency[operationID]; exists {
		a.joinIdempotency[operationID] = joinIdempotencyEntry{
			SubscriptionID: subscriptionID,
			IntentHash:     append([]byte(nil), intentHash...),
			ExpiresAt:      now.Add(joinIdempotencyTTL),
		}
		return
	}
	for len(a.joinIdempotencyOrder) >= maxJoinIdempotencyEntries {
		oldest := a.joinIdempotencyOrder[0]
		a.joinIdempotencyOrder = a.joinIdempotencyOrder[1:]
		delete(a.joinIdempotency, oldest)
	}
	a.joinIdempotency[operationID] = joinIdempotencyEntry{
		SubscriptionID: subscriptionID,
		IntentHash:     append([]byte(nil), intentHash...),
		ExpiresAt:      now.Add(joinIdempotencyTTL),
	}
	a.joinIdempotencyOrder = append(a.joinIdempotencyOrder, operationID)
}

func (a *FarmActor) ConfirmSnapshotQueued(join *JoinResult) ([]*farmpb.FarmChange, error) {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.Execute(context.Background(), a.farmID, a, func() (any, error) {
			return a.confirmSnapshotQueued(join)
		})
		if err != nil {
			return nil, err
		}
		changes, _ := value.([]*farmpb.FarmChange)
		return changes, nil
	}
	return a.confirmSnapshotQueued(join)
}

func (a *FarmActor) confirmSnapshotQueued(join *JoinResult) ([]*farmpb.FarmChange, error) {
	presence := a.matchJoin(join)
	if presence == nil || presence.Phase != PresenceJoining {
		return nil, ErrJoinNotFound
	}
	if a.joinExpired(presence, time.Now()) {
		a.removePresence(presence.SubscriptionID)
		return nil, ErrJoinOverflow
	}
	presence.Phase = PresenceFlushing
	changes := append([]*farmpb.FarmChange(nil), presence.Buffer...)
	presence.Buffer = nil
	presence.BufferBytes = 0
	return changes, nil
}

func (a *FarmActor) DrainChangesOrConfirm(join *JoinResult) ([]*farmpb.FarmChange, error) {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.Execute(context.Background(), a.farmID, a, func() (any, error) {
			return a.drainChangesOrConfirm(join)
		})
		if err != nil {
			return nil, err
		}
		changes, _ := value.([]*farmpb.FarmChange)
		if len(changes) == 0 {
			// 已进入 IN_FARM(Fresh Baseline 完成):按 29 §8.5.1 的代次条件
			// 尝试清除 Farm 级失效状态。
			_, _ = a.subscriptionHub.InstallBaseline(context.Background(), a.farmID, a.realtimeState.DeliveryGeneration)
		}
		return changes, nil
	}
	return a.drainChangesOrConfirm(join)
}

func (a *FarmActor) drainChangesOrConfirm(join *JoinResult) ([]*farmpb.FarmChange, error) {
	presence := a.matchJoin(join)
	if presence == nil || presence.Phase != PresenceFlushing {
		return nil, ErrJoinNotFound
	}
	if a.joinExpired(presence, time.Now()) {
		a.removePresence(presence.SubscriptionID)
		return nil, ErrJoinOverflow
	}
	if len(presence.Buffer) > 0 {
		changes := append([]*farmpb.FarmChange(nil), presence.Buffer...)
		presence.Buffer = nil
		presence.BufferBytes = 0
		return changes, nil
	}
	presence.Phase = PresenceInFarm
	return nil, nil
}

func (a *FarmActor) matchJoin(join *JoinResult) *Presence {
	if join == nil {
		return nil
	}
	presence := a.presence[join.SubscriptionID]
	snapshotVersion := uint64(0)
	if join.Snapshot != nil {
		snapshotVersion = join.Snapshot.FarmVersion
	}
	if presence == nil || presence.ViewerPlayerID != join.ViewerPlayerID ||
		(join.JoinAttemptID != "" && presence.JoinAttemptID != join.JoinAttemptID) ||
		presence.SessionEpoch != join.SessionEpoch ||
		presence.GatewayID != join.GatewayID || presence.GatewayBootID != join.GatewayBootID ||
		presence.ConnectionID != join.ConnectionID ||
		presence.TransportGeneration != join.Generation || presence.PresenceEpoch != join.PresenceEpoch ||
		presence.RouteRevision != join.RouteRevision ||
		(snapshotVersion != 0 && presence.SnapshotVersion != snapshotVersion) {
		return nil
	}
	return presence
}

// LeaveFenced only removes the exact Presence generation. Delayed disconnect,
// failed downlink callbacks and reused connection IDs therefore cannot delete a
// newer viewer session.
func (a *FarmActor) LeaveFenced(req LeaveRequest) bool {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			return a.leaveFenced(req), nil
		})
		removed, _ := value.(bool)
		return err == nil && removed
	}
	return a.leaveFenced(req)
}

func (a *FarmActor) leaveFenced(req LeaveRequest) bool {
	p := a.presence[req.SubscriptionID]
	if !subscription.LeaveFenced(a.presence, a.presenceByPlayer, req) {
		return false
	}
	if p != nil && p.JoinAttemptID != "" && a.presenceByAttempt[p.JoinAttemptID] == req.SubscriptionID {
		delete(a.presenceByAttempt, p.JoinAttemptID)
	}
	return true
}

func (a *FarmActor) LeaveJoin(join *JoinResult) bool {
	if join == nil {
		return false
	}
	req := LeaveRequest{
		SubscriptionID: join.SubscriptionID, ViewerPlayerID: join.ViewerPlayerID,
		SessionEpoch: join.SessionEpoch, GatewayID: join.GatewayID, GatewayBootID: join.GatewayBootID,
		ConnectionID: join.ConnectionID, TransportGeneration: join.Generation,
		RouteRevision: join.RouteRevision, PresenceEpoch: join.PresenceEpoch,
	}
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			return a.leaveFenced(req), nil
		})
		removed, _ := value.(bool)
		return err == nil && removed
	}
	return a.leaveFenced(req)
}

// CancelJoin removes only the Presence created by the matching Join intent
// and full connection fence. It is idempotent and cannot delete a later
// connection's subscription even when connection_id has been reused.
func (a *FarmActor) CancelJoin(req CancelJoinRequest) bool {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			return a.cancelJoin(req), nil
		})
		canceled, _ := value.(bool)
		return err == nil && canceled
	}
	return a.cancelJoin(req)
}

func (a *FarmActor) cancelJoin(req CancelJoinRequest) bool {
	return subscription.CancelJoin(a.presence, a.presenceByPlayer, a.joinIdempotency, req)
}

// PresencePhase exposes the Join barrier stage for fault-injection tests.
func (a *FarmActor) PresencePhase(subscriptionID string) (PresencePhase, bool) {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			phase, ok := a.presencePhase(subscriptionID)
			return struct {
				phase PresencePhase
				ok    bool
			}{phase: phase, ok: ok}, nil
		})
		if err != nil {
			return 0, false
		}
		result := value.(struct {
			phase PresencePhase
			ok    bool
		})
		return result.phase, result.ok
	}
	return a.presencePhase(subscriptionID)
}

func (a *FarmActor) presencePhase(subscriptionID string) (PresencePhase, bool) {
	presence := a.presence[subscriptionID]
	if presence == nil {
		return 0, false
	}
	return presence.Phase, true
}

// ForcePresenceCursorForTest rewinds LastPushedVersion so the next publish
// observes farm_change_version_gap. Ingress fault-injection tests only.
func (a *FarmActor) ForcePresenceCursorForTest(subscriptionID string, lastPushed uint64) bool {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			return a.forcePresenceCursor(subscriptionID, lastPushed), nil
		})
		forced, _ := value.(bool)
		return err == nil && forced
	}
	return a.forcePresenceCursor(subscriptionID, lastPushed)
}

func (a *FarmActor) forcePresenceCursor(subscriptionID string, lastPushed uint64) bool {
	presence := a.presence[subscriptionID]
	if presence == nil {
		return false
	}
	presence.LastPushedVersion = lastPushed
	return true
}

func (a *FarmActor) PresenceCount() int {
	if a.subscriptionHub != nil {
		count, err := a.subscriptionHub.PresenceCount(context.Background(), a.farmID)
		if err == nil {
			return count
		}
	}
	return len(a.presence)
}

// Leave is retained for local compatibility callers. Production command and
// async callback paths use LeaveFenced/LeaveJoin.
func (a *FarmActor) Leave(subscriptionID, connectionID string) bool {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			return a.leave(subscriptionID, connectionID), nil
		})
		removed, _ := value.(bool)
		return err == nil && removed
	}
	return a.leave(subscriptionID, connectionID)
}

func (a *FarmActor) leave(subscriptionID, connectionID string) bool {
	presence := a.presence[subscriptionID]
	if presence == nil || (connectionID != "" && presence.ConnectionID != connectionID) {
		return false
	}
	a.removePresence(subscriptionID)
	return true
}

func (a *FarmActor) LeaveConnection(connectionID string) {
	if a.subscriptionHub != nil {
		_, _ = a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			a.leaveConnection(connectionID)
			return nil, nil
		})
		return
	}
	a.leaveConnection(connectionID)
}

func (a *FarmActor) leaveConnection(connectionID string) {
	for id, presence := range a.presence {
		if presence != nil && presence.ConnectionID == connectionID {
			a.removePresence(id)
		}
	}
}

func (a *FarmActor) LeaveConnectionFence(route session.ActiveRoute) []Presence {
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.ExecuteSystem(context.Background(), a.farmID, a, func() (any, error) {
			return a.leaveConnectionFence(route), nil
		})
		if err != nil {
			return nil
		}
		removed, _ := value.([]Presence)
		return removed
	}
	return a.leaveConnectionFence(route)
}

func (a *FarmActor) leaveConnectionFence(route session.ActiveRoute) []Presence {
	var removed []Presence
	for id, presence := range a.presence {
		if presence != nil &&
			presence.ViewerPlayerID == route.PlayerID &&
			presence.SessionEpoch == route.SessionEpoch &&
			presence.GatewayID == route.GatewayID &&
			presence.GatewayBootID == route.GatewayBootID &&
			presence.ConnectionID == route.ConnectionID &&
			presence.TransportGeneration == route.ConnectionGeneration &&
			presence.RouteRevision == route.RouteRevision {
			removed = append(removed, *presence)
			a.removePresence(id)
		}
	}
	return removed
}

func sameJoinRequest(p *Presence, req JoinRequest) bool {
	return p != nil &&
		(req.JoinAttemptID == "" || p.JoinAttemptID == req.JoinAttemptID) &&
		p.SubscriptionID == req.SubscriptionID &&
		p.ViewerPlayerID == req.ViewerPlayerID &&
		p.SessionEpoch == req.SessionEpoch &&
		p.GatewayID == req.GatewayID &&
		p.GatewayBootID == req.GatewayBootID &&
		p.ConnectionID == req.ConnectionID &&
		p.TransportGeneration == req.TransportGeneration &&
		p.RouteRevision == req.RouteRevision
}

func sameLeaveFence(p *Presence, req LeaveRequest) bool {
	return p != nil &&
		p.SubscriptionID == req.SubscriptionID &&
		p.ViewerPlayerID == req.ViewerPlayerID &&
		p.SessionEpoch == req.SessionEpoch &&
		p.GatewayID == req.GatewayID &&
		p.GatewayBootID == req.GatewayBootID &&
		p.ConnectionID == req.ConnectionID &&
		p.TransportGeneration == req.TransportGeneration &&
		p.RouteRevision == req.RouteRevision &&
		(req.PresenceEpoch == 0 || p.PresenceEpoch == req.PresenceEpoch)
}

// sameConnectionFence deliberately excludes SubscriptionID. The subscription
// is a server-issued result, so a new Join intent from the same transport does
// not know it. Session and full Gate route fencing still prevent another
// connection in the same session from taking over by accident.
func sameConnectionFence(p *Presence, req JoinRequest) bool {
	return p != nil &&
		p.ViewerPlayerID == req.ViewerPlayerID &&
		p.SessionEpoch == req.SessionEpoch &&
		p.GatewayID == req.GatewayID &&
		p.GatewayBootID == req.GatewayBootID &&
		p.ConnectionID == req.ConnectionID &&
		p.TransportGeneration == req.TransportGeneration &&
		p.RouteRevision == req.RouteRevision
}

func (a *FarmActor) joinResult(p *Presence, snapshotValue *farmpb.FarmSnapshot, replayed bool, now time.Time) *JoinResult {
	acceptedTTL := p.LeaseExpiresAt.Sub(now)
	if acceptedTTL < 0 {
		acceptedTTL = 0
	}
	return &JoinResult{
		SubscriptionID: p.SubscriptionID, JoinAttemptID: p.JoinAttemptID,
		ViewerPlayerID: p.ViewerPlayerID,
		SessionEpoch:   p.SessionEpoch, GatewayID: p.GatewayID, GatewayBootID: p.GatewayBootID,
		ConnectionID: p.ConnectionID, Generation: p.TransportGeneration,
		RouteRevision: p.RouteRevision, PresenceEpoch: p.PresenceEpoch,
		ActivationEpoch: a.activationEpoch, Snapshot: snapshotValue, Replayed: replayed,
		AcceptedSubscriptionTTL: acceptedTTL,
	}
}

func (a *FarmActor) removePresence(subscriptionID string) {
	p := a.presence[subscriptionID]
	if p == nil {
		return
	}
	delete(a.presence, subscriptionID)
	if a.presenceByPlayer[p.ViewerPlayerID] == subscriptionID {
		delete(a.presenceByPlayer, p.ViewerPlayerID)
	}
	if p.JoinAttemptID != "" && a.presenceByAttempt[p.JoinAttemptID] == subscriptionID {
		delete(a.presenceByAttempt, p.JoinAttemptID)
	}
}

func (a *FarmActor) joinExpired(p *Presence, now time.Time) bool {
	return p != nil && !p.JoiningStartedAt.IsZero() &&
		now.Sub(p.JoiningStartedAt) > a.maxJoinDuration
}

func (a *FarmActor) pruneExpiredJoins(now time.Time) {
	for id, p := range a.presence {
		if p != nil && (p.Phase == PresenceJoining || p.Phase == PresenceFlushing ||
			p.Phase == PresenceActivating) && a.joinExpired(p, now) {
			a.removePresence(id)
		}
	}
}

func (a *FarmActor) pruneExpiredSubscriptionLeases(now time.Time) {
	for id, presence := range a.presence {
		if presence != nil && !presence.LeaseExpiresAt.IsZero() && !now.Before(presence.LeaseExpiresAt) {
			a.removePresence(id)
		}
	}
}

func (a *FarmActor) Plant(ctx context.Context, operationID, cropCycleID string, payloadHash []byte, req *farmpb.PlantCropRequest, expectedFarm, expectedPlot uint64, now time.Time, matureAfter time.Duration) (*ActionResult, error) {
	if replay, err := a.replayCommand(operationID, payloadHash); replay != nil || err != nil {
		return replay, err
	}
	if err := workidentity.ValidateStableOpaqueID(cropCycleID); err != nil {
		return nil, farmError(commonpb.ErrorCode_ERR_DECODE, "invalid crop_cycle_id")
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	plot, cycle, err := a.validatePlot(req.GetPlotIndex(), expectedFarm, expectedPlot, "")
	if err != nil {
		return nil, err
	}
	if cycle != nil || plot.ActiveCropCycleId != "" {
		return nil, farmError(commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY, "地块不是空地")
	}
	if err := a.ensureReceiptCapacity(now); err != nil {
		return nil, err
	}
	if req.CropDefinitionId == 0 {
		req.CropDefinitionId = cropcatalog.CarrotDefinitionID
	}
	definition, ok := cropcatalog.ByDefinitionID(req.CropDefinitionId)
	if !ok {
		return nil, farmError(commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH, "作物定义不存在")
	}
	if matureAfter <= 0 {
		matureAfter = definition.MatureAfter
	}
	crop := &farmpb.CropCycle{
		CropCycleId: cropCycleID, PlotIndex: plot.Index,
		CropDefinitionId: req.CropDefinitionId, RuleVersion: 1,
		PlantAt: now.UnixMilli(), MatureAt: now.Add(matureAfter).UnixMilli(),
		BaseYield: definition.BaseYield, StealableLimit: definition.StealableLimit,
	}
	plot.ActiveCropCycleId = crop.CropCycleId
	plot.PlotVersion++
	a.record.Core.CropCycles = append(a.record.Core.CropCycles, crop)
	a.record.Core.FarmVersion++
	result := &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now)}
	if err := a.putReceipt(operationID, payloadHash, commonpb.CommandStatus_PROCESSING, result, now); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, operationID, commonpb.CommandType_PLANT); err != nil {
		return nil, err
	}
	change, recipients, resyncs, err := a.publish(plot, now)
	if err != nil {
		return nil, err
	}
	return &ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: result, Change: change, Recipients: recipients, Resyncs: resyncs, ActivationEpoch: a.activationEpoch}, nil
}

func (a *FarmActor) Water(ctx context.Context, operationID string, payloadHash []byte, req *farmpb.WaterCropRequest, expectedFarm, expectedPlot uint64, expectedCycle string, now time.Time) (*ActionResult, error) {
	if replay, err := a.replayCommand(operationID, payloadHash); replay != nil || err != nil {
		return replay, err
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	plot, cycle, err := a.validatePlot(req.GetPlotIndex(), expectedFarm, expectedPlot, expectedCycle)
	if err != nil {
		return nil, err
	}
	if cycle == nil {
		return nil, farmError(commonpb.ErrorCode_ERR_PLOT_EMPTY, "地块为空")
	}
	if now.UnixMilli() >= cycle.MatureAt {
		return nil, farmError(commonpb.ErrorCode_ERR_CROP_ALREADY_MATURE, "作物已成熟，不能再浇水")
	}
	if cycle.Watered {
		result := &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now)}
		return &ActionResult{Status: commonpb.CommandStatus_NO_CHANGE, Result: result}, nil
	}
	if err := a.ensureReceiptCapacity(now); err != nil {
		return nil, err
	}
	cycle.Watered = true
	plot.PlotVersion++
	a.record.Core.FarmVersion++
	result := &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now)}
	if err := a.putReceipt(operationID, payloadHash, commonpb.CommandStatus_SUCCEEDED, result, now); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, operationID, commonpb.CommandType_CARE); err != nil {
		return nil, err
	}
	change, recipients, resyncs, err := a.publish(plot, now)
	if err != nil {
		return nil, err
	}
	return &ActionResult{Status: commonpb.CommandStatus_SUCCEEDED, Result: result, Change: change,
		FriendOpportunityInvalidated: true,
		Recipients:                   recipients, Resyncs: resyncs, ActivationEpoch: a.activationEpoch}, nil
}

func (a *FarmActor) PrepareHarvest(ctx context.Context, operationID string, payloadHash []byte, beneficiary uint64, req *farmpb.HarvestCropRequest, expectedFarm, expectedPlot uint64, expectedCycle string, now time.Time) (*ActionResult, error) {
	if replay, err := a.replayCommand(operationID, payloadHash); replay != nil || err != nil {
		if err != nil {
			return nil, err
		}
		replay.Settlement = a.findSettlement(operationID)
		return replay, nil
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	plot, cycle, err := a.validatePlot(req.GetPlotIndex(), expectedFarm, expectedPlot, expectedCycle)
	if err != nil {
		return nil, err
	}
	if cycle == nil {
		return nil, farmError(commonpb.ErrorCode_ERR_PLOT_EMPTY, "地块为空")
	}
	if now.UnixMilli() < cycle.MatureAt {
		return nil, farmError(commonpb.ErrorCode_ERR_CROP_NOT_MATURE, "作物尚未成熟")
	}
	if err := a.ensureReceiptCapacity(now); err != nil {
		return nil, err
	}
	if err := a.ensureSettlementCapacity(now); err != nil {
		return nil, err
	}
	amount := int64(cycle.BaseYield - cycle.StolenTotal)
	if amount < 0 {
		amount = 0
	}
	settlement := &farmpb.FarmSettlement{
		SettlementId: operationID, Kind: farmpb.FarmSettlementKind_HARVEST,
		State:               farmpb.FarmSettlementState_TARGET_COMMITTED,
		BeneficiaryPlayerId: beneficiary, ItemId: cropItemID(cycle), ItemCount: amount,
		CropCycleId: cycle.CropCycleId, UpdatedAtMs: now.UnixMilli(),
		Participants: []*farmpb.FarmSettlementParticipant{{
			PlayerId: beneficiary, Role: farmpb.FarmParticipantRole_HARVESTER,
			ItemId: cropItemID(cycle), ItemCount: amount, ExpDelta: amount,
			CodexCropDefinitionId: cycle.CropDefinitionId,
		}},
	}
	a.record.ActiveSettlements = append(a.record.ActiveSettlements, settlement)
	a.removeCycle(cycle.CropCycleId)
	plot.ActiveCropCycleId = ""
	plot.PlotVersion++
	a.record.Core.FarmVersion++
	result := &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now), GrantedItemId: settlement.ItemId, GrantedItemCount: settlement.ItemCount, SettlementId: operationID}
	if err := a.putReceipt(operationID, payloadHash, commonpb.CommandStatus_PROCESSING, result, now); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, operationID, commonpb.CommandType_HARVEST); err != nil {
		return nil, err
	}
	change, recipients, resyncs, err := a.publish(plot, now)
	if err != nil {
		return nil, err
	}
	return &ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: result, Change: change,
		FriendOpportunityInvalidated: true,
		Recipients:                   recipients, Resyncs: resyncs, Settlement: settlement, ActivationEpoch: a.activationEpoch}, nil
}

func (a *FarmActor) PrepareTheft(ctx context.Context, operationID string, payloadHash []byte, visitor, relationEpoch uint64, req *farmpb.BeginTheftRequest, expectedFarm, expectedPlot uint64, expectedCycle string, now time.Time) (*ActionResult, error) {
	if visitor == 0 || visitor == a.farmID || relationEpoch == 0 {
		return nil, farmError(commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, "invalid theft authorization")
	}
	if replay, err := a.replayCommand(operationID, payloadHash); replay != nil || err != nil {
		if err != nil {
			return nil, err
		}
		replay.Settlement = a.findSettlement(operationID)
		return replay, nil
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	plot, cycle, err := a.validatePlot(req.GetPlotIndex(), expectedFarm, expectedPlot, expectedCycle)
	if err != nil {
		return nil, err
	}
	if cycle == nil {
		return nil, farmError(commonpb.ErrorCode_ERR_PLOT_EMPTY, "地块为空")
	}
	if now.UnixMilli() < cycle.MatureAt {
		return nil, farmError(commonpb.ErrorCode_ERR_CROP_NOT_MATURE, "作物尚未成熟")
	}
	for _, receipt := range a.record.RecentTheftAttempts {
		if receipt != nil && receipt.VisitorPlayerId == visitor && receipt.CropCycleId == cycle.CropCycleId {
			return nil, farmError(commonpb.ErrorCode_ERR_STEAL_ALREADY_DONE, "这一茬已经偷过")
		}
	}
	if cycle.StolenTotal >= cycle.StealableLimit {
		return nil, farmError(commonpb.ErrorCode_ERR_STEAL_QUOTA_EXHAUSTED, "这一茬已没有可偷份额")
	}
	if err := a.ensureReceiptCapacity(now); err != nil {
		return nil, err
	}
	if err := a.ensureSettlementCapacity(now); err != nil {
		return nil, err
	}
	cycle.StolenTotal++
	plot.PlotVersion++
	a.record.Core.FarmVersion++
	a.record.RecentTheftAttempts = append(a.record.RecentTheftAttempts, &farmpb.TheftAttemptReceipt{
		VisitorPlayerId: visitor, CropCycleId: cycle.CropCycleId,
		OperationId: operationID, CommittedAtMs: now.UnixMilli(),
	})
	settlement := &farmpb.FarmSettlement{
		SettlementId: operationID, Kind: farmpb.FarmSettlementKind_THEFT,
		State:               farmpb.FarmSettlementState_TARGET_COMMITTED,
		BeneficiaryPlayerId: visitor, ItemId: cropItemID(cycle), ItemCount: 1,
		CropCycleId: cycle.CropCycleId, UpdatedAtMs: now.UnixMilli(),
		RelationEpoch: relationEpoch,
		Participants: []*farmpb.FarmSettlementParticipant{
			{
				PlayerId: visitor, Role: farmpb.FarmParticipantRole_VISITOR,
				ItemId: cropItemID(cycle), ItemCount: 1, ExpDelta: 1,
			},
			{
				PlayerId: a.farmID, Role: farmpb.FarmParticipantRole_OWNER,
				CoinDelta: 1,
			},
		},
	}
	a.record.ActiveSettlements = append(a.record.ActiveSettlements, settlement)
	result := &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now), GrantedItemId: settlement.ItemId, GrantedItemCount: 1, SettlementId: operationID}
	if err := a.putReceipt(operationID, payloadHash, commonpb.CommandStatus_PROCESSING, result, now); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, operationID, commonpb.CommandType_THEFT); err != nil {
		return nil, err
	}
	change, recipients, resyncs, err := a.publish(plot, now)
	if err != nil {
		return nil, err
	}
	return &ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: result, Change: change,
		FriendOpportunityInvalidated: true,
		Recipients:                   recipients, Resyncs: resyncs, Settlement: settlement, ActivationEpoch: a.activationEpoch}, nil
}

func (a *FarmActor) PrepareCare(ctx context.Context, operationID string, payloadHash []byte, helper, relationEpoch uint64, req *farmpb.WaterCropRequest, expectedFarm, expectedPlot uint64, expectedCycle string, now time.Time) (*ActionResult, error) {
	if helper == 0 || helper == a.farmID || relationEpoch == 0 {
		return nil, farmError(commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, "invalid care authorization")
	}
	if replay, err := a.replayCommand(operationID, payloadHash); replay != nil || err != nil {
		if err != nil {
			return nil, err
		}
		replay.Settlement = a.findSettlement(operationID)
		return replay, nil
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	plot, cycle, err := a.validatePlot(req.GetPlotIndex(), expectedFarm, expectedPlot, expectedCycle)
	if err != nil {
		return nil, err
	}
	if cycle == nil {
		return nil, farmError(commonpb.ErrorCode_ERR_PLOT_EMPTY, "地块为空")
	}
	if now.UnixMilli() >= cycle.MatureAt {
		return nil, farmError(commonpb.ErrorCode_ERR_CROP_ALREADY_MATURE, "作物已成熟，不能再浇水")
	}
	if cycle.Watered {
		return &ActionResult{
			Status: commonpb.CommandStatus_NO_CHANGE,
			Result: &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now)},
		}, nil
	}
	if err := a.ensureReceiptCapacity(now); err != nil {
		return nil, err
	}
	if err := a.ensureSettlementCapacity(now); err != nil {
		return nil, err
	}
	cycle.Watered = true
	plot.PlotVersion++
	a.record.Core.FarmVersion++
	a.record.RecentCareAttempts = append(a.record.RecentCareAttempts, &farmpb.CareAttemptReceipt{
		HelperPlayerId: helper, CropCycleId: cycle.CropCycleId,
		OperationId: operationID, CommittedAtMs: now.UnixMilli(),
	})
	settlement := &farmpb.FarmSettlement{
		SettlementId: operationID, Kind: farmpb.FarmSettlementKind_CARE,
		State:               farmpb.FarmSettlementState_TARGET_COMMITTED,
		BeneficiaryPlayerId: helper, CropCycleId: cycle.CropCycleId,
		UpdatedAtMs: now.UnixMilli(), RelationEpoch: relationEpoch,
		Participants: []*farmpb.FarmSettlementParticipant{
			{PlayerId: helper, Role: farmpb.FarmParticipantRole_HELPER, ExpDelta: 2},
			{PlayerId: a.farmID, Role: farmpb.FarmParticipantRole_OWNER, ExpDelta: 1},
		},
	}
	a.record.ActiveSettlements = append(a.record.ActiveSettlements, settlement)
	result := &farmpb.FarmCommandResult{
		Snapshot: a.Snapshot(now), SettlementId: operationID,
	}
	if err := a.putReceipt(operationID, payloadHash, commonpb.CommandStatus_PROCESSING, result, now); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, operationID, commonpb.CommandType_CARE); err != nil {
		return nil, err
	}
	change, recipients, resyncs, err := a.publish(plot, now)
	if err != nil {
		return nil, err
	}
	return &ActionResult{
		Status: commonpb.CommandStatus_PROCESSING, Result: result,
		Change: change, FriendOpportunityInvalidated: true,
		Recipients: recipients, Resyncs: resyncs, Settlement: settlement,
		ActivationEpoch: a.activationEpoch,
	}, nil
}

func (a *FarmActor) CompleteOperation(ctx context.Context, operationID string, final *farmpb.FarmCommandResult, now time.Time) (*ActionResult, error) {
	receipt := a.findReceipt(operationID)
	if receipt == nil {
		return nil, farmError(commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND, "operation not found")
	}
	if receipt.Status == commonpb.CommandStatus_SUCCEEDED {
		return a.replay(operationID), nil
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	for _, settlement := range a.record.ActiveSettlements {
		if settlement != nil && settlement.SettlementId == operationID {
			settlement.State = farmpb.FarmSettlementState_COMPLETED
			settlement.UpdatedAtMs = now.UnixMilli()
		}
	}
	_, header, hasHeader, headerErr := decodeFarmMutationReceiptPayload(receipt.ResultPayload)
	if headerErr != nil {
		// Keep the legacy operation replayable, but retain an explicitly invalid
		// header marker so typed Participant admission cannot infer continuity
		// from a receipt whose opportunity provenance was not proved.
		header = domain.OpportunityHeader{}
		hasHeader = true
	}
	var data []byte
	var err error
	if hasHeader {
		data, err = encodeFarmMutationReceiptPayload(final, header)
	} else {
		data, err = proto.MarshalOptions{Deterministic: true}.Marshal(compactOperationResult(final))
	}
	if err != nil {
		return nil, err
	}
	receipt.Status = commonpb.CommandStatus_SUCCEEDED
	receipt.ErrorCode = commonpb.ErrorCode_OK
	receipt.ResultPayload = data
	receipt.CommittedAtMs = now.UnixMilli()
	if err := a.save(ctx, before, operationID, commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED); err != nil {
		return nil, err
	}
	return &ActionResult{Status: commonpb.CommandStatus_SUCCEEDED, Result: final, ActivationEpoch: a.activationEpoch}, nil
}

func (a *FarmActor) validatePlot(index uint32, expectedFarm, expectedPlot uint64, expectedCycle string) (*farmpb.Plot, *farmpb.CropCycle, error) {
	if expectedFarm != 0 && expectedFarm != a.record.Core.FarmVersion {
		return nil, nil, farmError(commonpb.ErrorCode_ERR_VERSION_CONFLICT, "farm_version 已变化")
	}
	plot := a.findPlot(index)
	if plot == nil {
		return nil, nil, farmError(commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, "地块不存在")
	}
	if expectedPlot != 0 && expectedPlot != plot.PlotVersion {
		return nil, nil, farmError(commonpb.ErrorCode_ERR_VERSION_CONFLICT, "plot_version 已变化")
	}
	if plot.GetLockState() == farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED {
		return nil, nil, farmError(commonpb.ErrorCode_ERR_PLOT_LOCKED, "地块尚未解锁")
	}
	cycle := a.findCycle(plot.ActiveCropCycleId)
	if expectedCycle != "" && (cycle == nil || expectedCycle != cycle.CropCycleId) {
		return nil, nil, farmError(commonpb.ErrorCode_ERR_VERSION_CONFLICT, "crop_cycle 已变化")
	}
	return plot, cycle, nil
}

func (a *FarmActor) publish(plot *farmpb.Plot, now time.Time) (*farmpb.FarmChange, []Presence, []ResyncNotice, error) {
	return a.publishCandidate(plot, now)
}

// publishCandidate consumes the immutable change candidate installed in the
// PostCommitDeliveryGuard by a Participant turn. Legacy callers have no
// candidate and continue to use the existing public-projection builder.
func (a *FarmActor) publishCandidate(plot *farmpb.Plot, now time.Time) (*farmpb.FarmChange, []Presence, []ResyncNotice, error) {
	var candidate *farmpb.FarmChange
	if guardVersion, _, guardChange, ok := a.guardCandidateSnapshot(); ok && guardChange != nil && guardVersion == a.record.Core.FarmVersion {
		candidate = guardChange
	}
	if a.subscriptionHub != nil {
		value, err := a.subscriptionHub.Execute(context.Background(), a.farmID, a, func() (any, error) {
			change, recipients, resyncs, err := a.publishInlineCandidate(plot, now, candidate)
			return struct {
				change     *farmpb.FarmChange
				recipients []Presence
				resyncs    []ResyncNotice
			}{change: change, recipients: recipients, resyncs: resyncs}, err
		})
		if errors.Is(err, ErrFarmEventLaneFull) {
			// The durable CAS has already succeeded. Never roll back business
			// truth because a runtime delivery lane is saturated. Retain every
			// old subscription as a terminal fence until REJOIN_REQUIRED reaches
			// the corresponding Gate FIFO (or that connection is unreachable).
			if candidate == nil {
				candidate = &farmpb.FarmChange{OwnerId: a.farmID, FarmVersion: a.record.Core.FarmVersion,
					ChangedPlots: []*farmpb.PlotView{plotView(a.record, plot, now)}}
			}
			change := candidate
			// 27 §5.17:提交成功但无法证明交接成功 → level-triggered 置失效;
			// Lane 恢复不自动清除,只有全部旧代订阅终止 + 新代 Fresh Baseline 才清除。
			_, _, invalidateErr := a.subscriptionHub.InvalidateFarm(context.Background(), a.farmID, change.FarmVersion, "lane_full")
			if invalidateErr != nil {
				return change, nil, nil, invalidateErr
			}
			resyncs, markErr := a.subscriptionHub.MarkFarmResyncRequired(context.Background(), a.farmID,
				"farm_event_lane_full", change.FarmVersion, a.activationEpoch)
			if markErr != nil {
				return change, nil, nil, markErr
			}
			// Guard 有定论(Invalidate + Mark 均已成功):清除。
			a.guardClear()
			a.subscriptionHub.ObserveLaneResync(len(resyncs))
			return change, nil, resyncs, nil
		}
		if err != nil {
			if candidate != nil {
				// The authority fact is already committed. If the live delivery
				// lane fails for a reason other than a bounded full queue, close
				// the same guard through the level-triggered Fresh Join fence.
				_, _, invalidateErr := a.subscriptionHub.InvalidateFarm(context.Background(), a.farmID, candidate.FarmVersion, "post_commit_delivery_failed")
				if invalidateErr == nil {
					resyncs, markErr := a.subscriptionHub.MarkFarmResyncRequired(context.Background(), a.farmID,
						"post_commit_delivery_failed", candidate.FarmVersion, a.activationEpoch)
					if markErr == nil {
						a.subscriptionHub.ObserveLaneResync(len(resyncs))
						a.guardClear()
						return candidate, nil, resyncs, nil
					}
				}
			}
			return nil, nil, nil, err
		}
		result := value.(struct {
			change     *farmpb.FarmChange
			recipients []Presence
			resyncs    []ResyncNotice
		})
		if result.change != nil {
			// 27 §5.17:变化已进入实时交接流,推进 last_enqueued_version 并清除 Guard。
			_ = a.subscriptionHub.MarkEnqueued(context.Background(), a.farmID, result.change.FarmVersion)
			a.guardClear()
		}
		return result.change, result.recipients, result.resyncs, nil
	}
	change, recipients, resyncs, err := a.publishInlineCandidate(plot, now, candidate)
	if err == nil && candidate != nil {
		a.guardClear()
	}
	return change, recipients, resyncs, err
}

func (a *FarmActor) publishInline(plot *farmpb.Plot, now time.Time) (*farmpb.FarmChange, []Presence, []ResyncNotice, error) {
	return a.publishInlineCandidate(plot, now, nil)
}

func (a *FarmActor) publishInlineCandidate(plot *farmpb.Plot, now time.Time, candidate *farmpb.FarmChange) (*farmpb.FarmChange, []Presence, []ResyncNotice, error) {
	change := candidate
	if change == nil {
		change = &farmpb.FarmChange{OwnerId: a.farmID, FarmVersion: a.record.Core.FarmVersion, ChangedPlots: []*farmpb.PlotView{plotView(a.record, plot, now)}}
	}
	var recipients []Presence
	var resyncs []ResyncNotice
	for _, presence := range a.presence {
		if presence == nil {
			continue
		}
		// 26 HC-ACCESS-37:未 Arm 的订阅不得捕获任何 Change(快照在 Arm 时构建,
		// 自 V+1 起才建立捕获游标)。当前双锁存路径创建即 armed,不受影响。
		if presence.Phase == PresenceJoining && !presence.Armed {
			continue
		}
		switch presence.Phase {
		case PresenceJoining, PresenceFlushing:
			// 冻结 ready_cut_version=N 后,version>N 的变化进入 post-ready 缓冲
			// 排在 SUBSCRIPTION_READY 后(HC-ACCESS-39),<=N 继续 catch-up 缓冲。
			if presence.ReadyCutFrozen && change.FarmVersion > presence.ReadyCutVersion {
				a.bufferPostReady(presence, change, now, &resyncs)
				continue
			}
			changeBytes := proto.Size(change)
			reason := ""
			if a.joinExpired(presence, now) {
				reason = "joining_buffer_duration_exceeded"
			} else if len(presence.Buffer) >= a.maxBufferItems {
				reason = "joining_buffer_items_exceeded"
			} else if presence.BufferBytes+changeBytes > a.maxBufferBytes {
				reason = "joining_buffer_bytes_exceeded"
			} else if presence.LastPushedVersion != 0 && change.FarmVersion != presence.LastPushedVersion+1 {
				reason = resyncReasonVersionGap
			}
			if reason != "" {
				resyncs = append(resyncs, ResyncNotice{
					Presence: *presence, Reason: reason,
					LatestKnownFarmVersion: change.FarmVersion, ActivationEpoch: a.activationEpoch,
				})
				markPresenceResyncRequired(presence)
				if a.subscriptionHub != nil {
					a.subscriptionHub.ObserveJoiningBufferDropped()
					a.observeVersionGap(reason)
				}
				continue
			}
			presence.Buffer = append(presence.Buffer, proto.Clone(change).(*farmpb.FarmChange))
			presence.BufferBytes += changeBytes
			presence.LastPushedVersion = change.FarmVersion
		case PresenceActivating:
			a.bufferPostReady(presence, change, now, &resyncs)
		case PresenceInFarm:
			if presence.LastPushedVersion != 0 && change.FarmVersion != presence.LastPushedVersion+1 {
				resyncs = append(resyncs, ResyncNotice{
					Presence: *presence, Reason: resyncReasonVersionGap,
					LatestKnownFarmVersion: change.FarmVersion, ActivationEpoch: a.activationEpoch,
				})
				markPresenceResyncRequired(presence)
				a.observeVersionGap(resyncReasonVersionGap)
				continue
			}
			presence.LastPushedVersion = change.FarmVersion
			recipients = append(recipients, *presence)
		case PresenceResyncRequired:
			// Terminal read-only fence. Do not append additional changes while
			// REJOIN_REQUIRED is waiting for the exact Gate queue result.
		}
	}
	return change, recipients, resyncs, nil
}

// bufferPostReady 把 version>N 的变化按序存入 post-ready 缓冲(HC-ACCESS-39)。
// 容量边界与 JoiningBuffer 一致;溢出即订阅失效(进入 Resync,而非无限增长)。
func (a *FarmActor) bufferPostReady(presence *Presence, change *farmpb.FarmChange, now time.Time, resyncs *[]ResyncNotice) {
	changeBytes := proto.Size(change)
	expected := presence.ReadyCutVersion + 1
	if presence.PostReadyLastVersion != 0 {
		expected = presence.PostReadyLastVersion + 1
	}
	reason := ""
	if len(presence.PostReadyBuffer) >= a.maxBufferItems {
		reason = "post_ready_buffer_items_exceeded"
	} else if presence.PostReadyBufferBytes+changeBytes > a.maxBufferBytes {
		reason = "post_ready_buffer_bytes_exceeded"
	} else if change.FarmVersion != expected {
		reason = resyncReasonVersionGap
	}
	if reason != "" {
		*resyncs = append(*resyncs, ResyncNotice{
			Presence: *presence, Reason: reason,
			LatestKnownFarmVersion: change.FarmVersion, ActivationEpoch: a.activationEpoch,
		})
		markPresenceResyncRequired(presence)
		if a.subscriptionHub != nil {
			a.subscriptionHub.ObserveJoiningBufferDropped()
		}
		a.observeVersionGap(reason)
		return
	}
	presence.PostReadyBuffer = append(presence.PostReadyBuffer, proto.Clone(change).(*farmpb.FarmChange))
	presence.PostReadyBufferBytes += changeBytes
	presence.PostReadyLastVersion = change.FarmVersion
}

// resyncReasonVersionGap marks the resync caused by a break in the delivered
// FarmChange sequence, as opposed to one caused by buffer capacity.
const resyncReasonVersionGap = "farm_change_version_gap"

// observeVersionGap counts only the delivery gap. Buffer overflow also ends in
// a resync, but that is capacity pushing back, while a gap means a change the
// subscriber was owed never reached it.
func (a *FarmActor) observeVersionGap(reason string) {
	if reason == resyncReasonVersionGap && a.subscriptionHub != nil {
		a.subscriptionHub.ObserveVersionGap()
	}
}

func markPresenceResyncRequired(presence *Presence) {
	if presence == nil {
		return
	}
	presence.Phase = PresenceResyncRequired
	presence.Buffer = nil
	presence.BufferBytes = 0
	presence.PostReadyBuffer = nil
	presence.PostReadyBufferBytes = 0
}

func (a *FarmActor) save(ctx context.Context, before *farmpb.FarmRecord, operationID string, commandType commonpb.CommandType) error {
	return a.saveWithCandidate(ctx, before, operationID, commandType, nil)
}

// saveWithCandidate installs the DeliveryGuard before the one authority CAS.
// Participant callers pass the exact immutable FarmChange built from their
// candidate record; legacy callers pass nil and retain the old projection
// builder in publish.
func (a *FarmActor) saveWithCandidate(ctx context.Context, before *farmpb.FarmRecord, operationID string, commandType commonpb.CommandType, change *farmpb.FarmChange) error {
	return a.saveWithCandidateAndOutcomeAndOrders(ctx, before, operationID, commandType, change,
		authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, nil)
}

// attachedOrderEpochStore is the deliberately generic Authority Binding
// extension used by source-local orders. Keeping it optional leaves the
// existing CASEpoch path byte-for-byte unchanged for Plant/Harvest and makes
// legacy blobstore registries fail closed instead of partially saving CARE.
type attachedOrderEpochStore interface {
	CASEpochWithOutcomeAndOrders(context.Context, uint64, uint64, uint64, []byte, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder) error
}

type farmAuthorityModelStore interface {
	GetModelState(context.Context, uint64) (authoritystore.ModelState, uint64, error)
	CASEpochModelStateWithOutcomeAndOrders(context.Context, uint64, uint64, uint64, authoritystore.ModelState, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder) error
}

type farmAuthorityExactModelStore interface {
	CASEpochModelStateWithOutcomeAndOrdersExact(context.Context, uint64, uint64, uint64, authoritystore.ModelState, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder) (*authoritystore.ExactCommitEvidence, error)
}

// farmAuthorityScheduleStore is optional so legacy/in-memory actor tests keep
// their existing persistence seam. Production Authority Binding implements it
// and therefore commits the current farm wakeup generation atomically with
// FarmRecord + Header + Facts.
type farmAuthorityScheduleStore interface {
	CASEpochModelStateWithOutcomeOrdersAndSchedules(context.Context, uint64, uint64, uint64, authoritystore.ModelState, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, []authoritystore.AttachedSchedule) error
}

type farmAuthorityExactScheduleStore interface {
	CASEpochModelStateWithOutcomeOrdersAndSchedulesExact(context.Context, uint64, uint64, uint64, authoritystore.ModelState, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, []authoritystore.AttachedSchedule) (*authoritystore.ExactCommitEvidence, error)
}

type farmAuthorityLayoutStore interface {
	ModelLayout() authoritystore.ModelLayoutVersion
}

func authorityStoreModelLayout(store blobstore.Store) authoritystore.ModelLayoutVersion {
	if layoutStore, ok := store.(farmAuthorityLayoutStore); ok {
		return layoutStore.ModelLayout()
	}
	return authoritystore.ModelLayoutV1
}

// saveWithCandidateAndOrders installs the same post-commit delivery guard as
// the existing save path, then chooses one Authority Commit carrying the
// candidate record and all attached source orders. There is intentionally no
// fallback order write after the record CAS.
func (a *FarmActor) saveWithCandidateAndOrders(ctx context.Context, before *farmpb.FarmRecord, operationID string, commandType commonpb.CommandType, change *farmpb.FarmChange, orders []authoritystore.AttachedOrder) error {
	return a.saveWithCandidateAndOutcomeAndOrders(ctx, before, operationID, commandType, change,
		authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, orders)
}

func (a *FarmActor) saveWithCandidateAndOutcomeAndOrders(ctx context.Context, before *farmpb.FarmRecord, operationID string, commandType commonpb.CommandType, change *farmpb.FarmChange, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) error {
	_, err := a.saveWithCandidateAndOutcomeAndOrdersInternal(ctx, before, operationID, commandType, change, outcome, orders, false)
	return err
}

func (a *FarmActor) saveWithCandidateAndOutcomeAndOrdersExact(ctx context.Context, before *farmpb.FarmRecord, operationID string, commandType commonpb.CommandType, change *farmpb.FarmChange, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) (*authoritystore.ExactCommitEvidence, error) {
	return a.saveWithCandidateAndOutcomeAndOrdersInternal(ctx, before, operationID, commandType, change, outcome, orders, true)
}

func (a *FarmActor) saveWithCandidateAndOutcomeAndOrdersInternal(ctx context.Context, before *farmpb.FarmRecord, operationID string, commandType commonpb.CommandType, change *farmpb.FarmChange, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, wantExact bool) (*authoritystore.ExactCommitEvidence, error) {
	binding, softSave := a.store.(*authority.Binding)
	if softSave && binding.SoftSaveEnabled() {
		a.record.ActorMemoryVersion = a.recordVersion + 1
	}
	modelState, headerV2, factsV1, err := buildFarmAuthorityCandidate(authorityStoreModelLayout(a.store), a.record)
	if err != nil {
		a.record = before
		return nil, err
	}
	data := modelState["record"]
	var schedules []authoritystore.AttachedSchedule
	if a.opportunityWakeupEnabled {
		if _, ok := a.store.(farmAuthorityScheduleStore); !ok {
			a.record = before
			return nil, errors.New("farm actor: opportunity wakeup authority seam unavailable")
		}
	}
	if a.opportunityWakeupEnabled && factsV1.SourceFarmVersion != 0 {
		schedule, scheduleErr := currentFarmOpportunitySchedule(a.farmID, factsV1)
		if scheduleErr != nil {
			a.record = before
			return nil, scheduleErr
		}
		schedules = []authoritystore.AttachedSchedule{schedule}
	}
	// 27 §5.17:发起强提交前安装 PostCommitDeliveryGuard(候选变化版本 V)。
	if change == nil {
		a.guardInstall(a.record.Core.FarmVersion)
	} else {
		a.guardInstallWithChange(a.record.Core.FarmVersion, change)
	}
	if softSave && binding.SoftSaveEnabled() {
		if err := binding.ValidateCurrentGeneration(ctx); err != nil {
			a.record = before
			a.guardClear()
			return nil, err
		}
		// dirty_best_effort deliberately drops durable business outcomes and
		// SourceOrders from the persistence boundary. Cross-actor completion is
		// attempted immediately by the coordinator; a hard crash inside the
		// writeback window may lose both the mutation and the follow-up step.
		// buildFarmAuthorityCandidate returns a fresh ModelState used only by
		// this mailbox turn. Transfer ownership to the dirty Binding instead of
		// cloning the complete Farm state again on every operation.
		memoryVersion, err := binding.StageSoftModelStateOwned(a.farmID, modelState, schedules)
		if err != nil {
			a.record = before
			a.guardClear()
			return nil, err
		}
		a.unknownContext = nil
		a.recordVersion = memoryVersion
		a.opportunityHeaderV2 = headerV2
		a.opportunityFactsV1 = factsV1
		a.opportunityModelsAvailable = true
		a.guardMarkCommitted()
		return nil, nil
	}
	var saveErr error
	var exactEvidence *authoritystore.ExactCommitEvidence
	if store, ok := a.store.(farmAuthorityExactScheduleStore); wantExact && ok {
		exactEvidence, saveErr = store.CASEpochModelStateWithOutcomeOrdersAndSchedulesExact(ctx, a.farmID, a.recordVersion, a.writerEpoch,
			modelState, outcome, orders, schedules)
	} else if store, ok := a.store.(farmAuthorityScheduleStore); ok {
		saveErr = store.CASEpochModelStateWithOutcomeOrdersAndSchedules(ctx, a.farmID, a.recordVersion, a.writerEpoch,
			modelState, outcome, orders, schedules)
	} else if store, ok := a.store.(farmAuthorityExactModelStore); wantExact && ok {
		exactEvidence, saveErr = store.CASEpochModelStateWithOutcomeAndOrdersExact(ctx, a.farmID, a.recordVersion, a.writerEpoch,
			modelState, outcome, orders)
	} else if store, ok := a.store.(farmAuthorityModelStore); ok {
		saveErr = store.CASEpochModelStateWithOutcomeAndOrders(ctx, a.farmID, a.recordVersion, a.writerEpoch,
			modelState, outcome, orders)
	} else if len(orders) > 0 || outcome.Status != authoritystore.TerminalSucceeded {
		store, ok := a.store.(attachedOrderEpochStore)
		if !ok {
			a.record = before
			a.guardClear()
			return nil, ErrCareOrderAuthorityUnavailable
		}
		saveErr = store.CASEpochWithOutcomeAndOrders(ctx, a.farmID, a.recordVersion, a.writerEpoch, data,
			outcome, orders)
	} else {
		// Preserve the existing Plant/Harvest/legacy CASEpoch behavior.
		saveErr = blobstore.CASAtEpoch(ctx, a.store, a.farmID, a.recordVersion, a.writerEpoch, data)
	}
	if saveErr != nil {
		err := saveErr
		a.record = before
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			// UNKNOWN:保留 Guard,先按权威 Receipt/Version 解析。
			a.guardMarkUnknown()
			a.unknownContext = &UnknownContext{
				OperationID: operationID, BeforeRecordVersion: a.recordVersion,
				ExpectedWriterEpoch: a.writerEpoch, ActivationEpoch: a.activationEpoch,
				CommandType: commandType,
			}
		} else {
			// 明确未提交:清除 Guard,不得发布 FarmChange。
			a.guardClear()
		}
		if errors.Is(err, blobstore.ErrCASConflict) {
			_ = a.loadOrCreate(ctx)
		}
		// TIMEOUT_UNKNOWN is deliberately returned unchanged. The farm
		// record contains operation receipts, but this save path currently
		// lacks a storage-level resolver that can re-read and prove this
		// operation was committed. WRITER_EPOCH_MISMATCH likewise must not
		// be treated as a retryable version conflict; unloading this
		// activation remains a known gap in this batch.
		return nil, err
	}
	a.unknownContext = nil
	a.recordVersion++
	a.opportunityHeaderV2 = headerV2
	a.opportunityFactsV1 = factsV1
	a.opportunityModelsAvailable = true
	a.guardMarkCommitted()
	return exactEvidence, nil
}

func (a *FarmActor) replay(operationID string) *ActionResult {
	receipt := a.findReceipt(operationID)
	if receipt == nil {
		return nil
	}
	if care, found, err := decodeCareProof(receipt.ResultPayload); found {
		if err != nil {
			return &ActionResult{Status: commonpb.CommandStatus_UNAVAILABLE, Result: &farmpb.FarmCommandResult{}, Replayed: true}
		}
		if err := validateCareReceiptBinding(receipt, care); err != nil {
			return &ActionResult{Status: commonpb.CommandStatus_UNAVAILABLE, Result: &farmpb.FarmCommandResult{}, Replayed: true}
		}
		return a.replayCareProof(receipt.Status, care)
	}
	result := &farmpb.FarmCommandResult{}
	if decoded, _, found, err := decodeFarmMutationReceiptPayload(receipt.ResultPayload); found {
		if err != nil {
			if decoded == nil {
				return &ActionResult{Status: commonpb.CommandStatus_UNAVAILABLE, Result: &farmpb.FarmCommandResult{}, Replayed: true}
			}
		}
		if decoded != nil {
			result = decoded
		}
	} else if err := proto.Unmarshal(receipt.ResultPayload, result); err != nil {
		return &ActionResult{Status: commonpb.CommandStatus_UNAVAILABLE, Result: &farmpb.FarmCommandResult{}, Replayed: true}
	}
	return &ActionResult{Status: receipt.Status, Result: result, ActivationEpoch: a.activationEpoch, Replayed: true}
}

func (a *FarmActor) replayCommand(operationID string, payloadHash []byte) (*ActionResult, error) {
	receipt := a.findReceipt(operationID)
	if receipt == nil {
		return nil, nil
	}
	if len(payloadHash) == 0 || !bytes.Equal(receipt.PayloadHash, payloadHash) {
		return nil, farmError(commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, "operation_id 已绑定到不同请求")
	}
	return a.replay(operationID), nil
}

func (a *FarmActor) putReceipt(operationID string, payloadHash []byte, status commonpb.CommandStatus, result *farmpb.FarmCommandResult, now time.Time) error {
	header, headerErr := a.nextLegacyOpportunityHeader(now)
	if headerErr != nil {
		// Preserve the legacy mutation and its WriterEpoch/RecordVersion
		// fencing. The invalid marker is durable, so a later typed Participant
		// cannot mistake an older header for current continuity.
		header = domain.OpportunityHeader{}
		if a != nil && a.record != nil && a.record.Core != nil {
			header.SourceFarmVersion = a.record.Core.FarmVersion
		}
	}
	return a.putReceiptWithOpportunityHeader(operationID, payloadHash, status, result, header, now)
}

// putReceiptWithOpportunityHeader persists a receipt with the opportunity
// header already adjudicated by the same domain decision as the FarmRecord.
// Typed mutations must use this path: recomputing from mutable actor caches can
// detach the receipt proof from the exact candidate committed by Authority.
func (a *FarmActor) putReceiptWithOpportunityHeader(operationID string, payloadHash []byte, status commonpb.CommandStatus, result *farmpb.FarmCommandResult, header domain.OpportunityHeader, now time.Time) error {
	cutoffMs := now.Add(-operationReceiptRetention).UnixMilli()
	receipts := a.record.RecentOperationReceipts[:0]
	for _, receipt := range a.record.RecentOperationReceipts {
		// Participant proof is the durable Plant/Attempt/Decision evidence used
		// to rebuild an active CropCycle.  It must not be deleted by the legacy
		// seven-day command GC; the bounded capacity guard is the physical limit.
		if receipt != nil && (strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) || receipt.CommittedAtMs >= cutoffMs) {
			receipts = append(receipts, receipt)
		}
	}
	a.record.RecentOperationReceipts = receipts

	settlements := a.record.ActiveSettlements[:0]
	for _, settlement := range a.record.ActiveSettlements {
		if settlement != nil && (settlement.State != farmpb.FarmSettlementState_COMPLETED || settlement.UpdatedAtMs >= cutoffMs) {
			settlements = append(settlements, settlement)
		}
	}
	a.record.ActiveSettlements = settlements

	theftAttempts := a.record.RecentTheftAttempts[:0]
	for _, attempt := range a.record.RecentTheftAttempts {
		if attempt != nil && (attempt.CommittedAtMs >= cutoffMs || activeParticipantCropCycle(a.record, attempt.GetCropCycleId())) {
			theftAttempts = append(theftAttempts, attempt)
		}
	}
	a.record.RecentTheftAttempts = theftAttempts

	careAttempts := a.record.RecentCareAttempts[:0]
	for _, attempt := range a.record.RecentCareAttempts {
		if attempt != nil && attempt.CommittedAtMs >= cutoffMs {
			careAttempts = append(careAttempts, attempt)
		}
	}
	a.record.RecentCareAttempts = careAttempts

	data, err := encodeFarmMutationReceiptPayload(result, header)
	if err != nil {
		return err
	}
	a.record.RecentOperationReceipts = append(a.record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{
		OperationId: operationID, Status: status, ErrorCode: commonpb.ErrorCode_OK,
		ResultPayload: data, CommittedAtMs: now.UnixMilli(),
		PayloadHash: append([]byte(nil), payloadHash...),
	})
	return nil
}

func activeParticipantCropCycle(record *farmpb.FarmRecord, cycleID string) bool {
	if record == nil || record.Core == nil || cycleID == "" {
		return false
	}
	for _, cycle := range record.Core.CropCycles {
		if cycle != nil && cycle.GetCropCycleId() == cycleID {
			return true
		}
	}
	return false
}

// compactOperationResult deliberately excludes FarmSnapshot. Snapshot and
// FarmChange are recoverable projections delivered through their own ordered
// channel; repeating a full 12-plot snapshot in every seven-day idempotency
// receipt creates quadratic full-record CAS amplification. The durable receipt
// retains only the non-reconstructable business outcome needed by
// GetOperationResult and replay.
func compactOperationResult(result *farmpb.FarmCommandResult) *farmpb.FarmCommandResult {
	if result == nil {
		return &farmpb.FarmCommandResult{}
	}
	return &farmpb.FarmCommandResult{
		GrantedItemId:    result.GrantedItemId,
		GrantedItemCount: result.GrantedItemCount,
		PrivateState:     append([]byte(nil), result.PrivateState...),
		SettlementId:     result.SettlementId,
	}
}

func (a *FarmActor) ensureReceiptCapacity(now time.Time) error {
	cutoffMs := now.Add(-operationReceiptRetention).UnixMilli()
	active := 0
	for _, receipt := range a.record.RecentOperationReceipts {
		if receipt != nil && receipt.CommittedAtMs >= cutoffMs {
			active++
		}
	}
	if active >= maxRecentReceipts {
		return farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "operation receipt retention window is full; retry after older receipts expire")
	}
	return nil
}

func (a *FarmActor) ensureSettlementCapacity(now time.Time) error {
	cutoffMs := now.Add(-operationReceiptRetention).UnixMilli()
	active := 0
	for _, settlement := range a.record.ActiveSettlements {
		if settlement == nil {
			continue
		}
		if settlement.State != farmpb.FarmSettlementState_COMPLETED || settlement.UpdatedAtMs >= cutoffMs {
			active++
		}
	}
	if active >= maxActiveSettlements {
		return farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "farm settlement retention window is full; retry after older settlements expire")
	}
	return nil
}

func (a *FarmActor) findReceipt(operationID string) *farmpb.FarmOperationReceipt {
	for i := len(a.record.RecentOperationReceipts) - 1; i >= 0; i-- {
		if receipt := a.record.RecentOperationReceipts[i]; receipt != nil && receipt.OperationId == operationID {
			return receipt
		}
	}
	return nil
}

// hasReceiptForOperation accepts both the legacy command receipt key and the
// namespaced Participant receipt key.  The Registry's generic Unknown context
// stores the invocation ID, while Participant persistence deliberately stores
// a collision-proof key derived from that ID.
func (a *FarmActor) hasReceiptForOperation(operationID string) bool {
	if a == nil {
		return false
	}
	if a.findReceipt(operationID) != nil {
		return true
	}
	return a.findReceipt(ParticipantReceiptOperationKey(operationID)) != nil
}

func (a *FarmActor) findSettlement(operationID string) *farmpb.FarmSettlement {
	for _, settlement := range a.record.ActiveSettlements {
		if settlement != nil && settlement.SettlementId == operationID {
			return settlement
		}
	}
	return nil
}

func (a *FarmActor) findPlot(index uint32) *farmpb.Plot {
	for _, plot := range a.record.Core.Plots {
		if plot != nil && plot.Index >= 0 && uint32(plot.Index) == index {
			return plot
		}
	}
	return nil
}

func (a *FarmActor) findCycle(id string) *farmpb.CropCycle {
	if id == "" {
		return nil
	}
	for _, cycle := range a.record.Core.CropCycles {
		if cycle != nil && cycle.CropCycleId == id {
			return cycle
		}
	}
	return nil
}

func (a *FarmActor) removeCycle(id string) {
	for index, cycle := range a.record.Core.CropCycles {
		if cycle != nil && cycle.CropCycleId == id {
			a.record.Core.CropCycles = append(a.record.Core.CropCycles[:index], a.record.Core.CropCycles[index+1:]...)
			return
		}
	}
}

func snapshot(record *farmpb.FarmRecord, now time.Time) *farmpb.FarmSnapshot {
	value := &farmpb.FarmSnapshot{OwnerId: record.FarmId, SnapshotTimeMs: now.UnixMilli(), FarmVersion: record.Core.FarmVersion}
	nextUnlockOrder := nextLockedPlotOrder(record)
	cycleByID := make(map[string]*farmpb.CropCycle, len(record.Core.CropCycles))
	for _, cycle := range record.Core.CropCycles {
		if cycle == nil {
			continue
		}
		// ActiveCropCycleId is the durable plot-to-cycle binding. Preserve the
		// historical first-match behavior even for malformed legacy records
		// whose PlotIndex is stale or whose cycle ID is duplicated.
		if _, exists := cycleByID[cycle.GetCropCycleId()]; !exists {
			cycleByID[cycle.GetCropCycleId()] = cycle
		}
	}
	for _, plot := range record.Core.Plots {
		if plot != nil {
			cycle := cycleByID[plot.GetActiveCropCycleId()]
			value.Plots = append(value.Plots, plotViewWithContext(plot, cycle, now, nextUnlockOrder))
		}
	}
	sort.Slice(value.Plots, func(i, j int) bool { return value.Plots[i].PlotIndex < value.Plots[j].PlotIndex })
	return value
}

func plotView(record *farmpb.FarmRecord, plot *farmpb.Plot, now time.Time) *farmpb.PlotView {
	var cycle *farmpb.CropCycle
	for _, candidate := range record.Core.CropCycles {
		if candidate != nil && candidate.CropCycleId == plot.ActiveCropCycleId {
			cycle = candidate
			break
		}
	}
	return plotViewWithContext(plot, cycle, now, nextLockedPlotOrder(record))
}

func plotViewWithContext(plot *farmpb.Plot, cycle *farmpb.CropCycle, now time.Time, nextUnlockOrder uint32) *farmpb.PlotView {
	rule := domain.DefaultPlotUnlockRule()
	view := &farmpb.PlotView{PlotIndex: uint32(max(plot.Index, 0)), PlotVersion: plot.PlotVersion,
		LockState: plot.GetLockState(), UnlockRuleVersion: rule.RuleVersion}
	plotID := uint32(max(plot.Index, 0)) + 1
	if view.LockState == farmpb.PlotLockState_PLOT_LOCK_STATE_UNSPECIFIED {
		if plotID <= domain.P0InitialUnlockedPlots {
			view.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
		} else {
			view.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
		}
	}
	if entry, ok := rule.EntryOf(plotID); ok {
		view.UnlockOrder = entry.UnlockOrder
		view.RequiredLevel = entry.RequiredLevel
		view.NormalCoinPrice = entry.NormalCoinPrice
		view.UnlockCandidate = view.LockState == farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED && entry.UnlockOrder == nextUnlockOrder
	}
	if cycle == nil {
		return view
	}
	view.CropCycleId = cycle.CropCycleId
	view.CropDefinitionId = cycle.CropDefinitionId
	view.CropPhase = farmpb.CropPhase_GROWING
	if now.UnixMilli() >= cycle.MatureAt {
		view.CropPhase = farmpb.CropPhase_MATURE
	}
	view.PlantAtMs = cycle.PlantAt
	view.MatureAtMs = cycle.MatureAt
	view.HarvestableCount = cycle.BaseYield - cycle.StolenTotal
	view.StolenTotal = cycle.StolenTotal
	view.Watered = cycle.Watered
	return view
}

func nextLockedPlotOrder(record *farmpb.FarmRecord) uint32 {
	for plotID := uint32(domain.P0InitialUnlockedPlots + 1); plotID <= domain.P0MaxPlots; plotID++ {
		plot := findRecordPlot(record, int32(plotID-1))
		if plot == nil || plot.GetLockState() != farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED {
			return plotID
		}
	}
	return 0
}

func findRecordPlot(record *farmpb.FarmRecord, index int32) *farmpb.Plot {
	if record == nil || record.Core == nil {
		return nil
	}
	for _, plot := range record.Core.Plots {
		if plot != nil && plot.Index == index {
			return plot
		}
	}
	return nil
}

func cropItemID(cycle *farmpb.CropCycle) int32 {
	if cycle == nil {
		return cropcatalog.CarrotItemID
	}
	if definition, ok := cropcatalog.ByDefinitionID(cycle.CropDefinitionId); ok {
		return definition.ProduceItemID
	}
	// Records created before the catalog was introduced used carrots as the
	// only valid crop. Keeping this fallback makes old Redis/Farm records
	// harvestable while unknown future definitions remain non-plantable.
	return cropcatalog.CarrotItemID
}

func farmError(code commonpb.ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
