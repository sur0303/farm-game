package subscription

import (
	"context"
	"errors"
	"fmt"

	farmpb "farmgame/api/gen/farm"

	"google.golang.org/protobuf/proto"
)

// 合同 26 §12 的 Hub Join 状态机原语。全部经目标 Farm 的 Lane 串行执行,
// 与 CreateOrReuseJoining/CancelJoinAttempt 同 Lane 排序。
// 当前双锁存路径驱动兼容子集;W2 的 Join RPC 家族将驱动完整握手。

var (
	ErrSubscriptionNotFound       = errors.New("farm subscription hub: subscription not found")
	ErrInvalidJoinTransition      = errors.New("farm subscription hub: invalid join state transition")
	ErrStaleJoinFence             = errors.New("farm subscription hub: stale join fence")
	ErrJoinCallbackAlreadyApplied = errors.New("farm subscription hub: join callback already applied")
)

// SnapshotQueueAccepted 把 Gate 已确认入队的 Snapshot(V) 变成有限追赶目标。
// N 在同一 Farm Lane 内取当前连续捕获的最高版本；此后的 N+1 变化只进入
// post-ready buffer，不再延长本次 Join。
func (h *SubscriptionHub) SnapshotQueueAccepted(ctx context.Context, farmID uint64,
	fence JoinControlFence, snapshotVersion uint64) (catchUp []*farmpb.FarmChange,
	readyCut uint64, activating bool, presenceEpoch uint64, err error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return nil, 0, false, 0, ErrSubscriptionNotFound
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[fence.SubscriptionID]
		if presence == nil {
			return nil, ErrSubscriptionNotFound
		}
		if !matchesJoinControlFence(presence, fence) {
			return nil, ErrStaleJoinFence
		}
		if presence.ReadyCutFrozen && presence.SnapshotVersion == snapshotVersion &&
			(presence.Phase == PresenceFlushing || presence.Phase == PresenceActivating ||
				presence.Phase == PresenceInFarm) {
			return struct {
					changes    []*farmpb.FarmChange
					cut        uint64
					activating bool
					epoch      uint64
				}{nil, presence.ReadyCutVersion, presence.Phase != PresenceFlushing,
					presence.PresenceEpoch},
				ErrJoinCallbackAlreadyApplied
		}
		if presence.Phase != PresenceJoining || !presence.Armed ||
			presence.SnapshotVersion != snapshotVersion {
			return nil, fmt.Errorf("%w: snapshot ack phase=%d armed=%v snapshot=%d/%d",
				ErrInvalidJoinTransition, presence.Phase, presence.Armed,
				snapshotVersion, presence.SnapshotVersion)
		}
		cut := presence.LastPushedVersion
		if cut < snapshotVersion {
			cut = snapshotVersion
		}
		presence.ReadyCutVersion = cut
		presence.ReadyCutFrozen = true
		presence.CatchupAcceptedThrough = snapshotVersion
		presence.Phase = PresenceFlushing
		out := append([]*farmpb.FarmChange(nil), presence.Buffer...)
		if len(out) == 0 {
			presence.Phase = PresenceActivating
		}
		return struct {
			changes    []*farmpb.FarmChange
			cut        uint64
			activating bool
			epoch      uint64
		}{out, cut, len(out) == 0, presence.PresenceEpoch}, nil
	})
	if err != nil && !errors.Is(err, ErrJoinCallbackAlreadyApplied) {
		return nil, 0, false, 0, err
	}
	result := value.(struct {
		changes    []*farmpb.FarmChange
		cut        uint64
		activating bool
		epoch      uint64
	})
	return result.changes, result.cut, result.activating, result.epoch, err
}

// CatchupBatchQueueAccepted 只删除与当前缓冲连续前缀完全一致的批次。
// 跳跃、旧 attempt 或错误版本范围都不能推进状态。
func (h *SubscriptionHub) CatchupBatchQueueAccepted(ctx context.Context, farmID uint64,
	fence JoinControlFence, fromVersion, throughVersion uint64) (activating bool, presenceEpoch uint64, err error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return false, 0, ErrSubscriptionNotFound
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[fence.SubscriptionID]
		if presence == nil {
			return nil, ErrSubscriptionNotFound
		}
		if !matchesJoinControlFence(presence, fence) {
			return nil, ErrStaleJoinFence
		}
		epoch := presence.PresenceEpoch
		if presence.ReadyCutFrozen && throughVersion <= presence.CatchupAcceptedThrough {
			return catchupAckResult{presence.Phase != PresenceFlushing, epoch}, ErrJoinCallbackAlreadyApplied
		}
		if presence.Phase == PresenceActivating && throughVersion == presence.ReadyCutVersion {
			return catchupAckResult{true, epoch}, nil
		}
		if presence.Phase != PresenceFlushing || fromVersion == 0 || throughVersion < fromVersion ||
			throughVersion > presence.ReadyCutVersion || len(presence.Buffer) == 0 {
			return nil, fmt.Errorf("%w: catchup ack phase=%d range=%d..%d cut=%d buffer=%d",
				ErrInvalidJoinTransition, presence.Phase, fromVersion, throughVersion,
				presence.ReadyCutVersion, len(presence.Buffer))
		}
		first := presence.Buffer[0].GetFarmVersion()
		if first != fromVersion {
			return nil, fmt.Errorf("%w: catchup starts at %d, want %d",
				ErrInvalidJoinTransition, fromVersion, first)
		}
		remove := 0
		expected := fromVersion
		removedBytes := 0
		for remove < len(presence.Buffer) {
			change := presence.Buffer[remove]
			if change == nil || change.FarmVersion != expected || change.FarmVersion > throughVersion {
				break
			}
			removedBytes += proto.Size(change)
			remove++
			expected++
		}
		if remove == 0 || expected-1 != throughVersion {
			return nil, fmt.Errorf("%w: catchup range is not a complete buffered prefix",
				ErrInvalidJoinTransition)
		}
		presence.Buffer = append([]*farmpb.FarmChange(nil), presence.Buffer[remove:]...)
		presence.CatchupAcceptedThrough = throughVersion
		presence.BufferBytes -= removedBytes
		if presence.BufferBytes < 0 {
			presence.BufferBytes = 0
		}
		if throughVersion == presence.ReadyCutVersion && len(presence.Buffer) == 0 {
			presence.Phase = PresenceActivating
			return catchupAckResult{true, epoch}, nil
		}
		return catchupAckResult{false, epoch}, nil
	})
	if err != nil && !errors.Is(err, ErrJoinCallbackAlreadyApplied) {
		return false, 0, err
	}
	result, _ := value.(catchupAckResult)
	return result.activating, result.epoch, err
}

type catchupAckResult struct {
	activating bool
	epoch      uint64
}

// ReadyQueueAccepted validates Ready(N) and performs ACTIVATING -> IN_FARM in
// one Lane operation. It returns the N+1 post-ready suffix, the delivery
// generation whose baseline may now be installed, and the presence epoch that
// post-ready downlink items must carry(26 §3.5 全量 Fence)。
func (h *SubscriptionHub) ReadyQueueAccepted(ctx context.Context, farmID uint64,
	fence JoinControlFence, readyThrough uint64) (postReady []*farmpb.FarmChange,
	deliveryGeneration uint64, presenceEpoch uint64, err error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return nil, 0, 0, ErrSubscriptionNotFound
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[fence.SubscriptionID]
		if presence == nil || !matchesJoinControlFence(presence, fence) {
			return nil, ErrStaleJoinFence
		}
		if presence.Phase == PresenceInFarm && presence.ReadyCutFrozen &&
			presence.ReadyCutVersion == readyThrough {
			return struct {
					changes    []*farmpb.FarmChange
					generation uint64
					epoch      uint64
				}{nil, presence.DeliveryGeneration, presence.PresenceEpoch},
				ErrJoinCallbackAlreadyApplied
		}
		if presence.Phase != PresenceActivating || !presence.ReadyCutFrozen ||
			presence.ReadyCutVersion != readyThrough {
			return nil, fmt.Errorf("%w: ready ack phase=%d through=%d cut=%d",
				ErrInvalidJoinTransition, presence.Phase, readyThrough, presence.ReadyCutVersion)
		}
		out := append([]*farmpb.FarmChange(nil), presence.PostReadyBuffer...)
		presence.PostReadyBuffer = nil
		presence.PostReadyBufferBytes = 0
		if presence.PostReadyLastVersion > presence.LastPushedVersion {
			presence.LastPushedVersion = presence.PostReadyLastVersion
		}
		presence.Phase = PresenceInFarm
		return struct {
			changes    []*farmpb.FarmChange
			generation uint64
			epoch      uint64
		}{out, presence.DeliveryGeneration, presence.PresenceEpoch}, nil
	})
	if err != nil && !errors.Is(err, ErrJoinCallbackAlreadyApplied) {
		return nil, 0, 0, err
	}
	result := value.(struct {
		changes    []*farmpb.FarmChange
		generation uint64
		epoch      uint64
	})
	return result.changes, result.generation, result.epoch, err
}

func matchesJoinControlFence(presence *Presence, fence JoinControlFence) bool {
	return presence != nil && presence.SubscriptionID == fence.SubscriptionID &&
		presence.JoinAttemptID == fence.JoinAttemptID &&
		presence.ViewerPlayerID == fence.ViewerPlayerID &&
		presence.SessionEpoch == fence.SessionEpoch && presence.GatewayID == fence.GatewayID &&
		presence.GatewayBootID == fence.GatewayBootID && presence.ConnectionID == fence.ConnectionID &&
		presence.TransportGeneration == fence.TransportGeneration &&
		presence.RouteRevision == fence.RouteRevision
}

// ArmSubscription:JOINING(unarmed) → JOINING(armed)(26 HC-ACCESS-24)。
// Arm 之后该订阅才允许发快照,并从快照版本 V+1 起捕获 Change。
func (h *SubscriptionHub) ArmSubscription(ctx context.Context, farmID uint64, subscriptionID string) (bool, error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return false, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[subscriptionID]
		if presence == nil {
			return false, nil
		}
		if presence.Phase != PresenceJoining || presence.Armed {
			return false, fmt.Errorf("%w: arm from phase=%d armed=%v",
				ErrInvalidJoinTransition, presence.Phase, presence.Armed)
		}
		presence.Armed = true
		return true, nil
	})
	if err != nil {
		return false, err
	}
	armed, _ := value.(bool)
	return armed, nil
}

// FreezeReadyCut 冻结有限 ready_cut_version=N(26 HC-ACCESS-25),并把捕获缓冲
// 按 N 切分:version<=N 进入 catch-up(调用方按 FIFO 下发追赶批次),
// version>N 移入 post-ready 缓冲排在 Ready 后(HC-ACCESS-39)。
// 只允许从 JOINING(armed) 进入 FLUSHING。
func (h *SubscriptionHub) FreezeReadyCut(ctx context.Context, farmID uint64, subscriptionID string,
	cut uint64) (catchUp []*farmpb.FarmChange, err error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return nil, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[subscriptionID]
		if presence == nil {
			return nil, nil
		}
		if presence.Phase != PresenceJoining || !presence.Armed {
			return nil, fmt.Errorf("%w: freeze from phase=%d armed=%v",
				ErrInvalidJoinTransition, presence.Phase, presence.Armed)
		}
		if presence.ReadyCutFrozen {
			return nil, fmt.Errorf("%w: ready_cut already frozen at %d",
				ErrInvalidJoinTransition, presence.ReadyCutVersion)
		}
		presence.ReadyCutVersion = cut
		presence.ReadyCutFrozen = true
		presence.Phase = PresenceFlushing
		var remaining []*farmpb.FarmChange
		var out []*farmpb.FarmChange
		bufferBytes := 0
		for _, change := range presence.Buffer {
			if change != nil && change.FarmVersion > cut {
				// >N 的变化转入 post-ready 缓冲,排在 SUBSCRIPTION_READY 后。
				presence.PostReadyBuffer = append(presence.PostReadyBuffer, change)
				presence.PostReadyBufferBytes += proto.Size(change)
				presence.PostReadyLastVersion = change.FarmVersion
				continue
			}
			out = append(out, change)
			remaining = append(remaining, change)
			bufferBytes += proto.Size(change)
		}
		presence.Buffer = remaining
		presence.BufferBytes = bufferBytes
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	catchUp, _ = value.([]*farmpb.FarmChange)
	return catchUp, nil
}

// MarkActivating:catch-up 已连续推进到 ready_cut_version=N 后,
// FLUSHING → ACTIVATING(ready_through_version=N 成立,无空洞/inflight)。
func (h *SubscriptionHub) MarkActivating(ctx context.Context, farmID uint64, subscriptionID string,
	readyThrough uint64) (bool, error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return false, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[subscriptionID]
		if presence == nil {
			return false, nil
		}
		if presence.Phase != PresenceFlushing {
			return false, fmt.Errorf("%w: activating from phase=%d", ErrInvalidJoinTransition, presence.Phase)
		}
		if !presence.ReadyCutFrozen || readyThrough != presence.ReadyCutVersion {
			return false, fmt.Errorf("%w: ready_through=%d, cut=%d",
				ErrInvalidJoinTransition, readyThrough, presence.ReadyCutVersion)
		}
		if len(presence.Buffer) != 0 {
			return false, fmt.Errorf("%w: catch-up buffer not drained", ErrInvalidJoinTransition)
		}
		presence.Phase = PresenceActivating
		return true, nil
	})
	if err != nil {
		return false, err
	}
	ok, _ = value.(bool)
	return ok, nil
}

// MarkInFarm:ACTIVATING → IN_FARM(ReadyQueueAccepted 之后),返回按序排在
// Ready 后的 post-ready 缓冲,调用方立即按 live 变化下发。
func (h *SubscriptionHub) MarkInFarm(ctx context.Context, farmID uint64, subscriptionID string) (postReady []*farmpb.FarmChange, err error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return nil, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[subscriptionID]
		if presence == nil {
			return nil, nil
		}
		if presence.Phase != PresenceActivating {
			return nil, fmt.Errorf("%w: in_farm from phase=%d", ErrInvalidJoinTransition, presence.Phase)
		}
		out := presence.PostReadyBuffer
		presence.PostReadyBuffer = nil
		presence.PostReadyBufferBytes = 0
		if presence.PostReadyLastVersion > presence.LastPushedVersion {
			presence.LastPushedVersion = presence.PostReadyLastVersion
		}
		presence.Phase = PresenceInFarm
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	postReady, _ = value.([]*farmpb.FarmChange)
	return postReady, nil
}

// StateCounts 是 W1A 观测:按 Lane 快照汇总各 Join 阶段订阅数与 post-ready 缓冲长度。
// /metrics 只读这些快照,不再对每个农场排队 execute。
type JoinStateCounts struct {
	JoiningUnarmed int
	JoiningArmed   int
	Flushing       int
	Activating     int
	InFarm         int
	PostReadyLen   int
}

func countPresence(state *subscriptionState) JoinStateCounts {
	counts := JoinStateCounts{}
	if state == nil {
		return counts
	}
	for _, presence := range state.presence {
		if presence == nil {
			continue
		}
		switch presence.Phase {
		case PresenceJoining:
			if presence.Armed {
				counts.JoiningArmed++
			} else {
				counts.JoiningUnarmed++
			}
		case PresenceFlushing:
			counts.Flushing++
		case PresenceActivating:
			counts.Activating++
		case PresenceInFarm:
			counts.InFarm++
		}
		counts.PostReadyLen += len(presence.PostReadyBuffer)
	}
	return counts
}

func (h *SubscriptionHub) StateCounts(ctx context.Context) JoinStateCounts {
	counts := JoinStateCounts{}
	if h == nil {
		return counts
	}
	_ = ctx
	h.mu.Lock()
	lanes := make([]*FarmEventLane, 0, len(h.lanes))
	for _, lane := range h.lanes {
		lanes = append(lanes, lane)
	}
	h.mu.Unlock()
	for _, lane := range lanes {
		if lane == nil {
			continue
		}
		if current := lane.counts.Load(); current != nil {
			counts.JoiningUnarmed += current.JoiningUnarmed
			counts.JoiningArmed += current.JoiningArmed
			counts.Flushing += current.Flushing
			counts.Activating += current.Activating
			counts.InFarm += current.InFarm
			counts.PostReadyLen += current.PostReadyLen
		}
	}
	return counts
}

// ObserveJoiningBufferDropped 统计 JoiningBuffer 溢出导致的订阅失效(进入
// INVALID/Resync 路径),按原因区分。
func (h *SubscriptionHub) ObserveJoiningBufferDropped() {
	if h != nil {
		h.joiningBufferDropped.Add(1)
	}
}

// JoiningBufferDropped 返回累计溢出失效数(指标导出用)。
func (h *SubscriptionHub) JoiningBufferDropped() uint64 {
	if h == nil {
		return 0
	}
	return h.joiningBufferDropped.Load()
}

// ObserveVersionGap 统计投递序列断档:订阅者本应收到 LastPushed+1,实际收到
// 的 FarmVersion 与之不连续。系统随后会 Resync 修复,但断档本身是硬正确性
// 事件,容量结论必须能看见它,因此与缓冲溢出分开计数。
func (h *SubscriptionHub) ObserveVersionGap() {
	if h != nil {
		h.versionGaps.Add(1)
	}
}

// VersionGaps 返回累计断档数(指标导出用)。
func (h *SubscriptionHub) VersionGaps() uint64 {
	if h == nil {
		return 0
	}
	return h.versionGaps.Load()
}
