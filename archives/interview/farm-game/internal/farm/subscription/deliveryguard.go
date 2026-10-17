package subscription

import (
	"context"
)

// PostCommitDeliveryGuard + FarmRealtimeState(27 §5.17、29 §8.5.1)。
//
// 硬不变量:FarmActor 成功提交后只允许两个结果——变化进入可证明连续的实时流,
// 或实时流被标记 INVALID 并要求 Fresh Join。不允许第三种:业务事实已提交,
// 但 Hub 既没收到 Change,也不知道需要 Resync。
// invalid_from_version 是 level-triggered:Lane 恢复不自动清除;
// 清除条件(29 §8.5.1)是所有旧代订阅已终止,且新 Join 安装了更高
// delivery_generation 并完成 Fresh Baseline。

// InvalidateFarm 标记实时流从 fromVersion 起无法证明连续:
// invalid_from_version = min_non_zero(existing, fromVersion),delivery_generation++。
// 幂等:重复置位取较早版本。返回置位后的 generation 与是否首次置位。
func (h *SubscriptionHub) InvalidateFarm(ctx context.Context, farmID uint64, fromVersion uint64, reason string) (uint64, bool, error) {
	if h == nil || farmID == 0 || fromVersion == 0 {
		return 0, false, nil
	}
	lane := h.liveLane(farmID)
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		first := state.realtime.InvalidFromVersion == 0
		if first || fromVersion < state.realtime.InvalidFromVersion {
			state.realtime.InvalidFromVersion = fromVersion
		}
		state.realtime.DeliveryGeneration++
		return []any{state.realtime.DeliveryGeneration, first}, nil
	})
	if err != nil {
		return 0, false, err
	}
	result := value.([]any)
	if first, _ := result[1].(bool); first {
		h.invalidTotal.Add(1)
		h.observeInvalid(reason)
	}
	return result[0].(uint64), result[1].(bool), nil
}

// MarkFarmResyncRequired converts every still-live subscription into a
// terminal read-only fence without removing it. The caller must send one
// public REJOIN_REQUIRED per returned notice; exact cleanup happens only after
// Gate reports that terminal frame queued or the connection is unreachable.
func (h *SubscriptionHub) MarkFarmResyncRequired(ctx context.Context, farmID uint64,
	reason string, latestKnownVersion, activationEpoch uint64) ([]ResyncNotice, error) {
	if h == nil || farmID == 0 {
		return nil, nil
	}
	lane := h.liveLane(farmID)
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		notices := make([]ResyncNotice, 0, len(state.presence))
		for _, presence := range state.presence {
			if presence == nil || presence.Phase == PresenceResyncRequired {
				continue
			}
			presence.Phase = PresenceResyncRequired
			presence.Buffer = nil
			presence.BufferBytes = 0
			presence.PostReadyBuffer = nil
			presence.PostReadyBufferBytes = 0
			notices = append(notices, ResyncNotice{
				Presence: *presence, Reason: reason,
				LatestKnownFarmVersion: latestKnownVersion, ActivationEpoch: activationEpoch,
			})
		}
		return notices, nil
	})
	if err != nil {
		return nil, err
	}
	notices, _ := value.([]ResyncNotice)
	return notices, nil
}

// MarkSubscriptionResyncRequired fences one exact Presence after its ordinary
// FarmChange could not enter the bounded downlink lane. It never creates a Hub
// lane or activates a FarmActor. A stale fence is a no-op; an exact repeated
// call returns the same terminal Presence so delivery may be retried safely.
func (h *SubscriptionHub) MarkSubscriptionResyncRequired(ctx context.Context, farmID uint64,
	req LeaveRequest, reason string, latestKnownVersion, activationEpoch uint64) (*ResyncNotice, bool, error) {
	if h == nil || farmID == 0 || req.SubscriptionID == "" {
		return nil, false, nil
	}
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return nil, false, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		presence := state.presence[req.SubscriptionID]
		if !matchesLeaveFence(presence, req) {
			return nil, nil
		}
		presence.Phase = PresenceResyncRequired
		presence.Buffer = nil
		presence.BufferBytes = 0
		presence.PostReadyBuffer = nil
		presence.PostReadyBufferBytes = 0
		return &ResyncNotice{
			Presence: *presence, Reason: reason,
			LatestKnownFarmVersion: latestKnownVersion, ActivationEpoch: activationEpoch,
		}, nil
	})
	if err != nil {
		return nil, false, err
	}
	notice, _ := value.(*ResyncNotice)
	return notice, notice != nil, nil
}

// MarkEnqueued 在变化成功进入实时流后推进 last_enqueued_version。
func (h *SubscriptionHub) MarkEnqueued(ctx context.Context, farmID uint64, version uint64) error {
	if h == nil || farmID == 0 || version == 0 {
		return nil
	}
	lane := h.liveLane(farmID)
	_, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		if version > state.realtime.LastEnqueuedVersion {
			state.realtime.LastEnqueuedVersion = version
		}
		return nil, nil
	})
	return err
}

// RealtimeState 读取 Farm 当前实时状态快照。
func (h *SubscriptionHub) RealtimeState(ctx context.Context, farmID uint64) (FarmRealtimeState, bool, error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return FarmRealtimeState{}, false, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		return state.realtime, nil
	})
	if err != nil {
		return FarmRealtimeState{}, false, err
	}
	state, _ := value.(FarmRealtimeState)
	return state, true, nil
}

// RealtimeGeneration 返回当前 delivery_generation(Join 安装基线时记录到 Presence)。
func (h *SubscriptionHub) RealtimeGeneration(ctx context.Context, farmID uint64) uint64 {
	state, ok, err := h.RealtimeState(ctx, farmID)
	if err != nil || !ok {
		return 0
	}
	return state.DeliveryGeneration
}

// InstallBaseline 在新 Join 完成 Fresh Baseline 后调用(29 §8.5.1 清除条件):
// 若所有在册订阅都不早于当前 generation(旧代已全部终止),则清除
// invalid_from_version,实时流重新可证明连续。返回是否执行了清除。
func (h *SubscriptionHub) InstallBaseline(ctx context.Context, farmID uint64, generation uint64) (bool, error) {
	if h == nil || farmID == 0 {
		return false, nil
	}
	lane := h.liveLane(farmID)
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		if state.realtime.InvalidFromVersion == 0 {
			return false, nil
		}
		if generation < state.realtime.DeliveryGeneration {
			return false, nil
		}
		for _, presence := range state.presence {
			if presence != nil && presence.DeliveryGeneration < state.realtime.DeliveryGeneration {
				return false, nil
			}
		}
		state.realtime.InvalidFromVersion = 0
		return true, nil
	})
	if err != nil {
		return false, err
	}
	cleared, _ := value.(bool)
	return cleared, nil
}

// RealtimeCounts 是 W1B 观测:当前处于失效态的 Farm 数与各代次汇总(低频抓取)。
type RealtimeCounts struct {
	InvalidFarms          int
	DeliveryGenerationSum uint64
}

func (h *SubscriptionHub) RealtimeCounts(ctx context.Context) RealtimeCounts {
	counts := RealtimeCounts{}
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
		state := lane.realtime.Load()
		if state == nil {
			continue
		}
		if state.InvalidFromVersion != 0 {
			counts.InvalidFarms++
		}
		counts.DeliveryGenerationSum += state.DeliveryGeneration
	}
	return counts
}

// InvalidTotal 返回累计置位次数(指标导出用)。
func (h *SubscriptionHub) InvalidTotal() uint64 {
	if h == nil {
		return 0
	}
	return h.invalidTotal.Load()
}
