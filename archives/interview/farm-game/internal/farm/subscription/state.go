package subscription

// subscriptionState 是单个 Farm Lane 内的 Presence/Join 幂等状态,只被该 Lane
// 的串行事件流访问。
type subscriptionState struct {
	presence              map[string]*Presence
	presenceByPlayer      map[uint64]string
	presenceByAttempt     map[string]string
	joinIdempotency       map[string]JoinIdempotencyEntry
	joinIdempotencyOrder  []string
	cancelledAttempts     map[string]CancelTombstone
	cancelledAttemptOrder []string
	cancelledAttemptBytes int
	// realtime 是 27 §5.17 的 FarmRealtimeState:独立于普通变化队列的
	// level-triggered 失效状态。invalid_from_version == 0 表示实时流可证明连续。
	realtime FarmRealtimeState
}

// FarmRealtimeState 只被所在 Farm Lane 串行访问。
type FarmRealtimeState struct {
	DeliveryGeneration  uint64
	LastEnqueuedVersion uint64
	InvalidFromVersion  uint64
}

func newSubscriptionState() *subscriptionState {
	return &subscriptionState{
		presence:          make(map[string]*Presence),
		presenceByPlayer:  make(map[uint64]string),
		presenceByAttempt: make(map[string]string),
		joinIdempotency:   make(map[string]JoinIdempotencyEntry),
		cancelledAttempts: make(map[string]CancelTombstone),
	}
}

func removePresence(state *subscriptionState, subscriptionID string) {
	p := state.presence[subscriptionID]
	if p == nil {
		return
	}
	delete(state.presence, subscriptionID)
	if state.presenceByPlayer[p.ViewerPlayerID] == subscriptionID {
		delete(state.presenceByPlayer, p.ViewerPlayerID)
	}
	if p.JoinAttemptID != "" && state.presenceByAttempt[p.JoinAttemptID] == subscriptionID {
		delete(state.presenceByAttempt, p.JoinAttemptID)
	}
}

// LeaveFenced 只在完整 Fence 匹配时移除 Presence;供 Hub 的精确清理与
// FarmActor 的薄封装共用同一实现。
func LeaveFenced(presence map[string]*Presence, presenceByPlayer map[uint64]string, req LeaveRequest) bool {
	p := presence[req.SubscriptionID]
	if !matchesLeaveFence(p, req) {
		return false
	}
	delete(presence, req.SubscriptionID)
	if presenceByPlayer[p.ViewerPlayerID] == req.SubscriptionID {
		delete(presenceByPlayer, p.ViewerPlayerID)
	}
	return true
}

func matchesLeaveFence(p *Presence, req LeaveRequest) bool {
	return p != nil &&
		p.SubscriptionID == req.SubscriptionID &&
		p.ViewerPlayerID == req.ViewerPlayerID &&
		p.SessionEpoch == req.SessionEpoch &&
		p.GatewayID == req.GatewayID &&
		p.GatewayBootID == req.GatewayBootID &&
		p.ConnectionID == req.ConnectionID &&
		p.TransportGeneration == req.TransportGeneration &&
		p.RouteRevision == req.RouteRevision &&
		(req.JoinAttemptID == "" || p.JoinAttemptID == req.JoinAttemptID) &&
		(req.PresenceEpoch == 0 || p.PresenceEpoch == req.PresenceEpoch)
}

func leaveFenced(state *subscriptionState, req LeaveRequest) bool {
	p := state.presence[req.SubscriptionID]
	if !LeaveFenced(state.presence, state.presenceByPlayer, req) {
		return false
	}
	if p != nil && p.JoinAttemptID != "" && state.presenceByAttempt[p.JoinAttemptID] == req.SubscriptionID {
		delete(state.presenceByAttempt, p.JoinAttemptID)
	}
	return true
}

// CancelJoin 只移除匹配 Join intent 与完整 Fence 的 Presence;幂等,且不会删除
// 后续连接复用 connection_id 后的新订阅。
func CancelJoin(presence map[string]*Presence, presenceByPlayer map[uint64]string,
	idem map[string]JoinIdempotencyEntry, req CancelJoinRequest) bool {
	if req.OperationID == "" {
		return false
	}
	subscriptionID := req.SubscriptionID
	if subscriptionID == "" {
		if entry, ok := idem[req.OperationID]; ok {
			subscriptionID = entry.SubscriptionID
		}
	}
	p := presence[subscriptionID]
	if p == nil || p.JoinOperationID != req.OperationID ||
		(req.SubscriptionID != "" && p.SubscriptionID != req.SubscriptionID) {
		return false
	}
	fence := req.Fence
	fence.SubscriptionID = subscriptionID
	return LeaveFenced(presence, presenceByPlayer, fence)
}

func cancelJoin(state *subscriptionState, req CancelJoinRequest) bool {
	return CancelJoin(state.presence, state.presenceByPlayer, state.joinIdempotency, req)
}
