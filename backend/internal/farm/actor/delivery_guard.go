package actor

import (
	farmpb "farmgame/api/gen/farm"

	"google.golang.org/protobuf/proto"
)

// PostCommitDeliveryGuard(27 §5.17):在发起强提交前安装,保护"提交后的实时交接"。
// 不变量:已生成候选 FarmChange(V) 后,最终只允许两个结果——变化进入可证明连续
// 的实时流(Guard 清除),或实时流被标记 INVALID(Supervisor 幂等安装
// invalid_from_version<=V)。不存在第三种:Persist 已成功,但既没入队也没标记失效。
//
// Guard 不需要跨进程持久化:Holder 进程退出时 Hub 与订阅同时丢失,客户端按
// Holder 重建合同 Fresh Join。
type commitGuardState int

const (
	guardCommitting commitGuardState = iota + 1
	guardCommitted
	guardUnknown
)

type commitGuard struct {
	version uint64
	state   commitGuardState
	// change is the immutable FarmChange candidate built from the same
	// CandidateState as the authority mutation. It is intentionally runtime
	// only: the authority record stores the business fact and receipt, while
	// the delivery guard protects the post-commit handoff.
	change *farmpb.FarmChange
}

// guardInstall 在发起 Authority Store 强提交前安装(不能等 COMMITTED 后才创建,
// 否则两步之间 panic 会留下空窗)。Actor Turn 串行,单 Actor 至多一个 Guard。
func (a *FarmActor) guardInstall(version uint64) {
	a.guardInstallWithChange(version, nil)
}

func (a *FarmActor) guardInstallWithChange(version uint64, change *farmpb.FarmChange) {
	if change != nil {
		change = proto.Clone(change).(*farmpb.FarmChange)
	}
	a.guardMu.Lock()
	defer a.guardMu.Unlock()
	a.pendingGuard = &commitGuard{version: version, state: guardCommitting, change: change}
}

func (a *FarmActor) guardMarkCommitted() {
	a.guardMu.Lock()
	defer a.guardMu.Unlock()
	if a.pendingGuard != nil {
		a.pendingGuard.state = guardCommitted
	}
}

// guardMarkUnknown:TIMEOUT_UNKNOWN 时保留 Guard 为 UNKNOWN,先按权威
// Receipt/Version 解析,不得猜测提交成败直接广播或丢弃。
func (a *FarmActor) guardMarkUnknown() {
	a.guardMu.Lock()
	defer a.guardMu.Unlock()
	if a.pendingGuard != nil {
		a.pendingGuard.state = guardUnknown
	}
}

// guardClear 在实时交接有定论后调用:入队成功 / 已置失效 / 明确未提交 /
// UNKNOWN 已解析。
func (a *FarmActor) guardClear() {
	a.guardMu.Lock()
	defer a.guardMu.Unlock()
	a.pendingGuard = nil
}

// guardSnapshot 供 Supervisor(Registry)观察:COMMITTING/UNKNOWN/COMMITTED 的
// Guard 都意味着提交后的交接可能未完成。
func (a *FarmActor) guardSnapshot() (version uint64, state commitGuardState, ok bool) {
	a.guardMu.Lock()
	defer a.guardMu.Unlock()
	if a.pendingGuard == nil {
		return 0, 0, false
	}
	return a.pendingGuard.version, a.pendingGuard.state, true
}

// guardCandidateSnapshot returns the immutable FarmChange installed before an
// Authority Commit.  The guard mutex makes supervisor/passivation inspection
// safe even when a turn is just installing or closing the guard.
func (a *FarmActor) guardCandidateSnapshot() (version uint64, state commitGuardState, change *farmpb.FarmChange, ok bool) {
	a.guardMu.Lock()
	defer a.guardMu.Unlock()
	if a.pendingGuard == nil {
		return 0, 0, nil, false
	}
	return a.pendingGuard.version, a.pendingGuard.state, a.pendingGuard.change, true
}
