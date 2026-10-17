package commandcatalog

import (
	"fmt"

	commonpb "farmgame/api/gen/common"
)

// PushContextKind 是 28 §2.4 push_context oneof 的下行 Scope 分类。
type PushContextKind uint8

const (
	PushContextUnspecified PushContextKind = iota
	PushContextSession
	PushContextPlayer
	PushContextFarmSubscription
)

func (k PushContextKind) String() string {
	switch k {
	case PushContextSession:
		return "session"
	case PushContextPlayer:
		return "player"
	case PushContextFarmSubscription:
		return "farm_subscription"
	default:
		return "unspecified"
	}
}

// PushSpec 是公开 PushType 的静态说明书(28 §2.4:每个公开 PushType 必须注册
// 静态 PushSpec;28 §6.4 完整性测试枚举对照)。
type PushSpec struct {
	PushType    commonpb.PushType
	PayloadType string // fully-qualified proto message name
	Context     PushContextKind
	// LegacyDoubleLatch 标记仅旧双锁存器链路使用的类型;W2 切换到
	// FarmSnapshotPush/REJOIN_REQUIRED 后随旧链路删除。
	LegacyDoubleLatch bool
}

var allPushSpecs = []PushSpec{
	// farm_subscription 域(28 §2.4:Snapshot、Delta、FarmChange、同场广播)。
	{PushType: commonpb.PushType_FARM_SNAPSHOT, PayloadType: "farm.FarmSnapshotPush", Context: PushContextFarmSubscription},
	{PushType: commonpb.PushType_FARM_CHANGE, PayloadType: "farm.FarmChangePush", Context: PushContextFarmSubscription},
	{PushType: commonpb.PushType_FARM_DELTA_BATCH, PayloadType: "farm.FarmDeltaBatchPush", Context: PushContextFarmSubscription},
	{PushType: commonpb.PushType_SUBSCRIPTION_READY, PayloadType: "farm.SubscriptionReadyPush", Context: PushContextFarmSubscription},
	{PushType: commonpb.PushType_REJOIN_REQUIRED, PayloadType: "farm.RejoinRequiredPush", Context: PushContextFarmSubscription},
	// session 域(KICK、Drain、重连或重新认证通知)。
	{PushType: commonpb.PushType_KICK, PayloadType: "common.ConnectionError", Context: PushContextSession},
	{PushType: commonpb.PushType_DRAIN_NOTICE, PayloadType: "common.DrainNotice", Context: PushContextSession},
	// player 域(好友机会提示、任务、红点、个人资产变化)。
	{PushType: commonpb.PushType_FRIEND_ROSTER_CHANGED, PayloadType: "friend.FriendRosterChanged", Context: PushContextPlayer},
	{PushType: commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED, PayloadType: "friend.FriendOpportunityChanged", Context: PushContextPlayer},
	{PushType: commonpb.PushType_SYSTEM_NOTICE, PayloadType: "bytes", Context: PushContextPlayer},
	{PushType: commonpb.PushType_OPERATION_RESULT, PayloadType: "settlement.OperationResult", Context: PushContextPlayer},
}

// LookupPush 返回 PushType 的注册说明书。
func LookupPush(pushType commonpb.PushType) (PushSpec, bool) {
	for _, spec := range allPushSpecs {
		if spec.PushType == pushType {
			return spec, true
		}
	}
	return PushSpec{}, false
}

// AllPushes 返回全部注册的 PushSpec(稳定顺序)。
func AllPushes() []PushSpec {
	out := make([]PushSpec, len(allPushSpecs))
	copy(out, allPushSpecs)
	return out
}

// ValidatePushCatalog 是 28 §6.4 的完整性自检:每个公开 PushType 都有注册
// PushSpec,且每个 Spec 的 PayloadType/Context 合法。启动时与 CI 均应执行。
func ValidatePushCatalog() error {
	for value, name := range commonpb.PushType_name {
		pushType := commonpb.PushType(value)
		if pushType == commonpb.PushType_PUSH_TYPE_UNSPECIFIED {
			continue
		}
		spec, ok := LookupPush(pushType)
		if !ok {
			return fmt.Errorf("push type %s has no registered PushSpec", name)
		}
		if spec.PayloadType == "" {
			return fmt.Errorf("push type %s has empty payload type", name)
		}
		if spec.Context == PushContextUnspecified {
			return fmt.Errorf("push type %s has unspecified push context", name)
		}
	}
	return nil
}

// ValidateCommandCatalog 是 28 §6.4 的命令侧完整性自检:每个公开 CommandType
// 都能 Lookup 到 CommandSpec。LEAVE_FARM(7)/GET_FARM_SNAPSHOT(10) 已从公开
// 目录删除,仅以 deprecated 枚举值保留编译迁移窗口,不属于公开命令集合。
func ValidateCommandCatalog() error {
	for value, name := range commonpb.CommandType_name {
		commandType := commonpb.CommandType(value)
		if commandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
			continue
		}
		if commandType == commonpb.CommandType(7) || // LEAVE_FARM(已删除,编号 reserved)
			commandType == commonpb.CommandType(10) { // GET_FARM_SNAPSHOT(已删除,编号 reserved)
			// 已删除命令:必须恰好没有注册,形成"删除即不可路由"的双向证明。
			if _, registered := Lookup(commandType); registered {
				return fmt.Errorf("removed command %s must not have a registered CommandSpec", name)
			}
			continue
		}
		if _, ok := Lookup(commandType); !ok {
			return fmt.Errorf("command type %s has no registered CommandSpec", name)
		}
	}
	return nil
}
