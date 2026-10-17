package commandcatalog

import (
	"testing"

	commonpb "farmgame/api/gen/common"
)

// TestPushCatalogCompleteness 是 28 §6.4 完整性 CI:每个公开 PushType 都有注册
// PushSpec,且 PayloadType/Context 合法;未注册即测试失败。
func TestPushCatalogCompleteness(t *testing.T) {
	if err := ValidatePushCatalog(); err != nil {
		t.Fatal(err)
	}
}

// TestCommandCatalogCompleteness 是 28 §6.4 命令侧完整性 CI:每个公开
// CommandType 都有注册 CommandSpec。
func TestCommandCatalogCompleteness(t *testing.T) {
	if err := ValidateCommandCatalog(); err != nil {
		t.Fatal(err)
	}
}

func TestPushSpecScopePartition(t *testing.T) {
	// 28 §2.4 的 Scope 归属表:农场类 Push 必须走 farm_subscription,
	// KICK/Drain 必须走 session,好友变化必须走 player。
	want := map[string]PushContextKind{
		"FARM_SNAPSHOT":         PushContextFarmSubscription,
		"FARM_CHANGE":           PushContextFarmSubscription,
		"FARM_DELTA_BATCH":      PushContextFarmSubscription,
		"SUBSCRIPTION_READY":    PushContextFarmSubscription,
		"REJOIN_REQUIRED":       PushContextFarmSubscription,
		"KICK":                  PushContextSession,
		"DRAIN_NOTICE":          PushContextSession,
		"FRIEND_ROSTER_CHANGED": PushContextPlayer,
	}
	for pushName, contextKind := range want {
		value, ok := commonpb.PushType_value[pushName]
		if !ok {
			t.Fatalf("push type %s missing from enum", pushName)
		}
		spec, ok := LookupPush(commonpb.PushType(value))
		if !ok {
			t.Fatalf("push type %s missing PushSpec", pushName)
		}
		if spec.Context != contextKind {
			t.Fatalf("push type %s context=%s, want %s", pushName, spec.Context, contextKind)
		}
	}
}

func TestFarmSnapshotPushSpecUsesCurrentWrapper(t *testing.T) {
	spec, ok := LookupPush(commonpb.PushType_FARM_SNAPSHOT)
	if !ok {
		t.Fatal("FARM_SNAPSHOT push spec missing")
	}
	if spec.PayloadType != "farm.FarmSnapshotPush" || spec.LegacyDoubleLatch {
		t.Fatalf("FARM_SNAPSHOT spec=%+v", spec)
	}
}

func TestFarmChangeUsesFrozenPublicWrapper(t *testing.T) {
	spec, ok := LookupPush(commonpb.PushType_FARM_CHANGE)
	if !ok {
		t.Fatal("FARM_CHANGE missing PushSpec")
	}
	if spec.PayloadType != "farm.FarmChangePush" {
		t.Fatalf("FARM_CHANGE payload=%q, want farm.FarmChangePush", spec.PayloadType)
	}
}
