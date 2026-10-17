// Package ownerstore 定义 OwnerCore 权威存储接口与实现。
//
// 冻结结论(见 docs/design/00-overview.md §3):TcaplusDB 是唯一权威主库,Redis 只存
// 可重建状态;本地 memory 实现必须真实模拟 CAS/条件写(冲突真失败),用于单测与离线开发,
// 集成/压测走真库实现。
//
// 版本语义(见 D4 §2 / D7):RecordVersion 是 Tcaplus 物理版本,用于条件写(CAS);
// OwnerMeta.state_version 是逻辑状态版本,只进内部观测,不下发 H5。本接口以 RecordVersion
// 为 CAS 条件,Get 返回当前 RecordVersion 供调用方构造下一次 CAS。
package ownerstore

import (
	"context"
	"errors"

	ownercorepb "farmgame/api/gen/ownercore"
)

// ErrOwnerNotFound 表示 OwnerCore 不存在。
var ErrOwnerNotFound = errors.New("ownerstore: owner not found")

// ErrOwnerExists 表示 Insert 时记录已存在(建档首次创建竞争)。
var ErrOwnerExists = errors.New("ownerstore: owner already exists")

// ErrCASConflict 表示 CAS 条件写版本冲突(应重试整个 turn,见 §3 冻结结论 4)。
var ErrCASConflict = errors.New("ownerstore: cas version conflict")

// These classifications let the actor distinguish retryable conflicts from an
// ambiguous write or a temporary backend outage without depending on SDK error text.
var (
	ErrOwnerEpochStale = errors.New("ownerstore: owner epoch stale")
	ErrTimeoutUnknown  = errors.New("ownerstore: timeout or unknown result")
	ErrUnavailable     = errors.New("ownerstore: unavailable")
)

// OwnerStore 是 OwnerCore 权威存储接口。
// 实现必须对 Insert/CAS 的并发竞争给出真实失败(不静默覆盖),以便上层幂等与重试生效。
type OwnerStore interface {
	// Get 返回 OwnerCore 及当前 RecordVersion;不存在返回 ErrOwnerNotFound。
	Get(ctx context.Context, ownerID uint64) (*ownercorepb.OwnerCoreRecord, uint64, error)
	// Insert 首次条件创建;记录已存在返回 ErrOwnerExists。
	Insert(ctx context.Context, rec *ownercorepb.OwnerCoreRecord) error
	// CAS 以 expectVersion 为条件写;版本不匹配返回 ErrCASConflict。
	CAS(ctx context.Context, rec *ownercorepb.OwnerCoreRecord, expectVersion uint64) error
}

// EpochOwnerStore is an optional stronger contract used by multi-writer
// deployments. MemoryStore and TcaplusStore can be upgraded to enforce it
// without changing the minimal S2 OwnerStore callers.
type EpochOwnerStore interface {
	OwnerStore
	CompareAndSwap(ctx context.Context, ownerID uint64, expectVersion, expectOwnerEpoch uint64, next *ownercorepb.OwnerCoreRecord) error
}

// Clone 深拷贝一份 OwnerCoreRecord,避免调用方持有存储内部引用导致竞态/别名。
func Clone(rec *ownercorepb.OwnerCoreRecord) *ownercorepb.OwnerCoreRecord {
	if rec == nil {
		return nil
	}
	return protoClone(rec)
}
