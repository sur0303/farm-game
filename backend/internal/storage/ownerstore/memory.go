package ownerstore

import (
	"context"
	"errors"
	"sync"

	ownercorepb "farmgame/api/gen/ownercore"

	"google.golang.org/protobuf/proto"
)

// MemoryStore 是 OwnerStore 的内存实现,真实模拟 Insert 条件创建与 CAS 版本冲突。
// 用于单测、离线开发与本地链路;集成/压测使用真库(TcaplusDB)实现。
type MemoryStore struct {
	mu       sync.Mutex
	records  map[uint64]*ownercorepb.OwnerCoreRecord
	versions map[uint64]uint64
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records:  make(map[uint64]*ownercorepb.OwnerCoreRecord),
		versions: make(map[uint64]uint64),
	}
}

// Get 返回记录的深拷贝与当前 RecordVersion;不存在返回 ErrOwnerNotFound。
func (m *MemoryStore) Get(ctx context.Context, ownerID uint64) (*ownercorepb.OwnerCoreRecord, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[ownerID]
	if !ok {
		return nil, 0, ErrOwnerNotFound
	}
	return proto.Clone(rec).(*ownercorepb.OwnerCoreRecord), m.versions[ownerID], nil
}

// Insert 首次条件创建:记录已存在返回 ErrOwnerExists(并发建档竞争只一个成功)。
func (m *MemoryStore) Insert(ctx context.Context, rec *ownercorepb.OwnerCoreRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec == nil || rec.OwnerMeta == nil {
		return errors.New("ownerstore: invalid record")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[rec.OwnerId]; ok {
		return ErrOwnerExists
	}
	next := proto.Clone(rec).(*ownercorepb.OwnerCoreRecord)
	if next.OwnerMeta != nil && next.OwnerMeta.StateVersion <= curOwnerStateVersion(m.records[rec.OwnerId]) {
		next.OwnerMeta.StateVersion = curOwnerStateVersion(m.records[rec.OwnerId]) + 1
	}
	m.records[rec.OwnerId] = next
	m.versions[rec.OwnerId] = 1
	return nil
}

// CAS 以 expectVersion 为条件写;版本不匹配返回 ErrCASConflict(真失败,需重试整个 turn)。
func (m *MemoryStore) CAS(ctx context.Context, rec *ownercorepb.OwnerCoreRecord, expectVersion uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec == nil {
		return errors.New("ownerstore: nil record")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.versions[rec.OwnerId]
	if !ok {
		return ErrOwnerNotFound
	}
	if cur != expectVersion {
		return ErrCASConflict
	}
	previous := m.records[rec.OwnerId]
	clone := proto.Clone(rec).(*ownercorepb.OwnerCoreRecord)
	if clone.OwnerMeta != nil && clone.OwnerMeta.StateVersion <= curOwnerStateVersion(previous) {
		clone.OwnerMeta.StateVersion = curOwnerStateVersion(previous) + 1
	}
	m.records[rec.OwnerId] = clone
	m.versions[rec.OwnerId] = cur + 1
	return nil
}

func (m *MemoryStore) CompareAndSwap(ctx context.Context, ownerID uint64, expectVersion, expectOwnerEpoch uint64, next *ownercorepb.OwnerCoreRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.records[ownerID]
	if !ok {
		return ErrOwnerNotFound
	}
	if m.versions[ownerID] != expectVersion {
		return ErrCASConflict
	}
	if cur.OwnerMeta != nil && cur.OwnerMeta.OwnerEpoch != expectOwnerEpoch {
		return ErrOwnerEpochStale
	}
	if next == nil || next.OwnerId != ownerID {
		return errors.New("ownerstore: owner id mismatch")
	}
	clone := proto.Clone(next).(*ownercorepb.OwnerCoreRecord)
	if clone.OwnerMeta != nil && clone.OwnerMeta.StateVersion <= curOwnerStateVersion(cur) {
		clone.OwnerMeta.StateVersion = curOwnerStateVersion(cur) + 1
	}
	m.records[ownerID] = clone
	m.versions[ownerID]++
	return nil
}

func curOwnerStateVersion(rec *ownercorepb.OwnerCoreRecord) uint64 {
	if rec == nil || rec.OwnerMeta == nil {
		return 0
	}
	return rec.OwnerMeta.StateVersion
}

// Len 返回当前 Owner 数,观测用。
func (m *MemoryStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}
