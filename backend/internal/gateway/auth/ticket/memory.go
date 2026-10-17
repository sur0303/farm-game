package ticket

import (
	"context"
	"sync"
	"time"
)

// MemoryStore 是票据的内存实现,真实模拟"原子核销一次"(并发只能一个成功)。
// 用于单测、离线开发与本地链路;集成/压测使用 RedisStore。
type MemoryStore struct {
	mu   sync.Mutex
	ents map[string]entry
}

type entry struct {
	bound Bound
	exp   time.Time
}

// NewMemoryStore 创建内存票据存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{ents: make(map[string]entry)}
}

// Save 覆盖保存 token(以 hash 为键),刷新过期时间。
func (m *MemoryStore) Save(_ context.Context, token string, bound Bound, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ents[Hash(token)] = entry{bound: bound, exp: time.Now().Add(ttl)}
	return nil
}

// Redeem 原子核销:存在且未过期则删除并返回;否则返回 ErrRedeemFailed。
// 并发两个相同 token 同时到达,只有一个能删到条目,另一个必失败。
func (m *MemoryStore) Redeem(_ context.Context, token string) (Bound, error) {
	key := Hash(token)
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.ents[key]
	if !ok {
		return Bound{}, ErrRedeemFailed
	}
	if time.Now().After(e.exp) {
		delete(m.ents, key)
		return Bound{}, ErrRedeemFailed
	}
	delete(m.ents, key) // 原子核销一次
	return e.bound, nil
}

// Len 返回当前仍存活(可能含未清理过期)的票据数,观测用。
func (m *MemoryStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.ents)
}
