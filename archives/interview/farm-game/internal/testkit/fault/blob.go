package fault

import (
	"context"
	"sync"

	"farmgame/internal/storage/blobstore"
)

// BlobMethod 枚举 blobstore.Store/EpochStore 的可注入方法。
type BlobMethod uint8

const (
	BlobGet BlobMethod = iota
	BlobInsert
	BlobCAS
	BlobInsertEpoch
	BlobCASEpoch
	BlobFence
)

// BlobMode 语义与 authoritystore.FaultMode 对齐,但作用于 blobstore 接缝
// (当前生产主链使用的 RedisStore 边界),错误映射到 blobstore 哨兵错误。
type BlobMode uint8

const (
	// BlobAllow 放行。
	BlobAllow BlobMode = iota
	// BlobUnavailableNotSent 请求未发出:返回 ErrUnavailable,底层不变。
	BlobUnavailableNotSent
	// BlobTimeoutUnknownBeforeApply 请求未生效但调用方不可知:
	// 返回 ErrTimeoutUnknown,底层不变(response-lost 前置形态)。
	BlobTimeoutUnknownBeforeApply
	// BlobTimeoutUnknownAfterApply 请求已生效但响应丢失:
	// 先应用到底层,再返回 ErrTimeoutUnknown。
	BlobTimeoutUnknownAfterApply
	// BlobCrash panic,模型存储适配层崩溃点(Actor turn 中途崩溃)。
	BlobCrash
)

// BlobStore 包装 blobstore.Store(及 EpochStore),按方法 FIFO 注入故障。
// 脚本耗尽后一律放行。它不实现任何生产语义,只做边界故障映射。
type BlobStore struct {
	inner  blobstore.Store
	mu     sync.Mutex
	queues map[BlobMethod][]BlobMode
	calls  map[BlobMethod]uint64
}

var _ blobstore.Store = (*BlobStore)(nil)
var _ blobstore.EpochStore = (*BlobStore)(nil)

// NewBlobStore 包装 inner(通常是 blobstore.MemoryStore 或测试内的
// RedisStore 实例)。
func NewBlobStore(inner blobstore.Store) *BlobStore {
	return &BlobStore{inner: inner, queues: make(map[BlobMethod][]BlobMode), calls: make(map[BlobMethod]uint64)}
}

// Inject 为 method 追加 FIFO 故障脚本。
func (s *BlobStore) Inject(method BlobMethod, modes ...BlobMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[method] = append(s.queues[method], modes...)
}

// Calls 返回 method 已经过包装器的次数(含被拦截的)。
func (s *BlobStore) Calls(method BlobMethod) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[method]
}

func (s *BlobStore) next(method BlobMethod) BlobMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[method]++
	queue := s.queues[method]
	if len(queue) == 0 {
		return BlobAllow
	}
	mode := queue[0]
	s.queues[method] = queue[1:]
	return mode
}

// applyRead 处理无底层变更的方法(Get/Fence 之外的读取路径只有 Get)。
// 对写路径,before/after 语义由 applyWrite 处理。
func (s *BlobStore) applyWrite(method BlobMethod, apply func() error) error {
	switch s.next(method) {
	case BlobUnavailableNotSent:
		return blobstore.ErrUnavailable
	case BlobTimeoutUnknownBeforeApply:
		return blobstore.ErrTimeoutUnknown
	case BlobTimeoutUnknownAfterApply:
		if err := apply(); err != nil {
			return err
		}
		return blobstore.ErrTimeoutUnknown
	case BlobCrash:
		panic("testkit/fault: injected blob store crash")
	default:
		return apply()
	}
}

// Get 只读方法:仅注入 Unavailable/TimeoutUnknown(BeforeApply 等价)。
func (s *BlobStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	switch s.next(BlobGet) {
	case BlobUnavailableNotSent:
		return nil, 0, blobstore.ErrUnavailable
	case BlobTimeoutUnknownBeforeApply, BlobTimeoutUnknownAfterApply:
		return nil, 0, blobstore.ErrTimeoutUnknown
	case BlobCrash:
		panic("testkit/fault: injected blob store crash")
	default:
		return s.inner.Get(ctx, id)
	}
}

func (s *BlobStore) Insert(ctx context.Context, id uint64, data []byte) error {
	return s.applyWrite(BlobInsert, func() error { return s.inner.Insert(ctx, id, data) })
}

func (s *BlobStore) CAS(ctx context.Context, id uint64, expectVersion uint64, data []byte) error {
	return s.applyWrite(BlobCAS, func() error { return s.inner.CAS(ctx, id, expectVersion, data) })
}

func (s *BlobStore) InsertEpoch(ctx context.Context, id uint64, writerEpoch uint64, data []byte) error {
	return s.applyWrite(BlobInsertEpoch, func() error {
		return blobstore.InsertAtEpoch(ctx, s.inner, id, writerEpoch, data)
	})
}

func (s *BlobStore) CASEpoch(ctx context.Context, id uint64, expectVersion, writerEpoch uint64, data []byte) error {
	return s.applyWrite(BlobCASEpoch, func() error {
		return blobstore.CASAtEpoch(ctx, s.inner, id, expectVersion, writerEpoch, data)
	})
}

// Fence 无返回值语义变化,但仍可注入 Unavailable/TimeoutUnknown/Crash。
func (s *BlobStore) Fence(ctx context.Context, id uint64, writerEpoch uint64) error {
	return s.applyWrite(BlobFence, func() error {
		return blobstore.FenceAtEpoch(ctx, s.inner, id, writerEpoch)
	})
}
