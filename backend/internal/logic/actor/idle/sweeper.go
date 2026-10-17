// Package idle 实现分桶 Idle Sweeper:按 idle deadline 把 key 放入时间桶,
// 每个 tick 只处理当前到期桶,扫描成本与该桶候选数相关,不为每个 Actor 建独立 Timer。
//
// 语义对齐 docs/verification/s2-handoff.md §6.3.3:
//   - 活跃时只更新 deadline/桶归属;
//   - 候选仍需二次校验(RetireFunc 内部检查 idle deadline、generation 与 blocker);
//   - 被阻塞的候选按原因统计并放入后续桶,不 busy loop;
//   - 扫描周期、idle timeout、单 tick 上限可配置;
//   - 时钟可注入(fake clock),测试不依赖长时间 sleep。
package idle

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Decision 是 RetireFunc 对单个候选的裁决。
type Decision int

const (
	// DecisionRetired 表示资源已完整释放,Sweeper 停止跟踪该 key。
	DecisionRetired Decision = iota
	// DecisionNotIdle 表示二次校验发现新活动,按当前时间重新计 idle。
	DecisionNotIdle
	// DecisionBusy 表示存在 blocker;按 BlockedRetryDelay 放入后续桶重试。
	DecisionBusy
)

// Result 是 RetireFunc 的返回;Reasons 只在 DecisionBusy 时有意义。
type Result struct {
	Decision Decision
	Reasons  []string
}

// RetireFunc 对候选 key 做二次校验并尝试释放资源。实现必须保证:
// 有新活动时返回 NotIdle,有 blocker 时返回 Busy 并给出稳定 reason 字符串,
// 完整释放后返回 Retired。重复调用必须幂等。
type RetireFunc func(ctx context.Context, key uint64) Result

// Options 控制 Sweeper 行为;零值字段由 New 填默认。
type Options struct {
	// IdleTimeout 是 key 最后一次活动到成为候选的时间;默认 5 分钟。
	IdleTimeout time.Duration
	// BucketWidth 是时间桶宽度;默认 30s。候选最早在 idleTimeout 到期后的
	// 下一个桶边界被处理,实际退休延迟在 [idleTimeout, idleTimeout+BucketWidth) 之间。
	BucketWidth time.Duration
	// TickInterval 是后台 Run 的扫描周期;默认等于 BucketWidth。
	TickInterval time.Duration
	// MaxPerTick 限制单次 Sweep 处理的候选数;默认 256。超出的候选留在桶中,
	// 通过 lag 指标观察积压。
	MaxPerTick int
	// BlockedRetryDelay 是被阻塞候选重新到期的延迟;默认 5s,避免 busy loop。
	BlockedRetryDelay time.Duration
	// Now 注入时钟;默认 time.Now。测试用 fake clock。
	Now func() time.Time
}

func (o Options) withDefaults() Options {
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 5 * time.Minute
	}
	if o.BucketWidth <= 0 {
		o.BucketWidth = 30 * time.Second
	}
	if o.TickInterval <= 0 {
		o.TickInterval = o.BucketWidth
	}
	if o.MaxPerTick <= 0 {
		o.MaxPerTick = 256
	}
	if o.BlockedRetryDelay <= 0 {
		o.BlockedRetryDelay = 5 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Sweeper 是分桶 idle 扫描器。零值不可用,请用 New。
type Sweeper struct {
	opts   Options
	retire RetireFunc

	mu      sync.Mutex
	buckets map[int64]map[uint64]struct{}
	keyPos  map[uint64]int64

	sweepsTotal       uint64
	candidatesTotal   uint64
	retiredTotal      uint64
	notIdleTotal      uint64
	blockedTotal      map[string]uint64
	lastLagSeconds    float64
	lastSweepDuration time.Duration
}

// New 创建 Sweeper;retire 必填。
func New(opts Options, retire RetireFunc) *Sweeper {
	opts = opts.withDefaults()
	return &Sweeper{
		opts:         opts,
		retire:       retire,
		buckets:      make(map[int64]map[uint64]struct{}),
		keyPos:       make(map[uint64]int64),
		blockedTotal: make(map[string]uint64),
	}
}

func (s *Sweeper) bucketIndex(t time.Time) int64 {
	return t.UnixNano() / s.opts.BucketWidth.Nanoseconds()
}

// Touch 记录 key 的一次活动:只更新 deadline 与桶归属,不创建 Timer。
func (s *Sweeper) Touch(key uint64) {
	s.touchAt(key, s.opts.Now().Add(s.opts.IdleTimeout))
}

func (s *Sweeper) touchAt(key uint64, deadline time.Time) {
	b := s.bucketIndex(deadline)
	s.mu.Lock()
	defer s.mu.Unlock()
	if pos, ok := s.keyPos[key]; ok && pos == b {
		return
	}
	if pos, ok := s.keyPos[key]; ok {
		delete(s.buckets[pos], key)
		if len(s.buckets[pos]) == 0 {
			delete(s.buckets, pos)
		}
	}
	bucket := s.buckets[b]
	if bucket == nil {
		bucket = make(map[uint64]struct{})
		s.buckets[b] = bucket
	}
	bucket[key] = struct{}{}
	s.keyPos[key] = b
}

// Forget 停止跟踪 key(资源已释放或永久失效)。
func (s *Sweeper) Forget(key uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeLocked(key)
}

func (s *Sweeper) removeLocked(key uint64) {
	pos, ok := s.keyPos[key]
	if !ok {
		return
	}
	delete(s.keyPos, key)
	delete(s.buckets[pos], key)
	if len(s.buckets[pos]) == 0 {
		delete(s.buckets, pos)
	}
}

// Run 按 TickInterval 周期扫描,直到 ctx 取消。普通 actor passivation 路径;
// 进程退出时不要求调用(Executor.Stop 有全局语义)。
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.opts.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.SweepOnce(ctx)
		}
	}
}

// SweepOnce 处理当前所有到期桶,最多 MaxPerTick 个候选。
// 候选二次校验由 RetireFunc 完成;本方法只做桶维护与统计。测试可直接调用。
func (s *Sweeper) SweepOnce(ctx context.Context) int {
	start := s.opts.Now()
	now := start
	current := s.bucketIndex(now)

	s.mu.Lock()
	expired := make([]int64, 0, len(s.buckets))
	for b := range s.buckets {
		if b <= current {
			expired = append(expired, b)
		}
	}
	sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
	candidates := make([]uint64, 0, s.opts.MaxPerTick)
	overflowOldest := int64(0)
	for _, b := range expired {
		if len(candidates) >= s.opts.MaxPerTick {
			overflowOldest = b
			break
		}
		for key := range s.buckets[b] {
			if len(candidates) >= s.opts.MaxPerTick {
				overflowOldest = b
				break
			}
			candidates = append(candidates, key)
		}
	}
	s.sweepsTotal++
	s.mu.Unlock()

	for _, key := range candidates {
		result := s.retire(ctx, key)
		s.mu.Lock()
		s.candidatesTotal++
		switch result.Decision {
		case DecisionRetired:
			s.retiredTotal++
			s.removeLocked(key)
		case DecisionNotIdle:
			s.notIdleTotal++
			// 重新计 idle;若期间有 Touch 更新过桶归属,touchAt 会自然覆盖。
			s.removeLocked(key)
			s.mu.Unlock()
			s.Touch(key)
			s.mu.Lock()
		case DecisionBusy:
			for _, reason := range result.Reasons {
				s.blockedTotal[reason]++
			}
			if len(result.Reasons) == 0 {
				s.blockedTotal["unspecified"]++
			}
			s.removeLocked(key)
			s.mu.Unlock()
			s.touchAt(key, now.Add(s.opts.BlockedRetryDelay))
			s.mu.Lock()
		}
		s.mu.Unlock()
	}

	s.mu.Lock()
	if overflowOldest > 0 {
		s.lastLagSeconds = float64(current-overflowOldest) * s.opts.BucketWidth.Seconds()
	} else {
		s.lastLagSeconds = 0
	}
	s.lastSweepDuration = s.opts.Now().Sub(start)
	s.mu.Unlock()
	return len(candidates)
}

// Stats 是 Sweeper 观测快照。
type Stats struct {
	TrackedKeys             int
	Buckets                 int
	SweepsTotal             uint64
	CandidatesTotal         uint64
	RetiredTotal            uint64
	NotIdleTotal            uint64
	BlockedTotal            map[string]uint64
	LastLagSeconds          float64
	LastSweepDurationSecond float64
}

// Stats 返回观测快照;BlockedTotal 是拷贝,调用方可安全持有。
func (s *Sweeper) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocked := make(map[string]uint64, len(s.blockedTotal))
	for reason, count := range s.blockedTotal {
		blocked[reason] = count
	}
	return Stats{
		TrackedKeys:             len(s.keyPos),
		Buckets:                 len(s.buckets),
		SweepsTotal:             s.sweepsTotal,
		CandidatesTotal:         s.candidatesTotal,
		RetiredTotal:            s.retiredTotal,
		NotIdleTotal:            s.notIdleTotal,
		BlockedTotal:            blocked,
		LastLagSeconds:          s.lastLagSeconds,
		LastSweepDurationSecond: s.lastSweepDuration.Seconds(),
	}
}
