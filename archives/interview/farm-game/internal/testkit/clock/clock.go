// Package clock 提供确定性的可控时钟，替代各测试文件里重复的 fakeClock。
// 生产接缝形态为 `SetClock(now func() time.Time)`(见
// internal/logic/actor/activation.Controller.SetClock 等),NowFunc 直接适配。
// 本包不做真实 sleep;deadline/超时测试通过 Advance 驱动。
package clock

import (
	"sync"
	"time"
)

// Clock 是手动推进的时钟。零值不可用,必须用 New 构造。
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// New 返回锚定在 start 的时钟。调用方应使用固定时间(如 time.UnixMilli)
// 保证 deterministic seed。
func New(start time.Time) *Clock {
	return &Clock{now: start}
}

// Now 返回当前虚构时间。
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NowFunc 返回适配生产 `SetClock(func() time.Time)` 接缝的闭包。
func (c *Clock) NowFunc() func() time.Time {
	return c.Now
}

// Advance 将时钟推进 d 并返回新时间。d<=0 时不变。
func (c *Clock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
	return c.now
}

// Set 直接设定当前时间(可用于跨日、跨 lease 边界场景)。
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// Deadline 返回相对于当前虚构时间 d 后的绝对 deadline,
// 用于 Gate/Hub 的单调 deadline 断言(如 renewStarted+accepted_ttl-margin)。
func (c *Clock) Deadline(d time.Duration) time.Time {
	return c.Now().Add(d)
}

// Expired 报告 deadline 是否已不晚于当前虚构时间。
func (c *Clock) Expired(deadline time.Time) bool {
	return !c.Now().Before(deadline)
}
