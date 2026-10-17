// Package fault 提供确定性的故障注入原语,覆盖 RPC gate/延迟/断连、
// 存储 timeout/response-lost、Actor crash point 三类接缝。
// 设计对齐 internal/storage/authoritystore/fault.go 的语义:
// "BeforeApply" 表示请求未生效但调用方不可知,"AfterApply" 表示已生效但
// 响应丢失。所有注入按 FIFO 脚本消费,不用 sleep,不用真实定时器。
package fault

import (
	"context"
	"errors"
	"sync"
)

// ErrInjected 标记由本包注入的错误,便于与生产错误区分。
var ErrInjected = errors.New("testkit/fault: injected fault")

// Action 是一次调用被注入的行为。
type Action uint8

const (
	// Allow 放行,调用真实实现。
	Allow Action = iota
	// Fail 以脚本化错误失败,不调用真实实现(模型 RPC 错误/断连)。
	Fail
	// Hang 阻塞直到 ReleaseHang/ReleaseAll(模型延迟/慢响应);
	// ctx 取消则返回 ctx.Err(模型调用方超时)。释放后放行到真实实现。
	Hang
	// Crash panic(模型 Actor/进程崩溃点),调用方应用 recover 验证
	// 注册表清理与重载语义。
	Crash
)

type step struct {
	action  Action
	err     error
	msg     string
	entered chan struct{} // Hang 专用:Do 消费该步时关闭,作为确定性握手点
}

// Injector 按调用名维护 FIFO 故障脚本。脚本耗尽后一律 Allow。
type Injector struct {
	mu      sync.Mutex
	queues  map[string][]step
	calls   map[string]uint64
	pending map[string]chan struct{} // 每个 call 最近一个未放行的 Hang gate
	release chan struct{}            // 关闭后放行所有现存及后续 Hang
}

// NewInjector 构造空注入器(默认全部 Allow)。
func NewInjector() *Injector {
	return &Injector{
		queues:  make(map[string][]step),
		calls:   make(map[string]uint64),
		pending: make(map[string]chan struct{}),
		release: make(chan struct{}),
	}
}

// Script 追加脚本:Script(name, Allow) 表示显式放行一步。
func (i *Injector) Script(call string, action Action) {
	i.ScriptErr(call, action, nil)
}

// ScriptErr 追加带错误的 Fail 脚本;err 为 nil 时包装 ErrInjected。
func (i *Injector) ScriptErr(call string, action Action, err error) {
	if action == Fail && err == nil {
		err = ErrInjected
	}
	s := step{action: action, err: err}
	if action == Hang {
		s.entered = make(chan struct{})
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.queues[call] = append(i.queues[call], s)
}

// ScriptCrash 追加 Crash 脚本,panic 值为 msg。
func (i *Injector) ScriptCrash(call, msg string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.queues[call] = append(i.queues[call], step{action: Crash, msg: msg})
}

// Calls 返回 call 已经过注入器的次数(含被故障拦截的)。
func (i *Injector) Calls(call string) uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls[call]
}

// ReleaseHang 放行 call 最近一次进入的 Hang;无挂起调用时无操作。
func (i *Injector) ReleaseHang(call string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if g, ok := i.pending[call]; ok {
		delete(i.pending, call)
		close(g)
	}
}

// ReleaseAll 放行当前及后续所有 Hang。幂等。
func (i *Injector) ReleaseAll() {
	i.mu.Lock()
	defer i.mu.Unlock()
	select {
	case <-i.release:
	default:
		close(i.release)
	}
}

// HangEntered 返回一个 channel:当 call 下一个(或当前在飞的)Hang
// 真正进入 Do 的阻塞点时关闭。这是确定性握手点,测试必须先等它关闭
// 再调用 ReleaseHang,禁止用 sleep 猜时序。无 Hang 脚本时返回 nil。
func (i *Injector) HangEntered(call string) <-chan struct{} {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.pending[call]; ok {
		// 已在阻塞点:返回已关闭 channel。
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	for _, s := range i.queues[call] {
		if s.action == Hang {
			return s.entered
		}
	}
	return nil
}

func (i *Injector) next(call string) (step, chan struct{}) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls[call]++
	queue := i.queues[call]
	if len(queue) == 0 {
		return step{action: Allow}, nil
	}
	s := queue[0]
	i.queues[call] = queue[1:]
	if s.action == Hang {
		gate := make(chan struct{})
		i.pending[call] = gate
		// 先登记 pending 再发握手信号,保证观察到 entered 的调用方
		// 随后的 ReleaseHang 必然命中。
		close(s.entered)
		return s, gate
	}
	return s, nil
}

// Do 以注入语义执行一元调用 fn。可用于任何 `func(ctx, req) (resp, err)`
// 形态的 RPC/存储接缝(如 command.LogicClient、downlink.GatewayClient)。
func Do[Req, Resp any](ctx context.Context, i *Injector, call string, req Req, fn func(context.Context, Req) (Resp, error)) (Resp, error) {
	var zero Resp
	if i == nil {
		return fn(ctx, req)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	s, gate := i.next(call)
	switch s.action {
	case Fail:
		return zero, s.err
	case Crash:
		panic(s.msg)
	case Hang:
		select {
		case <-i.release:
		case <-gate:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
		i.mu.Lock()
		if g, ok := i.pending[call]; ok && g == gate {
			delete(i.pending, call)
		}
		i.mu.Unlock()
		return fn(ctx, req)
	default:
		return fn(ctx, req)
	}
}
