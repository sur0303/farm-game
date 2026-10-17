// Package slowconn 提供慢连接/有界队列的确定性工具,复用
// internal/gateway/transport.FakeTransport(唯一的传输 Fake,不造第二套)。
// 所有消费动作都是步进的,不用 sleep:测试通过 Step/Drain 显式驱动节奏,
// 通过 FillOutgoing 精确触发 ErrQueueFull 背压边界(合同 26 §HC-ACCESS-08/10)。
package slowconn

import (
	"fmt"

	"farmgame/internal/gateway/transport"
)

// FillOutgoing 向 Gate→client 方向写入 n 条消息,返回最后一次错误。
// 用于把有界下行队列精确填满(之后 Gate 再 Send 必须得到 ErrQueueFull)。
func FillOutgoing(t *transport.FakeTransport, n int) error {
	var err error
	for i := 0; i < n; i++ {
		if e := t.Send([]byte(fmt.Sprintf("fill-%d", i))); e != nil {
			return e
		}
	}
	return err
}

// FillIncoming 向 client→Gate 方向写入 n 条消息,语义同 FillOutgoing。
func FillIncoming(t *transport.FakeTransport, n int) error {
	for i := 0; i < n; i++ {
		if err := t.Data([]byte(fmt.Sprintf("fill-%d", i))); err != nil {
			return err
		}
	}
	return nil
}

// DrainOutgoing 非阻塞地取空 Gate→client 队列,返回按序消息。
func DrainOutgoing(t *transport.FakeTransport) [][]byte {
	var out [][]byte
	for {
		data, err := t.Sent()
		if err != nil || data == nil {
			return out
		}
		out = append(out, data)
	}
}

// DrainIncoming 非阻塞地取空 client→Gate 队列,返回按序消息。
func DrainIncoming(t *transport.FakeTransport) [][]byte {
	var out [][]byte
	for {
		data, err := t.Receive()
		if err != nil || data == nil {
			return out
		}
		out = append(out, data)
	}
}

// SlowConsumer 模型一个下行消费缓慢的连接:Gate 侧持续 Send,
// 消费端只在 Step 被调用时取走一条。配合有界队列可构造
// "广播积压在慢连接上" 的确定性场景,替代真实 sleep 网络。
type SlowConsumer struct {
	t *transport.FakeTransport
}

// NewSlowConsumer 包装一条 FakeTransport。
func NewSlowConsumer(t *transport.FakeTransport) *SlowConsumer {
	return &SlowConsumer{t: t}
}

// Step 消费一条下行消息;无消息时返回 nil。
func (c *SlowConsumer) Step() []byte {
	data, err := c.t.Sent()
	if err != nil || data == nil {
		return nil
	}
	return data
}
