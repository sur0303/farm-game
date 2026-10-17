// Package transport 提供 S2 可替换的传输抽象与 FakeTransport。
// FakeTransport 用于在没有 TCONND 时验证 FarmFrame 的边界、队列和断线语义。
package transport

import (
	"errors"
	"sync"
)

var (
	ErrDisconnected = errors.New("transport: disconnected")
	ErrQueueFull    = errors.New("transport: queue full")
)

// FakeTransport 是一条有界的 binary message 双向通道。
// 每次 Data 入队对应一条 WebSocket binary message，不拼接也不拆分消息。
type FakeTransport struct {
	mu           sync.Mutex
	incoming     chan []byte
	outgoing     chan []byte
	disconnected bool
}

func NewFakeTransport(queueSize int) *FakeTransport {
	if queueSize <= 0 {
		queueSize = 16
	}
	return &FakeTransport{incoming: make(chan []byte, queueSize), outgoing: make(chan []byte, queueSize)}
}

// Data 模拟浏览器/TCONND 向 Gate 发送一条完整 binary message。
func (t *FakeTransport) Data(data []byte) error {
	t.mu.Lock()
	if t.disconnected {
		t.mu.Unlock()
		return ErrDisconnected
	}
	t.mu.Unlock()
	copyData := append([]byte(nil), data...)
	select {
	case t.incoming <- copyData:
		return nil
	default:
		return ErrQueueFull
	}
}

// Receive 返回 Gate 尚未消费的入站消息。
func (t *FakeTransport) Receive() ([]byte, error) {
	select {
	case data := <-t.incoming:
		return data, nil
	default:
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.disconnected {
			return nil, ErrDisconnected
		}
		return nil, nil
	}
}

// Send 模拟 Gate/TCONND 向浏览器发送一条完整 binary message。
func (t *FakeTransport) Send(data []byte) error {
	t.mu.Lock()
	if t.disconnected {
		t.mu.Unlock()
		return ErrDisconnected
	}
	t.mu.Unlock()
	select {
	case t.outgoing <- append([]byte(nil), data...):
		return nil
	default:
		return ErrQueueFull
	}
}

// Sent 返回下一条出站消息；无消息时返回 nil,nil。
func (t *FakeTransport) Sent() ([]byte, error) {
	select {
	case data := <-t.outgoing:
		return data, nil
	default:
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.disconnected {
			return nil, ErrDisconnected
		}
		return nil, nil
	}
}

func (t *FakeTransport) Disconnect() {
	t.mu.Lock()
	t.disconnected = true
	t.mu.Unlock()
}

func (t *FakeTransport) Connected() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.disconnected
}
