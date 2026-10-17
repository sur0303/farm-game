package slowconn

import (
	"errors"
	"testing"

	"farmgame/internal/gateway/transport"
)

func TestFillOutgoingTriggersQueueFullExactly(t *testing.T) {
	tr := transport.NewFakeTransport(4)
	if err := FillOutgoing(tr, 4); err != nil {
		t.Fatalf("fill within capacity err=%v", err)
	}
	// 第 5 条必须触发有界队列背压。
	if err := tr.Send([]byte("overflow")); !errors.Is(err, transport.ErrQueueFull) {
		t.Fatalf("overflow err=%v", err)
	}
}

func TestSlowConsumerStepControlsBacklog(t *testing.T) {
	tr := transport.NewFakeTransport(2)
	consumer := NewSlowConsumer(tr)
	if err := FillOutgoing(tr, 2); err != nil {
		t.Fatal(err)
	}
	if err := tr.Send([]byte("third")); !errors.Is(err, transport.ErrQueueFull) {
		t.Fatalf("queue should be full while consumer is slow, err=%v", err)
	}
	if got := consumer.Step(); string(got) != "fill-0" {
		t.Fatalf("Step=%q", got)
	}
	// 消费一条后队列让出一个位置。
	if err := tr.Send([]byte("third")); err != nil {
		t.Fatalf("after one Step send should succeed, err=%v", err)
	}
}

func TestDrainOutgoingPreservesOrder(t *testing.T) {
	tr := transport.NewFakeTransport(8)
	if err := FillOutgoing(tr, 3); err != nil {
		t.Fatal(err)
	}
	msgs := DrainOutgoing(tr)
	if len(msgs) != 3 || string(msgs[0]) != "fill-0" || string(msgs[2]) != "fill-2" {
		t.Fatalf("drain=%q", msgs)
	}
	if rest := DrainOutgoing(tr); len(rest) != 0 {
		t.Fatalf("second drain=%q", rest)
	}
}

func TestDrainIncomingMirrorsClientToGate(t *testing.T) {
	tr := transport.NewFakeTransport(4)
	if err := FillIncoming(tr, 2); err != nil {
		t.Fatal(err)
	}
	msgs := DrainIncoming(tr)
	if len(msgs) != 2 || string(msgs[1]) != "fill-1" {
		t.Fatalf("drain=%q", msgs)
	}
}

func TestDisconnectFailsClosed(t *testing.T) {
	tr := transport.NewFakeTransport(2)
	tr.Disconnect()
	if err := FillOutgoing(tr, 1); !errors.Is(err, transport.ErrDisconnected) {
		t.Fatalf("send on disconnected err=%v", err)
	}
	if msgs := DrainOutgoing(tr); msgs != nil {
		t.Fatalf("drain on disconnected=%q", msgs)
	}
}
