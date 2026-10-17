package transport

import (
	"testing"
)

func TestFakeTransportPreservesMessageBoundaries(t *testing.T) {
	tr := NewFakeTransport(2)
	if err := tr.Data([]byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	got, err := tr.Receive()
	if err != nil || string(got) != string([]byte{1, 2}) {
		t.Fatalf("receive=%v err=%v", got, err)
	}
	if err := tr.Send([]byte{3, 4}); err != nil {
		t.Fatal(err)
	}
	got, err = tr.Sent()
	if err != nil || string(got) != string([]byte{3, 4}) {
		t.Fatalf("sent=%v err=%v", got, err)
	}
}

func TestFakeTransportQueueFullAndDisconnect(t *testing.T) {
	tr := NewFakeTransport(1)
	if err := tr.Data([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Data([]byte{2}); err != ErrQueueFull {
		t.Fatalf("want queue full, got %v", err)
	}
	tr.Disconnect()
	if err := tr.Data([]byte{3}); err != ErrDisconnected {
		t.Fatalf("want disconnected, got %v", err)
	}
}
