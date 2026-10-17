package downlink

import "sync"

// LevelTriggeredNotifier is readable for as long as work remains pending.
// WaitCh never consumes the signal: this matters because a select may choose a
// simultaneously-ready command response before the downlink case. The owner
// resets the notifier only after it has observed an empty queue under the same
// lock that protects enqueue/dequeue.
type LevelTriggeredNotifier struct {
	mu      sync.Mutex
	closed  bool
	pending bool
	ch      chan struct{}
}

// Signal transitions the notifier from idle to pending. Repeated signals while
// pending are coalesced and leave the same closed channel readable.
func (n *LevelTriggeredNotifier) Signal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	if n.pending {
		return
	}
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	n.pending = true
	close(n.ch)
}

// WaitCh returns the current level-triggered wait channel. While pending (or
// after terminal Close), repeated calls return the same readable channel.
func (n *LevelTriggeredNotifier) WaitCh() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch == nil {
		n.ch = make(chan struct{})
		if n.closed || n.pending {
			close(n.ch)
		}
	}
	return n.ch
}

// Pending reports whether a notification is currently unconsumed.
func (n *LevelTriggeredNotifier) Pending() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.pending
}

// Reset returns a pending notifier to idle after the owner has proved that its
// queue is empty. It must be called while holding the owner's queue lock so a
// concurrent enqueue cannot be cleared accidentally.
func (n *LevelTriggeredNotifier) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || !n.pending {
		return
	}
	n.pending = false
	n.ch = make(chan struct{})
}

// Close marks the notifier closed forever. Every current and future WaitCh
// channel is readable, which the pump interprets as writer teardown.
func (n *LevelTriggeredNotifier) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	if !n.pending {
		close(n.ch)
	}
	n.pending = true
	n.mu.Unlock()
}
