package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"farmgame/internal/holdercontrol"
	core "farmgame/internal/routecontrol"
)

var (
	errHolderControlPoolClosed    = errors.New("route-controller: HolderControl pool closed")
	errHolderControlPoolExhausted = errors.New("route-controller: HolderControl pool exhausted")
)

type holderControlPoolKey struct {
	domain      string
	holderID    string
	bootID      string
	endpoint    string
	tlsIdentity string
}

type holderControlDialFunc func(context.Context, core.HolderRecord) (*holdercontrol.HolderControlClient, error)

type holderControlPoolEntry struct {
	ready  chan struct{}
	client *holdercontrol.HolderControlClient
	err    error
	refs   int
}

// holderControlClientPool keeps one HTTP/2 gRPC connection per exact Holder
// process generation. Concurrent Shard moves to the same Pod therefore share
// a connection instead of opening two new TLS connections per RoutePlan.
// Exact boot/endpoint/TLS identity is part of the key, so a restarted Pod can
// never inherit the previous process generation's connection.
type holderControlClientPool struct {
	mu      sync.Mutex
	max     int
	dial    holderControlDialFunc
	entries map[holderControlPoolKey]*holderControlPoolEntry
	closed  bool
}

func newHolderControlClientPool(max int, dial holderControlDialFunc) *holderControlClientPool {
	if max <= 0 {
		max = defaultHolderControlMaxClients
	}
	return &holderControlClientPool{max: max, dial: dial, entries: make(map[holderControlPoolKey]*holderControlPoolEntry)}
}

func newProductionHolderControlDialer(cfg config) holderControlDialFunc {
	var once sync.Once
	var certPEM, keyPEM, caPEM []byte
	var loadErr error
	load := func() {
		certPEM, loadErr = os.ReadFile(cfg.holderClientCert)
		if loadErr != nil {
			loadErr = errors.New("route-controller: holder client certificate unavailable")
			return
		}
		keyPEM, loadErr = os.ReadFile(cfg.holderClientKey)
		if loadErr != nil {
			loadErr = errors.New("route-controller: holder client key unavailable")
			return
		}
		caPEM, loadErr = os.ReadFile(cfg.holderClientCA)
		if loadErr != nil {
			loadErr = errors.New("route-controller: holder server CA unavailable")
		}
	}
	return func(ctx context.Context, record core.HolderRecord) (*holdercontrol.HolderControlClient, error) {
		once.Do(load)
		if loadErr != nil {
			return nil, loadErr
		}
		timeout := holderControlClientTimeout(cfg.planOperation)
		return holdercontrol.DialHolderControl(ctx, record.HolderControlEndpoint, certPEM, keyPEM, caPEM,
			holdercontrol.HolderControlDialOptions{
				ServerName: record.TLSDNSIdentity, ExpectedServerIdentity: record.TLSDNSIdentity,
				MaxMessageBytes: cfg.maxMessageBytes, DialTimeout: timeout, RPCTimeout: timeout,
			})
	}
}

func holderControlKey(record core.HolderRecord) (holderControlPoolKey, error) {
	key := holderControlPoolKey{
		domain: string(record.Domain), holderID: record.HolderID, bootID: record.BootID,
		endpoint: record.HolderControlEndpoint, tlsIdentity: record.TLSDNSIdentity,
	}
	if key.domain == "" || key.holderID == "" || key.bootID == "" || key.endpoint == "" || key.tlsIdentity == "" {
		return holderControlPoolKey{}, errors.New("route-controller: incomplete HolderControl pool identity")
	}
	return key, nil
}

func (p *holderControlClientPool) Acquire(ctx context.Context, record core.HolderRecord) (*holdercontrol.HolderControlClient, func(), error) {
	if p == nil || ctx == nil || p.dial == nil || p.max < 1 {
		return nil, nil, errHolderControlPoolClosed
	}
	key, err := holderControlKey(record)
	if err != nil {
		return nil, nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, errHolderControlPoolClosed
	}
	if entry := p.entries[key]; entry != nil {
		entry.refs++
		ready := entry.ready
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			p.release(key)
			return nil, nil, ctx.Err()
		case <-ready:
		}
		if entry.err != nil || entry.client == nil {
			p.release(key)
			if entry.err != nil {
				return nil, nil, entry.err
			}
			return nil, nil, errors.New("route-controller: HolderControl dial returned no client")
		}
		return entry.client, p.releaseFunc(key), nil
	}

	var evicted *holdercontrol.HolderControlClient
	if len(p.entries) >= p.max {
		for candidateKey, candidate := range p.entries {
			if candidate.refs == 0 && candidate.client != nil {
				delete(p.entries, candidateKey)
				evicted = candidate.client
				break
			}
		}
		if len(p.entries) >= p.max {
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("%w: max=%d", errHolderControlPoolExhausted, p.max)
		}
	}
	entry := &holderControlPoolEntry{ready: make(chan struct{}), refs: 1}
	p.entries[key] = entry
	p.mu.Unlock()
	if evicted != nil {
		_ = evicted.Close()
	}

	client, dialErr := p.dial(ctx, record)
	p.mu.Lock()
	if p.closed && dialErr == nil {
		dialErr = errHolderControlPoolClosed
	}
	entry.client = client
	entry.err = dialErr
	if dialErr != nil {
		delete(p.entries, key)
	}
	close(entry.ready)
	p.mu.Unlock()
	if dialErr != nil {
		if client != nil {
			_ = client.Close()
		}
		return nil, nil, dialErr
	}
	return client, p.releaseFunc(key), nil
}

func (p *holderControlClientPool) releaseFunc(key holderControlPoolKey) func() {
	var once sync.Once
	return func() { once.Do(func() { p.release(key) }) }
}

func (p *holderControlClientPool) release(key holderControlPoolKey) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if entry := p.entries[key]; entry != nil && entry.refs > 0 {
		entry.refs--
	}
	p.mu.Unlock()
}

func (p *holderControlClientPool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	clients := make([]*holdercontrol.HolderControlClient, 0, len(p.entries))
	for _, entry := range p.entries {
		if entry.client != nil {
			clients = append(clients, entry.client)
		}
	}
	p.entries = make(map[holderControlPoolKey]*holderControlPoolEntry)
	p.mu.Unlock()
	var firstErr error
	for _, client := range clients {
		if err := client.Close(); firstErr == nil && err != nil {
			firstErr = err
		}
	}
	return firstErr
}
