package livebot

import (
	"context"
	"time"

	"farmgame/internal/demokit"
)

// Executor is the only client seam required by Runner. Real and fake adapters
// can share scheduling and evidence production without coupling either layer
// to the farm wire protocol.
type Executor interface {
	Run(ctx context.Context, spec BotSpec, observer Observer) BotResult
}

// Observation is a safe, typed observation emitted by an Executor. Producer
// only accepts the lifecycle kinds below and whitelists their fields.
type Observation struct {
	Kind             demokit.EventKind
	Action           string
	Latency          time.Duration
	ReconnectAttempt int
	Status           BotStatus
	Fields           map[string]any
}

// BotObservation is an expressive alias for adapters that prefer the more
// specific name.
type BotObservation = Observation

// Observer is bound to one BotSpec by Producer. The generic Observe method is
// useful for adapters; named lifecycle methods keep small fake executors
// readable. Variadic arguments preserve convenient no-argument calls while
// allowing action latency and reconnect attempt metadata.
type Observer interface {
	Observe(Observation)
	Login()
	Hello()
	Join()
	Ready()
	Action(action string, latency ...time.Duration)
	Reconnect(attempt ...int)
	Terminal(result BotResult)
	// Phase records one latency sample against the closed phase vocabulary.
	// Unlike Action it emits no event, so a long online session can be
	// measured without flooding the bounded event store.
	Phase(phase string, elapsed time.Duration)
	OnLogin()
	OnHello()
	OnJoin()
	OnReady()
	OnAction(action string, latency ...time.Duration)
	OnReconnect(attempt ...int)
	OnTerminal(result BotResult)
}
