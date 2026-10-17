// Package redisscript owns the fixed Lua registries used only by FarmGate.
// It keeps startup preloading and EVALSHA/NOSCRIPT recovery consistent without
// changing the Player, Farm, Worker, or Authority persistence script families.
package redisscript

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Definition is one immutable, named Gate script. Names are process-local
// diagnostics; only the source-derived SHA and exact source reach Redis.
type Definition struct {
	Name   string
	Source string
}

// Registry owns one fixed script family for one Redis domain client.
type Registry struct {
	client  redis.UniversalClient
	names   []string
	sources map[string]string
	scripts map[string]*redis.Script
}

// New builds a closed registry. Definitions cannot be added after construction,
// which keeps the script set and metric cardinality bounded.
func New(client redis.UniversalClient, definitions ...Definition) (*Registry, error) {
	if client == nil {
		return nil, errors.New("gate redis script registry: client required")
	}
	registry := &Registry{
		client:  client,
		names:   make([]string, 0, len(definitions)),
		sources: make(map[string]string, len(definitions)),
		scripts: make(map[string]*redis.Script, len(definitions)),
	}
	for _, definition := range definitions {
		name := strings.TrimSpace(definition.Name)
		if name == "" || definition.Source == "" {
			return nil, errors.New("gate redis script registry: name and source required")
		}
		if _, exists := registry.scripts[name]; exists {
			return nil, fmt.Errorf("gate redis script registry: duplicate script %q", name)
		}
		registry.names = append(registry.names, name)
		registry.sources[name] = definition.Source
		registry.scripts[name] = redis.NewScript(definition.Source)
	}
	if len(registry.names) == 0 {
		return nil, errors.New("gate redis script registry: at least one script required")
	}
	sort.Strings(registry.names)
	return registry, nil
}

// EnsureLoaded batches the independent SCRIPT LOAD operations on every known
// primary. No SCRIPT FLUSH or cache-wide mutation is ever used. Loading every
// primary is required because Redis Cluster script caches are node-local.
func (r *Registry) EnsureLoaded(ctx context.Context) error {
	if r == nil || r.client == nil {
		return errors.New("gate redis script registry: unavailable")
	}
	switch client := r.client.(type) {
	case *redis.ClusterClient:
		return client.ForEachMaster(ctx, func(nodeCtx context.Context, node *redis.Client) error {
			return r.loadOn(nodeCtx, node)
		})
	case *redis.Ring:
		return client.ForEachShard(ctx, func(nodeCtx context.Context, node *redis.Client) error {
			return r.loadOn(nodeCtx, node)
		})
	default:
		return r.loadOn(ctx, r.client)
	}
}

type pipelineLoader interface {
	Pipelined(context.Context, func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

func (r *Registry) loadOn(ctx context.Context, client pipelineLoader) error {
	commands := make([]*redis.StringCmd, len(r.names))
	_, execErr := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, name := range r.names {
			commands[index] = pipe.ScriptLoad(ctx, r.sources[name])
		}
		return nil
	})
	for index, command := range commands {
		sha, err := command.Result()
		if err != nil {
			return fmt.Errorf("load %s: %w", r.names[index], err)
		}
		want := r.scripts[r.names[index]].Hash()
		if sha != want {
			return fmt.Errorf("load %s: sha mismatch got=%s want=%s", r.names[index], sha, want)
		}
	}
	if execErr != nil {
		return execErr
	}
	return nil
}

// Run uses EVALSHA in the normal path and the go-redis exact-source EVAL
// fallback only after NOSCRIPT proves that the script did not execute.
func (r *Registry) Run(ctx context.Context, name string, keys []string, args ...interface{}) *redis.Cmd {
	script, err := r.script(name)
	if err != nil {
		return errorCommand(ctx, err)
	}
	return script.Run(ctx, r.client, keys, args...)
}

// EvalSha queues or executes only the compact SHA form. Pipeline callers use
// Eval as a per-item recovery only when the corresponding EVALSHA returned
// NOSCRIPT; successful independent items are never replayed.
func (r *Registry) EvalSha(ctx context.Context, client redis.Scripter, name string, keys []string, args ...interface{}) *redis.Cmd {
	script, err := r.script(name)
	if err != nil {
		return errorCommand(ctx, err)
	}
	return script.EvalSha(ctx, client, keys, args...)
}

// Eval executes the immutable exact source. It is intentionally exposed only
// for definitely-not-executed NOSCRIPT recovery in a pipelined caller.
func (r *Registry) Eval(ctx context.Context, name string, keys []string, args ...interface{}) *redis.Cmd {
	script, err := r.script(name)
	if err != nil {
		return errorCommand(ctx, err)
	}
	return script.Eval(ctx, r.client, keys, args...)
}

func (r *Registry) script(name string) (*redis.Script, error) {
	if r == nil {
		return nil, errors.New("gate redis script registry: unavailable")
	}
	script, ok := r.scripts[name]
	if !ok {
		return nil, fmt.Errorf("gate redis script registry: unknown script %q", name)
	}
	return script, nil
}

// IsNoScript recognizes only Redis's definitely-not-executed cache miss.
func IsNoScript(err error) bool {
	return errors.Is(err, redis.ErrNoScript)
}

func errorCommand(ctx context.Context, err error) *redis.Cmd {
	command := redis.NewCmd(ctx)
	command.SetErr(err)
	return command
}
