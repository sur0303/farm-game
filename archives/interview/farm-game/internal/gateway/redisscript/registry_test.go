package redisscript

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type commandLog struct {
	mu        sync.Mutex
	commands  []string
	pipelines int
}

func (h *commandLog) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *commandLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		err := next(ctx, command)
		h.mu.Lock()
		h.commands = append(h.commands, command.Name())
		h.mu.Unlock()
		return err
	}
}

func (h *commandLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		h.mu.Lock()
		h.pipelines++
		for _, command := range commands {
			h.commands = append(h.commands, command.Name())
		}
		h.mu.Unlock()
		return err
	}
}

func (h *commandLog) reset() {
	h.mu.Lock()
	h.commands = nil
	h.pipelines = 0
	h.mu.Unlock()
}

func (h *commandLog) snapshot() ([]string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...), h.pipelines
}

func TestRegistryPreloadsIndependentScriptsAndRunsEvalSHA(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	log := &commandLog{}
	client.AddHook(log)
	registry, err := New(client,
		Definition{Name: "write", Source: `return redis.call('SET', KEYS[1], ARGV[1])`},
		Definition{Name: "read", Source: `return redis.call('GET', KEYS[1])`},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := registry.EnsureLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	if commands, pipelines := log.snapshot(); !reflect.DeepEqual(commands, []string{"script", "script"}) || pipelines != 1 {
		t.Fatalf("preload commands=%v pipelines=%d want two SCRIPT LOADs in one pipeline", commands, pipelines)
	}

	log.reset()
	if value, err := registry.Run(ctx, "write", []string{"key"}, "value").Text(); err != nil || value != "OK" {
		t.Fatalf("write value=%q err=%v", value, err)
	}
	if value, err := registry.Run(ctx, "read", []string{"key"}).Text(); err != nil || value != "value" {
		t.Fatalf("read value=%q err=%v", value, err)
	}
	if commands, pipelines := log.snapshot(); !reflect.DeepEqual(commands, []string{"evalsha", "evalsha"}) || pipelines != 0 {
		t.Fatalf("warm commands=%v pipelines=%d want only EVALSHA", commands, pipelines)
	}
}

func TestRegistryRecoversOnlyNoScriptWithExactSource(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	log := &commandLog{}
	client.AddHook(log)
	registry, err := New(client, Definition{
		Name: "increment", Source: `return redis.call('INCR', KEYS[1])`,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := registry.EnsureLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	log.reset()
	if value, err := registry.Run(ctx, "increment", []string{"counter"}).Int64(); err != nil || value != 1 {
		t.Fatalf("increment value=%d err=%v", value, err)
	}
	if commands, _ := log.snapshot(); !reflect.DeepEqual(commands, []string{"evalsha", "eval"}) {
		t.Fatalf("NOSCRIPT recovery commands=%v want [evalsha eval]", commands)
	}
	if value, err := client.Get(ctx, "counter").Int64(); err != nil || value != 1 {
		t.Fatalf("counter=%d err=%v; recovery executed more than once", value, err)
	}
}

func TestRegistryRejectsUnboundedDefinitions(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if _, err := New(client); err == nil {
		t.Fatal("empty registry succeeded")
	}
	if _, err := New(client,
		Definition{Name: "same", Source: "return 1"},
		Definition{Name: "same", Source: "return 2"},
	); err == nil {
		t.Fatal("duplicate script name succeeded")
	}
}
