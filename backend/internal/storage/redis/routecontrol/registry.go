package routecontrol

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type scriptDef struct {
	name   string
	source string
	sha    string
}

type scriptRegistry struct {
	client  redis.UniversalClient
	scripts map[string]scriptDef
}

type ScriptRegistry = scriptRegistry

func NewScriptRegistry(client redis.UniversalClient) (*ScriptRegistry, error) {
	return newScriptRegistry(client)
}

func newScriptRegistry(client redis.UniversalClient) (*scriptRegistry, error) {
	if client == nil {
		return nil, ErrInvalidOptions
	}
	registry := &scriptRegistry{client: client, scripts: make(map[string]scriptDef)}
	for _, def := range builtinScripts() {
		if _, exists := registry.scripts[def.name]; exists {
			return nil, fmt.Errorf("%w: duplicate script %q", ErrScriptContract, def.name)
		}
		registry.scripts[def.name] = def
	}
	return registry, nil
}

func (r *scriptRegistry) scriptSHA(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	def, ok := r.scripts[name]
	return def.sha, ok
}

func (r *scriptRegistry) ensureLoaded(ctx context.Context) error {
	if r == nil {
		return ErrScriptContract
	}
	for _, def := range r.scripts {
		if err := r.loadScript(ctx, def); err != nil {
			return fmt.Errorf("%w: load %s: %v", ErrScriptContract, def.name, err)
		}
	}
	return nil
}

func (r *scriptRegistry) loadScript(ctx context.Context, def scriptDef) error {
	if cluster, ok := r.client.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(nodeCtx context.Context, node *redis.Client) error {
			sha, err := node.ScriptLoad(nodeCtx, def.source).Result()
			if err != nil {
				return err
			}
			if sha != def.sha {
				return fmt.Errorf("script sha mismatch got=%s want=%s", sha, def.sha)
			}
			return nil
		})
	}
	sha, err := r.client.ScriptLoad(ctx, def.source).Result()
	if err != nil {
		return err
	}
	if sha != def.sha {
		return fmt.Errorf("script sha mismatch got=%s want=%s", sha, def.sha)
	}
	return nil
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

const noScriptMaxRetries = 2

func (r *scriptRegistry) run(ctx context.Context, name string, keys []string, args ...interface{}) ([]string, error) {
	if r == nil {
		return nil, ErrScriptContract
	}
	def, ok := r.scripts[name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown script %q", ErrScriptContract, name)
	}
	var lastErr error
	for attempt := 0; attempt <= noScriptMaxRetries; attempt++ {
		reply, err := r.client.EvalSha(ctx, def.sha, keys, args...).Result()
		if err == nil {
			return scriptStrings(reply)
		}
		if !isNoScript(err) {
			return nil, err
		}
		lastErr = err
		if err := r.loadScript(ctx, def); err != nil {
			return nil, fmt.Errorf("%w: NOSCRIPT reload %s: %v", ErrScriptContract, def.name, err)
		}
	}
	return nil, lastErr
}

func scriptStrings(reply interface{}) ([]string, error) {
	array, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: reply type %T", ErrScriptContract, reply)
	}
	out := make([]string, 0, len(array))
	for _, item := range array {
		switch value := item.(type) {
		case string:
			out = append(out, value)
		case []byte:
			out = append(out, string(value))
		case int64:
			out = append(out, fmt.Sprintf("%d", value))
		case nil:
			out = append(out, "")
		default:
			return nil, fmt.Errorf("%w: reply element %T", ErrScriptContract, item)
		}
	}
	return out, nil
}

func isCrossSlotReply(reply []string) bool {
	return len(reply) > 0 && reply[0] == "CROSS_SLOT"
}
