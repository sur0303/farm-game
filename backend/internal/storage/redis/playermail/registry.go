package playermail

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// scriptRegistry owns the fixed, versioned PlayerMail Lua family. The hot
// path is EVALSHA; NOSCRIPT is recovered by reloading the exact same source
// with bounded retries, because a NOSCRIPT reply proves the script never
// entered execution. This mirrors the contract-31 ScriptRegistry discipline
// for the separate 35A script family (35A §8).
type scriptRegistry struct {
	client  redis.UniversalClient
	scripts map[string]scriptDef
}

func newScriptRegistry(client redis.UniversalClient) (*scriptRegistry, error) {
	if client == nil {
		return nil, errors.New("playermail: redis client required")
	}
	r := &scriptRegistry{client: client, scripts: make(map[string]scriptDef)}
	for _, def := range builtinScripts() {
		if _, exists := r.scripts[def.name]; exists {
			return nil, fmt.Errorf("playermail: duplicate script %q", def.name)
		}
		r.scripts[def.name] = def
	}
	return r, nil
}

// scriptSHA exposes the frozen SHA-1 for goldens and diagnostics.
func (r *scriptRegistry) scriptSHA(name string) (string, bool) {
	def, ok := r.scripts[name]
	if !ok {
		return "", false
	}
	return def.sha, true
}

// ensureLoaded preloads every script on every known primary.
func (r *scriptRegistry) ensureLoaded(ctx context.Context) error {
	for _, def := range r.scripts {
		if err := r.loadScript(ctx, def); err != nil {
			return fmt.Errorf("playermail: script load %s: %w", def.name, err)
		}
	}
	return nil
}

func (r *scriptRegistry) loadScript(ctx context.Context, def scriptDef) error {
	if cluster, ok := r.client.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			sha, err := node.ScriptLoad(ctx, def.source).Result()
			if err != nil {
				return err
			}
			if sha != def.sha {
				return fmt.Errorf("sha mismatch: got %s want %s", sha, def.sha)
			}
			return nil
		})
	}
	sha, err := r.client.ScriptLoad(ctx, def.source).Result()
	if err != nil {
		return err
	}
	if sha != def.sha {
		return fmt.Errorf("sha mismatch: got %s want %s", sha, def.sha)
	}
	return nil
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

const noScriptMaxRetries = 2

// run executes a script by name with EVALSHA and bounded NOSCRIPT recovery.
// After retries are exhausted the NOSCRIPT error is returned to the caller,
// which classifies it as "definitely not executed" (UNAVAILABLE_NOT_SENT).
func (r *scriptRegistry) run(ctx context.Context, name string, keys []string, args ...interface{}) ([]string, error) {
	def, ok := r.scripts[name]
	if !ok {
		return nil, fmt.Errorf("playermail: unknown script %q", name)
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
		if lerr := r.loadScript(ctx, def); lerr != nil {
			return nil, fmt.Errorf("playermail: noscript reload %s: %w", def.name, lerr)
		}
	}
	return nil, lastErr
}

func scriptStrings(reply interface{}) ([]string, error) {
	arr, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("playermail: script reply is not an array: %T", reply)
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		switch s := v.(type) {
		case string:
			out = append(out, s)
		case []byte:
			out = append(out, string(s))
		case int64:
			out = append(out, fmt.Sprintf("%d", s))
		case nil:
			out = append(out, "")
		default:
			return nil, fmt.Errorf("playermail: script reply element %T", v)
		}
	}
	return out, nil
}
