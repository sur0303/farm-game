package authority

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ScriptRegistry owns the fixed, versioned Lua family. Hot path is EVALSHA;
// NOSCRIPT is recovered by reloading the exact same source (bounded retries),
// because a NOSCRIPT reply proves the script never entered execution.
type ScriptRegistry struct {
	client  redis.UniversalClient
	scripts map[string]scriptDef
}

func NewScriptRegistry(client redis.UniversalClient) (*ScriptRegistry, error) {
	if client == nil {
		return nil, errors.New("redisauthority: redis client required")
	}
	r := &ScriptRegistry{client: client, scripts: make(map[string]scriptDef)}
	for _, def := range builtinScripts() {
		if _, exists := r.scripts[def.name]; exists {
			return nil, fmt.Errorf("redisauthority: duplicate script %q", def.name)
		}
		r.scripts[def.name] = def
	}
	return r, nil
}

// ScriptSHA exposes the frozen SHA-1 for goldens and diagnostics.
func (r *ScriptRegistry) ScriptSHA(name string) (string, bool) {
	def, ok := r.scripts[name]
	if !ok {
		return "", false
	}
	return def.sha, true
}

// EnsureLoaded preloads every script on every known primary (31 §15.1).
func (r *ScriptRegistry) EnsureLoaded(ctx context.Context) error {
	for _, def := range r.scripts {
		if err := r.loadScript(ctx, def); err != nil {
			return fmt.Errorf("redisauthority: script load %s: %w", def.name, err)
		}
	}
	return nil
}

func (r *ScriptRegistry) loadScript(ctx context.Context, def scriptDef) error {
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

// Run executes a script by name with EVALSHA and bounded NOSCRIPT recovery.
// After retries are exhausted the NOSCRIPT error is returned to the caller,
// which classifies it as "definitely not executed" (UNAVAILABLE_NOT_SENT).
func (r *ScriptRegistry) Run(ctx context.Context, name string, keys []string, args ...interface{}) ([]string, error) {
	return r.run(ctx, name, keys, false, args...)
}

type scriptBatchCall struct {
	keys []string
	args []interface{}
}

type scriptBatchResult struct {
	reply []string
	err   error
}

// runBatch pipelines independent invocations of one frozen terminal script.
// Redis Cluster splits the pipeline by primary/slot; each EVALSHA remains its
// own atomic operation and has automatic command retries disabled. A NOSCRIPT
// command is retried individually because that reply proves non-execution.
func (r *ScriptRegistry) runBatch(ctx context.Context, name string, calls []scriptBatchCall) []scriptBatchResult {
	results := make([]scriptBatchResult, len(calls))
	if len(calls) == 0 {
		return results
	}
	def, ok := r.scripts[name]
	if !ok {
		err := fmt.Errorf("redisauthority: unknown script %q", name)
		for i := range results {
			results[i].err = err
		}
		return results
	}
	pipe := r.client.Pipeline()
	commands := make([]*noRetryEvalShaCommand, len(calls))
	for i, call := range calls {
		commands[i] = newNoRetryEvalShaCommand(ctx, def.sha, call.keys, call.args...)
		_ = pipe.Process(ctx, commands[i])
	}
	_, _ = pipe.Exec(ctx)
	for i, command := range commands {
		reply, err := command.Result()
		if isNoScript(err) {
			results[i].reply, results[i].err = r.RunNoRetry(ctx, name, calls[i].keys, calls[i].args...)
			continue
		}
		if err != nil {
			results[i].err = err
			continue
		}
		results[i].reply, results[i].err = scriptStrings(reply)
	}
	return results
}

// RunNoRetry executes one Authority mutation attempt with the go-redis retry
// bit disabled on the command itself. A final dial error can prove NOT_SENT
// only when no earlier hidden client retry could already have executed the
// script. Explicit NOSCRIPT reload/retry remains safe because NOSCRIPT is a
// definitive non-execution reply for that attempt.
func (r *ScriptRegistry) RunNoRetry(ctx context.Context, name string, keys []string, args ...interface{}) ([]string, error) {
	return r.run(ctx, name, keys, true, args...)
}

func (r *ScriptRegistry) run(ctx context.Context, name string, keys []string, noRetry bool, args ...interface{}) ([]string, error) {
	def, ok := r.scripts[name]
	if !ok {
		return nil, fmt.Errorf("redisauthority: unknown script %q", name)
	}
	var lastErr error
	for attempt := 0; attempt <= noScriptMaxRetries; attempt++ {
		var reply interface{}
		var err error
		if noRetry {
			reply, err = evalShaNoRetry(ctx, r.client, def.sha, keys, args...)
		} else {
			reply, err = r.client.EvalSha(ctx, def.sha, keys, args...).Result()
		}
		if err == nil {
			return scriptStrings(reply)
		}
		if !isNoScript(err) {
			return nil, err
		}
		lastErr = err
		if lerr := r.loadScript(ctx, def); lerr != nil {
			return nil, fmt.Errorf("redisauthority: noscript reload %s: %w", def.name, lerr)
		}
	}
	return nil, lastErr
}

type noRetryEvalShaCommand struct{ *redis.Cmd }

func (*noRetryEvalShaCommand) NoRetry() bool { return true }

func newNoRetryEvalShaCommand(ctx context.Context, sha string, keys []string, args ...interface{}) *noRetryEvalShaCommand {
	commandArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	commandArgs[0] = "evalsha"
	commandArgs[1] = sha
	commandArgs[2] = len(keys)
	for index, key := range keys {
		commandArgs[3+index] = key
	}
	commandArgs = append(commandArgs, args...)
	base := redis.NewCmd(ctx, commandArgs...)
	if len(keys) > 0 {
		base.SetFirstKeyPos(3)
	}
	return &noRetryEvalShaCommand{Cmd: base}
}

func evalShaNoRetry(ctx context.Context, client redis.UniversalClient, sha string, keys []string, args ...interface{}) (interface{}, error) {
	command := newNoRetryEvalShaCommand(ctx, sha, keys, args...)
	processErr := client.Process(ctx, command)
	reply, resultErr := command.Result()
	if resultErr != nil {
		return nil, resultErr
	}
	if processErr != nil {
		return nil, processErr
	}
	return reply, nil
}
