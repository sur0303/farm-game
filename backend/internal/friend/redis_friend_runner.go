package friend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
)

// These errors keep transport fate distinct from business REJECTED/NO_CHANGE
// outcomes.  A timeout is never silently retried as a destructive write.
var (
	ErrRedisUnavailableNotSent = errors.New("friend: redis unavailable not sent")
	ErrRedisTimeoutUnknown     = errors.New("friend: redis timeout unknown")
	ErrRedisContractViolation  = errors.New("friend: redis contract violation")
)

type friendScriptSet struct {
	client  redis.UniversalClient
	scripts map[string]*redis.Script
}

func newFriendScriptSet(client redis.UniversalClient) (*friendScriptSet, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client", ErrInvalidCommand)
	}
	set := &friendScriptSet{client: client, scripts: make(map[string]*redis.Script)}
	for name, source := range friendScriptSources() {
		set.scripts[name] = redis.NewScript(source)
	}
	return set, nil
}

func (s *friendScriptSet) scriptSHA(name string) (string, bool) {
	script, ok := s.scripts[name]
	if !ok {
		return "", false
	}
	return script.Hash(), true
}

// ensureLoaded is a startup/diagnostic warm-up.  The go-redis Script.Run path
// still performs a target-node EVAL fallback on NOSCRIPT, which is required
// after a Cluster failover.  No script-cache state is treated as cluster-wide.
func (s *friendScriptSet) ensureLoaded(ctx context.Context) error {
	if cluster, ok := s.client.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			for name, script := range s.scripts {
				got, err := script.Load(ctx, node).Result()
				if err != nil {
					return fmt.Errorf("load %s: %w", name, err)
				}
				if got != script.Hash() {
					return fmt.Errorf("load %s sha=%s want=%s", name, got, script.Hash())
				}
			}
			return nil
		})
	}
	for name, script := range s.scripts {
		got, err := script.Load(ctx, s.client).Result()
		if err != nil {
			return fmt.Errorf("load %s: %w", name, err)
		}
		if got != script.Hash() {
			return fmt.Errorf("load %s sha=%s want=%s", name, got, script.Hash())
		}
	}
	return nil
}

func (s *friendScriptSet) run(ctx context.Context, name string, keys []string, args ...interface{}) ([]string, error) {
	script, ok := s.scripts[name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown script %q", ErrRedisContractViolation, name)
	}
	result, err := script.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return nil, classifyFriendWriteError(err)
	}
	values, decodeErr := friendScriptStrings(result)
	if decodeErr != nil {
		// A reply-shape failure happens after EVAL has been accepted.  The
		// script may already have committed its state, so it must enter the
		// same reconciliation path as a runtime/transport-unknown result.
		return nil, fmt.Errorf("%w: %v", ErrRedisTimeoutUnknown, decodeErr)
	}
	return values, nil
}

func classifyFriendWriteError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "CROSSSLOT") {
		return fmt.Errorf("%w: %v", ErrRedisContractViolation, err)
	}
	// NOSCRIPT is a pre-execution reply.  go-redis normally reloads the
	// exact source inside Script.Run; if that bounded recovery still fails,
	// the destructive script was not entered and may safely be retried by the
	// caller as UNAVAILABLE_NOT_SENT.
	if strings.Contains(err.Error(), "NOSCRIPT") {
		return fmt.Errorf("%w: %v", ErrRedisUnavailableNotSent, err)
	}
	// EVAL may have mutated one or more keys before Redis reports a Lua/runtime
	// error.  Treat every such server-side execution failure as an unknown
	// outcome; repository callers must reconcile by identity/receipt.
	if strings.HasPrefix(err.Error(), "ERR") || strings.Contains(err.Error(), "Error running script") ||
		strings.Contains(err.Error(), "WRONGTYPE") {
		return fmt.Errorf("%w: %v", ErrRedisTimeoutUnknown, err)
	}
	if errors.Is(err, syscall.ECONNREFUSED) ||
		strings.Contains(err.Error(), "redis: client is closed") ||
		strings.Contains(err.Error(), "connection pool is closed") ||
		strings.HasPrefix(err.Error(), "MOVED") || strings.HasPrefix(err.Error(), "ASK") ||
		strings.Contains(err.Error(), "CLUSTERDOWN") {
		return fmt.Errorf("%w: %v", ErrRedisUnavailableNotSent, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrRedisTimeoutUnknown, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w: %v", ErrRedisTimeoutUnknown, err)
	}
	// A server-side error after a command was accepted is conservative
	// unknown for a mutating call.  Contract errors returned by Lua are
	// translated separately by repository result decoders.
	return fmt.Errorf("%w: %v", ErrRedisTimeoutUnknown, err)
}

func friendScriptStrings(reply interface{}) ([]string, error) {
	items, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: script reply is %T", ErrRedisContractViolation, reply)
	}
	out := make([]string, len(items))
	for i, item := range items {
		switch value := item.(type) {
		case string:
			out[i] = value
		case []byte:
			out[i] = string(value)
		case int64:
			out[i] = fmt.Sprintf("%d", value)
		case nil:
			out[i] = ""
		default:
			return nil, fmt.Errorf("%w: script reply element %T", ErrRedisContractViolation, item)
		}
	}
	return out, nil
}

func scriptStatus(out []string) (string, error) {
	if len(out) == 0 || out[0] == "" {
		return "", fmt.Errorf("%w: empty script result", ErrRedisContractViolation)
	}
	return out[0], nil
}
