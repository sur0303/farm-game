package capaudit

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workflow"

	"github.com/redis/go-redis/v9"
)

// NamespaceResidualMetric is the contract name this count is published under.
const NamespaceResidualMetric = "namespace_residual_before_run"

// DefaultResidualCeiling is the largest count this precheck will take.
//
// Anything above zero already refuses the run, so a bigger number buys no
// decision, only scan time. The value is the same per-pass bound the other
// checks here read their indexes with.
const DefaultResidualCeiling = 65536

const (
	// residualScanPage is the COUNT hint given to one SCAN call.
	residualScanPage = 512
	// residualScanPages bounds the cursor loop. MATCH is applied by the server
	// after it walks the slots, so a database holding somebody else's millions
	// of keys is iterated in full before the cursor comes home. At the page
	// size above, this gives up after roughly two million keys visited rather
	// than stalling the oracle's startup on them.
	residualScanPages = 4096
)

// ResidualCount is what a run's keyspace held before the run began.
type ResidualCount struct {
	Keys int64
	// AtLeast marks a count that stopped before the keyspace was exhausted.
	// Keys is then a floor, which is all the run needs: any residue at all
	// means it is not starting from clean storage.
	AtLeast bool
}

// NamespaceResidual counts the keys a capacity run's keyspace already holds.
//
// This is a before-the-run fact and has to be taken before the run writes
// anything. Taken later it would count the run's own writes and climb as the
// run proceeds, so nothing here caches or repeats a count: the caller takes it
// once and publishes exactly what it got.
//
// What it can attribute to a run is what carries the run's namespace or its
// work store prefix. The legacy session and ticket rows the connection chain
// writes carry no namespace at all, so on a Redis shared with anything else
// they cannot be told from another tenant's and are outside this count.
type NamespaceResidual struct {
	client   redis.UniversalClient
	patterns []string
	ceiling  int64
}

type NamespaceResidualConfig struct {
	Client redis.UniversalClient
	// Namespace is the keyspace the services under test are pointed at,
	// matching REDIS_NAMESPACE_ENVIRONMENT and REDIS_NAMESPACE_ZONE.
	Namespace rediskey.Namespace
	// WorkStorePrefix is WORKFLOW_STORE_PREFIX as given to the services. The
	// generic work store is addressed by its own prefix rather than through the
	// namespace, so without it a keyspace still holding a previous run's
	// workflow records would be reported clean.
	WorkStorePrefix string
	Ceiling         int64
}

func NewNamespaceResidual(config NamespaceResidualConfig) (*NamespaceResidual, error) {
	if config.Client == nil {
		return nil, errors.New("capaudit: redis client is required")
	}
	if err := config.Namespace.Validate(); err != nil {
		return nil, err
	}
	if strings.ContainsAny(config.WorkStorePrefix, `*?[]\`) {
		return nil, fmt.Errorf("capaudit: work store prefix %q holds a match character, "+
			"so a scan for it would not name one keyspace", config.WorkStorePrefix)
	}
	work, err := workflow.NewWorkKeys(config.WorkStorePrefix)
	if err != nil {
		return nil, err
	}
	ceiling := config.Ceiling
	if ceiling <= 0 {
		ceiling = DefaultResidualCeiling
	}
	// Authority, workflow and runtime keys open with the hash tag that carries
	// the namespace; projections and friend rows carry it plain. Asking for
	// only one of the two shapes would miss half of what a run leaves behind.
	prefix := config.Namespace.KeyPrefix() + ":"
	return &NamespaceResidual{
		client:   config.Client,
		patterns: []string{prefix + "*", "{" + prefix + "*", work.KeyPrefix() + "*"},
		ceiling:  ceiling,
	}, nil
}

// controlPlaneSegments name the key families a deployment writes before any
// load reaches it: the authority rows and scan leases the Holders claim as they
// start, the route controller's own records, and the boot markers a Gate writes
// to identify its process. None of them can be left behind by a previous run,
// because they are rewritten by the deployment that is about to be measured, and
// on a dynamic-routing deployment they are never absent -- the route has to
// exist before anything can serve. Counting them would refuse every run for a
// reason that has nothing to do with residue.
//
// Everything else still counts. Actors, workflow records, projections, friend
// rows, receipts and sessions are written because somebody played, so finding
// one before the run means the keyspace is not clean.
var controlPlaneSegments = []string{
	":shard-authority",
	":scan-lease",
	":route-control:",
	":gate-boot:",
}

func deploymentControlPlane(key string) bool {
	for _, segment := range controlPlaneSegments {
		if strings.Contains(key, segment) {
			return true
		}
	}
	return false
}

// Count reads how much the keyspace already holds. An error means the count
// could not be taken, and the caller has to publish nothing rather than a zero
// it did not measure.
func (r *NamespaceResidual) Count(ctx context.Context) (ResidualCount, error) {
	// Two prefixes can name the same key and SCAN may return one twice, so what
	// is counted is distinct keys rather than the length of the pages read.
	seen := make(map[string]struct{})
	for _, pattern := range r.patterns {
		done, err := collect(r.pages(ctx, pattern), seen, r.ceiling)
		if err != nil {
			return ResidualCount{}, fmt.Errorf("count %s: %w", pattern, err)
		}
		if !done {
			return ResidualCount{Keys: min(int64(len(seen)), r.ceiling), AtLeast: true}, nil
		}
	}
	return ResidualCount{Keys: int64(len(seen))}, nil
}

// scanner reads one page of keys and returns the cursor the next page starts
// at, zero once the keyspace has been walked in full.
type scanner func(cursor uint64) ([]string, uint64, error)

func (r *NamespaceResidual) pages(ctx context.Context, pattern string) scanner {
	return func(cursor uint64) ([]string, uint64, error) {
		return r.client.Scan(ctx, cursor, pattern, residualScanPage).Result()
	}
}

// collect walks one cursor loop into seen and reports whether it got to the end
// of it. Stopping early is only an answer while something has been found: a
// loop that ran out of pages having seen nothing did not read enough of the
// keyspace to call the namespace empty, and says so instead.
func collect(page scanner, seen map[string]struct{}, ceiling int64) (bool, error) {
	var cursor uint64
	for range residualScanPages {
		keys, next, err := page(cursor)
		if err != nil {
			return false, err
		}
		for _, key := range keys {
			if deploymentControlPlane(key) {
				continue
			}
			seen[key] = struct{}{}
		}
		if int64(len(seen)) >= ceiling {
			return false, nil
		}
		if next == 0 {
			return true, nil
		}
		cursor = next
	}
	if len(seen) == 0 {
		return false, errors.New("the scan did not reach the end of the keyspace, " +
			"so an empty namespace could not be proven")
	}
	return false, nil
}
