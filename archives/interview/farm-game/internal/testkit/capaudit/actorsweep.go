package capaudit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

// Gates decided by one sweep of the run's actors.
const (
	RecordVersionRegressionGate = "record_version_regression_total"
	StaleHolderCommitGate       = "stale_holder_commit_success_total"
	// AuthorityClientMismatchGate is folded into the stale holder evidence.
	// Nothing in storage records which authority served which client, and a
	// refused mismatch writes nothing at all, so the only durable trace a
	// wrong authority can leave is a commit from a generation that no longer
	// held the shard. That is the same fact as a stale holder commit, and it
	// is reported under both names rather than under a fabricated zero.
	AuthorityClientMismatchGate = "authority_client_mismatch_total"
)

const actorSweepDefaultLimit = 200000

// actor meta fields, as written by the authority Lua.
const (
	metaRecordVersion = "_meta:record_version"
	metaWriterEpoch   = "_meta:writer_epoch"
)

// ActorSweep watches the run's actors for two faults that only show up by
// comparing readings over time.
//
// A single read of a record version cannot show that it went backwards, and a
// commit from a superseded holder is indistinguishable from an untouched actor
// unless you can see that the record moved. So the sweep remembers what it saw
// and reports what changed between readings, which makes both counts
// cumulative for the run rather than a snapshot.
type ActorSweep struct {
	client  *redis.Client
	keys    rediskey.AuthorityKeys
	profile rediskey.PhysicalProfile
	shards  rediskey.AuthorityShardResolver
	ledger  Ledger
	limit   int

	mu           sync.Mutex
	seen         map[actorKey]uint64
	regressions  int64
	staleCommits int64
	// Duplicates are counted fresh each pass. They sit in the record rather
	// than happening between readings, so accumulating them would report the
	// same fault again on every sweep.
	grants int64
	debits int64
}

type actorKey struct {
	domain actorrouting.RouteDomain
	actor  uint64
}

type ActorSweepConfig struct {
	Client  *redis.Client
	Profile rediskey.PhysicalProfile
	Shards  rediskey.AuthorityShardResolver
	Ledger  Ledger
	// Limit bounds how many actor reads one sweep may do. A run larger than
	// this cannot be swept whole, and the sweep says so rather than reporting
	// a clean count over the part it managed to read.
	Limit int
}

func NewActorSweep(config ActorSweepConfig) (*ActorSweep, error) {
	if config.Client == nil {
		return nil, errors.New("capaudit: redis client is required")
	}
	if err := config.Ledger.validate(); err != nil {
		return nil, err
	}
	keys, err := rediskey.NewAuthorityKeys(config.Profile, config.Shards)
	if err != nil {
		return nil, err
	}
	limit := config.Limit
	if limit <= 0 {
		limit = actorSweepDefaultLimit
	}
	return &ActorSweep{
		client: config.Client, keys: keys, profile: config.Profile, shards: config.Shards,
		ledger: config.Ledger, limit: limit, seen: map[actorKey]uint64{},
	}, nil
}

func (s *ActorSweep) Gates() []string {
	return []string{
		RecordVersionRegressionGate, StaleHolderCommitGate, AuthorityClientMismatchGate,
		DuplicateGrantGate, DuplicateDebitGate,
	}
}

func (s *ActorSweep) ObserveAll(ctx context.Context) (map[string]int64, error) {
	domains := []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm}
	reads := int(s.ledger.ActorCount) * len(domains)
	if reads > s.limit {
		return nil, fmt.Errorf("the run covers %d actor reads, beyond the %d this sweep can do in one pass", reads, s.limit)
	}
	fence, err := s.readFence(ctx, domains)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants, s.debits = 0, 0
	for _, domain := range domains {
		if err := s.sweepDomain(ctx, domain, fence[domain]); err != nil {
			return nil, err
		}
	}
	return map[string]int64{
		RecordVersionRegressionGate: s.regressions,
		StaleHolderCommitGate:       s.staleCommits,
		AuthorityClientMismatchGate: s.staleCommits,
		DuplicateGrantGate:          s.grants,
		DuplicateDebitGate:          s.debits,
	}, nil
}

// readFence reads the writer epoch each shard currently belongs to. It is one
// read per shard, which is cheap, and it is what an actor's own epoch is judged
// against.
func (s *ActorSweep) readFence(ctx context.Context, domains []actorrouting.RouteDomain) (map[actorrouting.RouteDomain][]uint64, error) {
	fence := map[actorrouting.RouteDomain][]uint64{}
	for _, domain := range domains {
		count, err := s.shards.ShardCount(domain)
		if err != nil {
			return nil, fmt.Errorf("shard count for %s: %w", domain, err)
		}
		epochs := make([]uint64, count)
		for shard := uint32(0); shard < count; shard++ {
			ref, err := s.profile.AuthorityBucketForShard(s.shards, domain, shard)
			if err != nil {
				return nil, fmt.Errorf("bucket for %s shard %d: %w", domain, shard, err)
			}
			key, err := s.keys.ShardAuthority(ref)
			if err != nil {
				return nil, err
			}
			raw, err := s.client.HGet(ctx, key.String(), "writer_epoch").Result()
			if errors.Is(err, redis.Nil) {
				// A shard with no authority row has never been claimed, so no
				// actor in it can have been committed by a stale holder.
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", key, err)
			}
			epoch, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%s writer_epoch %q is not a number", key, raw)
			}
			epochs[shard] = epoch
		}
		fence[domain] = epochs
	}
	return fence, nil
}

func (s *ActorSweep) sweepDomain(ctx context.Context, domain actorrouting.RouteDomain, epochs []uint64) error {
	return s.ledger.Actors(func(actorID uint64) error {
		address := actorrouting.ActorAddress{Domain: domain, ActorID: actorID}
		key, err := s.keys.ActorRecordFor(address)
		if err != nil {
			return fmt.Errorf("actor key for %s %d: %w", domain, actorID, err)
		}
		wanted := s.wantedFields(domain)
		fields, err := s.client.HMGet(ctx, key.String(), wanted...).Result()
		if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}
		if len(fields) != len(wanted) || fields[0] == nil {
			// The actor was never created. An identity the run never reached
			// is not a fault.
			return nil
		}
		version, err := parseUnsigned(fields[0], metaRecordVersion)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		writerEpoch, err := parseUnsigned(fields[1], metaWriterEpoch)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		shard, err := s.shards.ShardFor(domain, actorID)
		if err != nil {
			return fmt.Errorf("shard for %s %d: %w", domain, actorID, err)
		}
		s.compare(actorKey{domain: domain, actor: actorID}, version, writerEpoch, epochs[shard])
		if domain != actorrouting.DomainPlayer {
			return nil
		}
		if err := s.countDuplicates(wanted, fields); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		return nil
	})
}

// wantedFields asks for the receipts only where they live. Farm records carry
// their own activity, which this sweep does not read.
func (s *ActorSweep) wantedFields(domain actorrouting.RouteDomain) []string {
	base := []string{metaRecordVersion, metaWriterEpoch}
	if domain != actorrouting.DomainPlayer {
		return base
	}
	return append(base, metaModelLayoutVersion,
		modelRecordField, modelTaskField, modelInventoryField, modelEconomyField)
}

// countDuplicates reads what the record says was applied to it. A row whose
// model cannot be decoded fails the sweep: an unreadable record is not an
// innocent one.
func (s *ActorSweep) countDuplicates(wanted []string, fields []any) error {
	values := map[string][]byte{}
	var layout string
	for index, name := range wanted {
		raw, ok := fields[index].(string)
		if !ok {
			continue
		}
		if name == metaModelLayoutVersion {
			layout = raw
			continue
		}
		values[name] = []byte(raw)
	}
	record, err := decodePlayer(layout, values)
	if err != nil {
		return err
	}
	s.grants += duplicateGrants(record.receipts)
	s.debits += duplicateDebits(record)
	return nil
}

// compare judges one actor against what it looked like last time.
func (s *ActorSweep) compare(who actorKey, version, writerEpoch, shardEpoch uint64) {
	previous, known := s.seen[who]
	s.seen[who] = version
	if !known {
		return
	}
	if version < previous {
		s.regressions++
		return
	}
	if version == previous {
		return
	}
	// The record advanced. A holder claims an actor before committing to it,
	// and claiming stamps the current epoch, so an advance under an older
	// epoch is a write by a generation that had already been superseded.
	// A stale write that a later claim overwrote is not visible here, which
	// makes this count a floor rather than a total.
	if writerEpoch < shardEpoch {
		s.staleCommits++
	}
}

func parseUnsigned(field any, name string) (uint64, error) {
	text, ok := field.(string)
	if !ok || text == "" {
		return 0, fmt.Errorf("%s is missing", name)
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a number", name, text)
	}
	return value, nil
}

// SweepInterval is how often the sweep should run. It has to be short enough
// that a record cannot regress and recover between two readings.
const SweepInterval = 15 * time.Second
