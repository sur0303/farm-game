// Command route-authority-fence-probe is a local formal-verification probe.
// It refuses to run unless durable Authority has already moved to the supplied
// current holder generation, then submits one old-holder Renew and requires a
// fenced result. It never scans Redis or exposes the Redis URL.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

type output struct {
	Status              string `json:"status"`
	Fenced              bool   `json:"fenced"`
	DurablePrecondition bool   `json:"durable_precondition"`
	CurrentHolderID     string `json:"current_holder_id"`
	CurrentBootID       string `json:"current_boot_id"`
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	var (
		domainValue        string
		shard              uint
		oldHolderID        string
		oldBootID          string
		assignmentRevision uint64
		writerEpoch        uint64
		leaseEpoch         uint64
		currentHolderID    string
		currentBootID      string
		realm              string
		hashVersion        uint
		hashSeed           uint64
		playerShards       uint
		farmShards         uint
	)
	flag.StringVar(&domainValue, "domain", "", "PLAYER, FARM, or FARM_WORKER")
	flag.UintVar(&shard, "shard", 0, "virtual shard")
	flag.StringVar(&oldHolderID, "old-holder-id", "", "old holder identity")
	flag.StringVar(&oldBootID, "old-boot-id", "", "old holder boot identity")
	flag.Uint64Var(
		&assignmentRevision,
		"old-assignment-revision",
		0,
		"old assignment revision",
	)
	flag.Uint64Var(&writerEpoch, "old-writer-epoch", 0, "old writer epoch")
	flag.Uint64Var(&leaseEpoch, "old-lease-epoch", 0, "old lease epoch")
	flag.StringVar(
		&currentHolderID,
		"current-holder-id",
		"",
		"expected current holder identity",
	)
	flag.StringVar(
		&currentBootID,
		"current-boot-id",
		"",
		"expected current holder boot identity",
	)
	flag.StringVar(&realm, "realm", "", "route realm")
	flag.UintVar(&hashVersion, "hash-version", 0, "route hash version")
	flag.Uint64Var(&hashSeed, "hash-seed", 0, "route hash seed")
	flag.UintVar(&playerShards, "player-shards", 0, "PLAYER shard count")
	flag.UintVar(&farmShards, "farm-shards", 0, "FARM shard count")
	flag.Parse()

	domain := actorrouting.RouteDomain(domainValue)
	if !validProbeIdentity(
		domain,
		oldHolderID,
		oldBootID,
		currentHolderID,
		currentBootID,
		assignmentRevision,
		writerEpoch,
		leaseEpoch,
	) ||
		realm == "" ||
		hashVersion != uint(actorrouting.HashVersionXXH64V2) ||
		hashSeed == 0 ||
		playerShards == 0 ||
		farmShards == 0 ||
		flag.NArg() != 0 {
		return errors.New("route-authority-fence-probe: invalid arguments")
	}
	rawURL := strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	if rawURL == "" {
		return errors.New("route-authority-fence-probe: Redis URL unavailable")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return errors.New("route-authority-fence-probe: invalid Redis URL")
	}
	client := redis.NewClient(options)
	defer client.Close()

	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: os.Getenv(
			rediskey.RedisNamespaceEnvironmentEnv,
		),
		NamespaceZone:       os.Getenv(rediskey.RedisNamespaceZoneEnv),
		PhysicalProfile:     os.Getenv(rediskey.RedisPhysicalProfileEnv),
		AuthorityLayout:     os.Getenv(rediskey.RedisAuthorityLayoutEnv),
		WorkflowLayout:      os.Getenv(rediskey.RedisWorkflowLayoutEnv),
		WorkflowHashVersion: os.Getenv(rediskey.RedisWorkflowHashEnv),
		WorkflowBucketCount: os.Getenv(rediskey.RedisWorkflowBucketCountEnv),
	})
	if err != nil {
		return fmt.Errorf("route-authority-fence-probe: keyspace: %w", err)
	}
	calculator, err := actorrouting.NewCalculator(
		realm,
		actorrouting.HashProfile{
			Version: uint32(hashVersion),
			Seed:    hashSeed,
		},
		uint32(playerShards),
		uint32(farmShards),
	)
	if err != nil {
		return fmt.Errorf("route-authority-fence-probe: calculator: %w", err)
	}
	reader, err := redisauthority.NewShardAuthorityReader(
		client,
		redisauthority.DefaultAuthorityReaderOptions(
			tuple.Profile(),
			calculator,
		),
	)
	if err != nil {
		return fmt.Errorf("route-authority-fence-probe: reader: %w", err)
	}
	store, err := redisauthority.NewStore(
		client,
		nil,
		redisauthority.Options{
			Profile:  tuple.Profile(),
			Resolver: calculator,
		},
	)
	if err != nil {
		return fmt.Errorf("route-authority-fence-probe: store: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	current, found, err := reader.ReadShardWriteAuthority(
		ctx,
		domain,
		uint32(shard),
	)
	if err != nil || !found ||
		current.State != authoritystore.ShardAuthorityActive ||
		current.HolderID != currentHolderID ||
		current.HolderBootID != currentBootID ||
		current.AssignmentRevision <= assignmentRevision ||
		current.WriterEpoch <= writerEpoch ||
		current.LeaseEpoch <= leaseEpoch {
		return errors.New(
			"route-authority-fence-probe: current Authority precondition failed",
		)
	}
	renew, err := store.RenewShardAuthority(
		ctx,
		authoritystore.RenewShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{
				Kind:       authoritystore.ShardAuthorityCallerHolder,
				InstanceID: oldHolderID,
				BootID:     oldBootID,
			},
			AuthorityLayoutVersion: tuple.Profile().AuthorityLayoutVersion,
			Domain:                 domain,
			VirtualShard:           uint32(shard),
			ExpectedFence: authoritystore.ShardWriteFence{
				ExpectedHolderID:           oldHolderID,
				ExpectedHolderBootID:       oldBootID,
				ExpectedAssignmentRevision: assignmentRevision,
				ExpectedWriterEpoch:        writerEpoch,
				ExpectedLeaseEpoch:         leaseEpoch,
			},
			RequestedTTLms: 60_000,
		},
	)
	if err != nil {
		return fmt.Errorf("route-authority-fence-probe: renew: %w", err)
	}
	fenced := fencedStatus(renew.Status)
	value := output{
		Status:              renew.Status.String(),
		Fenced:              fenced,
		DurablePrecondition: true,
		CurrentHolderID:     current.HolderID,
		CurrentBootID:       current.HolderBootID,
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		return err
	}
	if !fenced {
		return errors.New(
			"route-authority-fence-probe: old generation was not fenced",
		)
	}
	return nil
}

func validProbeIdentity(
	domain actorrouting.RouteDomain,
	oldHolderID,
	oldBootID,
	currentHolderID,
	currentBootID string,
	assignmentRevision,
	writerEpoch,
	leaseEpoch uint64,
) bool {
	return (domain == actorrouting.DomainPlayer ||
		domain == actorrouting.DomainFarm ||
		domain == actorrouting.DomainFarmWorker) &&
		oldHolderID != "" &&
		oldBootID != "" &&
		currentHolderID != "" &&
		currentBootID != "" &&
		(oldHolderID != currentHolderID || oldBootID != currentBootID) &&
		assignmentRevision > 0 &&
		writerEpoch > 0 &&
		leaseEpoch > 0
}

func fencedStatus(status authoritystore.ShardAuthorityTransitionStatus) bool {
	return status == authoritystore.ShardTransitionFenceMismatch ||
		status == authoritystore.ShardTransitionUnauthorized ||
		status == authoritystore.ShardTransitionLeaseExpired
}
