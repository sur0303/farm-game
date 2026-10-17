package capstaticroute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/actorrouting/authority"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

type Initializer struct {
	Store  authoritystore.ShardWriteAuthorityStore
	Reader routeauthority.AuthorityReader
	Config Config
}

type Result struct {
	Snapshot    actorrouting.RouteSnapshot
	Receipt     Receipt
	Ledger      Ledger
	Bytes       []byte
	LedgerBytes []byte
}

type shardKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

func (i *Initializer) Run(ctx context.Context) (Result, error) {
	if i == nil || i.Store == nil || i.Reader == nil || ctx == nil {
		return Result{}, ErrInvalidConfig
	}
	cfg := i.Config
	if err := cfg.Validate(); err != nil {
		return Result{Receipt: failureReceipt(cfg, "INVALID_CONFIG")}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{Receipt: failureReceipt(cfg, "CONTEXT_CANCELLED")}, err
	}
	now := cfg.clock()
	initial, existing, counters, err := i.buildInitial(ctx, now())
	if err != nil {
		return Result{Receipt: failureReceipt(cfg, classifyFailure(err))}, err
	}
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(initial, 16)
	if err != nil {
		return Result{Receipt: failureReceipt(cfg, "INVALID_INITIAL_SNAPSHOT")}, err
	}
	manager, err := routeauthority.NewRouteAssignmentManager(routeauthority.ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: i.Store, Publisher: publisher, AuthorityReader: i.Reader, InitialSnapshot: initial,
		AuthorityLayoutVersion: cfg.Profile.AuthorityLayoutVersion,
		RouteManagerID:         cfg.RouteManagerID, RouteManagerBootID: cfg.RouteManagerBootID,
		Now: now, MemoLimit: int(cfg.ShardCount)*2 + 64,
		MemoRetention: time.Hour, TombstoneStore: routeauthority.NewMemoryTransitionTombstoneStore(int(cfg.ShardCount)*2 + 64),
		InitializationContext: ctx,
	})
	if err != nil {
		return Result{Receipt: failureReceipt(cfg, "MANAGER_INITIALIZATION_FAILED")}, err
	}

	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		for shard := uint32(0); shard < cfg.ShardCount; shard++ {
			key := shardKey{domain: domain, shard: shard}
			if existing[key] {
				continue
			}
			holder, err := cfg.holderFor(domain, shard)
			if err != nil {
				return Result{Receipt: failureReceiptWithCounters(cfg, "PLACEMENT_FAILED", counters)}, err
			}
			result, err := manager.Claim(ctx, routeauthority.ClaimRequest{
				TransitionID: cfg.TransitionID(domain, shard), Domain: domain, VirtualShard: shard,
				NewHolderID: holder.ID, NewHolderBootID: holder.BootID, Endpoint: holder.Endpoint, RequestedTTL: cfg.LeaseTTL,
			})
			if err != nil {
				return Result{Receipt: failureReceiptWithCounters(cfg, classifyFailure(err), counters)}, fmt.Errorf("claim %s/%d: %w", domain, shard, err)
			}
			switch result.Status {
			case authoritystore.ShardTransitionApplied:
				counters.ClaimApplied++
			case authoritystore.ShardTransitionAlreadyApplied:
				counters.ClaimReplay++
			default:
				return Result{Receipt: failureReceiptWithCounters(cfg, "CLAIM_NOT_COMMITTED", counters)}, fmt.Errorf("%w: claim %s/%d status=%s", ErrIncomplete, domain, shard, result.Status)
			}
		}
	}

	// Refresh every exact generation after all claims. This prevents a large
	// 1024/1024 initialization from consuming the first shards' startup budget.
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		for shard := uint32(0); shard < cfg.ShardCount; shard++ {
			snapshot := manager.CurrentSnapshot()
			route := domainSnapshot(snapshot, domain).Routes[shard]
			holder := snapshot.Holders[route.HolderIndex]
			fence := authoritystore.ShardWriteFence{
				ExpectedHolderID: holder.InstanceID, ExpectedHolderBootID: holder.BootID,
				ExpectedAssignmentRevision: route.ShardRevision, ExpectedWriterEpoch: route.ExpectedWriterEpoch,
				ExpectedLeaseEpoch: route.ExpectedLeaseEpoch,
			}
			result, err := manager.Renew(ctx, routeauthority.RenewRequest{
				Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: holder.InstanceID, BootID: holder.BootID},
				Domain: domain, VirtualShard: shard, ExpectedFence: fence, RequestedTTL: cfg.LeaseTTL,
			})
			if err != nil {
				return Result{Receipt: failureReceiptWithCounters(cfg, classifyFailure(err), counters)}, fmt.Errorf("renew %s/%d: %w", domain, shard, err)
			}
			switch result.Status {
			case authoritystore.ShardTransitionApplied:
				counters.RenewApplied++
			case authoritystore.ShardTransitionAlreadyApplied:
				counters.RenewReplay++
			default:
				return Result{Receipt: failureReceiptWithCounters(cfg, "RENEW_NOT_COMMITTED", counters)}, fmt.Errorf("%w: renew %s/%d status=%s", ErrIncomplete, domain, shard, result.Status)
			}
		}
	}

	final := provider.CurrentSnapshot()
	minimum, maximum, records, err := i.verifyFinal(ctx, final, now(), &counters)
	if err != nil {
		return Result{Receipt: failureReceiptWithCounters(cfg, classifyFailure(err), counters)}, err
	}
	managerWorkingRevision := final.GlobalRevision
	// The Manager's per-shard bootstrap publications are an internal working
	// sequence. The first durable control-plane snapshot always starts at
	// GlobalRevision=1; later revisions belong only to the sole live Manager.
	final.GlobalRevision = 1
	bytes, hash, err := MarshalSnapshot(final)
	if err != nil {
		return Result{Receipt: failureReceiptWithCounters(cfg, "SNAPSHOT_MARSHAL_FAILED", counters)}, err
	}
	ledger := Ledger{
		SchemaVersion: LedgerSchema, RunID: cfg.RunID, ProfileID: cfg.ProfileID, Generation: cfg.Generation,
		RouteSnapshotSHA256: hash, ExpectedRecordCount: int(cfg.ShardCount) * 2, Records: records,
		Complete: true, BootstrapMaterializable: true, Activatable: false,
		CompanyRedisContacted: false, ActiveBindingAuthorized: false,
		CapacityConstantEligible: false, Supports30mDAU: false, BindingScope: "LOCAL_FUNCTIONAL_ONLY",
	}
	ledgerBytes, ledgerHash, err := MarshalLedger(ledger)
	if err != nil {
		return Result{Receipt: failureReceiptWithCounters(cfg, "LEDGER_MARSHAL_FAILED", counters)}, err
	}
	receipt := successReceipt(cfg, counters, minimum, maximum, hash, ledgerHash, managerWorkingRevision)
	return Result{Snapshot: final, Receipt: receipt, Ledger: ledger, Bytes: bytes, LedgerBytes: ledgerBytes}, nil
}

func (i *Initializer) buildInitial(ctx context.Context, current time.Time) (actorrouting.RouteSnapshot, map[shardKey]bool, Counters, error) {
	cfg := i.Config
	holders := make([]actorrouting.HolderRef, len(cfg.Holders))
	for index, holder := range cfg.Holders {
		holders[index] = actorrouting.HolderRef{InstanceID: holder.ID, BootID: holder.BootID, Endpoint: holder.Endpoint, State: actorrouting.HolderStateActive, Weight: 1}
	}
	player := actorrouting.DomainSnapshot{ShardCount: cfg.ShardCount, Routes: make([]actorrouting.ShardRoute, cfg.ShardCount)}
	farm := actorrouting.DomainSnapshot{ShardCount: cfg.ShardCount, Routes: make([]actorrouting.ShardRoute, cfg.ShardCount)}
	existing := make(map[shardKey]bool, int(cfg.ShardCount)*2)
	var counters Counters
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		for shard := uint32(0); shard < cfg.ShardCount; shard++ {
			index, err := cfg.HolderIndex(domain, shard)
			if err != nil {
				return actorrouting.RouteSnapshot{}, nil, counters, err
			}
			route := actorrouting.ShardRoute{HolderIndex: index, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}
			authority, found, err := i.Reader.ReadShardWriteAuthority(ctx, domain, shard)
			if err != nil {
				return actorrouting.RouteSnapshot{}, nil, counters, fmt.Errorf("pre-read %s/%d: %w", domain, shard, err)
			}
			if found {
				holder := cfg.Holders[index]
				if !exactExisting(cfg, domain, shard, holder, authority) {
					return actorrouting.RouteSnapshot{}, nil, counters, fmt.Errorf("%w: %s/%d", ErrIdentityConflict, domain, shard)
				}
				if authority.LeaseUntilMs <= current.UnixMilli() {
					return actorrouting.RouteSnapshot{}, nil, counters, fmt.Errorf("%w: expired existing lease %s/%d", ErrIncomplete, domain, shard)
				}
				route.State = actorrouting.ShardStateActive
				route.ShardRevision = authority.AssignmentRevision
				route.ExpectedWriterEpoch = authority.WriterEpoch
				route.ExpectedLeaseEpoch = authority.LeaseEpoch
				route.LeaseUntilMs = authority.LeaseUntilMs
				existing[shardKey{domain: domain, shard: shard}] = true
				counters.ExistingExact++
			}
			if domain == actorrouting.DomainPlayer {
				player.Routes[shard] = route
			} else {
				farm.Routes[shard] = route
			}
		}
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: cfg.Realm, GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: cfg.HashSeed,
		Player: player, Farm: farm, Holders: holders,
	}
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return actorrouting.RouteSnapshot{}, nil, counters, err
	}
	return snapshot, existing, counters, nil
}

func (i *Initializer) verifyFinal(ctx context.Context, snapshot actorrouting.RouteSnapshot, current time.Time, counters *Counters) (int64, int64, []LedgerRecord, error) {
	cfg := i.Config
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return 0, 0, nil, err
	}
	calculator, err := cfg.Calculator()
	if err != nil {
		return 0, 0, nil, err
	}
	keys, err := rediskey.NewAuthorityKeys(cfg.Profile, calculator)
	if err != nil {
		return 0, 0, nil, err
	}
	exactReader, ok := i.Reader.(interface {
		ReadShardWriteAuthorityObservation(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthorityObservation, bool, error)
	})
	if !ok {
		return 0, 0, nil, fmt.Errorf("%w: exact authority reader required", ErrInvalidConfig)
	}
	minimum := int64(math.MaxInt64)
	maximum := int64(0)
	records := make([]LedgerRecord, 0, int(cfg.ShardCount)*2)
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		domainRoutes := domainSnapshot(snapshot, domain)
		for shard := uint32(0); shard < cfg.ShardCount; shard++ {
			route := domainRoutes.Routes[shard]
			holder := snapshot.Holders[route.HolderIndex]
			observation, found, err := exactReader.ReadShardWriteAuthorityObservation(ctx, domain, shard)
			if err != nil || !found {
				if err == nil {
					err = ErrIncomplete
				}
				return 0, 0, nil, fmt.Errorf("verify %s/%d: %w", domain, shard, err)
			}
			authority := observation.Authority
			if authority.HolderID != holder.InstanceID || authority.HolderBootID != holder.BootID ||
				authority.AssignmentRevision != route.ShardRevision || authority.WriterEpoch != route.ExpectedWriterEpoch ||
				authority.LeaseEpoch != route.ExpectedLeaseEpoch || authority.LeaseUntilMs != route.LeaseUntilMs ||
				authority.State != authoritystore.ShardAuthorityActive || authority.LastTransitionID != cfg.TransitionID(domain, shard) {
				return 0, 0, nil, fmt.Errorf("%w: final %s/%d", ErrIdentityConflict, domain, shard)
			}
			if route.LeaseUntilMs-current.UnixMilli() < cfg.MinimumRemainingLease.Milliseconds() {
				return 0, 0, nil, fmt.Errorf("%w: startup lease budget %s/%d", ErrIncomplete, domain, shard)
			}
			if route.LeaseUntilMs < minimum {
				minimum = route.LeaseUntilMs
			}
			if route.LeaseUntilMs > maximum {
				maximum = route.LeaseUntilMs
			}
			bucket, err := cfg.Profile.AuthorityBucketForShard(calculator, domain, shard)
			if err != nil {
				return 0, 0, nil, err
			}
			redisKey, err := keys.ShardAuthority(bucket)
			if err != nil {
				return 0, 0, nil, err
			}
			record := LedgerRecord{
				Domain: domain, VirtualShard: shard, RedisKey: redisKey.String(),
				AuthorityLayoutVersion: authority.AuthorityLayoutVersion, HashVersion: authority.HashVersion, HashSeed: authority.HashSeed,
				HolderID: authority.HolderID, HolderBootID: authority.HolderBootID, AssignmentRevision: authority.AssignmentRevision,
				WriterEpoch: authority.WriterEpoch, LeaseEpoch: authority.LeaseEpoch, LeaseState: "ACTIVE", LeaseUntilMs: authority.LeaseUntilMs,
				TransitionID: authority.LastTransitionID, TransitionAtMs: authority.LastTransitionAtMs, TransitionDigest: observation.LastTransitionDigest,
			}
			record.RecordSHA256, err = ledgerRecordHash(record)
			if err != nil {
				return 0, 0, nil, err
			}
			records = append(records, record)
			counters.Verified++
		}
	}
	if counters.Verified != int(cfg.ShardCount)*2 || len(records) != int(cfg.ShardCount)*2 {
		return 0, 0, nil, ErrIncomplete
	}
	return minimum, maximum, records, nil
}

func exactExisting(cfg Config, domain actorrouting.RouteDomain, shard uint32, holder Holder, authority authoritystore.ShardWriteAuthority) bool {
	return authority.AuthorityLayoutVersion == cfg.Profile.AuthorityLayoutVersion && authority.HashVersion == actorrouting.HashVersionXXH64V2 &&
		authority.HashSeed == cfg.HashSeed && authority.Domain == domain && authority.VirtualShard == shard &&
		authority.HolderID == holder.ID && authority.HolderBootID == holder.BootID && authority.State == authoritystore.ShardAuthorityActive &&
		authority.AssignmentRevision > 0 && authority.WriterEpoch > 0 && authority.LeaseEpoch > 0 &&
		authority.LastTransitionID == cfg.TransitionID(domain, shard)
}

func (c Config) holderFor(domain actorrouting.RouteDomain, shard uint32) (Holder, error) {
	index, err := c.HolderIndex(domain, shard)
	if err != nil {
		return Holder{}, err
	}
	return c.Holders[index], nil
}

func domainSnapshot(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) actorrouting.DomainSnapshot {
	if domain == actorrouting.DomainPlayer {
		return snapshot.Player
	}
	return snapshot.Farm
}

func MarshalSnapshot(snapshot actorrouting.RouteSnapshot) ([]byte, string, error) {
	bytes, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return nil, "", err
	}
	bytes = append(bytes, '\n')
	digest := sha256.Sum256(bytes)
	return bytes, hex.EncodeToString(digest[:]), nil
}

func MarshalLedger(ledger Ledger) ([]byte, string, error) {
	bytes, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return nil, "", err
	}
	bytes = append(bytes, '\n')
	digest := sha256.Sum256(bytes)
	return bytes, hex.EncodeToString(digest[:]), nil
}

func ledgerRecordHash(record LedgerRecord) (string, error) {
	record.RecordSHA256 = ""
	bytes, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(bytes)
	return hex.EncodeToString(digest[:]), nil
}

func successReceipt(cfg Config, counters Counters, minimum, maximum int64, snapshotHash, ledgerHash string, managerWorkingRevision uint64) Receipt {
	nonceDigest := sha256.Sum256([]byte(cfg.ControllerNonce))
	holders := append([]Holder(nil), cfg.Holders...)
	return Receipt{
		SchemaVersion: ReceiptSchema, Status: "INITIALIZED_LOCAL_PROFILE_B_NOT_QUALIFIED",
		RunID: cfg.RunID, ProfileID: cfg.ProfileID, Generation: cfg.Generation, ManagerWorkingRevision: managerWorkingRevision,
		ControllerNonceSHA256: hex.EncodeToString(nonceDigest[:]),
		Realm:                 cfg.Realm, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: cfg.HashSeed,
		PlayerShardCount: cfg.ShardCount, FarmShardCount: cfg.ShardCount, Holders: holders, Counters: counters,
		RequestedTTLms: cfg.LeaseTTL.Milliseconds(), MinimumRemainingLeaseMs: cfg.MinimumRemainingLease.Milliseconds(),
		MinimumLeaseUntilMs: minimum, MaximumLeaseUntilMs: maximum, RouteSnapshotSHA256: snapshotHash, ExactLedgerSHA256: ledgerHash,
		NamespaceSHA256: sha256Text(cfg.Profile.Namespace.Environment + "/" + cfg.Profile.Namespace.Zone),
		Complete:        true, BootstrapMaterializable: true, Activatable: false,
		CompanyRedisContacted: false, ActiveBindingAuthorized: false,
		CapacityConstantEligible: false, Supports30mDAU: false, BindingScope: "LOCAL_FUNCTIONAL_ONLY", FormalL2Authorized: false,
	}
}

func FailureReceipt(cfg Config, class string) Receipt {
	return failureReceipt(cfg, class)
}

func ClassifyFailure(err error) string {
	return classifyFailure(err)
}

func failureReceipt(cfg Config, class string) Receipt {
	return failureReceiptWithCounters(cfg, class, Counters{})
}

func failureReceiptWithCounters(cfg Config, class string, counters Counters) Receipt {
	nonce := ""
	if cfg.ControllerNonce != "" {
		nonce = sha256Text(cfg.ControllerNonce)
	}
	return Receipt{
		SchemaVersion: ReceiptSchema, Status: "REJECTED_NOT_PUBLISHED", RunID: cfg.RunID, ProfileID: cfg.ProfileID,
		Generation: cfg.Generation, ControllerNonceSHA256: nonce, Realm: cfg.Realm,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: cfg.HashSeed,
		PlayerShardCount: cfg.ShardCount, FarmShardCount: cfg.ShardCount, Holders: append([]Holder(nil), cfg.Holders...),
		Counters: counters, RequestedTTLms: cfg.LeaseTTL.Milliseconds(), MinimumRemainingLeaseMs: cfg.MinimumRemainingLease.Milliseconds(),
		NamespaceSHA256: sha256Text(cfg.Profile.Namespace.Environment + "/" + cfg.Profile.Namespace.Zone),
		Complete:        false, BootstrapMaterializable: false, Activatable: false,
		CompanyRedisContacted: false, ActiveBindingAuthorized: false,
		CapacityConstantEligible: false, Supports30mDAU: false, BindingScope: "LOCAL_FUNCTIONAL_ONLY", FormalL2Authorized: false, FailureClass: class,
	}
}

func classifyFailure(err error) string {
	switch {
	case err == nil:
		return "UNKNOWN"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "CONTEXT_CANCELLED"
	case errors.Is(err, routeauthority.ErrAuthorityCommitUnknown):
		return "AUTHORITY_COMMIT_UNKNOWN"
	case errors.Is(err, ErrIdentityConflict):
		return "IDENTITY_CONFLICT"
	case errors.Is(err, redisauthority.ErrAuthorityReaderCorrupt):
		return "CORRUPT_AUTHORITY"
	case errors.Is(err, ErrIncomplete):
		return "INCOMPLETE"
	default:
		return "INITIALIZATION_FAILED"
	}
}
