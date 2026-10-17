package control

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

var (
	ErrPlacementAdapterInvalid = fmt.Errorf("scaleplan control adapter: invalid placement dependency")
	ErrPlacementAdapterDrift   = fmt.Errorf("scaleplan control adapter: placement snapshot drift")
)

type ShardPlacementReaderOptions struct {
	Snapshots      routecore.RouteSnapshotStore
	Holders        HolderRecordReader
	Authorities    AuthorityReader
	Domain         actorrouting.RouteDomain
	Scope          string
	WorkloadUID    string
	Now            func() time.Time
	LeaseCheckMode actorrouting.LeaseCheckMode
}

type placementReaderAdapter struct {
	snapshots      routecore.RouteSnapshotStore
	holders        HolderRecordReader
	authorities    AuthorityReader
	domain         actorrouting.RouteDomain
	scope          string
	workloadUID    string
	now            func() time.Time
	leaseCheckMode actorrouting.LeaseCheckMode
}

// NewShardPlacementReader constructs the exact placement adapter. One
// Capture* call loads one complete durable RouteSnapshot and one complete
// HolderRegistry listing; it never assembles a result from per-shard snapshot
// reads.
func NewShardPlacementReader(options ShardPlacementReaderOptions) (core.ShardPlacementReader, error) {
	if options.Snapshots == nil || options.Holders == nil || options.Authorities == nil {
		return nil, ErrPlacementAdapterInvalid
	}
	if options.Domain != actorrouting.DomainPlayer && options.Domain != actorrouting.DomainFarm {
		return nil, fmt.Errorf("%w: unsupported domain", ErrPlacementAdapterInvalid)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.LeaseCheckMode == "" {
		options.LeaseCheckMode = actorrouting.LeaseCheckSnapshot
	}
	if options.LeaseCheckMode != actorrouting.LeaseCheckSnapshot && options.LeaseCheckMode != actorrouting.LeaseCheckAuthorityDelegated {
		return nil, fmt.Errorf("%w: unsupported lease check mode", ErrPlacementAdapterInvalid)
	}
	if options.Scope == "" {
		options.Scope = "route-snapshot/" + string(options.Domain)
	}
	return &placementReaderAdapter{
		snapshots: options.Snapshots, holders: options.Holders, authorities: options.Authorities,
		domain: options.Domain, scope: options.Scope, workloadUID: options.WorkloadUID, now: options.Now,
		leaseCheckMode: options.LeaseCheckMode,
	}, nil
}

func (r *placementReaderAdapter) CaptureComplete(ctx context.Context) (CompletePlacementCapture, error) {
	if err := placementContext(ctx); err != nil {
		return CompletePlacementCapture{}, err
	}
	if r == nil || r.snapshots == nil || r.holders == nil || r.authorities == nil {
		return CompletePlacementCapture{}, ErrPlacementAdapterInvalid
	}
	stored, found, err := r.snapshots.LoadCurrent(ctx)
	if err != nil {
		return CompletePlacementCapture{}, err
	}
	if !found {
		return CompletePlacementCapture{}, fmt.Errorf("%w: route snapshot absent", ErrPlacementAdapterDrift)
	}
	if stored.Revision == 0 || stored.Snapshot.GlobalRevision != stored.Revision || stored.Realm == "" {
		return CompletePlacementCapture{}, fmt.Errorf("%w: malformed stored snapshot", ErrPlacementAdapterDrift)
	}
	domainSnapshot, err := snapshotDomain(stored.Snapshot, r.domain)
	if err != nil {
		return CompletePlacementCapture{}, err
	}
	if domainSnapshot.ShardCount == 0 || uint32(len(domainSnapshot.Routes)) != domainSnapshot.ShardCount {
		return CompletePlacementCapture{}, fmt.Errorf("%w: incomplete route table", ErrPlacementAdapterDrift)
	}
	records, err := r.holders.List(ctx, r.domain)
	if err != nil {
		return CompletePlacementCapture{}, err
	}
	recordsByGeneration := make(map[string]routecore.HolderRecord, len(records))
	holderParts := make([]string, 0, len(records))
	for _, record := range records {
		if record.Domain != r.domain {
			return CompletePlacementCapture{}, fmt.Errorf("%w: holder domain mismatch", ErrPlacementAdapterDrift)
		}
		ref, refErr := holderRefFromRecord(record)
		if refErr != nil {
			return CompletePlacementCapture{}, refErr
		}
		generationKey := record.HolderID + "\x1f" + record.BootID
		if _, duplicate := recordsByGeneration[generationKey]; duplicate {
			return CompletePlacementCapture{}, fmt.Errorf("%w: duplicate holder generation", ErrPlacementAdapterDrift)
		}
		recordsByGeneration[generationKey] = record
		holderParts = append(holderParts, holderRecordCapturePart(record, ref))
	}
	sort.Strings(holderParts)
	holderDigest := digestText("scaleplan-holder-capture-v1", holderParts...)

	authorityParts := make([]string, 0, len(domainSnapshot.Routes))
	assignmentParts := make([]string, 0, len(domainSnapshot.Routes))
	fencingParts := make([]string, 0, len(domainSnapshot.Routes))
	leaseParts := make([]string, 0, len(domainSnapshot.Routes))
	placements := make([]core.ShardPlacement, 0, len(domainSnapshot.Routes))
	activeCardinality := uint64(0)
	seenActive := make(map[core.ShardKey]struct{})
	for shard, route := range domainSnapshot.Routes {
		if route.HolderIndex < 0 || route.HolderIndex >= len(stored.Snapshot.Holders) {
			return CompletePlacementCapture{}, fmt.Errorf("%w: holder index %d", ErrPlacementAdapterDrift, route.HolderIndex)
		}
		snapshotHolder := stored.Snapshot.Holders[route.HolderIndex]
		generationKey := snapshotHolder.InstanceID + "\x1f" + snapshotHolder.BootID
		record, found := recordsByGeneration[generationKey]
		if !found {
			return CompletePlacementCapture{}, fmt.Errorf("%w: route holder absent %s", ErrPlacementAdapterDrift, snapshotHolder.InstanceID)
		}
		ref, refErr := holderRefFromRecord(record)
		if refErr != nil {
			return CompletePlacementCapture{}, refErr
		}
		if snapshotHolder.InstanceID != ref.HolderID || snapshotHolder.BootID != ref.BootID ||
			snapshotHolder.Endpoint != record.LogicEndpoint || ref.Domain != r.domain {
			return CompletePlacementCapture{}, fmt.Errorf("%w: route/holder identity mismatch", ErrPlacementAdapterDrift)
		}
		active := route.State == actorrouting.ShardStateActive
		if route.State != actorrouting.ShardStateActive && route.State != actorrouting.ShardStateDraining &&
			route.State != actorrouting.ShardStateAssigning {
			return CompletePlacementCapture{}, fmt.Errorf("%w: unknown shard route state", ErrPlacementAdapterDrift)
		}
		authorityParts = append(authorityParts, routeCapturePart(uint32(shard), route, ref))
		assignmentParts = append(assignmentParts, fmt.Sprintf("%d:%d", shard, route.ShardRevision))
		fencingParts = append(fencingParts, fmt.Sprintf("%d:%d", shard, route.ExpectedWriterEpoch))
		leaseParts = append(leaseParts, fmt.Sprintf("%d:%d:%s:%s", shard, route.ExpectedLeaseEpoch, ref.HolderID, ref.BootID))
		if !active {
			continue
		}
		activeCardinality++
		key := core.ShardKey{Domain: r.domain, VirtualShard: uint32(shard)}
		seenActive[key] = struct{}{}
		if route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 ||
			record.SnapshotRevision != stored.Revision || !record.ProcessReady || record.Draining ||
			record.State != routecore.HolderStateServing || record.LogicEndpoint == "" {
			return CompletePlacementCapture{}, fmt.Errorf("%w: active holder is not fresh", ErrPlacementAdapterDrift)
		}
		value, authorityFound, err := r.authorities.ReadShardWriteAuthority(ctx, r.domain, uint32(shard))
		if err != nil {
			return CompletePlacementCapture{}, err
		}
		if !authorityFound {
			return CompletePlacementCapture{}, fmt.Errorf("%w: authority absent for shard %d", ErrPlacementAdapterDrift, shard)
		}
		if err := validateAuthorityProjection(value, r.domain, uint32(shard), ref, route, r.leaseCheckMode, r.now()); err != nil {
			return CompletePlacementCapture{}, err
		}
		authorityParts = append(authorityParts, authorityCapturePart(value))
		leaseParts = append(leaseParts, fmt.Sprintf("%d:%d:%s:%s", shard, value.LeaseEpoch, value.HolderID, value.HolderBootID))
		placements = append(placements, core.ShardPlacement{
			Domain: r.domain, VirtualShard: uint32(shard), Active: true,
			SnapshotRevision: stored.Revision, Holder: ref, Endpoint: record.LogicEndpoint,
			ExpectedSourceFence: authoritystore.ShardWriteFence{
				ExpectedHolderID: ref.HolderID, ExpectedHolderBootID: ref.BootID,
				ExpectedAssignmentRevision: value.AssignmentRevision,
				ExpectedWriterEpoch:        value.WriterEpoch, ExpectedLeaseEpoch: value.LeaseEpoch,
			},
			KnownFresh: true,
			LeaseUntil: effectiveLeaseUntil(route.LeaseUntilMs, value.LeaseUntilMs, r.leaseCheckMode),
		})
	}
	sort.Slice(placements, func(i, j int) bool {
		return placements[i].VirtualShard < placements[j].VirtualShard
	})
	sort.Strings(authorityParts)
	sort.Strings(assignmentParts)
	sort.Strings(fencingParts)
	sort.Strings(leaseParts)
	authorityDigest := digestText("scaleplan-authority-capture-v1", authorityParts...)
	leaseDigest := digestText("scaleplan-lease-epochs-v1", leaseParts...)
	token := core.PlacementCaptureToken{
		ID:                       "pc-" + digestText("scaleplan-placement-capture-v1", stored.ChecksumSHA256, holderDigest, authorityDigest),
		Scope:                    r.scope + "/" + stored.Realm,
		WorkloadUID:              r.workloadUID,
		SnapshotRevision:         stored.Revision,
		SnapshotChecksum:         stored.ChecksumSHA256,
		TopologyFingerprint:      stored.TopologyFingerprint,
		ActiveCardinality:        activeCardinality,
		HolderRegistryRevision:   digestUint64(holderDigest),
		HolderRegistryChecksum:   holderDigest,
		AuthorityCaptureRevision: digestUint64(authorityDigest),
		LeaseEpochDigest:         leaseDigest,
		AssignmentRevision:       digestUint64(digestText("scaleplan-assignment-revisions-v1", assignmentParts...)),
		FencingEpoch:             digestUint64(digestText("scaleplan-writer-epochs-v1", fencingParts...)),
	}
	if token.ActiveCardinality != uint64(len(seenActive)) {
		return CompletePlacementCapture{}, fmt.Errorf("%w: active cardinality", ErrPlacementAdapterDrift)
	}
	return CompletePlacementCapture{Token: token, Placements: placements}, nil
}

func (r *placementReaderAdapter) CaptureShard(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (core.ShardPlacementCapture, error) {
	if domain != r.domain {
		return core.ShardPlacementCapture{}, fmt.Errorf("%w: domain mismatch", ErrPlacementAdapterInvalid)
	}
	capture, err := r.CaptureComplete(ctx)
	if err != nil {
		return core.ShardPlacementCapture{}, err
	}
	for _, placement := range capture.Placements {
		if placement.VirtualShard == shard {
			return core.ShardPlacementCapture{Token: capture.Token, Placement: placement, Found: true}, nil
		}
	}
	return core.ShardPlacementCapture{Token: capture.Token, Found: false}, nil
}

func (r *placementReaderAdapter) CaptureShards(ctx context.Context, keys []core.ShardKey) (core.ShardSetCapture, error) {
	capture, err := r.CaptureComplete(ctx)
	if err != nil {
		return core.ShardSetCapture{}, err
	}
	requested := make(map[core.ShardKey]struct{}, len(keys))
	for _, key := range keys {
		if key.Domain != r.domain {
			return core.ShardSetCapture{}, fmt.Errorf("%w: domain mismatch", ErrPlacementAdapterInvalid)
		}
		if _, duplicate := requested[key]; duplicate {
			return core.ShardSetCapture{}, fmt.Errorf("%w: duplicate shard key", ErrPlacementAdapterInvalid)
		}
		requested[key] = struct{}{}
	}
	observed := make(map[core.ShardKey]core.ShardPlacement, len(capture.Placements))
	for _, placement := range capture.Placements {
		observed[core.ShardKey{Domain: placement.Domain, VirtualShard: placement.VirtualShard}] = placement
	}
	out := make([]core.ShardPlacement, 0, len(keys))
	for _, key := range keys {
		placement, found := observed[key]
		if !found {
			return core.ShardSetCapture{}, fmt.Errorf("%w: requested active placement missing", ErrPlacementAdapterDrift)
		}
		out = append(out, placement)
	}
	return core.ShardSetCapture{Token: capture.Token, Placements: out}, nil
}

func (r *placementReaderAdapter) CaptureHolderShards(ctx context.Context, holder core.HolderRef) (core.HolderShardCapture, error) {
	if !holder.Exact() || holder.Domain != r.domain {
		return core.HolderShardCapture{}, fmt.Errorf("%w: holder mismatch", ErrPlacementAdapterInvalid)
	}
	capture, err := r.CaptureComplete(ctx)
	if err != nil {
		return core.HolderShardCapture{}, err
	}
	out := make([]core.ShardPlacement, 0)
	for _, placement := range capture.Placements {
		if placement.Holder == holder {
			out = append(out, placement)
		}
	}
	return core.HolderShardCapture{Token: capture.Token, Holder: holder, Placements: out}, nil
}

func (r *placementReaderAdapter) Revalidate(ctx context.Context, token core.PlacementCaptureToken) error {
	if err := placementContext(ctx); err != nil {
		return err
	}
	capture, err := r.CaptureComplete(ctx)
	if err != nil {
		return err
	}
	if capture.Token != token {
		return fmt.Errorf("%w: placement capture token changed", ErrPlacementAdapterDrift)
	}
	return nil
}

func snapshotDomain(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) (actorrouting.DomainSnapshot, error) {
	switch domain {
	case actorrouting.DomainPlayer:
		return snapshot.Player, nil
	case actorrouting.DomainFarm:
		return snapshot.Farm, nil
	default:
		return actorrouting.DomainSnapshot{}, fmt.Errorf("%w: domain %s", ErrPlacementAdapterInvalid, domain)
	}
}

func validateAuthorityProjection(value authoritystore.ShardWriteAuthority, domain actorrouting.RouteDomain, shard uint32, ref core.HolderRef, route actorrouting.ShardRoute, leaseCheckMode actorrouting.LeaseCheckMode, now time.Time) error {
	if value.Domain != domain || value.VirtualShard != shard || value.State != authoritystore.ShardAuthorityActive ||
		value.HolderID != ref.HolderID || value.HolderBootID != ref.BootID ||
		value.AssignmentRevision != route.ShardRevision || value.WriterEpoch != route.ExpectedWriterEpoch ||
		value.LeaseEpoch != route.ExpectedLeaseEpoch || value.AssignmentRevision == 0 ||
		value.WriterEpoch == 0 || value.LeaseEpoch == 0 || value.LeaseUntilMs <= now.UnixMilli() ||
		(leaseCheckMode == actorrouting.LeaseCheckSnapshot && route.LeaseUntilMs <= now.UnixMilli()) {
		return fmt.Errorf("%w: authority/snapshot projection mismatch", ErrPlacementAdapterDrift)
	}
	return nil
}

func effectiveLeaseUntil(routeMs, authorityMs int64, leaseCheckMode actorrouting.LeaseCheckMode) time.Time {
	if leaseCheckMode == actorrouting.LeaseCheckAuthorityDelegated {
		if authorityMs <= 0 {
			return time.Time{}
		}
		return time.UnixMilli(authorityMs)
	}
	if routeMs <= 0 {
		return time.Time{}
	}
	if authorityMs <= 0 || authorityMs < routeMs {
		return time.UnixMilli(authorityMs)
	}
	return time.UnixMilli(routeMs)
}

func holderRecordCapturePart(record routecore.HolderRecord, ref core.HolderRef) string {
	values := []string{string(record.Domain), ref.HolderID, ref.BootID, ref.PodUID, ref.WorkloadUID,
		strconv.FormatInt(int64(ref.Ordinal), 10), record.LogicEndpoint, record.HolderControlEndpoint,
		record.TLSDNSIdentity, string(record.State), strconv.FormatBool(record.ProcessReady),
		strconv.FormatBool(record.Draining), strconv.FormatUint(uint64(record.ServingShardCount), 10),
		strconv.FormatUint(record.SnapshotRevision, 10), strconv.FormatUint(uint64(record.CapacityWeight), 10)}
	keys := make([]string, 0, len(record.ResourceSummary))
	for key := range record.ResourceSummary {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values = append(values, key, record.ResourceSummary[key])
	}
	return strings.Join(values, "\x1f")
}

func routeCapturePart(shard uint32, route actorrouting.ShardRoute, ref core.HolderRef) string {
	return strings.Join([]string{strconv.FormatUint(uint64(shard), 10), string(route.State),
		strconv.FormatUint(route.ShardRevision, 10), strconv.FormatUint(route.ExpectedWriterEpoch, 10),
		strconv.FormatUint(route.ExpectedLeaseEpoch, 10), ref.HolderID, ref.BootID}, "\x1f")
}

func authorityCapturePart(value authoritystore.ShardWriteAuthority) string {
	return strings.Join([]string{string(value.Domain), strconv.FormatUint(uint64(value.VirtualShard), 10),
		value.AuthorityLayoutVersion, strconv.FormatUint(uint64(value.HashVersion), 10),
		strconv.FormatUint(value.HashSeed, 10), value.HolderID, value.HolderBootID,
		strconv.FormatUint(value.AssignmentRevision, 10), strconv.FormatUint(value.WriterEpoch, 10),
		strconv.FormatUint(value.LeaseEpoch, 10), string(value.State), value.LastTransitionID,
		strconv.FormatInt(value.LastTransitionAtMs, 10)}, "\x1f")
}

func digestText(scope string, parts ...string) string {
	h := sha256.New()
	write := func(value string) {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	write(scope)
	for _, part := range parts {
		write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func digestUint64(value string) uint64 {
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) < 8 {
		return 1
	}
	result := binary.BigEndian.Uint64(raw[:8])
	if result == 0 {
		return 1
	}
	return result
}

func placementContext(ctx context.Context) error {
	if ctx == nil {
		return ErrPlacementAdapterInvalid
	}
	return ctx.Err()
}

var _ core.ShardPlacementReader = (*placementReaderAdapter)(nil)
var _ CompletePlacementReader = (*placementReaderAdapter)(nil)
