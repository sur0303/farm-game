package actorrouting

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
)

const (
	metadataRealm       = "x-farm-actor-realm"
	metadataKind        = "x-farm-actor-kind"
	metadataID          = "x-farm-actor-id"
	metadataShard       = "x-farm-virtual-shard"
	metadataRevision    = "x-farm-shard-revision"
	metadataWriterEpoch = "x-farm-writer-epoch"
	metadataHolder      = "x-farm-holder-pod"
	metadataHolderBoot  = "x-farm-holder-boot-id"
	metadataLeaseEpoch  = "x-farm-lease-epoch"
	metadataSnapshot    = "x-farm-snapshot-revision"
)

func WithRoute(ctx context.Context, address Address, assignment Assignment) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx,
		metadataRealm, address.Realm,
		metadataKind, string(address.Kind),
		metadataID, strconv.FormatUint(address.ID, 10),
		metadataShard, strconv.FormatUint(uint64(assignment.ShardID), 10),
		metadataRevision, strconv.FormatUint(assignment.ShardRevision, 10),
		metadataWriterEpoch, strconv.FormatUint(assignment.WriterEpoch, 10),
		metadataHolder, assignment.HolderPod,
	)
	values := []string{}
	if assignment.HolderBootID != "" {
		values = append(values, metadataHolderBoot, assignment.HolderBootID)
	}
	if assignment.LeaseEpoch > 0 {
		values = append(values, metadataLeaseEpoch, strconv.FormatUint(assignment.LeaseEpoch, 10))
	}
	if assignment.SnapshotRevision > 0 {
		values = append(values, metadataSnapshot, strconv.FormatUint(assignment.SnapshotRevision, 10))
	}
	if len(values) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, values...)
	}
	return ctx
}

type RoutedContext struct {
	Address    Address
	Assignment Assignment
}

func FromIncomingContext(ctx context.Context) (RoutedContext, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return RoutedContext{}, ErrStaleRoute
	}
	one := func(key string) string {
		values := md.Get(key)
		if len(values) != 1 {
			return ""
		}
		return values[0]
	}
	id, err := strconv.ParseUint(one(metadataID), 10, 64)
	if err != nil || id == 0 {
		return RoutedContext{}, ErrStaleRoute
	}
	shard, err := strconv.ParseUint(one(metadataShard), 10, 32)
	if err != nil {
		return RoutedContext{}, ErrStaleRoute
	}
	revision, err := strconv.ParseUint(one(metadataRevision), 10, 64)
	if err != nil || revision == 0 {
		return RoutedContext{}, ErrStaleRoute
	}
	epoch, err := strconv.ParseUint(one(metadataWriterEpoch), 10, 64)
	if err != nil || epoch == 0 {
		return RoutedContext{}, ErrStaleRoute
	}
	kind := Kind(one(metadataKind))
	if kind != KindPlayer && kind != KindFarm {
		return RoutedContext{}, ErrStaleRoute
	}
	leaseEpoch := uint64(0)
	if raw := one(metadataLeaseEpoch); raw != "" {
		leaseEpoch, err = strconv.ParseUint(raw, 10, 64)
		if err != nil || leaseEpoch == 0 {
			return RoutedContext{}, ErrStaleRoute
		}
	}
	snapshotRevision := uint64(0)
	if raw := one(metadataSnapshot); raw != "" {
		snapshotRevision, err = strconv.ParseUint(raw, 10, 64)
		if err != nil || snapshotRevision == 0 {
			return RoutedContext{}, ErrStaleRoute
		}
	}
	return RoutedContext{
		Address: Address{Realm: one(metadataRealm), Kind: kind, ID: id},
		Assignment: Assignment{
			Kind: kind, ShardID: uint32(shard), HolderPod: one(metadataHolder), HolderBootID: one(metadataHolderBoot),
			ShardRevision: revision, AssignmentRevision: revision, WriterEpoch: epoch,
			LeaseEpoch: leaseEpoch, SnapshotRevision: snapshotRevision,
		},
	}, nil
}
