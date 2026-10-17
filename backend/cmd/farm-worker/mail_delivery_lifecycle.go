package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"farmgame/internal/storage/redis/rediskey"
)

// mailDeliveryOwnedBuckets resolves the explicit shard subset assigned to
// this worker. An enabled worker fails closed when ownership is omitted.
func mailDeliveryOwnedBuckets(configured []rediskey.AuthorityBucketRef) ([]rediskey.AuthorityBucketRef, error) {
	raw := strings.TrimSpace(env("MAIL_DELIVERY_WORKER_OWNED_SHARDS", ""))
	if raw == "" {
		return nil, fmt.Errorf("MAIL_DELIVERY_WORKER_OWNED_SHARDS must be set when delivery recovery is enabled")
	}
	byShard := make(map[uint32]rediskey.AuthorityBucketRef, len(configured))
	for _, bucket := range configured {
		byShard[bucket.VirtualShard()] = bucket
	}
	parts := strings.Split(raw, ",")
	owned := make([]rediskey.AuthorityBucketRef, 0, len(parts))
	seen := make(map[uint32]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("MAIL_DELIVERY_WORKER_OWNED_SHARDS contains an empty shard")
		}
		shard64, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("MAIL_DELIVERY_WORKER_OWNED_SHARDS shard %q: %w", part, err)
		}
		shard := uint32(shard64)
		bucket, ok := byShard[shard]
		if !ok {
			return nil, fmt.Errorf("MAIL_DELIVERY_WORKER_OWNED_SHARDS shard %d is outside PLAYER Authority buckets", shard)
		}
		if _, duplicate := seen[shard]; duplicate {
			return nil, fmt.Errorf("MAIL_DELIVERY_WORKER_OWNED_SHARDS shard %d is duplicated", shard)
		}
		seen[shard] = struct{}{}
		owned = append(owned, bucket)
	}
	sort.Slice(owned, func(i, j int) bool {
		return owned[i].VirtualShard() < owned[j].VirtualShard()
	})
	return owned, nil
}
