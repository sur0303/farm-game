package friend

import (
	"context"
	"errors"
	"fmt"
	"time"

	friendpb "farmgame/api/gen/friend"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type FriendOpportunityNotifier interface {
	NotifyFriendOpportunityChanged(uint64, uint64, uint64, uint64) int
}

type RedisOpportunityHintBus struct {
	client          redis.UniversalClient
	channel         string
	targetedChannel string
}

func NewRedisOpportunityHintBus(client redis.UniversalClient, namespace rediskey.Namespace) (*RedisOpportunityHintBus, error) {
	if client == nil || namespace.Validate() != nil {
		return nil, fmt.Errorf("%w: opportunity hint bus", ErrInvalidCommand)
	}
	prefix := fmt.Sprintf("classic-farm:%s:%s:friend-opportunity-hint", namespace.Environment, namespace.Zone)
	return &RedisOpportunityHintBus{client: client, channel: prefix + "-v1", targetedChannel: prefix + "-targeted-v1"}, nil
}

// PublishFriendOpportunityHint publishes only invalidation metadata. It never
// publishes action_mask and is therefore incapable of authorizing a UI action.
func (b *RedisOpportunityHintBus) PublishFriendOpportunityHint(ctx context.Context, ownerID, farmVersion, opportunityRevision uint64) error {
	if b == nil || ownerID == 0 || farmVersion == 0 || opportunityRevision == 0 {
		return fmt.Errorf("%w: opportunity hint", ErrInvalidCommand)
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&friendpb.FriendOpportunityChanged{
		TargetOwnerId: ownerID, SourceFarmVersion: farmVersion, OpportunityRevision: opportunityRevision,
	})
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, b.channel, payload).Err()
}

// FanoutFriendOpportunityHint scans only the owner's bounded FriendIndex and
// notifies local connections. A stale projection can cause an extra pull hint
// but cannot enable an action; H5 re-reads canonical Pair + Facts afterward.
func FanoutFriendOpportunityHint(ctx context.Context, index FriendIndexRepository, notifier FriendOpportunityNotifier, hint *friendpb.FriendOpportunityChanged) error {
	if index == nil || notifier == nil || hint == nil || hint.TargetOwnerId == 0 || hint.OpportunityRevision == 0 {
		return fmt.Errorf("%w: opportunity hint fanout", ErrInvalidCommand)
	}
	cursor := ""
	for {
		scan, err := index.Scan(ctx, hint.TargetOwnerId, cursor, MaxPageSize*refillBudgetPages)
		if err != nil {
			return err
		}
		for _, entry := range scan.Entries {
			notifier.NotifyFriendOpportunityChanged(entry.FriendPlayerID, hint.TargetOwnerId,
				hint.SourceFarmVersion, hint.OpportunityRevision)
		}
		if !scan.HasMore || len(scan.Entries) == 0 {
			return nil
		}
		cursor = cursorAfterEntry(hint.TargetOwnerId, scan.RosterRevision, scan.Entries[len(scan.Entries)-1])
	}
}

func RunOpportunityHintSubscriber(ctx context.Context, bus *RedisOpportunityHintBus,
	handle func(context.Context, *friendpb.FriendOpportunityChanged) error, onError func(error)) {
	for ctx.Err() == nil {
		err := bus.Subscribe(ctx, handle)
		if err != nil && !errors.Is(err, context.Canceled) && onError != nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (b *RedisOpportunityHintBus) Subscribe(ctx context.Context, handle func(context.Context, *friendpb.FriendOpportunityChanged) error) error {
	if b == nil || handle == nil {
		return fmt.Errorf("%w: opportunity hint subscriber", ErrInvalidCommand)
	}
	subscription := b.client.Subscribe(ctx, b.channel)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-subscription.Channel():
			if !ok {
				return errors.New("friend: opportunity hint subscription closed")
			}
			var hint friendpb.FriendOpportunityChanged
			if err := proto.Unmarshal([]byte(message.Payload), &hint); err != nil ||
				hint.TargetOwnerId == 0 || hint.SourceFarmVersion == 0 || hint.OpportunityRevision == 0 {
				continue
			}
			if err := handle(ctx, &hint); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}
