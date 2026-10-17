package friend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	MaxTargetedOpportunityViewers = 300
	targetedHintHeaderBytes       = 32
)

var targetedHintMagic = [4]byte{'F', 'O', 'T', '1'}

// TargetedOpportunityHint is an internal fanout envelope. It still carries
// only invalidation metadata; viewer ids are routing hints and never prove a
// friendship or authorize an action.
type TargetedOpportunityHint struct {
	TargetOwnerID       uint64
	SourceFarmVersion   uint64
	OpportunityRevision uint64
	ViewerPlayerIDs     []uint64
}

func (h TargetedOpportunityHint) validate() error {
	if h.TargetOwnerID == 0 || h.SourceFarmVersion == 0 || h.OpportunityRevision == 0 ||
		len(h.ViewerPlayerIDs) == 0 || len(h.ViewerPlayerIDs) > MaxTargetedOpportunityViewers {
		return fmt.Errorf("%w: targeted opportunity hint", ErrInvalidCommand)
	}
	previous := uint64(0)
	for _, viewer := range h.ViewerPlayerIDs {
		if viewer == 0 || viewer == h.TargetOwnerID || viewer <= previous {
			return fmt.Errorf("%w: targeted opportunity viewers", ErrInvalidCommand)
		}
		previous = viewer
	}
	return nil
}

// CollectTargetedOpportunityViewers reads one owner's FriendIndex exactly
// once in the dispatcher. A stale projection may send an unnecessary pull,
// but the subsequent query still checks canonical Pair and authority Facts.
func CollectTargetedOpportunityViewers(ctx context.Context, index FriendIndexRepository, ownerID uint64) ([]uint64, error) {
	if index == nil || ownerID == 0 {
		return nil, fmt.Errorf("%w: targeted opportunity viewer collection", ErrInvalidCommand)
	}
	viewers := make([]uint64, 0, MaxTargetedOpportunityViewers)
	seen := make(map[uint64]struct{}, MaxTargetedOpportunityViewers)
	cursor := ""
	for {
		scan, err := index.Scan(ctx, ownerID, cursor, MaxPageSize*refillBudgetPages)
		if err != nil {
			return nil, err
		}
		for _, entry := range scan.Entries {
			viewer := entry.FriendPlayerID
			if viewer == 0 || viewer == ownerID {
				continue
			}
			if _, exists := seen[viewer]; exists {
				continue
			}
			if len(viewers) == MaxTargetedOpportunityViewers {
				return nil, fmt.Errorf("%w: targeted opportunity viewer limit", ErrInvalidCommand)
			}
			seen[viewer] = struct{}{}
			viewers = append(viewers, viewer)
		}
		if !scan.HasMore || len(scan.Entries) == 0 {
			break
		}
		cursor = cursorAfterEntry(ownerID, scan.RosterRevision, scan.Entries[len(scan.Entries)-1])
	}
	sort.Slice(viewers, func(i, j int) bool { return viewers[i] < viewers[j] })
	return viewers, nil
}

func (b *RedisOpportunityHintBus) PublishTargetedOpportunityHint(ctx context.Context, hint TargetedOpportunityHint) error {
	if b == nil {
		return fmt.Errorf("%w: targeted opportunity hint bus", ErrInvalidCommand)
	}
	payload, err := marshalTargetedOpportunityHint(hint)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, b.targetedChannel, payload).Err()
}

// DispatchTargetedOpportunityHint filters a centralized viewer list against
// the current Gate's local connections through the existing notifier.
func DispatchTargetedOpportunityHint(notifier FriendOpportunityNotifier, hint TargetedOpportunityHint) error {
	if notifier == nil {
		return fmt.Errorf("%w: targeted opportunity notifier", ErrInvalidCommand)
	}
	if err := hint.validate(); err != nil {
		return err
	}
	for _, viewer := range hint.ViewerPlayerIDs {
		notifier.NotifyFriendOpportunityChanged(viewer, hint.TargetOwnerID, hint.SourceFarmVersion, hint.OpportunityRevision)
	}
	return nil
}

func RunTargetedOpportunityHintSubscriber(ctx context.Context, bus *RedisOpportunityHintBus,
	handle func(context.Context, TargetedOpportunityHint) error, onError func(error)) {
	for ctx.Err() == nil {
		err := bus.SubscribeTargeted(ctx, handle)
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

func (b *RedisOpportunityHintBus) SubscribeTargeted(ctx context.Context, handle func(context.Context, TargetedOpportunityHint) error) error {
	if b == nil || handle == nil {
		return fmt.Errorf("%w: targeted opportunity hint subscriber", ErrInvalidCommand)
	}
	subscription := b.client.Subscribe(ctx, b.targetedChannel)
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
				return errors.New("friend: targeted opportunity hint subscription closed")
			}
			hint, err := unmarshalTargetedOpportunityHint([]byte(message.Payload))
			if err != nil {
				continue
			}
			if err := handle(ctx, hint); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

func marshalTargetedOpportunityHint(hint TargetedOpportunityHint) ([]byte, error) {
	if err := hint.validate(); err != nil {
		return nil, err
	}
	payload := make([]byte, targetedHintHeaderBytes+8*len(hint.ViewerPlayerIDs))
	copy(payload[:4], targetedHintMagic[:])
	binary.BigEndian.PutUint64(payload[4:12], hint.TargetOwnerID)
	binary.BigEndian.PutUint64(payload[12:20], hint.SourceFarmVersion)
	binary.BigEndian.PutUint64(payload[20:28], hint.OpportunityRevision)
	binary.BigEndian.PutUint32(payload[28:32], uint32(len(hint.ViewerPlayerIDs)))
	for i, viewer := range hint.ViewerPlayerIDs {
		binary.BigEndian.PutUint64(payload[targetedHintHeaderBytes+i*8:], viewer)
	}
	return payload, nil
}

func unmarshalTargetedOpportunityHint(payload []byte) (TargetedOpportunityHint, error) {
	if len(payload) < targetedHintHeaderBytes || string(payload[:4]) != string(targetedHintMagic[:]) {
		return TargetedOpportunityHint{}, errors.New("friend: targeted opportunity hint encoding is invalid")
	}
	count := binary.BigEndian.Uint32(payload[28:32])
	if count == 0 || count > MaxTargetedOpportunityViewers || len(payload) != targetedHintHeaderBytes+int(count)*8 {
		return TargetedOpportunityHint{}, errors.New("friend: targeted opportunity hint count is invalid")
	}
	hint := TargetedOpportunityHint{
		TargetOwnerID: binary.BigEndian.Uint64(payload[4:12]), SourceFarmVersion: binary.BigEndian.Uint64(payload[12:20]),
		OpportunityRevision: binary.BigEndian.Uint64(payload[20:28]),
		ViewerPlayerIDs:     make([]uint64, count),
	}
	for i := range hint.ViewerPlayerIDs {
		hint.ViewerPlayerIDs[i] = binary.BigEndian.Uint64(payload[targetedHintHeaderBytes+i*8:])
	}
	return hint, hint.validate()
}
