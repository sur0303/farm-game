package capaudit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"farmgame/internal/friend"
)

// FriendIndexGate is the contract name this check answers for.
const FriendIndexGate = "friend_pair_index_unrepaired"

// DefaultFriendRepairWindow is how long a pair change gets to reach both
// players' lists before the disagreement counts as abandoned.
//
// Repair is queued before the pair commits and the worker gets a bounded
// number of rounds at it, each holding a lease and pausing between attempts.
// Once those rounds are spent the queue drops the viewer, so nothing will fix
// it later. This window covers the rounds with room to spare.
const DefaultFriendRepairWindow = 40 * time.Second

// FriendIndexCheck counts friend list rows that no longer agree with the
// canonical pair, long after the repair worker should have fixed them.
//
// The judgement is the reconciler's own, asked in read-only form, so the audit
// cannot drift from the repair it is auditing. Enumeration comes from the run's
// ledger because there is no global index of pairs, and the read path forbids
// treating the projection as truth.
type FriendIndexCheck struct {
	reconciler ViewerInspector
	ledger     Ledger
	window     time.Duration
	now        func() time.Time
}

// ViewerInspector is the reconciler's read-only face. Taking it as an
// interface keeps the judgement tested where the friend fixtures live.
type ViewerInspector interface {
	InspectViewer(ctx context.Context, viewer uint64, settledBeforeMs int64) (friend.FriendIndexInspection, error)
}

type FriendIndexConfig struct {
	Reconciler ViewerInspector
	Ledger     Ledger
	// Window is how long a pair change may stay unprojected before it counts.
	Window time.Duration
	Now    func() time.Time
}

func NewFriendIndexCheck(config FriendIndexConfig) (*FriendIndexCheck, error) {
	if config.Reconciler == nil {
		return nil, errors.New("capaudit: friend index reconciler is required")
	}
	if err := config.Ledger.validate(); err != nil {
		return nil, err
	}
	window := config.Window
	if window <= 0 {
		window = DefaultFriendRepairWindow
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &FriendIndexCheck{reconciler: config.Reconciler, ledger: config.Ledger, window: window, now: now}, nil
}

func (c *FriendIndexCheck) Gate() string { return FriendIndexGate }

func (c *FriendIndexCheck) Observe(ctx context.Context) (int64, error) {
	settledBefore := c.now().Add(-c.window).UnixMilli()
	var unrepaired int64
	err := c.ledger.Actors(func(actorID uint64) error {
		inspection, err := c.reconciler.InspectViewer(ctx, actorID, settledBefore)
		if err != nil {
			return fmt.Errorf("inspect player %d: %w", actorID, err)
		}
		unrepaired += int64(inspection.Unrepaired())
		return nil
	})
	if err != nil {
		return 0, err
	}
	return unrepaired, nil
}
