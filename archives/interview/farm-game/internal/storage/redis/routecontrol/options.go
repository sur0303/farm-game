package routecontrol

import (
	"time"

	"farmgame/internal/storage/redis/rediskey"
)

const (
	defaultOperationTimeout     = 2 * time.Second
	defaultSnapshotBytes        = 4 << 20
	defaultTransitionItems      = 1024
	defaultTransitionBytes      = 8 << 20
	defaultTransitionRecordSize = 256 << 10
	maxTransitionItemsLimit     = 1 << 20
	maxTransitionBytesLimit     = 1 << 30
	maxTransitionRecordLimit    = 256 << 20
)

// Options is shared by the snapshot and transition adapters. Transition
// methods have no Context seam, so OperationTimeout is mandatory there.
type Options struct {
	Namespace rediskey.Namespace

	OperationTimeout time.Duration
	MaxSnapshotBytes int

	MaxTransitionItems      int
	MaxTransitionBytes      int
	MaxTransitionRecordSize int

	Now func() time.Time
}

type Config = Options
type SnapshotStoreOptions = Options
type TransitionStoreOptions = Options

func DefaultOptions(namespace rediskey.Namespace) Options {
	return Options{
		Namespace:               namespace,
		OperationTimeout:        defaultOperationTimeout,
		MaxSnapshotBytes:        defaultSnapshotBytes,
		MaxTransitionItems:      defaultTransitionItems,
		MaxTransitionBytes:      defaultTransitionBytes,
		MaxTransitionRecordSize: defaultTransitionRecordSize,
		Now:                     time.Now,
	}
}

func (o Options) normalized() (Options, error) {
	if err := o.Namespace.Validate(); err != nil {
		return Options{}, ErrInvalidOptions
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = defaultOperationTimeout
	}
	if o.MaxSnapshotBytes <= 0 {
		o.MaxSnapshotBytes = defaultSnapshotBytes
	}
	if o.MaxTransitionItems <= 0 {
		o.MaxTransitionItems = defaultTransitionItems
	}
	if o.MaxTransitionBytes <= 0 {
		o.MaxTransitionBytes = defaultTransitionBytes
	}
	if o.MaxTransitionRecordSize <= 0 {
		o.MaxTransitionRecordSize = defaultTransitionRecordSize
	}
	if o.MaxTransitionRecordSize > o.MaxTransitionBytes {
		return Options{}, ErrInvalidOptions
	}
	if o.MaxTransitionItems > maxTransitionItemsLimit || o.MaxTransitionBytes > maxTransitionBytesLimit ||
		o.MaxTransitionRecordSize > maxTransitionRecordLimit {
		return Options{}, ErrInvalidOptions
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o, nil
}
