package routeplan

import (
	"time"

	"farmgame/internal/storage/redis/rediskey"
)

const (
	defaultOperationTimeout = 2 * time.Second
	defaultMaxItems         = 1024
	defaultMaxBytes         = 8 << 20
	defaultMaxRecordBytes   = 256 << 10
	defaultPlanTTL          = 24 * time.Hour
	defaultAuditRetention   = 24 * time.Hour
	maxItemsLimit           = 1 << 20
	maxBytesLimit           = 1 << 30
	maxRecordBytesLimit     = 256 << 20
)

type Options struct {
	Namespace        rediskey.Namespace
	OperationTimeout time.Duration
	MaxItems         int
	MaxBytes         int
	MaxRecordBytes   int
	PlanTTL          time.Duration
	AuditRetention   time.Duration
	MaxRestartRead   int
	Now              func() time.Time
}

func DefaultOptions(namespace rediskey.Namespace) Options {
	return Options{Namespace: namespace, OperationTimeout: defaultOperationTimeout,
		MaxItems: defaultMaxItems, MaxBytes: defaultMaxBytes, MaxRecordBytes: defaultMaxRecordBytes,
		PlanTTL: defaultPlanTTL, AuditRetention: defaultAuditRetention, MaxRestartRead: defaultMaxItems, Now: time.Now}
}

type PlanStoreOptions = Options
type Config = Options

func (o Options) normalized() (Options, error) {
	if err := o.Namespace.Validate(); err != nil {
		return Options{}, ErrInvalidKey
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = defaultOperationTimeout
	}
	if o.MaxItems <= 0 {
		o.MaxItems = defaultMaxItems
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = defaultMaxBytes
	}
	if o.MaxRecordBytes <= 0 {
		o.MaxRecordBytes = defaultMaxRecordBytes
	}
	if o.PlanTTL <= 0 {
		o.PlanTTL = defaultPlanTTL
	}
	if o.AuditRetention <= 0 {
		o.AuditRetention = defaultAuditRetention
	}
	if o.MaxRestartRead <= 0 {
		o.MaxRestartRead = o.MaxItems
	}
	if o.MaxRestartRead > o.MaxItems {
		o.MaxRestartRead = o.MaxItems
	}
	if o.MaxRecordBytes > o.MaxBytes || o.MaxItems > maxItemsLimit || o.MaxBytes > maxBytesLimit || o.MaxRecordBytes > maxRecordBytesLimit ||
		o.OperationTimeout > 30*time.Second || o.PlanTTL > 7*24*time.Hour || o.AuditRetention > 38*24*time.Hour {
		return Options{}, ErrInvalidKey
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o, nil
}
