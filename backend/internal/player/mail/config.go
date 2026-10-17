package mail

import "errors"

// ErrInvalidLimits rejects an inconsistent mail limit profile.
var (
	ErrInvalidLimits           = errors.New("mail: invalid limit profile")
	ErrReceiptCapacityExceeded = errors.New("mail: receipt capacity exceeded")
)

// Limits is the versioned, configurable bound profile for PlayerMail (35
// Q35-07, 35A §6.1). PageSize and MaxVisibleCount are frozen product values;
// the retained/tombstone/scan numbers are first-round physical candidates
// that must be revised by real-distribution evidence. Every bound is fail
// closed: reaching either the item or the byte bound rejects the write.
type Limits struct {
	ProfileVersion string

	// Frozen product values (35 Q35-07).
	PageSize          int // 6
	MaxVisibleCount   int // 60
	MaxTitleBytes     int // 256
	MaxBodyBytes      int // 4 KiB
	MaxRewardKinds    int // 8
	MaxMailEntryBytes int // 8 KiB, encoded MailEntryV1 blob

	// First-round physical candidates (35A §6.1).
	MaxRetainedCount       int // 96
	MaxRetainedBytes       int // 768 KiB
	MaxDeliveryRecords     int // 256 delivery dedup + tombstone fields
	MaxLazyEvictPerInsert  int // 4
	PageScanItems          int // 24
	PageScanBytes          int // 64 KiB
	MaxMailStateBytes      int // encoded MailUserStateV1 blob bound
	MaxDeliveryRecordBytes int // encoded DeliveryDedupV1 blob bound
	MaxReceiptBytes        int // encoded single mutation/projection receipt bound
	MaxReceiptItems        int // aggregate mutation/projection receipt item bound
	MaxReceiptTotalBytes   int // aggregate mutation/projection receipt byte bound

	// Time bounds (35 Q35-07). Durations in milliseconds.
	DefaultRetentionMs int64 // 30 days
	MaxRetentionMs     int64 // 90 days
	// ClaimMaterialRetainAfterClaimEndMs is the minimum physical retention
	// of claim material after claim_end_at_ms. It must cover
	// max(client_retry, unknown_resolve, operation_result,
	// worker_reconcile, customer_service_query) windows (35 §2.7,
	// 35A §6.2). physical_purge_at_ms = claim_end_at_ms + this value.
	ClaimMaterialRetainAfterClaimEndMs int64
	// DeliveryRecordRetainMs bounds how long a DeliveryDedup/Tombstone
	// outlives its mail: source-order max retry + deadletter + customer
	// service + audit windows (35A §6.3).
	DeliveryRecordRetainMs int64
	// MutationReceiptRetainMs bounds MarkRead/Delete mutation receipts.
	MutationReceiptRetainMs int64
	// ProjectionReceiptRetainMs bounds claim-projection work receipts.
	ProjectionReceiptRetainMs int64
}

const (
	dayMs = int64(24) * 60 * 60 * 1000
)

// Scan lease TTL clamps (35A §7.2): the worker requests a TTL, the store
// decides within these bounds. Shared by every store implementation so the
// fake and the Redis adapter cannot drift.
const (
	MinScanLeaseTTLMs = int64(1000)
	MaxScanLeaseTTLMs = int64(60000)
)

// ClampScanLeaseTTL bounds a requested lease TTL.
func ClampScanLeaseTTL(requested int64) int64 {
	if requested < MinScanLeaseTTLMs {
		return MinScanLeaseTTLMs
	}
	if requested > MaxScanLeaseTTLMs {
		return MaxScanLeaseTTLMs
	}
	return requested
}

// LimitsV1 returns the frozen first-round candidate profile (35A §6.1).
func LimitsV1() Limits {
	return Limits{
		ProfileVersion:         "player-mail-limits-v1",
		PageSize:               6,
		MaxVisibleCount:        60,
		MaxTitleBytes:          256,
		MaxBodyBytes:           4 * 1024,
		MaxRewardKinds:         8,
		MaxMailEntryBytes:      8 * 1024,
		MaxRetainedCount:       96,
		MaxRetainedBytes:       768 * 1024,
		MaxDeliveryRecords:     256,
		MaxLazyEvictPerInsert:  4,
		PageScanItems:          24,
		PageScanBytes:          64 * 1024,
		MaxMailStateBytes:      1024,
		MaxDeliveryRecordBytes: 1024,
		MaxReceiptBytes:        1024,
		MaxReceiptItems:        256,
		MaxReceiptTotalBytes:   256 * 1024,

		DefaultRetentionMs:                 30 * dayMs,
		MaxRetentionMs:                     90 * dayMs,
		ClaimMaterialRetainAfterClaimEndMs: 37 * dayMs, // 30d customer service + 7d gc grace
		DeliveryRecordRetainMs:             90 * dayMs,
		MutationReceiptRetainMs:            30 * dayMs,
		ProjectionReceiptRetainMs:          30 * dayMs,
	}
}

// Validate enforces the internal consistency required by 35/35A.
func (l Limits) Validate() error {
	switch {
	case l.ProfileVersion == "":
		return ErrInvalidLimits
	case l.PageSize != 6 || l.MaxVisibleCount != 60:
		// Frozen product values; changing them is a contract revision.
		return ErrInvalidLimits
	case l.MaxTitleBytes <= 0 || l.MaxBodyBytes <= 0 || l.MaxRewardKinds <= 0:
		return ErrInvalidLimits
	case l.MaxMailEntryBytes <= 0 || l.MaxMailStateBytes <= 0 || l.MaxDeliveryRecordBytes <= 0 || l.MaxReceiptBytes <= 0:
		return ErrInvalidLimits
	case l.MaxReceiptItems <= 0 || l.MaxReceiptTotalBytes <= 0:
		return ErrInvalidLimits
	case l.MaxRetainedCount <= 0 || l.MaxRetainedBytes <= 0 || l.MaxDeliveryRecords <= 0:
		// Production profiles must keep MaxRetainedCount >= MaxVisibleCount;
		// smaller values exist so tests can exercise capacity limits.
		return ErrInvalidLimits
	case l.MaxLazyEvictPerInsert <= 0 || l.PageScanItems < l.PageSize || l.PageScanBytes <= 0:
		return ErrInvalidLimits
	case l.DefaultRetentionMs <= 0 || l.MaxRetentionMs < l.DefaultRetentionMs:
		return ErrInvalidLimits
	case l.ClaimMaterialRetainAfterClaimEndMs <= 0 || l.DeliveryRecordRetainMs <= 0:
		return ErrInvalidLimits
	case l.MutationReceiptRetainMs <= 0 || l.ProjectionReceiptRetainMs <= 0:
		return ErrInvalidLimits
	}
	return nil
}
