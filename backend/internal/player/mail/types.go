package mail

import "time"

// SourceType identifies the trusted origin of a system mail (35 Q35-02).
// P0 only accepts directed system mail from trusted internal callers.
type SourceType string

const (
	SourceTypeSystem    SourceType = "SYSTEM"
	SourceTypeOperation SourceType = "OPERATION"
	SourceTypeTest      SourceType = "TEST"
)

// MailEntry is the immutable authoritative mail record (35 Q35-03). After a
// successful insert, recipient, snapshots, hashes and time bounds never
// change in place; corrections require a new delivery.
type MailEntry struct {
	MailID              string
	MailVersion         uint64
	RecipientPlayerID   uint64
	DeliveryID          string
	SourceType          SourceType
	SourceID            string
	TitleSnapshot       string
	BodySnapshot        string
	Reward              RewardBundle
	TemplateVersion     string
	RewardConfigVersion string
	DeliveryPayloadHash string
	ClaimMaterialHash   string
	CreatedAtMs         int64
	VisibleAtMs         int64
	ExpireAtMs          int64
	ClaimEndAtMs        int64
	// PhysicalPurgeAtMs is a physical maintenance parameter (35 §2.3): it
	// never enters the claim material hash. GC may not run before it
	// (35A PM6).
	PhysicalPurgeAtMs int64
}

// HasAttachment reports whether the mail carries a RewardBundle.
func (e MailEntry) HasAttachment() bool { return !e.Reward.IsZero() }

// MailUserState is the per-recipient mutable state layered next to the
// immutable entry (35 Q35-03). Read, deleted and claimed projection are
// orthogonal facts; there is no collapsed UNREAD/READ/CLAIMED/EXPIRED enum.
type MailUserState struct {
	MailID      string
	ReadAtMs    int64
	DeletedAtMs int64
	// ExpiredAtMs records when the expire worker applied visibility
	// expiry. Reads also filter by trusted time without this flag.
	ExpiredAtMs            int64
	AutoEvicted            bool
	ClaimedProjection      bool
	ClaimReceiptRef        string
	ClaimMaterialHash      string
	ClaimedProjectionAtMs  int64
	ClaimProjectionVersion uint64
	StateVersion           uint64
}

// Visible reports whether the mail is currently list-visible at trusted
// time nowMs: not deleted, not auto-evicted and not expired (by worker flag
// or by time).
func (s MailUserState) Visible(entry MailEntry, nowMs int64) bool {
	if s.DeletedAtMs > 0 || s.AutoEvicted || s.ExpiredAtMs > 0 {
		return false
	}
	return entry.ExpireAtMs > nowMs
}

// DeliveryDedup is the persistent delivery identity record (35A §6.3). It
// outlives the entry and is only compressed to a tombstone, then removed
// after the source-order retry/deadletter/audit windows.
type DeliveryDedup struct {
	RecipientPlayerID     uint64
	DeliveryID            string
	DeliveryPayloadHash   string
	MailID                string
	FirstCommittedAtMs    int64
	DeliveryRetainUntilMs int64
	Tombstone             bool
}

// MutationKind identifies the mutation receipt families (35A §5).
type MutationKind string

const (
	MutationKindMarkRead MutationKind = "MARK_READ"
	MutationKindDelete   MutationKind = "DELETE"
)

// MutationReceipt is the stable stored result of a MarkRead/Delete
// mutation, keyed by the server-derived mutation digest. A lost response is
// recovered by re-entering with the same mutation identity (35A §5).
type MutationReceipt struct {
	Kind            MutationKind
	MailID          string
	MutationID      string
	ResultStatus    string
	AppliedAtMs     int64
	MailboxRevision uint64
	RetainUntilMs   int64
}

// ProjectionReceipt is the idempotency record of one ApplyMailClaimProjection
// work item (35A §4.4/§5).
type ProjectionReceipt struct {
	WorkID            string
	WorkVersion       uint64
	MailID            string
	ClaimMaterialHash string
	ResultStatus      string
	AppliedAtMs       int64
	RetainUntilMs     int64
}

// MailboxSummary is the atomic counter view maintained by every mutation
// (35 Q35-04/05). It feeds the red-point projection but never decides
// claims.
type MailboxSummary struct {
	MailboxRevision uint64
	VisibleCount    int64
	RetainedCount   int64
	RetainedBytes   int64
	UnreadCount     int64
	ClaimableCount  int64
	DeliveryRecords int64
}

// ListItem is one page entry: list views return title/time/read/attachment
// and claim summary only; the body is read via GetMailDetail (35 Q35-05).
type ListItem struct {
	MailID            string
	Title             string
	CreatedAtMs       int64
	ExpireAtMs        int64
	Read              bool
	HasAttachment     bool
	ClaimedProjection bool
}

// Page is one bounded list page. NextCursor is an opaque exclusive cursor;
// empty means the end of the mailbox.
type Page struct {
	Items      []ListItem
	NextCursor string
}

func ms(t time.Time) int64 { return t.UnixMilli() }
