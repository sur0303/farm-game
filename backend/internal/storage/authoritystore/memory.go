package authoritystore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	commitstamp "farmgame/internal/storage/internal/commitstamp"
	"farmgame/internal/workidentity"
)

type MemoryOptions struct {
	Resolver               ShardResolver
	AuthorityLayoutVersion string
	Now                    func() time.Time
	Limits                 Limits
	LeaseLimits            LeaseLimits
}

type shardKey struct {
	domain string
	shard  uint32
}

type memoryRecord struct {
	address              ActorAddress
	writerEpoch          uint64
	writerClaimID        string
	recordVersion        uint64
	modelLayout          ModelLayoutVersion
	modelFields          ModelState
	businessStateDigest  PayloadHash
	lastClaimOperationID string
	receipts             map[string]AuthorityReceipt
	receiptBytes         int
	resolutions          map[string]ResolutionReceipt
	orders               map[string]AttachedOrder
	schedules            map[string]*memorySchedule
	pendingScheduleCount int
	pendingScheduleBytes int
	outbox               map[string]PendingOutbox
}

type memorySchedule struct {
	AttachedSchedule
	State             string
	WorkVersion       uint64
	LeaseHolderID     string
	LeaseHolderBootID string
	LeaseEpoch        uint64
	LeaseTokenHash    string
	LeaseExpireAtMs   int64
	CreatedAtMs       int64
	UpdatedAtMs       int64
	CompletedAtMs     int64
}

type shardTransitionKind uint8

const (
	shardTransitionNone shardTransitionKind = iota
	shardTransitionClaim
	shardTransitionTransfer
	shardTransitionUnassign
)

type shardTransitionIntent struct {
	kind            shardTransitionKind
	transitionID    string
	layoutVersion   string
	domain          string
	shard           uint32
	expectedFence   ShardWriteFence
	newHolderID     string
	newHolderBootID string
	requestedTTLms  int64
}

type shardAuthorityRecord struct {
	authority      ShardWriteAuthority
	lastTransition shardTransitionIntent
}

type MemoryStore struct {
	mu            sync.Mutex
	resolver      ShardResolver
	hashProfile   actorrouting.HashProfile
	layoutVersion string
	now           func() time.Time
	limits        Limits
	leaseLimits   LeaseLimits
	shards        map[shardKey]*shardAuthorityRecord
	records       map[ActorAddress]*memoryRecord
	overdue       map[ActorAddress]map[string]PayloadHash
}

var _ ActorAuthorityStore = (*MemoryStore)(nil)
var _ ModelFieldBatchReader = (*MemoryStore)(nil)
var _ ShardWriteAuthorityStore = (*MemoryStore)(nil)
var _ PhysicalCapabilitiesProvider = (*MemoryStore)(nil)
var _ PhysicalCommitUsageProjector = (*MemoryStore)(nil)

func NewMemoryStore(options MemoryOptions) (*MemoryStore, error) {
	if options.Resolver == nil || options.AuthorityLayoutVersion == "" {
		return nil, fmt.Errorf("%w: resolver and layout version required", ErrInvalidRequest)
	}
	limits := NormalizeLimits(options.Limits)
	if limits.MaxModelFields <= 0 || limits.MaxModelBytes <= 0 || limits.MaxReceipts <= 0 || limits.MaxReceiptBytes <= 0 || limits.MaxSingleReceiptBytes <= 0 || limits.MaxResolutionReceipts <= 0 || limits.MaxOrders <= 0 || limits.MaxSchedules <= 0 || limits.MaxPendingWorkBytes <= 0 || limits.MaxOutbox <= 0 || limits.MaxCommitBytes <= 0 {
		return nil, fmt.Errorf("%w: limits", ErrInvalidRequest)
	}
	leaseLimits := options.LeaseLimits
	if leaseLimits == (LeaseLimits{}) {
		leaseLimits = DefaultLeaseLimits()
	}
	if err := leaseLimits.validate(); err != nil {
		return nil, err
	}
	hashProfile := options.Resolver.Profile()
	if err := hashProfile.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return &MemoryStore{
		resolver:      options.Resolver,
		hashProfile:   hashProfile,
		layoutVersion: options.AuthorityLayoutVersion,
		now:           options.Now,
		limits:        limits,
		leaseLimits:   leaseLimits,
		shards:        make(map[shardKey]*shardAuthorityRecord),
		records:       make(map[ActorAddress]*memoryRecord),
		overdue:       make(map[ActorAddress]map[string]PayloadHash),
	}, nil
}

// BatchReadModelFields implements the bounded read-only query path without
// taking or changing a WriterClaim. The returned slice preserves actorIDs
// order, including duplicates, so callers can correlate cards cheaply.
func (s *MemoryStore) BatchReadModelFields(_ context.Context, domain actorrouting.RouteDomain, actorIDs []uint64, fieldNames []string) ([]ModelFieldReadResult, error) {
	if s == nil || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || len(actorIDs) == 0 || len(actorIDs) > 512 || len(fieldNames) == 0 || len(fieldNames) > 16 {
		return nil, fmt.Errorf("%w: model field batch shape", ErrInvalidRequest)
	}
	for _, name := range fieldNames {
		if name == "" || len(name) > 128 {
			return nil, fmt.Errorf("%w: model field name", ErrInvalidRequest)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]ModelFieldReadResult, 0, len(actorIDs))
	for _, actorID := range actorIDs {
		result := ModelFieldReadResult{ActorID: actorID, ModelFields: ModelState{}}
		if actorID == 0 {
			result.Err = fmt.Errorf("%w: actor id", ErrInvalidRequest)
			results = append(results, result)
			continue
		}
		record := s.records[ActorAddress{Domain: domain, ActorID: actorID}]
		if record == nil {
			results = append(results, result)
			continue
		}
		result.Found = true
		result.RecordVersion = record.recordVersion
		result.ModelLayout = record.modelLayout
		if record.modelLayout == ModelLayoutV2 {
			if err := fieldv2.ValidateExactFields(domain, record.modelFields); err != nil {
				result.Err = fmt.Errorf("%w: malformed v2 model fields", ErrInvalidRequest)
				results = append(results, result)
				continue
			}
		}
		for _, name := range fieldNames {
			if value, ok := record.modelFields[name]; ok {
				result.ModelFields[name] = append([]byte(nil), value...)
			}
		}
		results = append(results, result)
	}
	return results, nil
}

// PhysicalCapabilities returns the exact immutable limits enforced by this
// MemoryStore instance.
func (s *MemoryStore) PhysicalCapabilities() (PhysicalCapabilities, error) {
	if s == nil {
		return PhysicalCapabilities{}, fmt.Errorf("%w: nil memory store", ErrInvalidRequest)
	}
	caps := PhysicalCapabilities{
		MaxModelFields:        s.limits.MaxModelFields,
		MaxModelBytes:         s.limits.MaxModelBytes,
		MaxReceiptItems:       s.limits.MaxReceipts,
		MaxReceiptBytes:       s.limits.MaxReceiptBytes,
		MaxSingleReceiptBytes: s.limits.MaxSingleReceiptBytes,
		MaxCommitBytes:        s.limits.MaxCommitBytes,
	}
	return caps, caps.Validate()
}

// ProjectPhysicalCommit uses the same logical model/receipt/commit accounting
// as Commit. It is pure: aggregate usage still comes only from the versioned
// snapshot returned by LoadAndClaim/Commit.
func (s *MemoryStore) ProjectPhysicalCommit(req AuthorityCommitRequest) (PhysicalCommitUsage, error) {
	if s == nil {
		return PhysicalCommitUsage{}, fmt.Errorf("%w: nil memory store", ErrInvalidRequest)
	}
	if _, err := NormalizeAttachedSchedules(req.AttachedSchedules, req.IdempotencyKey.SubjectPlayerID, s.limits); err != nil {
		return PhysicalCommitUsage{}, err
	}
	modelBytes := 0
	for name, value := range req.CandidateState {
		modelBytes += len(name) + len(value)
	}
	receiptBytes := AuthorityReceiptSerializedBytes(req.Receipt)
	usage := PhysicalCommitUsage{
		ModelBytes:         modelBytes,
		ReceiptItems:       1,
		ReceiptBytes:       receiptBytes,
		SingleReceiptBytes: receiptBytes,
		CommitBytes:        AuthorityCommitSerializedBytes(req),
	}
	return usage, usage.Validate()
}

// SetShardWriteAuthority is the deterministic control-plane seam used by the
// in-memory implementation. Production Claim/Renew/Transfer is W8C-L and does
// not use this helper.
func (s *MemoryStore) SetShardWriteAuthority(authority ShardWriteAuthority) error {
	validDomain := authority.Domain == actorrouting.DomainPlayer ||
		authority.Domain == actorrouting.DomainFarm ||
		authority.Domain == actorrouting.DomainFarmWorker
	if s == nil || !validDomain || authority.HolderID == "" || authority.HolderBootID == "" || authority.AssignmentRevision == 0 || authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 || authority.LeaseUntilMs <= 0 || authority.State != ShardAuthorityActive {
		return fmt.Errorf("%w: shard authority", ErrInvalidRequest)
	}
	if authority.AuthorityLayoutVersion == "" {
		authority.AuthorityLayoutVersion = s.layoutVersion
	} else if authority.AuthorityLayoutVersion != s.layoutVersion {
		return fmt.Errorf("%w: shard authority layout", ErrAuthorityAddressMismatch)
	}
	if authority.HashVersion == 0 {
		authority.HashVersion = s.hashProfile.Version
	} else if authority.HashVersion != s.hashProfile.Version {
		return fmt.Errorf("%w: shard authority hash version", ErrAuthorityAddressMismatch)
	}
	if authority.HashSeed == 0 {
		authority.HashSeed = s.hashProfile.Seed
	} else if authority.HashSeed != s.hashProfile.Seed {
		return fmt.Errorf("%w: shard authority hash seed", ErrAuthorityAddressMismatch)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shards[shardKey{domain: string(authority.Domain), shard: authority.VirtualShard}] = &shardAuthorityRecord{authority: authority}
	return nil
}

func (s *MemoryStore) LoadAndClaim(ctx context.Context, req LoadAndClaimRequest) (LoadAndClaimResult, error) {
	if err := contextError(ctx); err != nil {
		return LoadAndClaimResult{}, err
	}
	group, err := s.deriveGroup(req.ActorAddress, req.AuthorityLayoutVersion)
	if err != nil {
		return LoadAndClaimResult{}, err
	}
	if err := validateStableInternalID("claim_operation_id", req.ClaimOperationID); err != nil {
		return LoadAndClaimResult{}, err
	}
	if err := validateStableInternalID("new_writer_claim_id", req.NewWriterClaimID); err != nil {
		return LoadAndClaimResult{}, err
	}
	if req.InitialModelFields == nil {
		req.InitialModelFields = ModelState{}
	}
	if req.InitialModelLayout == ModelLayoutUnknown {
		req.InitialModelLayout = ModelLayoutV1
	}
	if req.InitialModelLayout != ModelLayoutV1 && req.InitialModelLayout != ModelLayoutV2 {
		return LoadAndClaimResult{}, fmt.Errorf("%w: initial model layout", ErrInvalidRequest)
	}
	if err := validateModelState(req.InitialModelFields, s.limits); err != nil {
		return LoadAndClaimResult{}, err
	}
	if req.InitialModelLayout == ModelLayoutV2 {
		mutation, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV2, req.InitialModelFields, req.InitialModelFields, nil)
		if err != nil {
			return LoadAndClaimResult{}, err
		}
		if _, _, err := NormalizeActorModelMutationBorrowed(req.ActorAddress, req.InitialModelFields, mutation); err != nil {
			return LoadAndClaimResult{}, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	authority, ok := s.validAuthorityLocked(group, req.ShardWriteFence)
	if !ok {
		return LoadAndClaimResult{Status: LoadAndClaimWriterEpochMismatch}, nil
	}
	record := s.records[req.ActorAddress]
	if record == nil {
		record = &memoryRecord{
			address:             req.ActorAddress,
			recordVersion:       1,
			modelLayout:         req.InitialModelLayout,
			modelFields:         cloneModelState(req.InitialModelFields),
			businessStateDigest: businessStateDigest(req.InitialModelFields),
			receipts:            make(map[string]AuthorityReceipt),
			resolutions:         make(map[string]ResolutionReceipt),
			orders:              make(map[string]AttachedOrder),
			schedules:           make(map[string]*memorySchedule),
			outbox:              make(map[string]PendingOutbox),
		}
		s.records[req.ActorAddress] = record
	}

	if record.lastClaimOperationID == req.ClaimOperationID {
		if record.writerClaimID != req.NewWriterClaimID {
			return LoadAndClaimResult{Status: LoadAndClaimReconcileRequired, Snapshot: snapshotOf(record)}, nil
		}
		return LoadAndClaimResult{Status: LoadAndClaimAlreadyClaimed, Snapshot: snapshotOf(record)}, nil
	}
	if record.writerClaimID == req.NewWriterClaimID && record.writerClaimID != "" {
		return LoadAndClaimResult{Status: LoadAndClaimReconcileRequired, Snapshot: snapshotOf(record)}, nil
	}
	record.writerEpoch = authority.WriterEpoch
	record.writerClaimID = req.NewWriterClaimID
	record.lastClaimOperationID = req.ClaimOperationID
	return LoadAndClaimResult{Status: LoadAndClaimClaimed, Snapshot: snapshotOf(record)}, nil
}

func (s *MemoryStore) Commit(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false)
}

// CommitExact is the explicit source-proof path. Ordinary Commit never pays
// for request fingerprinting, stamp issuance, or an additional commit-time
// clock read.
func (s *MemoryStore) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, true)
}

func (s *MemoryStore) commit(ctx context.Context, req AuthorityCommitRequest, exact bool) (CommitResult, error) {
	if err := contextError(ctx); err != nil {
		return CommitResult{}, err
	}
	group, err := s.deriveGroup(req.ActorAddress, req.AuthorityLayoutVersion)
	if err != nil {
		return CommitResult{}, err
	}
	if err := assertGroup(group, req.AssertedAuthorityGroup); err != nil {
		return CommitResult{}, err
	}
	if err := validateCommitIdentity(req); err != nil {
		return CommitResult{}, err
	}
	var stampInput AuthorityCommitRequest
	if exact {
		// Preserve the exact caller shape across adapter-local normalization only
		// on the explicit proof path.
		stampInput = req
	}
	schedules, err := NormalizeAttachedSchedules(req.AttachedSchedules, req.IdempotencyKey.SubjectPlayerID, s.limits)
	if err != nil {
		return CommitResult{}, err
	}
	req.AttachedSchedules = schedules
	digest, err := req.IdempotencyKey.OperationDigest()
	if err != nil {
		return CommitResult{}, fmt.Errorf("%w: idempotency key: %v", ErrInvalidRequest, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.validAuthorityLocked(group, req.ShardWriteFence); !ok {
		return CommitResult{Status: CommitWriterEpochMismatch}, nil
	}
	record := s.records[req.ActorAddress]
	if record == nil {
		return CommitResult{}, ErrActorNotFound
	}
	if record.writerClaimID != req.ExpectedWriterClaimID {
		return CommitResult{Status: CommitWriterClaimMismatch, Snapshot: snapshotOf(record)}, nil
	}

	// Contract 30: receipt comparison precedes RecordVersion comparison, but
	// only after the shard and activation fences have passed. The digest only
	// locates the receipt; the stored full key and payload hash decide.
	if receipt, exists := record.receipts[digest]; exists {
		if receipt.IdempotencyKey != req.IdempotencyKey || receipt.CanonicalPayloadHash != req.CanonicalPayloadHash {
			return CommitResult{Status: CommitIdempotencyConflict, Snapshot: snapshotOf(record)}, nil
		}
		return CommitResult{Status: CommitAlreadyCommitted, Snapshot: snapshotOf(record), Outcome: cloneOutcome(receipt.Outcome)}, nil
	}
	if hash, exists := s.overdue[req.ActorAddress][digest]; exists {
		if hash != req.CanonicalPayloadHash {
			return CommitResult{Status: CommitIdempotencyConflict, Snapshot: snapshotOf(record)}, nil
		}
		return CommitResult{}, ErrRecoveryOverdue
	}
	if record.recordVersion != req.ExpectedRecordVersion {
		return CommitResult{Status: CommitRecordVersionConflict, Snapshot: snapshotOf(record)}, nil
	}
	mutation, legacyReplace, err := NormalizeActorModelMutationBorrowed(req.ActorAddress, req.CandidateState, req.ModelMutation)
	if err != nil {
		return CommitResult{}, err
	}
	if err := validateCommitMutation(req, s.limits); err != nil {
		return CommitResult{}, err
	}
	if err := ValidateActorModelTransition(record.modelFields, record.modelLayout, req.CandidateState, mutation, legacyReplace); err != nil {
		return CommitResult{}, err
	}
	scheduleCountDelta, scheduleBytesDelta, err := validateAttachedSchedulesLocked(record, req.AttachedSchedules)
	if err != nil {
		return CommitResult{}, err
	}
	receiptBytes := AuthorityReceiptSerializedBytes(req.Receipt)
	if receiptBytes > s.limits.MaxSingleReceiptBytes || record.receiptBytes+receiptBytes > s.limits.MaxReceiptBytes ||
		len(record.receipts)+1 > s.limits.MaxReceipts || len(record.orders)+countNewOrders(record, req.AttachedOrders) > s.limits.MaxOrders ||
		len(record.schedules)+countNewSchedules(record, req.AttachedSchedules) > s.limits.MaxSchedules ||
		len(record.outbox)+countNewOutbox(record, req.PendingOutbox) > s.limits.MaxOutbox ||
		record.pendingScheduleCount+scheduleCountDelta < 0 ||
		record.pendingScheduleBytes+scheduleBytesDelta < 0 ||
		record.pendingScheduleBytes+scheduleBytesDelta > s.limits.MaxPendingWorkBytes {
		return CommitResult{}, ErrCapacityExceeded
	}
	if err := validateAttachedWorkLocked(record, req.AttachedOrders, req.PendingOutbox); err != nil {
		return CommitResult{}, err
	}
	var stampRequest *commitstamp.CapturedRequest
	if exact && len(req.AttachedOrders) > 0 {
		captured, err := commitstamp.CaptureExactRequest(stampInput)
		if err != nil {
			return CommitResult{}, fmt.Errorf("%w: commit stamp capture: %v", ErrInvalidRequest, err)
		}
		stampRequest = &captured
	}

	// Everything below is one mutex-protected linearization point in the Fake:
	// state + receipt + terminal outcome + source-local work are never partial.
	// Schedules need the authority time as durable state. Order-only ordinary
	// commits do not; only the explicit proof path takes that additional read.
	var committedAtMs int64
	if stampRequest != nil || len(req.AttachedSchedules) > 0 {
		committedAtMs = trustedNow(s.now).UnixMilli()
	}
	var stamp CommitStamp
	if stampRequest != nil {
		stamp = commitstamp.IssueFirstCommitted(*stampRequest, record.recordVersion+1, committedAtMs)
		if stamp == nil {
			return CommitResult{}, fmt.Errorf("%w: invalid first-commit stamp output", ErrInvalidRequest)
		}
	}
	record.modelFields = cloneModelState(req.CandidateState)
	record.modelLayout = mutation.ResultingModelLayout
	record.businessStateDigest = businessStateDigest(req.CandidateState)
	record.recordVersion++
	record.receipts[digest] = cloneReceipt(req.Receipt)
	record.receiptBytes += receiptBytes
	for _, order := range req.AttachedOrders {
		record.orders[order.OrderID] = cloneOrder(order)
	}
	applyAttachedSchedulesLocked(record, req.AttachedSchedules, committedAtMs, scheduleCountDelta, scheduleBytesDelta)
	for _, item := range req.PendingOutbox {
		record.outbox[item.OutboxID] = cloneOutbox(item)
	}
	return CommitResult{Status: CommitCommitted, Snapshot: snapshotOf(record), Outcome: cloneOutcome(req.OperationResult),
		CommitStamp: stamp}, nil
}

func (s *MemoryStore) FenceAndClaim(ctx context.Context, req FenceAndClaimRequest) (FenceAndClaimResult, error) {
	if err := contextError(ctx); err != nil {
		return FenceAndClaimResult{}, err
	}
	group, err := s.deriveGroup(req.ActorAddress, req.AuthorityLayoutVersion)
	if err != nil {
		return FenceAndClaimResult{}, err
	}
	if err := assertGroup(group, req.AssertedAuthorityGroup); err != nil {
		return FenceAndClaimResult{}, err
	}
	if err := validateFenceRequest(req); err != nil {
		return FenceAndClaimResult{}, err
	}
	digest, err := req.OriginalIdempotencyKey.OperationDigest()
	if err != nil {
		return FenceAndClaimResult{}, fmt.Errorf("%w: original idempotency key: %v", ErrInvalidRequest, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	authority, ok := s.validAuthorityLocked(group, req.ShardWriteFence)
	if !ok {
		return FenceAndClaimResult{Status: FenceResolutionWriterEpochMismatch}, nil
	}
	record := s.records[req.ActorAddress]
	if record == nil {
		return FenceAndClaimResult{}, ErrActorNotFound
	}
	if existing, ok := record.resolutions[req.ResolveOperationID]; ok {
		if !sameResolutionRequest(existing, req) {
			return FenceAndClaimResult{Status: FenceResolutionReconcileRequired, Snapshot: snapshotOf(record)}, nil
		}
		return fenceResult(existing, record, record.receipts[digest]), nil
	}
	if record.writerClaimID != req.OldWriterClaimID {
		return FenceAndClaimResult{Status: FenceResolutionWriterClaimMismatch, Snapshot: snapshotOf(record)}, nil
	}
	if len(record.resolutions)+1 > s.limits.MaxResolutionReceipts {
		return FenceAndClaimResult{}, ErrCapacityExceeded
	}

	status := FenceResolutionReconcileRequired
	var original AuthorityReceipt
	if receipt, exists := record.receipts[digest]; exists {
		if receipt.IdempotencyKey == req.OriginalIdempotencyKey && receipt.CanonicalPayloadHash == req.CanonicalPayloadHash {
			status = FenceResolutionCommitted
			original = receipt
		}
	} else if _, overdue := s.overdue[req.ActorAddress][digest]; overdue {
		status = FenceResolutionReconcileRequired
	} else if record.recordVersion == req.BeforeRecordVersion && record.businessStateDigest == req.BeforeBusinessStateDigest {
		status = FenceResolutionNotCommitted
	}

	record.writerEpoch = authority.WriterEpoch
	record.writerClaimID = req.NewWriterClaimID
	resolution := ResolutionReceipt{
		ResolveOperationID:        req.ResolveOperationID,
		ActorAddress:              req.ActorAddress,
		OldWriterClaimID:          req.OldWriterClaimID,
		NewWriterClaimID:          req.NewWriterClaimID,
		OriginalIdempotencyKey:    req.OriginalIdempotencyKey,
		CanonicalPayloadHash:      req.CanonicalPayloadHash,
		BeforeRecordVersion:       req.BeforeRecordVersion,
		BeforeBusinessStateDigest: req.BeforeBusinessStateDigest,
		CutRecordVersion:          record.recordVersion,
		CutBusinessStateDigest:    record.businessStateDigest,
		Status:                    status,
		OriginalReceiptID:         original.ReceiptID,
	}
	record.resolutions[req.ResolveOperationID] = resolution
	return fenceResult(resolution, record, original), nil
}

func (s *MemoryStore) LookupIdempotencyOutcome(ctx context.Context, req IdempotencyLookupRequest) (IdempotencyLookupResult, error) {
	if err := contextError(ctx); err != nil {
		return IdempotencyLookupResult{}, err
	}
	if _, err := s.deriveGroup(req.ActorAddress, s.layoutVersion); err != nil {
		return IdempotencyLookupResult{}, err
	}
	digest, err := req.IdempotencyKey.OperationDigest()
	if err != nil {
		return IdempotencyLookupResult{}, fmt.Errorf("%w: idempotency key: %v", ErrInvalidRequest, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[req.ActorAddress]
	if record == nil {
		return IdempotencyLookupResult{Status: IdempotencyNotFound}, nil
	}
	receipt, ok := record.receipts[digest]
	if !ok {
		if hash, overdue := s.overdue[req.ActorAddress][digest]; overdue {
			if hash != req.CanonicalPayloadHash {
				return IdempotencyLookupResult{Status: IdempotencyHashConflict}, nil
			}
			return IdempotencyLookupResult{Status: IdempotencyRecoveryOverdue}, nil
		}
		return IdempotencyLookupResult{Status: IdempotencyNotFound}, nil
	}
	if receipt.IdempotencyKey != req.IdempotencyKey || receipt.CanonicalPayloadHash != req.CanonicalPayloadHash {
		return IdempotencyLookupResult{Status: IdempotencyHashConflict, ReceiptRef: receipt.ReceiptID}, nil
	}
	return IdempotencyLookupResult{Status: IdempotencyFoundMatch, Outcome: cloneOutcome(receipt.Outcome), ReceiptRef: receipt.ReceiptID}, nil
}

// MarkRecoveryOverdue is a test seam for the compaction/archive boundary. It
// records that absence of a hot Receipt no longer proves an operation never
// executed. Production creates this evidence through W31-B/C compaction.
func (s *MemoryStore) MarkRecoveryOverdue(address ActorAddress, key IdempotencyKey, hash PayloadHash) error {
	if _, err := s.deriveGroup(address, s.layoutVersion); err != nil {
		return err
	}
	digest, err := key.OperationDigest()
	if err != nil {
		return fmt.Errorf("%w: idempotency key", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.overdue[address] == nil {
		s.overdue[address] = make(map[string]PayloadHash)
	}
	s.overdue[address][digest] = hash
	return nil
}

func (s *MemoryStore) Inspect(address ActorAddress) (AuthoritySnapshot, bool) {
	if s == nil {
		return AuthoritySnapshot{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[address]
	if record == nil {
		return AuthoritySnapshot{}, false
	}
	return snapshotOf(record), true
}

func (s *MemoryStore) AttachedOrderCount(address ActorAddress) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record := s.records[address]; record != nil {
		return len(record.orders)
	}
	return 0
}

func (s *MemoryStore) deriveGroup(address ActorAddress, layoutVersion string) (AuthorityGroupRef, error) {
	if s == nil {
		return AuthorityGroupRef{}, fmt.Errorf("%w: nil store", ErrInvalidRequest)
	}
	if err := validateAddress(address); err != nil {
		return AuthorityGroupRef{}, err
	}
	if layoutVersion != s.layoutVersion {
		return AuthorityGroupRef{}, fmt.Errorf("%w: layout version", ErrAuthorityAddressMismatch)
	}
	shard, err := s.resolver.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		return AuthorityGroupRef{}, fmt.Errorf("%w: derive shard: %v", ErrAuthorityAddressMismatch, err)
	}
	return AuthorityGroupRef{AuthorityLayoutVersion: layoutVersion, Domain: address.Domain, ActorID: address.ActorID, VirtualShard: shard}, nil
}

func (s *MemoryStore) validAuthorityLocked(group AuthorityGroupRef, fence ShardWriteFence) (ShardWriteAuthority, bool) {
	record, ok := s.shards[shardKey{domain: string(group.Domain), shard: group.VirtualShard}]
	if !ok || record == nil {
		return ShardWriteAuthority{}, false
	}
	authority := record.authority
	if authority.AuthorityLayoutVersion != s.layoutVersion ||
		authority.HashVersion != s.hashProfile.Version ||
		authority.HashSeed != s.hashProfile.Seed {
		return ShardWriteAuthority{}, false
	}
	if authority.State != ShardAuthorityActive || authority.LeaseUntilMs <= trustedNow(s.now).UnixMilli() {
		return ShardWriteAuthority{}, false
	}
	if authority.HolderID != fence.ExpectedHolderID || authority.HolderBootID != fence.ExpectedHolderBootID || authority.AssignmentRevision != fence.ExpectedAssignmentRevision || authority.WriterEpoch != fence.ExpectedWriterEpoch || authority.LeaseEpoch != fence.ExpectedLeaseEpoch {
		return ShardWriteAuthority{}, false
	}
	return authority, true
}

func assertGroup(derived AuthorityGroupRef, asserted *AuthorityGroupRef) error {
	if asserted != nil && *asserted != derived {
		return ErrAuthorityAddressMismatch
	}
	return nil
}

func validateCommitIdentity(req AuthorityCommitRequest) error {
	if err := validateStableInternalID("commit_request_id", req.CommitRequestID); err != nil {
		return err
	}
	if err := validateStableInternalID("expected_writer_claim_id", req.ExpectedWriterClaimID); err != nil {
		return err
	}
	if req.ExpectedRecordVersion == 0 {
		return fmt.Errorf("%w: expected record version", ErrInvalidRequest)
	}
	if _, err := req.IdempotencyKey.OperationDigest(); err != nil {
		return fmt.Errorf("%w: idempotency key", ErrInvalidRequest)
	}
	if req.Receipt.ReceiptID == "" || req.Receipt.IdempotencyKey != req.IdempotencyKey || req.Receipt.CanonicalPayloadHash != req.CanonicalPayloadHash {
		return fmt.Errorf("%w: receipt does not match commit", ErrInvalidRequest)
	}
	return nil
}

func validateCommitMutation(req AuthorityCommitRequest, limits Limits) error {
	if err := validateModelState(req.CandidateState, limits); err != nil {
		return err
	}
	if req.ModelMutation != nil && (len(req.ModelMutation.ModelUpserts) > limits.MaxModelFields || len(req.ModelMutation.ModelDeletes) > limits.MaxModelFields) {
		return ErrCapacityExceeded
	}
	if err := validateOutcome(req.OperationResult); err != nil {
		return err
	}
	if !sameOutcome(req.Receipt.Outcome, req.OperationResult) {
		return fmt.Errorf("%w: receipt outcome does not match commit", ErrInvalidRequest)
	}
	seenOrders := make(map[string]struct{}, len(req.AttachedOrders))
	for _, order := range req.AttachedOrders {
		if order.OrderID == "" || order.OrderType == "" {
			return fmt.Errorf("%w: attached order", ErrInvalidRequest)
		}
		if _, exists := seenOrders[order.OrderID]; exists {
			return fmt.Errorf("%w: duplicate order", ErrInvalidRequest)
		}
		seenOrders[order.OrderID] = struct{}{}
	}
	seenOutbox := make(map[string]struct{}, len(req.PendingOutbox))
	for _, item := range req.PendingOutbox {
		if item.OutboxID == "" || item.EventType == "" {
			return fmt.Errorf("%w: pending outbox", ErrInvalidRequest)
		}
		if _, exists := seenOutbox[item.OutboxID]; exists {
			return fmt.Errorf("%w: duplicate outbox", ErrInvalidRequest)
		}
		seenOutbox[item.OutboxID] = struct{}{}
	}
	if _, err := NormalizeAttachedSchedules(req.AttachedSchedules, req.IdempotencyKey.SubjectPlayerID, limits); err != nil {
		return err
	}
	if AuthorityCommitSerializedBytes(req) > limits.MaxCommitBytes {
		return ErrCapacityExceeded
	}
	return nil
}

func validateFenceRequest(req FenceAndClaimRequest) error {
	for name, value := range map[string]string{
		"resolve_operation_id": req.ResolveOperationID,
		"old_writer_claim_id":  req.OldWriterClaimID,
		"new_writer_claim_id":  req.NewWriterClaimID,
	} {
		if err := validateStableInternalID(name, value); err != nil {
			return err
		}
	}
	if req.OldWriterClaimID == req.NewWriterClaimID || req.BeforeRecordVersion == 0 {
		return fmt.Errorf("%w: fence identity/version", ErrInvalidRequest)
	}
	if _, err := req.OriginalIdempotencyKey.OperationDigest(); err != nil {
		return fmt.Errorf("%w: original idempotency key", ErrInvalidRequest)
	}
	return nil
}

func validateAttachedWorkLocked(record *memoryRecord, orders []AttachedOrder, outbox []PendingOutbox) error {
	for _, next := range orders {
		if current, exists := record.orders[next.OrderID]; exists && !sameOrder(current, next) {
			return ErrWorkConflict
		}
	}
	for _, next := range outbox {
		if current, exists := record.outbox[next.OutboxID]; exists && !sameOutbox(current, next) {
			return ErrWorkConflict
		}
	}
	return nil
}

func (s *MemoryStore) AttachedScheduleCount(address ActorAddress) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record := s.records[address]; record != nil {
		return len(record.schedules)
	}
	return 0
}

func (s *MemoryStore) PendingScheduleUsage(address ActorAddress) (count, bytes int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record := s.records[address]; record != nil {
		return record.pendingScheduleCount, record.pendingScheduleBytes, true
	}
	return 0, 0, false
}

func (s *MemoryStore) InspectSchedule(address ActorAddress, scheduleType, scheduleID string) (AttachedScheduleSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[address]
	if record == nil {
		return AttachedScheduleSnapshot{}, false
	}
	schedule := record.schedules[scheduleMapKey(scheduleType, scheduleID)]
	if schedule == nil {
		return AttachedScheduleSnapshot{}, false
	}
	return AttachedScheduleSnapshot{
		AttachedSchedule: cloneSchedule(schedule.AttachedSchedule),
		State:            schedule.State, WorkVersion: schedule.WorkVersion,
		LeaseHolderID: schedule.LeaseHolderID, LeaseHolderBootID: schedule.LeaseHolderBootID,
		LeaseEpoch: schedule.LeaseEpoch, LeaseExpireAtMs: schedule.LeaseExpireAtMs,
		CreatedAtMs: schedule.CreatedAtMs, UpdatedAtMs: schedule.UpdatedAtMs,
		CompletedAtMs: schedule.CompletedAtMs,
	}, true
}

func (s *MemoryStore) ClaimScheduledWork(ctx context.Context, address ActorAddress, scheduleType, scheduleID, holderID, bootID, leaseToken string, requestedTTLms int64) (ScheduleWorkClaim, error) {
	if err := contextError(ctx); err != nil {
		return ScheduleWorkClaim{}, err
	}
	if err := validateAddress(address); err != nil {
		return ScheduleWorkClaim{}, err
	}
	if !validScheduleLookup(scheduleType, scheduleID) {
		return ScheduleWorkClaim{}, fmt.Errorf("%w: schedule identity", ErrInvalidRequest)
	}
	for name, value := range map[string]string{"holder_id": holderID, "boot_id": bootID, "lease_token": leaseToken} {
		if err := validateStableInternalID(name, value); err != nil {
			return ScheduleWorkClaim{}, err
		}
	}
	if requestedTTLms <= 0 {
		return ScheduleWorkClaim{}, fmt.Errorf("%w: requested_ttl_ms", ErrInvalidRequest)
	}
	requested := time.Duration(requestedTTLms) * time.Millisecond
	if requested < s.leaseLimits.MinTTL {
		requested = s.leaseLimits.MinTTL
	}
	if requested > s.leaseLimits.MaxTTL {
		requested = s.leaseLimits.MaxTTL
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[address]
	if record == nil {
		return ScheduleWorkClaim{Status: ScheduleWorkNotFound}, nil
	}
	schedule := record.schedules[scheduleMapKey(scheduleType, scheduleID)]
	if schedule == nil {
		return ScheduleWorkClaim{Status: ScheduleWorkNotFound}, nil
	}
	now := trustedNow(s.now).UnixMilli()
	if !isPendingScheduleState(schedule.State) {
		return ScheduleWorkClaim{Status: ScheduleWorkNotClaimable, Snapshot: scheduleSnapshotOf(schedule)}, nil
	}
	if schedule.LeaseExpireAtMs > now {
		return ScheduleWorkClaim{Status: ScheduleWorkLeaseHeld, Snapshot: scheduleSnapshotOf(schedule), LeaseConflictAtMs: schedule.LeaseExpireAtMs}, nil
	}
	if schedule.DueAtMs <= 0 || schedule.DueAtMs > now {
		return ScheduleWorkClaim{Status: ScheduleWorkNotDue, Snapshot: scheduleSnapshotOf(schedule), LeaseConflictAtMs: schedule.DueAtMs}, nil
	}
	schedule.LeaseEpoch++
	schedule.LeaseHolderID = holderID
	schedule.LeaseHolderBootID = bootID
	schedule.LeaseTokenHash = memoryScheduleLeaseTokenHash(leaseToken)
	schedule.LeaseExpireAtMs = now + requested.Milliseconds()
	schedule.State = "SCHEDULE_DISPATCHING"
	schedule.UpdatedAtMs = now
	return ScheduleWorkClaim{Status: ScheduleWorkClaimed, Snapshot: scheduleSnapshotOf(schedule), AcceptedTTLms: requested.Milliseconds()}, nil
}

func (s *MemoryStore) AdvanceScheduledWork(ctx context.Context, address ActorAddress, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string, nextDueAtMs int64, payload []byte, payloadHash PayloadHash) (ScheduleWorkAdvanceResult, error) {
	if err := contextError(ctx); err != nil {
		return ScheduleWorkAdvanceResult{}, err
	}
	if err := validateAddress(address); err != nil {
		return ScheduleWorkAdvanceResult{}, err
	}
	if !validScheduleLookup(scheduleType, scheduleID) || leaseEpoch == 0 || workVersion == 0 || generation == 0 || nextDueAtMs <= 0 {
		return ScheduleWorkAdvanceResult{}, fmt.Errorf("%w: schedule advance fence/due", ErrInvalidRequest)
	}
	if len(payload) > s.limits.MaxSchedulePayloadBytes || len(payload) > s.limits.MaxCommitBytes || payloadHash != PayloadHash(sha256.Sum256(payload)) {
		return ScheduleWorkAdvanceResult{}, fmt.Errorf("%w: schedule payload hash", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[address]
	if record == nil {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkNotFound}, nil
	}
	schedule := record.schedules[scheduleMapKey(scheduleType, scheduleID)]
	if schedule == nil {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkNotFound}, nil
	}
	if schedule.Generation != generation {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkStaleGeneration}, nil
	}
	if schedule.WorkVersion != workVersion {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkStaleVersion}, nil
	}
	if schedule.State == "SUPERSEDED" || schedule.State == "COMPLETED" {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkStaleGeneration}, nil
	}
	now := trustedNow(s.now).UnixMilli()
	if schedule.State != "SCHEDULE_DISPATCHING" || schedule.LeaseHolderID != holderID || schedule.LeaseHolderBootID != bootID || schedule.LeaseEpoch != leaseEpoch || schedule.LeaseTokenHash != memoryScheduleLeaseTokenHash(leaseToken) {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkFenceMismatch}, nil
	}
	if schedule.LeaseExpireAtMs <= now {
		return ScheduleWorkAdvanceResult{Status: ScheduleWorkLeaseExpired}, nil
	}
	delta := len(payload) - len(schedule.Payload)
	if record.pendingScheduleBytes+delta < 0 || record.pendingScheduleBytes+delta > s.limits.MaxPendingWorkBytes {
		return ScheduleWorkAdvanceResult{}, ErrCapacityExceeded
	}
	schedule.Payload = append([]byte(nil), payload...)
	schedule.PayloadHash = payloadHash
	schedule.DueAtMs = nextDueAtMs
	schedule.WorkVersion++
	schedule.State = "PENDING"
	schedule.LeaseHolderID, schedule.LeaseHolderBootID, schedule.LeaseTokenHash = "", "", ""
	schedule.LeaseExpireAtMs = 0
	schedule.CompletedAtMs = 0
	schedule.UpdatedAtMs = now
	record.pendingScheduleBytes += delta
	return ScheduleWorkAdvanceResult{Status: ScheduleWorkAdvanced, WorkVersion: schedule.WorkVersion, Generation: schedule.Generation, NextDueAtMs: nextDueAtMs}, nil
}

func (s *MemoryStore) CompleteScheduledWork(ctx context.Context, address ActorAddress, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string) (ScheduleWorkCompleteResult, error) {
	if err := contextError(ctx); err != nil {
		return ScheduleWorkCompleteResult{}, err
	}
	if err := validateAddress(address); err != nil {
		return ScheduleWorkCompleteResult{}, err
	}
	if !validScheduleLookup(scheduleType, scheduleID) || leaseEpoch == 0 || workVersion == 0 || generation == 0 {
		return ScheduleWorkCompleteResult{}, fmt.Errorf("%w: schedule complete fence", ErrInvalidRequest)
	}
	if err := validateStableInternalID("lease_token", leaseToken); err != nil {
		return ScheduleWorkCompleteResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[address]
	if record == nil {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkNotFound}, nil
	}
	schedule := record.schedules[scheduleMapKey(scheduleType, scheduleID)]
	if schedule == nil {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkNotFound}, nil
	}
	if schedule.Generation != generation {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkStaleGeneration}, nil
	}
	if schedule.WorkVersion != workVersion {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkStaleVersion}, nil
	}
	if schedule.State == "COMPLETED" || schedule.State == "SUPERSEDED" {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkAlreadyCompleted}, nil
	}
	now := trustedNow(s.now).UnixMilli()
	if schedule.State != "SCHEDULE_DISPATCHING" || schedule.LeaseHolderID != holderID || schedule.LeaseHolderBootID != bootID || schedule.LeaseEpoch != leaseEpoch || schedule.LeaseTokenHash != memoryScheduleLeaseTokenHash(leaseToken) {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkFenceMismatch}, nil
	}
	if schedule.LeaseExpireAtMs <= now {
		return ScheduleWorkCompleteResult{Status: ScheduleWorkLeaseExpired}, nil
	}
	if record.pendingScheduleCount <= 0 || record.pendingScheduleBytes < len(schedule.Payload) {
		return ScheduleWorkCompleteResult{}, fmt.Errorf("%w: schedule pending counters", ErrInvalidRequest)
	}
	schedule.State = "COMPLETED"
	schedule.DueAtMs = 0
	schedule.CompletedAtMs = now
	schedule.UpdatedAtMs = now
	schedule.LeaseHolderID, schedule.LeaseHolderBootID, schedule.LeaseTokenHash = "", "", ""
	schedule.LeaseExpireAtMs = 0
	record.pendingScheduleCount--
	record.pendingScheduleBytes -= len(schedule.Payload)
	return ScheduleWorkCompleteResult{Status: ScheduleWorkCompleted}, nil
}

func validScheduleLookup(scheduleType, scheduleID string) bool {
	return validScheduleType(scheduleType) && workidentity.ValidateStableOpaqueID(scheduleID) == nil
}

func memoryScheduleLeaseTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}

func scheduleSnapshotOf(schedule *memorySchedule) AttachedScheduleSnapshot {
	if schedule == nil {
		return AttachedScheduleSnapshot{}
	}
	return AttachedScheduleSnapshot{AttachedSchedule: cloneSchedule(schedule.AttachedSchedule), State: schedule.State,
		WorkVersion: schedule.WorkVersion, LeaseHolderID: schedule.LeaseHolderID, LeaseHolderBootID: schedule.LeaseHolderBootID,
		LeaseEpoch: schedule.LeaseEpoch, LeaseExpireAtMs: schedule.LeaseExpireAtMs, CreatedAtMs: schedule.CreatedAtMs,
		UpdatedAtMs: schedule.UpdatedAtMs, CompletedAtMs: schedule.CompletedAtMs}
}

func scheduleMapKey(scheduleType, scheduleID string) string {
	return scheduleType + "\x00" + scheduleID
}

func isPendingScheduleState(state string) bool {
	return state == "PENDING" || state == "SCHEDULE_DISPATCHING"
}

func validateAttachedSchedulesLocked(record *memoryRecord, schedules []AttachedSchedule) (countDelta, bytesDelta int, err error) {
	for _, next := range schedules {
		key := scheduleMapKey(next.ScheduleType, next.ScheduleID)
		current, exists := record.schedules[key]
		if !exists {
			if next.DueAtMs > 0 {
				countDelta++
				bytesDelta += len(next.Payload)
			}
			continue
		}
		if current.WorkVersion == 0 || current.Generation == 0 {
			return 0, 0, fmt.Errorf("%w: malformed schedule %s", ErrInvalidRequest, next.ScheduleID)
		}
		switch {
		case next.Generation < current.Generation:
			// A stale schedule is deliberately a no-op. It must not fence the
			// enclosing Actor candidate commit.
		case next.Generation == current.Generation:
			if !sameScheduleContent(current.AttachedSchedule, next) {
				return 0, 0, ErrScheduleConflict
			}
		default:
			if next.DueAtMs > 0 {
				if isPendingScheduleState(current.State) {
					bytesDelta += len(next.Payload) - len(current.Payload)
				} else {
					countDelta++
					bytesDelta += len(next.Payload)
				}
			} else if isPendingScheduleState(current.State) {
				countDelta--
				bytesDelta -= len(current.Payload)
			}
		}
	}
	return countDelta, bytesDelta, nil
}

func applyAttachedSchedulesLocked(record *memoryRecord, schedules []AttachedSchedule, nowMs int64, countDelta, bytesDelta int) {
	for _, next := range schedules {
		key := scheduleMapKey(next.ScheduleType, next.ScheduleID)
		current, exists := record.schedules[key]
		if exists && next.Generation <= current.Generation {
			continue
		}
		workVersion := uint64(1)
		createdAt := nowMs
		if exists {
			workVersion = current.WorkVersion + 1
			createdAt = current.CreatedAtMs
		}
		state := "SUPERSEDED"
		completedAt := nowMs
		if next.DueAtMs > 0 {
			state = "PENDING"
			completedAt = 0
		}
		record.schedules[key] = &memorySchedule{
			AttachedSchedule: cloneSchedule(next),
			State:            state,
			WorkVersion:      workVersion,
			CreatedAtMs:      createdAt,
			UpdatedAtMs:      nowMs,
			CompletedAtMs:    completedAt,
		}
	}
	record.pendingScheduleCount += countDelta
	record.pendingScheduleBytes += bytesDelta
}

func countNewOrders(record *memoryRecord, orders []AttachedOrder) int {
	count := 0
	for _, order := range orders {
		if _, exists := record.orders[order.OrderID]; !exists {
			count++
		}
	}
	return count
}

func countNewSchedules(record *memoryRecord, schedules []AttachedSchedule) int {
	count := 0
	for _, schedule := range schedules {
		if _, exists := record.schedules[scheduleMapKey(schedule.ScheduleType, schedule.ScheduleID)]; !exists {
			count++
		}
	}
	return count
}

func countNewOutbox(record *memoryRecord, outbox []PendingOutbox) int {
	count := 0
	for _, item := range outbox {
		if _, exists := record.outbox[item.OutboxID]; !exists {
			count++
		}
	}
	return count
}

func snapshotOf(record *memoryRecord) AuthoritySnapshot {
	if record == nil {
		return AuthoritySnapshot{}
	}
	return AuthoritySnapshot{
		ActorAddress:        record.address,
		WriterEpoch:         record.writerEpoch,
		WriterClaimID:       record.writerClaimID,
		RecordVersion:       record.recordVersion,
		ModelLayout:         record.modelLayout,
		BusinessStateDigest: record.businessStateDigest,
		ModelFields:         cloneModelState(record.modelFields),
		PhysicalUsage: PhysicalUsage{
			Trustworthy:   true,
			RecordVersion: record.recordVersion,
			ModelBytes:    modelStatePhysicalBytes(record.modelFields),
			ReceiptItems:  len(record.receipts),
			ReceiptBytes:  record.receiptBytes,
		},
	}
}

func modelStatePhysicalBytes(state ModelState) int {
	total := 0
	for name, value := range state {
		total += len(name) + len(value)
	}
	return total
}

func fenceResult(resolution ResolutionReceipt, record *memoryRecord, receipt AuthorityReceipt) FenceAndClaimResult {
	return FenceAndClaimResult{
		Status:     resolution.Status,
		Snapshot:   snapshotOf(record),
		Outcome:    cloneOutcome(receipt.Outcome),
		Resolution: resolution,
	}
}

func sameResolutionRequest(receipt ResolutionReceipt, req FenceAndClaimRequest) bool {
	return receipt.ResolveOperationID == req.ResolveOperationID && receipt.ActorAddress == req.ActorAddress && receipt.OldWriterClaimID == req.OldWriterClaimID && receipt.NewWriterClaimID == req.NewWriterClaimID && receipt.OriginalIdempotencyKey == req.OriginalIdempotencyKey && receipt.CanonicalPayloadHash == req.CanonicalPayloadHash && receipt.BeforeRecordVersion == req.BeforeRecordVersion && receipt.BeforeBusinessStateDigest == req.BeforeBusinessStateDigest
}

func sameOutcome(a, b StoredOperationOutcome) bool {
	return a.Status == b.Status && a.ErrorCode == b.ErrorCode && string(a.Payload) == string(b.Payload)
}

func sameOrder(a, b AttachedOrder) bool {
	return a.OrderID == b.OrderID && a.OrderType == b.OrderType && a.PayloadHash == b.PayloadHash && a.DueAtMs == b.DueAtMs && a.OperationID == b.OperationID && a.SubjectPlayerID == b.SubjectPlayerID && string(a.Payload) == string(b.Payload)
}

func sameScheduleContent(a, b AttachedSchedule) bool {
	return a.ScheduleID == b.ScheduleID && a.ScheduleType == b.ScheduleType &&
		a.Generation == b.Generation && a.DueAtMs == b.DueAtMs &&
		a.PayloadHash == b.PayloadHash && a.SubjectPlayerID == b.SubjectPlayerID && string(a.Payload) == string(b.Payload)
}

func sameOutbox(a, b PendingOutbox) bool {
	return a.OutboxID == b.OutboxID && a.EventType == b.EventType && a.DueAtMs == b.DueAtMs && string(a.Payload) == string(b.Payload)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
