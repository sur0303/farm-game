package mail

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is the in-memory reference implementation of the three
// PlayerMail ports. It mirrors the Lua scripts' semantics decision-for-
// decision so domain tests and the Redis adapter tests assert identical
// behavior (35A §10). It is safe for concurrent use.
type MemoryStore struct {
	mu      sync.Mutex
	clock   func() int64
	limits  Limits
	boxes   map[uint64]*memoryBox
	buckets map[uint32]*memoryBucket
	mappers *bucketMapper
	faults  []FaultKind
}

// FaultKind simulates transport outcomes around an otherwise atomic apply.
type FaultKind int

const (
	// FaultBeforeApply returns TIMEOUT_UNKNOWN without applying anything.
	FaultBeforeApply FaultKind = iota + 1
	// FaultAfterApply applies the mutation and then returns
	// TIMEOUT_UNKNOWN, simulating a lost response.
	FaultAfterApply
)

type memoryBox struct {
	entries     map[string]MailEntry
	entryBytes  map[string]int
	states      map[string]MailUserState
	stateBytes  map[string]int
	dedup       map[string]DeliveryDedup
	mutations   map[string]MutationReceipt
	projections map[string]ProjectionReceipt
	summary     MailboxSummary
	index       []string // sort members, lex ascending (newest first)
	safeEvict   []string // sort members, lex ascending
}

type memoryBucket struct {
	expireDue       map[string]int64
	gcDue           map[string]int64
	scanLeaseOwner  string
	scanLeaseExpire int64
	scanLeaseEpoch  uint64
}

// NewMemoryStore builds a fake with the given trusted clock and limits.
func NewMemoryStore(clock func() int64, limits Limits) (*MemoryStore, error) {
	if clock == nil {
		return nil, ErrInvalidRequest
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &MemoryStore{
		clock:   clock,
		limits:  limits,
		boxes:   make(map[uint64]*memoryBox),
		buckets: make(map[uint32]*memoryBucket),
	}, nil
}

// InjectFault queues one fault consumed by the next mutating call (FIFO).
func (s *MemoryStore) InjectFault(kind FaultKind) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, kind)
}

func (s *MemoryStore) takeFault() (FaultKind, bool) {
	if len(s.faults) == 0 {
		return 0, false
	}
	k := s.faults[0]
	s.faults = s.faults[1:]
	return k, true
}

func (s *MemoryStore) box(playerID uint64) *memoryBox {
	b, ok := s.boxes[playerID]
	if !ok {
		b = &memoryBox{
			entries:     make(map[string]MailEntry),
			entryBytes:  make(map[string]int),
			states:      make(map[string]MailUserState),
			stateBytes:  make(map[string]int),
			dedup:       make(map[string]DeliveryDedup),
			mutations:   make(map[string]MutationReceipt),
			projections: make(map[string]ProjectionReceipt),
		}
		s.boxes[playerID] = b
	}
	return b
}

// MaintenanceBucket registers a maintenance bucket (tests may use any id).
func (s *MemoryStore) bucket(id uint32) *memoryBucket {
	b, ok := s.buckets[id]
	if !ok {
		b = &memoryBucket{
			expireDue: make(map[string]int64),
			gcDue:     make(map[string]int64),
		}
		s.buckets[id] = b
	}
	return b
}

// MaintenanceBucketID is the bucket the fake attributes to a recipient.
// The real mapping lives in the Redis adapter's KeyBuilder; the fake only
// needs a stable grouping, so it reuses the frozen hash through a test
// hook. Tests set it explicitly via SetBucketOf.
type bucketMapper struct {
	byPlayer map[uint64]uint32
}

// SetBucketOf pins the maintenance bucket of one recipient.
func (s *MemoryStore) SetBucketOf(playerID uint64, bucketID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mappers == nil {
		s.mappers = &bucketMapper{byPlayer: make(map[uint64]uint32)}
	}
	s.mappers.byPlayer[playerID] = bucketID
}

func (s *MemoryStore) bucketOf(playerID uint64) uint32 {
	if s.mappers != nil {
		if b, ok := s.mappers.byPlayer[playerID]; ok {
			return b
		}
	}
	return uint32(playerID % 4096)
}

// InsertMailIfAbsent implements MailEntryRepository.
func (s *MemoryStore) InsertMailIfAbsent(_ context.Context, entry MailEntry) (InsertResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fault, hasFault := s.takeFault()
	if hasFault && fault == FaultBeforeApply {
		return InsertResult{Status: InsertTimeoutUnknown}, nil
	}
	res, err := s.applyInsert(entry)
	if err != nil {
		return InsertResult{}, err
	}
	if hasFault && fault == FaultAfterApply {
		res.Status = InsertTimeoutUnknown
	}
	return res, nil
}

func (s *MemoryStore) applyInsert(entry MailEntry) (InsertResult, error) {
	now := s.clock()
	l := s.limits
	if entry.VisibleAtMs > now {
		return InsertResult{Status: InsertNotVisibleYet}, nil
	}
	entryBlob, err := EncodeMailEntry(entry, l)
	if err != nil {
		return InsertResult{}, err
	}
	state := NewUserState(entry)
	stateBlob, err := EncodeMailUserState(state, l)
	if err != nil {
		return InsertResult{}, err
	}
	digest, err := DeliveryDigest(entry.RecipientPlayerID, entry.DeliveryID)
	if err != nil {
		return InsertResult{}, err
	}
	box := s.box(entry.RecipientPlayerID)
	if existing, ok := box.dedup[digest]; ok {
		if existing.DeliveryPayloadHash == entry.DeliveryPayloadHash {
			return InsertResult{Status: InsertAlreadyDelivered, MailID: existing.MailID, Summary: box.summary}, nil
		}
		return InsertResult{Status: InsertDeliveryConflict}, nil
	}
	if _, ok := box.entries[entry.MailID]; ok {
		return InsertResult{}, ErrContractViolation
	}
	member, err := EncodeSortMember(entry.CreatedAtMs, entry.MailID)
	if err != nil {
		return InsertResult{}, err
	}

	// Every capacity check completes before the first write (31 §6.2),
	// mirroring the Lua script order.
	deltaBytes := int64(len(entryBlob) + len(stateBlob))
	if box.summary.RetainedCount+1 > int64(l.MaxRetainedCount) ||
		box.summary.RetainedBytes+deltaBytes > int64(l.MaxRetainedBytes) {
		return InsertResult{Status: InsertCapacityExceeded}, nil
	}
	if box.summary.DeliveryRecords+1 > int64(l.MaxDeliveryRecords) {
		return InsertResult{Status: InsertCapacityExceeded}, nil
	}

	// Bounded lazy eviction of the oldest safe mail (35A §4.1).
	evicted := 0
	iterations := 0
	for box.summary.VisibleCount >= int64(l.MaxVisibleCount) && evicted < l.MaxLazyEvictPerInsert && iterations < l.MaxLazyEvictPerInsert*3 {
		iterations++
		if len(box.safeEvict) == 0 {
			break
		}
		oldest := box.safeEvict[len(box.safeEvict)-1]
		mailID, err := MailIDFromSortMember([]byte(oldest))
		if err != nil {
			return InsertResult{}, err
		}
		oldEntry := box.entries[mailID]
		oldState := box.states[mailID]
		if isSafeEvict(oldEntry, oldState) {
			oldState.AutoEvicted = true
			oldState.StateVersion++
			if err := s.reencodeState(box, oldState); err != nil {
				return InsertResult{}, err
			}
			box.removeIndexMember(oldest)
			box.summary.VisibleCount--
			evicted++
		}
		box.removeSafeEvictMember(oldest)
	}
	if box.summary.VisibleCount >= int64(l.MaxVisibleCount) {
		return InsertResult{Status: InsertMailboxFullProtected}, nil
	}

	dedup := NewDeliveryDedup(entry, l, now)
	if _, err := EncodeDeliveryDedup(dedup, l); err != nil {
		return InsertResult{}, err
	}

	box.entries[entry.MailID] = entry
	box.entryBytes[entry.MailID] = len(entryBlob)
	box.states[entry.MailID] = state
	box.stateBytes[entry.MailID] = len(stateBlob)
	box.dedup[digest] = dedup
	box.insertIndexMember(string(member))
	box.summary.MailboxRevision++
	box.summary.VisibleCount++
	box.summary.UnreadCount++
	if entry.HasAttachment() {
		box.summary.ClaimableCount++
	}
	box.summary.RetainedCount++
	box.summary.RetainedBytes += deltaBytes
	box.summary.DeliveryRecords++

	bucket := s.bucket(s.bucketOf(entry.RecipientPlayerID))
	expireRef, err := EncodeDueRefV1(MailDueRefV1{Kind: DueKindExpireVisibility, PlayerID: entry.RecipientPlayerID, MailID: entry.MailID, MailVersion: entry.MailVersion, ExpectedStateVersion: state.StateVersion})
	if err != nil {
		return InsertResult{}, err
	}
	bucket.expireDue[expireRef] = entry.ExpireAtMs
	gcRef, err := EncodeDueRefV1(MailDueRefV1{Kind: DueKindPhysicalGC, PlayerID: entry.RecipientPlayerID, MailID: entry.MailID, MailVersion: entry.MailVersion, ExpectedStateVersion: state.StateVersion})
	if err != nil {
		return InsertResult{}, err
	}
	bucket.gcDue[gcRef] = entry.PhysicalPurgeAtMs

	return InsertResult{Status: InsertInserted, MailID: entry.MailID, Summary: box.summary}, nil
}

func (s *MemoryStore) reencodeState(box *memoryBox, state MailUserState) error {
	blob, err := EncodeMailUserState(state, s.limits)
	if err != nil {
		return err
	}
	// Retained bytes track the live encoded size: state rewrites (read /
	// delete / expiry flags) change the blob length and must adjust the
	// byte waterline (35A PM4).
	box.summary.RetainedBytes += int64(len(blob) - box.stateBytes[state.MailID])
	box.states[state.MailID] = state
	box.stateBytes[state.MailID] = len(blob)
	return nil
}

// MarkMailRead implements MailEntryRepository.
func (s *MemoryStore) MarkMailRead(_ context.Context, recipientPlayerID uint64, mailID, mutationID string) (MutationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fault, hasFault := s.takeFault()
	if hasFault && fault == FaultBeforeApply {
		return MutationResult{Status: MutationTimeoutUnknown}, nil
	}
	res, err := s.applyMarkRead(recipientPlayerID, mailID, mutationID)
	if err != nil {
		return MutationResult{}, err
	}
	if hasFault && fault == FaultAfterApply {
		res.Status = MutationTimeoutUnknown
	}
	return res, nil
}

func (s *MemoryStore) applyMarkRead(recipientPlayerID uint64, mailID, mutationID string) (MutationResult, error) {
	now := s.clock()
	digest, err := MutationDigest(recipientPlayerID, mailID, MutationKindMarkRead, mutationID)
	if err != nil {
		return MutationResult{}, err
	}
	box := s.box(recipientPlayerID)
	if receipt, ok := box.mutations[digest]; ok {
		return MutationResult{Status: MutationStatus(receipt.ResultStatus), Summary: box.summary, AppliedAtMs: receipt.AppliedAtMs}, nil
	}
	entry, entryOK := box.entries[mailID]
	state, stateOK := box.states[mailID]
	write := func(status MutationStatus, appliedAt int64) (MutationResult, error) {
		receipt := MutationReceipt{
			Kind: MutationKindMarkRead, MailID: mailID, MutationID: mutationID,
			ResultStatus: string(status), AppliedAtMs: appliedAt,
			MailboxRevision: box.summary.MailboxRevision,
			RetainUntilMs:   now + s.limits.MutationReceiptRetainMs,
		}
		if _, err := EncodeMutationReceipt(receipt, s.limits); err != nil {
			return MutationResult{}, err
		}
		box.mutations[digest] = receipt
		return MutationResult{Status: status, Summary: box.summary, AppliedAtMs: appliedAt}, nil
	}
	if !entryOK || !stateOK || state.DeletedAtMs > 0 || state.AutoEvicted {
		return write(MutationMailNotFound, now)
	}
	if state.ReadAtMs > 0 {
		return write(MutationNoChange, now)
	}
	// FIX-1: the materialized summary/index transition only asks whether
	// the old state was still counted (not deleted / not auto-evicted /
	// not yet materially expired). The expire_at wall-clock comparison
	// belongs to the read path, not to counter transitions.
	wasCounted := materiallyCounted(state)
	state.ReadAtMs = now
	state.StateVersion++
	if err := s.reencodeState(box, state); err != nil {
		return MutationResult{}, err
	}
	if wasCounted && box.summary.UnreadCount > 0 {
		box.summary.UnreadCount--
	}
	if isSafeEvict(entry, state) {
		member, err := EncodeSortMember(entry.CreatedAtMs, mailID)
		if err != nil {
			return MutationResult{}, err
		}
		box.insertSafeEvictMember(string(member))
	}
	box.summary.MailboxRevision++
	return write(MutationReadMarked, now)
}

// DeleteMail implements MailEntryRepository.
func (s *MemoryStore) DeleteMail(_ context.Context, recipientPlayerID uint64, mailID, mutationID string, auth DeleteAuthorization) (MutationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fault, hasFault := s.takeFault()
	if hasFault && fault == FaultBeforeApply {
		return MutationResult{Status: MutationTimeoutUnknown}, nil
	}
	res, err := s.applyDelete(recipientPlayerID, mailID, mutationID, auth)
	if err != nil {
		return MutationResult{}, err
	}
	if hasFault && fault == FaultAfterApply {
		res.Status = MutationTimeoutUnknown
	}
	return res, nil
}

func (s *MemoryStore) applyDelete(recipientPlayerID uint64, mailID, mutationID string, auth DeleteAuthorization) (MutationResult, error) {
	now := s.clock()
	if err := auth.Validate(mailID); err != nil {
		return MutationResult{}, err
	}
	digest, err := MutationDigest(recipientPlayerID, mailID, MutationKindDelete, mutationID)
	if err != nil {
		return MutationResult{}, err
	}
	box := s.box(recipientPlayerID)
	if receipt, ok := box.mutations[digest]; ok {
		return MutationResult{Status: MutationStatus(receipt.ResultStatus), Summary: box.summary, AppliedAtMs: receipt.AppliedAtMs}, nil
	}
	entry, entryOK := box.entries[mailID]
	state, stateOK := box.states[mailID]
	write := func(status MutationStatus) (MutationResult, error) {
		receipt := MutationReceipt{
			Kind: MutationKindDelete, MailID: mailID, MutationID: mutationID,
			ResultStatus: string(status), AppliedAtMs: now,
			MailboxRevision: box.summary.MailboxRevision,
			RetainUntilMs:   now + s.limits.MutationReceiptRetainMs,
		}
		if _, err := EncodeMutationReceipt(receipt, s.limits); err != nil {
			return MutationResult{}, err
		}
		box.mutations[digest] = receipt
		return MutationResult{Status: status, Summary: box.summary, AppliedAtMs: now}, nil
	}
	if !entryOK || !stateOK {
		return write(MutationMailNotFound)
	}
	if state.DeletedAtMs > 0 || state.AutoEvicted {
		return write(MutationNoChange)
	}
	// Defense in depth, mirroring the Lua re-check (35A §4.3): the proof
	// must bind this exact mail id and the immutable entry's
	// claim_material_hash with a non-empty receipt ref.
	switch {
	case !entry.HasAttachment():
	case now > entry.ClaimEndAtMs:
	case state.ClaimedProjection:
	case auth.Eligibility == EligibilityAllowWithClaimProof:
		proof := auth.ClaimProof
		if proof == nil || !proof.Claimed || proof.MailID != mailID ||
			proof.ClaimMaterialHash == "" || proof.ClaimMaterialHash != entry.ClaimMaterialHash ||
			proof.ReceiptRef == "" {
			return MutationResult{}, ErrContractViolation
		}
	default:
		return write(MutationRejectUnclaimed)
	}
	wasCounted := materiallyCounted(state)
	state.DeletedAtMs = now
	state.StateVersion++
	if err := s.reencodeState(box, state); err != nil {
		return MutationResult{}, err
	}
	if wasCounted {
		member, err := EncodeSortMember(entry.CreatedAtMs, mailID)
		if err != nil {
			return MutationResult{}, err
		}
		box.removeIndexMember(string(member))
		box.removeSafeEvictMember(string(member))
		box.summary.VisibleCount--
		if state.ReadAtMs == 0 && box.summary.UnreadCount > 0 {
			box.summary.UnreadCount--
		}
		if entry.HasAttachment() && !state.ClaimedProjection && box.summary.ClaimableCount > 0 {
			box.summary.ClaimableCount--
		}
	}
	box.summary.MailboxRevision++
	return write(MutationDeleted)
}

// ApplyMailClaimProjection implements MailEntryRepository.
func (s *MemoryStore) ApplyMailClaimProjection(_ context.Context, order ClaimProjectionOrder) (ProjectionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fault, hasFault := s.takeFault()
	if hasFault && fault == FaultBeforeApply {
		return ProjectionResult{Status: ProjectionTimeoutUnknown}, nil
	}
	res, err := s.applyProjection(order)
	if err != nil {
		return ProjectionResult{}, err
	}
	if hasFault && fault == FaultAfterApply {
		res.Status = ProjectionTimeoutUnknown
	}
	return res, nil
}

func (s *MemoryStore) applyProjection(order ClaimProjectionOrder) (ProjectionResult, error) {
	now := s.clock()
	digest, err := ProjectionDigest(order.RecipientPlayerID, order.WorkID, order.WorkVersion)
	if err != nil {
		return ProjectionResult{}, err
	}
	box := s.box(order.RecipientPlayerID)
	if receipt, ok := box.projections[digest]; ok {
		return ProjectionResult{Status: ProjectionStatus(receipt.ResultStatus), Summary: box.summary}, nil
	}
	write := func(status ProjectionStatus) (ProjectionResult, error) {
		receipt := ProjectionReceipt{
			WorkID: order.WorkID, WorkVersion: order.WorkVersion, MailID: order.MailID,
			ClaimMaterialHash: order.ClaimMaterialHash, ResultStatus: string(status),
			AppliedAtMs: now, RetainUntilMs: now + s.limits.ProjectionReceiptRetainMs,
		}
		if _, err := EncodeProjectionReceipt(receipt, s.limits); err != nil {
			return ProjectionResult{}, err
		}
		box.projections[digest] = receipt
		return ProjectionResult{Status: status, Summary: box.summary}, nil
	}
	entry, entryOK := box.entries[order.MailID]
	state, stateOK := box.states[order.MailID]
	if !entryOK || !stateOK {
		return write(ProjectionTargetAlreadyPurged)
	}
	if entry.ClaimMaterialHash == "" || entry.ClaimMaterialHash != order.ClaimMaterialHash {
		return write(ProjectionHashMismatch)
	}
	if state.ClaimedProjection {
		return write(ProjectionNoChange)
	}
	wasCounted := materiallyCounted(state)
	state.ClaimedProjection = true
	state.ClaimReceiptRef = order.ClaimReceiptRef
	state.ClaimedProjectionAtMs = order.ClaimedAtMs
	state.ClaimProjectionVersion = order.WorkVersion
	state.StateVersion++
	if err := s.reencodeState(box, state); err != nil {
		return ProjectionResult{}, err
	}
	// Monotonic false→true only: a late projection never clears
	// deleted_at_ms and never re-adds the index member (35A PM3/§4.3).
	if wasCounted && entry.HasAttachment() && box.summary.ClaimableCount > 0 {
		box.summary.ClaimableCount--
	}
	if state.ReadAtMs > 0 {
		member, err := EncodeSortMember(entry.CreatedAtMs, order.MailID)
		if err != nil {
			return ProjectionResult{}, err
		}
		box.insertSafeEvictMember(string(member))
	}
	box.summary.MailboxRevision++
	return write(ProjectionApplied)
}

// GetEntry implements MailReadRepository.
func (s *MemoryStore) GetEntry(_ context.Context, recipientPlayerID uint64, mailID string) (MailEntry, MailUserState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateMailID(mailID); err != nil {
		return MailEntry{}, MailUserState{}, err
	}
	box := s.box(recipientPlayerID)
	entry, ok := box.entries[mailID]
	if !ok {
		return MailEntry{}, MailUserState{}, ErrMailNotFound
	}
	return entry, box.states[mailID], nil
}

// GetSummary implements MailReadRepository.
func (s *MemoryStore) GetSummary(_ context.Context, recipientPlayerID uint64) (MailboxSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.box(recipientPlayerID).summary, nil
}

// ListPage implements MailReadRepository with the same bounded scan
// algorithm as the Redis adapter (35A §3).
func (s *MemoryStore) ListPage(_ context.Context, recipientPlayerID uint64, cursor string, nowMs int64) (Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var after []byte
	if cursor != "" {
		member, err := DecodeCursor(cursor)
		if err != nil {
			return Page{}, err
		}
		after = member
	}
	box := s.box(recipientPlayerID)
	start := 0
	if after != nil {
		start = sort.SearchStrings(box.index, string(after))
		for start < len(box.index) && box.index[start] <= string(after) {
			start++
		}
	}
	page := Page{Items: make([]ListItem, 0, s.limits.PageSize)}
	scannedItems := 0
	scannedBytes := 0
	lastMember := ""
	for i := start; i < len(box.index); i++ {
		if len(page.Items) >= s.limits.PageSize {
			break
		}
		if scannedItems >= s.limits.PageScanItems || scannedBytes >= s.limits.PageScanBytes {
			break
		}
		member := box.index[i]
		scannedItems++
		scannedBytes += len(member)
		lastMember = member
		mailID, err := MailIDFromSortMember([]byte(member))
		if err != nil {
			return Page{}, err
		}
		entry, ok := box.entries[mailID]
		if !ok {
			continue
		}
		state := box.states[mailID]
		scannedBytes += box.entryBytes[mailID] + box.stateBytes[mailID]
		if !state.Visible(entry, nowMs) {
			continue
		}
		page.Items = append(page.Items, ListItem{
			MailID:            mailID,
			Title:             entry.TitleSnapshot,
			CreatedAtMs:       entry.CreatedAtMs,
			ExpireAtMs:        entry.ExpireAtMs,
			Read:              state.ReadAtMs > 0,
			HasAttachment:     entry.HasAttachment(),
			ClaimedProjection: state.ClaimedProjection,
		})
	}
	if lastMember != "" {
		// The cursor advances past the last examined candidate only when the
		// scan stopped early; a full page uses its own last item.
		if len(page.Items) == s.limits.PageSize {
			last := page.Items[len(page.Items)-1]
			member, err := EncodeSortMember(last.CreatedAtMs, last.MailID)
			if err != nil {
				return Page{}, err
			}
			lastMember = string(member)
		}
		more := false
		idx := sort.SearchStrings(box.index, lastMember)
		for idx < len(box.index) && box.index[idx] <= lastMember {
			idx++
		}
		more = idx < len(box.index)
		if more {
			encoded, err := EncodeCursor([]byte(lastMember))
			if err != nil {
				return Page{}, err
			}
			page.NextCursor = encoded
		}
	}
	return page, nil
}

// ClaimScanLease implements MailMaintenanceRepository.
func (s *MemoryStore) ClaimScanLease(_ context.Context, bucketID uint32, ownerToken string, requestedTTLMs int64) (ScanLeaseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ownerToken == "" || requestedTTLMs <= 0 {
		return ScanLeaseResult{}, ErrInvalidRequest
	}
	now := s.clock()
	b := s.bucket(bucketID)
	if b.scanLeaseOwner != "" && b.scanLeaseExpire > now && b.scanLeaseOwner != ownerToken {
		return ScanLeaseResult{Status: ScanLeaseHeldByOther}, nil
	}
	ttl := ClampScanLeaseTTL(requestedTTLMs)
	b.scanLeaseOwner = ownerToken
	b.scanLeaseExpire = now + ttl
	b.scanLeaseEpoch++
	return ScanLeaseResult{Status: ScanLeaseAcquired, AcceptedTTLMs: ttl, LeaseEpoch: b.scanLeaseEpoch}, nil
}

// ListExpireDue implements MailMaintenanceRepository.
func (s *MemoryStore) ListExpireDue(_ context.Context, bucketID uint32, nowMs int64, limit int) ([]DueCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return listDue(s.bucket(bucketID).expireDue, nowMs, limit)
}

// ListGcDue implements MailMaintenanceRepository.
func (s *MemoryStore) ListGcDue(_ context.Context, bucketID uint32, nowMs int64, limit int) ([]DueCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return listDue(s.bucket(bucketID).gcDue, nowMs, limit)
}

func listDue(index map[string]int64, nowMs int64, limit int) ([]DueCandidate, error) {
	if limit <= 0 {
		return nil, ErrInvalidRequest
	}
	members := make([]string, 0, len(index))
	for member, score := range index {
		if score <= nowMs {
			members = append(members, member)
		}
	}
	sort.Slice(members, func(i, j int) bool {
		si, sj := index[members[i]], index[members[j]]
		if si != sj {
			return si < sj
		}
		return members[i] < members[j]
	})
	if len(members) > limit {
		members = members[:limit]
	}
	out := make([]DueCandidate, 0, len(members))
	for _, member := range members {
		ref, err := DecodeDueRefV1(member)
		if err != nil {
			return nil, err
		}
		out = append(out, DueCandidate{Ref: ref, DueAtMs: index[member], Member: member})
	}
	return out, nil
}

// ExpireVisibility implements MailMaintenanceRepository.
func (s *MemoryStore) ExpireVisibility(_ context.Context, ref MailDueRefV1) (ExpireResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fault, hasFault := s.takeFault()
	if hasFault && fault == FaultBeforeApply {
		return ExpireResult{Status: ExpireTimeoutUnknown}, nil
	}
	res, err := s.applyExpire(ref)
	if err != nil {
		return ExpireResult{}, err
	}
	if hasFault && fault == FaultAfterApply {
		res.Status = ExpireTimeoutUnknown
	}
	return res, nil
}

func (s *MemoryStore) applyExpire(ref MailDueRefV1) (ExpireResult, error) {
	if ref.Kind != DueKindExpireVisibility {
		return ExpireResult{}, ErrInvalidRequest
	}
	now := s.clock()
	bucket := s.bucket(s.bucketOf(ref.PlayerID))
	member, err := EncodeDueRefV1(ref)
	if err != nil {
		return ExpireResult{}, err
	}
	box := s.box(ref.PlayerID)
	entry, entryOK := box.entries[ref.MailID]
	state, stateOK := box.states[ref.MailID]
	if !stateOK {
		delete(bucket.expireDue, member)
		return ExpireResult{Status: ExpireTargetAlreadyPurged}, nil
	}
	if !entryOK || entry.MailVersion != ref.MailVersion {
		delete(bucket.expireDue, member)
		return ExpireResult{Status: ExpireStaleRef, CurrentStateVersion: state.StateVersion, ExpireAtMs: entry.ExpireAtMs, Summary: box.summary}, nil
	}
	if state.StateVersion != ref.ExpectedStateVersion {
		delete(bucket.expireDue, member)
		return ExpireResult{Status: ExpireStaleRef, CurrentStateVersion: state.StateVersion, ExpireAtMs: entry.ExpireAtMs, Summary: box.summary}, nil
	}
	if state.DeletedAtMs > 0 || state.AutoEvicted || state.ExpiredAtMs > 0 {
		delete(bucket.expireDue, member)
		return ExpireResult{Status: ExpireNoChange, Summary: box.summary}, nil
	}
	if now < entry.ExpireAtMs {
		return ExpireResult{Status: ExpireNotDue, CurrentStateVersion: state.StateVersion, ExpireAtMs: entry.ExpireAtMs, Summary: box.summary}, nil
	}
	state.ExpiredAtMs = now
	state.StateVersion++
	if err := s.reencodeState(box, state); err != nil {
		return ExpireResult{}, err
	}
	sortMember, err := EncodeSortMember(entry.CreatedAtMs, ref.MailID)
	if err != nil {
		return ExpireResult{}, err
	}
	box.removeIndexMember(string(sortMember))
	box.removeSafeEvictMember(string(sortMember))
	box.summary.VisibleCount--
	if state.ReadAtMs == 0 && box.summary.UnreadCount > 0 {
		box.summary.UnreadCount--
	}
	if entry.HasAttachment() && !state.ClaimedProjection && box.summary.ClaimableCount > 0 {
		box.summary.ClaimableCount--
	}
	box.summary.MailboxRevision++
	delete(bucket.expireDue, member)
	return ExpireResult{Status: ExpireApplied, Summary: box.summary}, nil
}

// GcMail implements MailMaintenanceRepository.
func (s *MemoryStore) GcMail(_ context.Context, ref MailDueRefV1) (GcResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fault, hasFault := s.takeFault()
	if hasFault && fault == FaultBeforeApply {
		return GcResult{Status: GcTimeoutUnknown}, nil
	}
	res, err := s.applyGc(ref)
	if err != nil {
		return GcResult{}, err
	}
	if hasFault && fault == FaultAfterApply {
		res.Status = GcTimeoutUnknown
	}
	return res, nil
}

func (s *MemoryStore) applyGc(ref MailDueRefV1) (GcResult, error) {
	if ref.Kind != DueKindPhysicalGC {
		return GcResult{}, ErrInvalidRequest
	}
	now := s.clock()
	bucket := s.bucket(s.bucketOf(ref.PlayerID))
	member, err := EncodeDueRefV1(ref)
	if err != nil {
		return GcResult{}, err
	}
	box := s.box(ref.PlayerID)
	entry, entryOK := box.entries[ref.MailID]
	state, stateOK := box.states[ref.MailID]
	if !stateOK {
		// Nothing left: clean residual index members and the due ref.
		if entryOK {
			return GcResult{}, ErrContractViolation
		}
		box.removeIndexMemberByMailID(ref.MailID)
		box.removeSafeEvictMemberByMailID(ref.MailID)
		delete(bucket.gcDue, member)
		return GcResult{Status: GcTargetAlreadyPurged}, nil
	}
	if !entryOK {
		return GcResult{Status: GcReconcileRequired}, nil
	}
	if entry.MailVersion != ref.MailVersion || state.StateVersion != ref.ExpectedStateVersion {
		delete(bucket.gcDue, member)
		return GcResult{
			Status:              GcStaleRef,
			PurgeAtMs:           entry.PhysicalPurgeAtMs,
			CurrentMailVersion:  entry.MailVersion,
			CurrentStateVersion: state.StateVersion,
		}, nil
	}
	// FIX-3: the dedup record must be readable in the same atomic step;
	// losing it silently would drop the delivery anti-replay evidence.
	digest, err := DeliveryDigest(entry.RecipientPlayerID, entry.DeliveryID)
	if err != nil {
		return GcResult{}, err
	}
	dedup, dedupOK := box.dedup[digest]
	if !dedupOK {
		return GcResult{Status: GcReconcileRequired}, nil
	}
	// Entry/State and the dedup record share one effective purge point:
	// the later of the claim-evidence window and the delivery retention
	// window. The same pass then removes all three (35A §6.3).
	effectivePurge := entry.PhysicalPurgeAtMs
	if dedup.DeliveryRetainUntilMs > effectivePurge {
		effectivePurge = dedup.DeliveryRetainUntilMs
	}
	deletable := state.DeletedAtMs > 0 || state.AutoEvicted || state.ExpiredAtMs > 0
	if !deletable || now < effectivePurge {
		return GcResult{
			Status:              GcNotDue,
			PurgeAtMs:           effectivePurge,
			CurrentMailVersion:  entry.MailVersion,
			CurrentStateVersion: state.StateVersion,
		}, nil
	}
	freed := int64(box.entryBytes[ref.MailID] + box.stateBytes[ref.MailID])
	delete(box.entries, ref.MailID)
	delete(box.entryBytes, ref.MailID)
	delete(box.states, ref.MailID)
	delete(box.stateBytes, ref.MailID)
	box.removeIndexMemberByMailID(ref.MailID)
	box.removeSafeEvictMemberByMailID(ref.MailID)
	box.summary.RetainedCount--
	box.summary.RetainedBytes -= freed
	delete(bucket.gcDue, member)
	delete(box.dedup, digest)
	box.summary.DeliveryRecords--
	return GcResult{Status: GcPurged, FreedBytes: freed}, nil
}

// RegisterDueRef implements MailMaintenanceRepository.
func (s *MemoryStore) RegisterDueRef(_ context.Context, ref MailDueRefV1, scoreMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	member, err := EncodeDueRefV1(ref)
	if err != nil {
		return err
	}
	if scoreMs <= 0 {
		return ErrInvalidRequest
	}
	bucket := s.bucket(s.bucketOf(ref.PlayerID))
	switch ref.Kind {
	case DueKindExpireVisibility:
		bucket.expireDue[member] = scoreMs
	case DueKindPhysicalGC:
		bucket.gcDue[member] = scoreMs
	default:
		return ErrInvalidRequest
	}
	return nil
}

// materiallyCounted is the FIX-1 counter predicate: a mail is counted in
// the materialized summary/index while it is not deleted, not auto-evicted
// and not yet materially expired. The wall-clock expire_at comparison is
// deliberately absent — it belongs to the read path only.
func materiallyCounted(state MailUserState) bool {
	return state.DeletedAtMs == 0 && !state.AutoEvicted && state.ExpiredAtMs == 0
}

func isSafeEvict(entry MailEntry, state MailUserState) bool {
	if state.DeletedAtMs > 0 || state.AutoEvicted || state.ExpiredAtMs > 0 || state.ReadAtMs == 0 {
		return false
	}
	return !entry.HasAttachment() || state.ClaimedProjection
}

func (b *memoryBox) insertIndexMember(member string) {
	i := sort.SearchStrings(b.index, member)
	b.index = append(b.index, "")
	copy(b.index[i+1:], b.index[i:])
	b.index[i] = member
}

func (b *memoryBox) removeIndexMember(member string) {
	i := sort.SearchStrings(b.index, member)
	if i < len(b.index) && b.index[i] == member {
		b.index = append(b.index[:i], b.index[i+1:]...)
	}
}

func (b *memoryBox) insertSafeEvictMember(member string) {
	i := sort.SearchStrings(b.safeEvict, member)
	if i < len(b.safeEvict) && b.safeEvict[i] == member {
		return
	}
	b.safeEvict = append(b.safeEvict, "")
	copy(b.safeEvict[i+1:], b.safeEvict[i:])
	b.safeEvict[i] = member
}

func (b *memoryBox) removeSafeEvictMember(member string) {
	i := sort.SearchStrings(b.safeEvict, member)
	if i < len(b.safeEvict) && b.safeEvict[i] == member {
		b.safeEvict = append(b.safeEvict[:i], b.safeEvict[i+1:]...)
	}
}

func (b *memoryBox) removeIndexMemberByMailID(mailID string) {
	b.index = removeMemberByMailID(b.index, mailID)
}

func (b *memoryBox) removeSafeEvictMemberByMailID(mailID string) {
	b.safeEvict = removeMemberByMailID(b.safeEvict, mailID)
}

func removeMemberByMailID(members []string, mailID string) []string {
	out := members[:0]
	for _, m := range members {
		id, err := MailIDFromSortMember([]byte(m))
		if err != nil || id != mailID {
			out = append(out, m)
		}
	}
	if out == nil {
		return []string{}
	}
	return out
}
