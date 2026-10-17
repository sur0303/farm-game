package authority

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	commitstamp "farmgame/internal/storage/internal/commitstamp"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// SchemaVersion is the actor record schema written by this adapter. Version 2
// adds the bounded receipt manifest needed to verify the aggregate receipt
// counters without an unbounded hash scan.
const SchemaVersion = "2"

// Hard script-side bounds (31 §14.1 layer 4). The Lua additionally enforces
// mutation/order counts; these Go-side values mirror them for early failure.
const (
	maxMutationsPerCommit = 32
	maxOrdersPerCommit    = 4
	maxSchedulesPerCommit = 16
	maxReceiptValueBytes  = 1024
	maxOrderPayloadHard   = 16384
)

var errScriptContract = errors.New("redisauthority: script contract violation")

// redisBinaryArg keeps immutable actor payloads as byte slices until go-redis
// writes the synchronous EVALSHA request. Converting them to string here would
// allocate and copy every model field, outcome and work payload once more on
// each commit. A nil slice must remain an empty bulk string rather than a Redis
// nil bulk because the frozen Lua envelope treats positional empty values as
// present arguments.
func redisBinaryArg(value []byte) interface{} {
	if value == nil {
		return ""
	}
	return value
}

// Options configures the Redis authority adapter. Values are placement and
// limit parameters only; semantic rules live in the frozen Lua family.
type Options struct {
	Profile     rediskey.PhysicalProfile
	Resolver    rediskey.AuthorityShardResolver
	Limits      authoritystore.Limits
	LeaseLimits authoritystore.LeaseLimits

	// ScheduleDueIndexMode selects the plan-82 C5 schedule discovery index
	// layout. The default is ScheduleDueIndexShared so existing deployments
	// keep reading and writing the legacy shared DueIndex until an operator
	// explicitly enables dual/shadow/dedicated modes.
	ScheduleDueIndexMode ScheduleDueIndexMode
	// ScheduleDueIndexDedicatedReady is an explicit operator proof that the
	// historical rebuild completed and every shared-only writer has drained.
	// Dedicated-only startup fails closed when this proof is absent.
	ScheduleDueIndexDedicatedReady bool

	// LimitProfileVersion is recorded on every record for observability.
	LimitProfileVersion string
	// MaxBucketPendingWork bounds PENDING source orders per bucket.
	MaxBucketPendingWork int
	// MaxBucketPendingWorkBytes bounds the aggregate payload bytes charged to
	// PENDING source orders and schedules in one authority bucket.
	MaxBucketPendingWorkBytes int
	// MaxOrderPayloadBytes bounds one order payload (<= 16 KiB hard).
	MaxOrderPayloadBytes int
	// MaxSchedulePayloadBytes bounds one mutable schedule payload (<= 16 KiB hard).
	MaxSchedulePayloadBytes int
	// MaxReceiptTotalBytes bounds summed receipt bytes per actor record.
	MaxReceiptTotalBytes int
	// ReceiptFullRetentionMs drives the receipt-due index score
	// (redis TIME + retention). Compaction itself is W31-B/C scope.
	ReceiptFullRetentionMs int64
	// WorkTotalDeadlineMs and WorkMaxRetryCount are persisted on every source
	// WorkRecord. They bound recovery independently of a worker process.
	WorkTotalDeadlineMs int64
	WorkMaxRetryCount   uint64
}

func (o Options) withDefaults() Options {
	o.Limits = authoritystore.NormalizeLimits(o.Limits)
	if o.LeaseLimits == (authoritystore.LeaseLimits{}) {
		o.LeaseLimits = authoritystore.DefaultLeaseLimits()
	}
	if o.LimitProfileVersion == "" {
		o.LimitProfileVersion = "limit-v0.1"
	}
	if o.MaxBucketPendingWork <= 0 {
		o.MaxBucketPendingWork = 65536
	}
	if o.MaxBucketPendingWorkBytes <= 0 {
		o.MaxBucketPendingWorkBytes = o.MaxBucketPendingWork * maxOrderPayloadHard
	}
	if o.MaxOrderPayloadBytes <= 0 {
		o.MaxOrderPayloadBytes = maxOrderPayloadHard
	}
	if o.MaxSchedulePayloadBytes <= 0 {
		o.MaxSchedulePayloadBytes = o.Limits.MaxSchedulePayloadBytes
		if o.MaxSchedulePayloadBytes <= 0 {
			o.MaxSchedulePayloadBytes = o.MaxOrderPayloadBytes
		}
	}
	if o.MaxSchedulePayloadBytes > maxOrderPayloadHard {
		o.MaxSchedulePayloadBytes = maxOrderPayloadHard
	}
	if o.MaxReceiptTotalBytes <= 0 {
		o.MaxReceiptTotalBytes = o.Limits.MaxReceiptBytes
	}
	if o.ReceiptFullRetentionMs <= 0 {
		o.ReceiptFullRetentionMs = 24 * 60 * 60 * 1000
	}
	if o.WorkTotalDeadlineMs <= 0 {
		o.WorkTotalDeadlineMs = 24 * 60 * 60 * 1000
	}
	if o.WorkMaxRetryCount == 0 {
		o.WorkMaxRetryCount = 8
	}
	if o.ScheduleDueIndexMode == "" {
		o.ScheduleDueIndexMode = ScheduleDueIndexShared
	}
	return o
}

// Store is the production Redis implementation of authoritystore.ActorAuthorityStore
// and the source-order/lease physical adapter shared by Farm, Player and the
// Care worker compositions.
type Store struct {
	client   redis.UniversalClient
	registry *ScriptRegistry
	opts     Options
	keys     rediskey.AuthorityKeys
	layout   string
}

var _ authoritystore.ActorAuthorityStore = (*Store)(nil)
var _ authoritystore.PhysicalCapabilitiesProvider = (*Store)(nil)
var _ authoritystore.PhysicalCommitUsageProjector = (*Store)(nil)
var _ authoritystore.ExactCommitter = (*Store)(nil)

func NewStore(client redis.UniversalClient, registry *ScriptRegistry, opts Options) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client required", authoritystore.ErrInvalidRequest)
	}
	opts = opts.withDefaults()
	if err := opts.ScheduleDueIndexMode.Validate(); err != nil {
		return nil, fmt.Errorf("%w: schedule due index mode: %v", authoritystore.ErrInvalidRequest, err)
	}
	if opts.ScheduleDueIndexMode == ScheduleDueIndexDedicatedOnly && !opts.ScheduleDueIndexDedicatedReady {
		return nil, fmt.Errorf("%w: dedicated schedule due index requires migration readiness proof", authoritystore.ErrInvalidRequest)
	}
	keys, err := rediskey.NewAuthorityKeys(opts.Profile, opts.Resolver)
	if err != nil {
		return nil, err
	}
	if registry == nil {
		if registry, err = NewScriptRegistry(client); err != nil {
			return nil, err
		}
	}
	return &Store{
		client:   client,
		registry: registry,
		opts:     opts,
		keys:     keys,
		layout:   opts.Profile.AuthorityLayoutVersion,
	}, nil
}

// Registry exposes the shared script registry (warmup, goldens, probes).
func (s *Store) Registry() *ScriptRegistry { return s.registry }

// Keys exposes the frozen auth-v1 key builder for same-package primitives
// (work lease, probes). Business code must not call it directly.
func (s *Store) Keys() rediskey.AuthorityKeys { return s.keys }

// Client exposes the underlying client for read-only bounded queries used by
// same-package primitives.
func (s *Store) Client() redis.UniversalClient { return s.client }

func (s *Store) options() Options { return s.opts }

// AdmissionSnapshotReuseSafe is true because every Redis actor model/usage
// mutation is one version-incrementing Authority Commit. A Participant may
// reuse its fully audited admission snapshot only while the local Binding is
// still at that exact RecordVersion; the eventual Commit remains the Fence.
func (s *Store) AdmissionSnapshotReuseSafe() bool { return s != nil }

// PhysicalCapabilities returns the exact immutable Go/Lua limits enforced by
// this Redis Store instance. The hard script family caps are folded in so an
// actor runtime can never admit against a wider caller-advertised profile.
func (s *Store) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	if s == nil {
		return authoritystore.PhysicalCapabilities{}, fmt.Errorf("%w: nil Redis authority store", authoritystore.ErrInvalidRequest)
	}
	maxFields := s.opts.Limits.MaxModelFields
	if maxFields > maxMutationsPerCommit {
		maxFields = maxMutationsPerCommit
	}
	maxSingleReceiptBytes := s.opts.Limits.MaxSingleReceiptBytes
	if maxSingleReceiptBytes > maxReceiptValueBytes {
		maxSingleReceiptBytes = maxReceiptValueBytes
	}
	caps := authoritystore.PhysicalCapabilities{
		MaxModelFields:        maxFields,
		MaxModelBytes:         s.opts.Limits.MaxModelBytes,
		MaxReceiptItems:       s.opts.Limits.MaxReceipts,
		MaxReceiptBytes:       s.opts.MaxReceiptTotalBytes,
		MaxSingleReceiptBytes: maxSingleReceiptBytes,
		MaxCommitBytes:        s.opts.Limits.MaxCommitBytes,
	}
	return caps, caps.Validate()
}

// ProjectPhysicalCommit returns the concrete Redis/Lua physical charge for a
// new commit. The receipt charge is the same bounded LP envelope used by the
// Go-side single-receipt guard (including its fixed timestamp reservation),
// so aggregate pre-admission cannot infer outer usage from a nested protobuf.
func (s *Store) ProjectPhysicalCommit(req authoritystore.AuthorityCommitRequest) (authoritystore.PhysicalCommitUsage, error) {
	if s == nil {
		return authoritystore.PhysicalCommitUsage{}, fmt.Errorf("%w: nil Redis authority store", authoritystore.ErrInvalidRequest)
	}
	mutation, _, err := authoritystore.NormalizeActorModelMutationBorrowed(req.ActorAddress, req.CandidateState, req.ModelMutation)
	if err != nil {
		return authoritystore.PhysicalCommitUsage{}, err
	}
	if _, err := authoritystore.NormalizeAttachedSchedules(req.AttachedSchedules, req.IdempotencyKey.SubjectPlayerID, s.opts.Limits); err != nil {
		return authoritystore.PhysicalCommitUsage{}, err
	}
	for _, schedule := range req.AttachedSchedules {
		if len(schedule.Payload) > s.opts.MaxSchedulePayloadBytes || len(schedule.Payload) > maxOrderPayloadHard {
			return authoritystore.PhysicalCommitUsage{}, authoritystore.ErrCapacityExceeded
		}
	}
	receiptBytes, err := redisReceiptValueBytes(req)
	if err != nil {
		return authoritystore.PhysicalCommitUsage{}, err
	}
	modelBytes := 0
	for name, value := range req.CandidateState {
		modelBytes += len(name) + len(value)
	}
	usage := authoritystore.PhysicalCommitUsage{
		ModelBytes:         modelBytes,
		ReceiptItems:       1,
		ReceiptBytes:       receiptBytes,
		SingleReceiptBytes: receiptBytes,
		CommitBytes:        redisCommitArgvBytes(req, mutation),
	}
	return usage, usage.Validate()
}

func (s *Store) bucketFor(address authoritystore.ActorAddress) (rediskey.AuthorityBucketRef, error) {
	if address.ActorID == 0 || (address.Domain != actorrouting.DomainPlayer && address.Domain != actorrouting.DomainFarm) {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: actor address", authoritystore.ErrInvalidRequest)
	}
	bucket, err := s.opts.Profile.AuthorityBucketForActor(s.opts.Resolver, address)
	if err != nil {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: derive bucket: %v", authoritystore.ErrAuthorityAddressMismatch, err)
	}
	return bucket, nil
}

func (s *Store) checkLayout(layout string) error {
	if layout != s.layout {
		return fmt.Errorf("%w: layout version", authoritystore.ErrAuthorityAddressMismatch)
	}
	return nil
}

func assertGroupDerived(bucket rediskey.AuthorityBucketRef, address authoritystore.ActorAddress, asserted *authoritystore.AuthorityGroupRef) error {
	if asserted == nil {
		return nil
	}
	if asserted.AuthorityLayoutVersion != bucket.LayoutVersion() || asserted.Domain != bucket.Domain() ||
		asserted.ActorID != address.ActorID || asserted.VirtualShard != bucket.VirtualShard() {
		return authoritystore.ErrAuthorityAddressMismatch
	}
	return nil
}

func validateStableID(name, value string) error {
	if strings.TrimSpace(value) == "" || len(value) > 256 {
		return fmt.Errorf("%w: %s", authoritystore.ErrInvalidRequest, name)
	}
	return nil
}

// validateModelState enforces the Redis adapter's model field naming so model
// fields can never collide with _meta:/receipt: reserved prefixes.
func validateModelState(state authoritystore.ModelState, limits authoritystore.Limits) error {
	if state == nil {
		return fmt.Errorf("%w: candidate state required", authoritystore.ErrInvalidRequest)
	}
	if len(state) > limits.MaxModelFields {
		return authoritystore.ErrCapacityExceeded
	}
	total := 0
	for key, value := range state {
		if !validModelFieldName(key) {
			return fmt.Errorf("%w: model field name %q", authoritystore.ErrInvalidRequest, key)
		}
		total += len(key) + len(value)
		if total > limits.MaxModelBytes {
			return authoritystore.ErrCapacityExceeded
		}
	}
	return nil
}

func validModelFieldName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func cloneModelState(in authoritystore.ModelState) authoritystore.ModelState {
	if in == nil {
		return nil
	}
	out := make(authoritystore.ModelState, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func cloneOutcome(in authoritystore.StoredOperationOutcome) authoritystore.StoredOperationOutcome {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

// ── LoadAndClaim ────────────────────────────────────────────────

func (s *Store) LoadAndClaim(ctx context.Context, req authoritystore.LoadAndClaimRequest) (authoritystore.LoadAndClaimResult, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	bucket, err := s.bucketFor(req.ActorAddress)
	if err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	if err := s.checkLayout(req.AuthorityLayoutVersion); err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	if err := validateStableID("claim_operation_id", req.ClaimOperationID); err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	if err := validateStableID("new_writer_claim_id", req.NewWriterClaimID); err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	initial := req.InitialModelFields
	if initial == nil {
		initial = authoritystore.ModelState{}
	}
	initialLayout := req.InitialModelLayout
	if initialLayout == authoritystore.ModelLayoutUnknown {
		initialLayout = authoritystore.ModelLayoutV1
	}
	if initialLayout != authoritystore.ModelLayoutV1 && initialLayout != authoritystore.ModelLayoutV2 {
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: initial model layout", authoritystore.ErrInvalidRequest)
	}
	if err := validateModelState(initial, s.opts.Limits); err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	if len(initial) > maxMutationsPerCommit {
		return authoritystore.LoadAndClaimResult{}, authoritystore.ErrCapacityExceeded
	}
	if initialLayout == authoritystore.ModelLayoutV2 {
		mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate, initialLayout, initial, initial, nil)
		if err != nil {
			return authoritystore.LoadAndClaimResult{}, err
		}
		if _, _, err := authoritystore.NormalizeActorModelMutationBorrowed(req.ActorAddress, initial, mutation); err != nil {
			return authoritystore.LoadAndClaimResult{}, err
		}
	}
	v2Manifest, err := exactV2ManifestLP(req.ActorAddress.Domain)
	if err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}

	tag, err := s.keys.HashTag(bucket)
	if err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	shardKey, err := s.keys.ShardAuthority(bucket)
	if err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	actorKey, err := s.keys.ActorRecord(bucket, req.ActorAddress.ActorID)
	if err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	if err := rediskey.AssertSameSlot(shardKey, actorKey); err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}

	args := []interface{}{
		tag, s.layout,
		req.ShardWriteFence.ExpectedHolderID, req.ShardWriteFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ShardWriteFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ShardWriteFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ShardWriteFence.ExpectedLeaseEpoch, 10),
		req.ClaimOperationID, req.NewWriterClaimID,
		SchemaVersion, s.opts.LimitProfileVersion,
		digestHex(businessStateDigest(initial)), manifestLP(initial),
		strconv.Itoa(len(initial)),
	}
	for _, name := range sortedModelNames(initial) {
		args = append(args, name, redisBinaryArg(initial[name]))
	}
	args = append(args,
		strconv.Itoa(s.opts.Limits.MaxModelFields),
		strconv.Itoa(s.opts.Limits.MaxModelBytes),
		strconv.Itoa(s.opts.Limits.MaxReceipts),
		strconv.Itoa(s.opts.MaxReceiptTotalBytes),
		strconv.Itoa(s.opts.Limits.MaxResolutionReceipts),
		initialLayout.String(), v2Manifest)

	reply, err := s.registry.Run(ctx, ScriptLoadAndClaimActor, []string{shardKey.String(), actorKey.String()}, args...)
	if err != nil {
		return s.loadAndClaimTransportError(err)
	}
	return s.decodeLoadAndClaimReply(ctx, bucket, req.ActorAddress, reply)
}

func (s *Store) loadAndClaimTransportError(err error) (authoritystore.LoadAndClaimResult, error) {
	if isContractViolation(err) {
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: %v", errScriptContract, err)
	}
	if classifyWriteError(err) == fateNotSent {
		return authoritystore.LoadAndClaimResult{Status: authoritystore.LoadAndClaimUnavailableNotSent}, nil
	}
	return authoritystore.LoadAndClaimResult{Status: authoritystore.LoadAndClaimTimeoutUnknown}, nil
}

// decodeLoadAndClaimReply decodes the V3 script's bounded metadata snapshot.
func (s *Store) decodeLoadAndClaimReply(ctx context.Context, bucket rediskey.AuthorityBucketRef, address authoritystore.ActorAddress, reply []string) (authoritystore.LoadAndClaimResult, error) {
	if len(reply) == 0 {
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "WRITER_EPOCH_MISMATCH":
		return authoritystore.LoadAndClaimResult{Status: authoritystore.LoadAndClaimWriterEpochMismatch}, nil
	case "LAYOUT_MISMATCH":
		return authoritystore.LoadAndClaimResult{}, authoritystore.ErrAuthorityAddressMismatch
	case "CAPACITY_EXCEEDED":
		return authoritystore.LoadAndClaimResult{}, authoritystore.ErrCapacityExceeded
	case "MALFORMED_RECORD":
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: actor record metadata", errMalformedBlob)
	case "CLAIMED", "ALREADY_CLAIMED", "RECONCILE_REQUIRED":
	default:
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: load status %q", errScriptContract, reply[0])
	}
	if len(reply) < 17 {
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: short load reply", errScriptContract)
	}
	preloadedHashLen, err := strconv.ParseInt(reply[16], 10, 64)
	if err != nil || preloadedHashLen < 0 {
		return authoritystore.LoadAndClaimResult{}, fmt.Errorf("%w: load hash length", errScriptContract)
	}
	preloadedValues := make([]interface{}, len(reply)-17)
	for i := 17; i < len(reply); i++ {
		preloadedValues[i-17] = reply[i]
	}
	snapshot, err := s.snapshotFromMeta(ctx, bucket, address, snapshotMeta{
		recordVersion:     reply[1],
		digest:            reply[2],
		modelManifest:     reply[3],
		writerClaimID:     reply[4],
		schemaVersion:     reply[5],
		writerEpoch:       reply[6],
		modelItems:        reply[7],
		modelBytes:        reply[8],
		receiptItems:      reply[9],
		receiptBytes:      reply[10],
		receiptManifest:   reply[11],
		resolutionItems:   reply[12],
		authorityLayout:   reply[13],
		limitProfile:      reply[14],
		modelLayout:       reply[15],
		preloaded:         true,
		preloadedReceipts: reply[0] != "ALREADY_CLAIMED",
		preloadedHashLen:  preloadedHashLen,
		preloadedValues:   preloadedValues,
	})
	if err != nil {
		return authoritystore.LoadAndClaimResult{}, err
	}
	status := map[string]authoritystore.LoadAndClaimStatus{
		"CLAIMED":            authoritystore.LoadAndClaimClaimed,
		"ALREADY_CLAIMED":    authoritystore.LoadAndClaimAlreadyClaimed,
		"RECONCILE_REQUIRED": authoritystore.LoadAndClaimReconcileRequired,
	}[reply[0]]
	return authoritystore.LoadAndClaimResult{Status: status, Snapshot: snapshot}, nil
}

// ── Commit ──────────────────────────────────────────────────────

func (s *Store) Commit(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	return s.commit(ctx, req, false, false, false)
}

// CommitAuditedClaim is used only by an Authority Binding established through
// LoadAndClaim. The Redis script still checks the exact Fence, WriterClaim,
// RecordVersion, targeted receipt, model state and all capacity limits; it can
// avoid re-reading every unrelated historical receipt because this claim's
// acquisition performed that full audit and Commit is their only mutation
// path.
func (s *Store) CommitAuditedClaim(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	return s.commit(ctx, req, true, false, false)
}

// CommitSoftFlush keeps the audited V15 write boundary while omitting the
// redundant full ModelFields clone from a fresh COMMITTED response. The actor
// Binding already owns the exact frozen candidate and needs only version,
// digest and physical-usage metadata to install its persisted baseline.
func (s *Store) CommitSoftFlush(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	return s.commit(ctx, req, true, false, true)
}

// CommitExact is the only Redis Store path that selects V14 and captures a
// request fingerprint. It retains the complete ordinary Commit audit; the
// audited-claim optimization never gains exact-output behavior implicitly.
func (s *Store) CommitExact(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	return s.commit(ctx, req, false, true, false)
}

func (s *Store) commit(ctx context.Context, req authoritystore.AuthorityCommitRequest, auditedClaim, exact, omitFreshModelState bool) (authoritystore.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.CommitResult{}, err
	}
	bucket, err := s.bucketFor(req.ActorAddress)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	if err := s.checkLayout(req.AuthorityLayoutVersion); err != nil {
		return authoritystore.CommitResult{}, err
	}
	if err := assertGroupDerived(bucket, req.ActorAddress, req.AssertedAuthorityGroup); err != nil {
		return authoritystore.CommitResult{}, err
	}
	var stampInput authoritystore.AuthorityCommitRequest
	if exact {
		// Retain the exact caller shape across adapter-local normalization only
		// for the explicit proof capability.
		stampInput = req
	}
	schedules, err := authoritystore.NormalizeAttachedSchedules(req.AttachedSchedules, req.IdempotencyKey.SubjectPlayerID, s.opts.Limits)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	req.AttachedSchedules = schedules
	mutation, legacyReplace, err := authoritystore.NormalizeActorModelMutationBorrowed(req.ActorAddress, req.CandidateState, req.ModelMutation)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	if err := s.validateCommitRequest(req, mutation, auditedClaim); err != nil {
		return authoritystore.CommitResult{}, err
	}
	var stampRequest *commitstamp.CapturedRequest
	if exact && len(req.AttachedOrders) > 0 {
		captured, err := commitstamp.CaptureExactRequest(stampInput)
		if err != nil {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: commit stamp capture: %v", authoritystore.ErrInvalidRequest, err)
		}
		stampRequest = &captured
	}
	digest, err := req.IdempotencyKey.OperationDigest()
	if err != nil {
		return authoritystore.CommitResult{}, fmt.Errorf("%w: idempotency key: %v", authoritystore.ErrInvalidRequest, err)
	}
	keyIdentity, err := keyIdentityLP(req.IdempotencyKey)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	outcomeStatus, err := terminalStatusToken(req.OperationResult.Status)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	receiptRef, err := workidentity.EncodeReceiptRefV1(req.ActorAddress.ActorID, "AUTHORITY_RECEIPT", digest)
	if err != nil {
		return authoritystore.CommitResult{}, fmt.Errorf("%w: receipt ref: %v", authoritystore.ErrInvalidRequest, err)
	}

	tag, err := s.keys.HashTag(bucket)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	shardKey, _ := s.keys.ShardAuthority(bucket)
	actorKey, _ := s.keys.ActorRecord(bucket, req.ActorAddress.ActorID)
	dueKey, _ := s.keys.SourceDueIndex(bucket)
	scheduleDueKey, _ := s.keys.SourceScheduleDueIndex(bucket)
	receiptDueKey, _ := s.keys.ReceiptDueIndex(bucket)
	bucketMetaKey, _ := s.keys.BucketMeta(bucket)

	orderKeys := make([]string, 0, len(req.AttachedOrders))
	orderRefs := make([]string, 0, len(req.AttachedOrders))
	for _, order := range req.AttachedOrders {
		workRef, err := workidentity.EncodeWorkRefV1(order.OrderType, order.OrderID)
		if err != nil {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: order ref: %v", authoritystore.ErrInvalidRequest, err)
		}
		orderKey, err := s.keys.SourceOrder(bucket, order.OrderType, order.OrderID)
		if err != nil {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: order key: %v", authoritystore.ErrInvalidRequest, err)
		}
		orderKeys = append(orderKeys, orderKey.String())
		orderRefs = append(orderRefs, workRef)
	}
	scheduleKeys := make([]string, 0, len(req.AttachedSchedules))
	scheduleRefs := make([]string, 0, len(req.AttachedSchedules))
	for _, schedule := range req.AttachedSchedules {
		scheduleRef, err := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
		if err != nil {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: schedule ref: %v", authoritystore.ErrInvalidRequest, err)
		}
		scheduleKey, err := s.keys.SourceSchedule(bucket, schedule.ScheduleType, schedule.ScheduleID)
		if err != nil {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: schedule key: %v", authoritystore.ErrInvalidRequest, err)
		}
		scheduleKeys = append(scheduleKeys, scheduleKey.String())
		scheduleRefs = append(scheduleRefs, scheduleRef)
	}
	scriptKeys := []string{shardKey.String(), actorKey.String(), dueKey.String(), receiptDueKey.String(), bucketMetaKey.String()}
	scriptKeys = append(scriptKeys, orderKeys...)
	scriptKeys = append(scriptKeys, scheduleKeys...)
	scheduleIndexMode := s.opts.ScheduleDueIndexMode.commitIndexMode()
	if scheduleIndexMode > 0 {
		scriptKeys = append(scriptKeys, scheduleDueKey.String())
	}
	if err := rediskey.AssertSameSlot(shardKey, actorKey, dueKey, receiptDueKey, bucketMetaKey); err != nil {
		return authoritystore.CommitResult{}, err
	}
	if scheduleIndexMode > 0 {
		if err := rediskey.AssertSameSlot(shardKey, scheduleDueKey); err != nil {
			return authoritystore.CommitResult{}, err
		}
	}
	for _, orderKey := range orderKeys {
		if err := rediskey.AssertSameSlot(shardKey, rediskey.RedisKey(orderKey)); err != nil {
			return authoritystore.CommitResult{}, err
		}
	}
	for _, scheduleKey := range scheduleKeys {
		if err := rediskey.AssertSameSlot(shardKey, rediskey.RedisKey(scheduleKey)); err != nil {
			return authoritystore.CommitResult{}, err
		}
	}

	args := []interface{}{
		tag,
		req.ShardWriteFence.ExpectedHolderID, req.ShardWriteFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ShardWriteFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ShardWriteFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ShardWriteFence.ExpectedLeaseEpoch, 10),
		req.ExpectedWriterClaimID,
		strconv.FormatUint(req.ExpectedRecordVersion, 10),
		s.layout, string(req.ActorAddress.Domain),
		strconv.FormatUint(req.ActorAddress.ActorID, 10),
		strconv.FormatUint(uint64(bucket.VirtualShard()), 10),
		digest, digestHex(req.CanonicalPayloadHash),
		keyIdentity,
		req.Receipt.ReceiptID,
		strconv.FormatUint(req.IdempotencyKey.SubjectPlayerID, 10),
		req.IdempotencyKey.OperationID,
		outcomeStatus, req.OperationResult.ErrorCode, redisBinaryArg(req.OperationResult.Payload),
		digestHex(mutation.ResultingBusinessStateDigest),
		SchemaVersion, s.opts.LimitProfileVersion, receiptRef,
		strconv.Itoa(len(mutation.ModelUpserts)),
	}
	for _, name := range sortedModelNames(mutation.ModelUpserts) {
		args = append(args, name, redisBinaryArg(mutation.ModelUpserts[name]))
	}
	args = append(args, namesManifestLP(mutation.ResultingManifest), strconv.Itoa(len(req.AttachedOrders)))
	for i, order := range req.AttachedOrders {
		payloadHash := order.PayloadHash
		if payloadHash == (authoritystore.PayloadHash{}) {
			// Older non-Care callers did not carry a semantic order hash. Keep
			// their physical record self-consistent while Care production always
			// supplies the canonical Participant hash at Farm commit time.
			payloadHash = authoritystore.PayloadHash(sha256.Sum256(order.Payload))
		}
		operationID := strings.TrimSpace(order.OperationID)
		if operationID == "" {
			operationID = req.IdempotencyKey.OperationID
		}
		args = append(args,
			order.OrderID, order.OrderType, redisBinaryArg(order.Payload),
			strconv.FormatInt(order.DueAtMs, 10), orderRefs[i], digestHex(payloadHash),
			strconv.FormatUint(order.SubjectPlayerID, 10), operationID)
	}
	args = append(args, strconv.Itoa(len(req.AttachedSchedules)))
	for i, schedule := range req.AttachedSchedules {
		args = append(args,
			schedule.ScheduleID, schedule.ScheduleType, redisBinaryArg(schedule.Payload),
			strconv.FormatInt(schedule.DueAtMs, 10), strconv.FormatUint(schedule.Generation, 10),
			scheduleRefs[i], digestHex(schedule.PayloadHash),
			strconv.FormatUint(schedule.SubjectPlayerID, 10))
	}
	args = append(args, strconv.Itoa(len(mutation.ModelDeletes)))
	for _, name := range mutation.ModelDeletes {
		args = append(args, name)
	}
	intent := "UPDATE"
	if mutation.Intent == authoritystore.ModelMutationIntentMigrateV1ToV2 {
		intent = "MIGRATE_V1_TO_V2"
	}
	v2Manifest, err := exactV2ManifestLP(req.ActorAddress.Domain)
	if err != nil {
		return authoritystore.CommitResult{}, err
	}
	legacyToken := "0"
	if legacyReplace {
		legacyToken = "1"
	}
	args = append(args, intent, mutation.ResultingModelLayout.String(),
		strconv.Itoa(mutation.ResultingTotalBytes), digestHex(mutation.ResultingBusinessStateDigest),
		v2Manifest, legacyToken)
	args = append(args, strconv.Itoa(scheduleIndexMode))
	args = append(args,
		strconv.Itoa(s.opts.Limits.MaxModelFields),
		strconv.Itoa(s.opts.Limits.MaxModelBytes),
		strconv.Itoa(s.opts.Limits.MaxReceipts),
		strconv.Itoa(s.opts.MaxReceiptTotalBytes),
		strconv.Itoa(s.opts.MaxBucketPendingWork),
		strconv.Itoa(s.opts.MaxBucketPendingWorkBytes),
		strconv.Itoa(s.opts.MaxOrderPayloadBytes),
		strconv.Itoa(s.opts.MaxSchedulePayloadBytes),
		strconv.FormatInt(s.opts.ReceiptFullRetentionMs, 10),
		strconv.FormatInt(s.opts.WorkTotalDeadlineMs, 10),
		strconv.FormatUint(s.opts.WorkMaxRetryCount, 10),
		strconv.Itoa(s.opts.Limits.MaxSchedules))
	if auditedClaim {
		args = append(args, "1")
	} else {
		args = append(args, "0")
	}
	candidateNames := mutation.ResultingManifest
	args = append(args, strconv.Itoa(len(candidateNames)))
	for _, name := range candidateNames {
		value := req.CandidateState[name]
		if auditedClaim {
			if _, changed := mutation.ModelUpserts[name]; !changed {
				// V15 proves unchanged fields from the LoadAndClaim audit plus the
				// WriterClaim/RecordVersion CAS. Keep the positional placeholder
				// required by the frozen Lua envelope, but do not send the full
				// unchanged Actor field over the Redis connection again.
				value = nil
			}
		}
		args = append(args, redisBinaryArg(value))
	}

	script := ScriptCommitActorWithOrders
	if stampRequest != nil {
		script = ScriptCommitActorWithOrdersExact
	}
	reply, err := s.registry.Run(ctx, script, scriptKeys, args...)
	if err != nil {
		return s.commitTransportError(err)
	}
	return s.decodeCommitReply(ctx, bucket, stampRequest, req, mutation, reply, omitFreshModelState)
}

func (s *Store) validateCommitRequest(req authoritystore.AuthorityCommitRequest, mutation *authoritystore.ActorModelMutation, auditedClaim bool) error {
	if err := validateStableID("commit_request_id", req.CommitRequestID); err != nil {
		return err
	}
	if err := validateStableID("expected_writer_claim_id", req.ExpectedWriterClaimID); err != nil {
		return err
	}
	if req.ExpectedRecordVersion == 0 {
		return fmt.Errorf("%w: expected record version", authoritystore.ErrInvalidRequest)
	}
	if req.Receipt.ReceiptID == "" || req.Receipt.IdempotencyKey != req.IdempotencyKey ||
		req.Receipt.CanonicalPayloadHash != req.CanonicalPayloadHash {
		return fmt.Errorf("%w: receipt does not match commit", authoritystore.ErrInvalidRequest)
	}
	// The receipt must store exactly the terminal outcome being committed.
	if req.Receipt.Outcome.Status != req.OperationResult.Status ||
		req.Receipt.Outcome.ErrorCode != req.OperationResult.ErrorCode ||
		!bytes.Equal(req.Receipt.Outcome.Payload, req.OperationResult.Payload) {
		return fmt.Errorf("%w: receipt outcome does not match commit", authoritystore.ErrInvalidRequest)
	}
	if len(req.PendingOutbox) > 0 {
		// Pending-8: no frozen physical layout for PendingOutbox yet; fail
		// closed instead of inventing keys.
		return fmt.Errorf("%w: pending outbox has no frozen physical layout", authoritystore.ErrInvalidRequest)
	}
	if err := validateModelState(req.CandidateState, s.opts.Limits); err != nil {
		return err
	}
	if mutation == nil {
		return fmt.Errorf("%w: normalized model mutation required", authoritystore.ErrInvalidRequest)
	}
	if len(mutation.ModelUpserts) > maxMutationsPerCommit || len(mutation.ModelDeletes) > maxMutationsPerCommit {
		return authoritystore.ErrCapacityExceeded
	}
	for name := range mutation.ModelUpserts {
		if !validModelFieldName(name) {
			return fmt.Errorf("%w: model upsert name", authoritystore.ErrInvalidRequest)
		}
	}
	for _, name := range mutation.ModelDeletes {
		if !validModelFieldName(name) {
			return fmt.Errorf("%w: model delete name", authoritystore.ErrInvalidRequest)
		}
	}
	if len(req.CandidateState) > maxMutationsPerCommit || len(req.AttachedOrders) > maxOrdersPerCommit || len(req.AttachedSchedules) > maxSchedulesPerCommit {
		return authoritystore.ErrCapacityExceeded
	}
	if _, err := terminalStatusToken(req.OperationResult.Status); err != nil {
		return err
	}
	seenOrders := make(map[string]struct{}, len(req.AttachedOrders))
	for _, order := range req.AttachedOrders {
		if order.OrderID == "" || order.OrderType == "" {
			return fmt.Errorf("%w: attached order", authoritystore.ErrInvalidRequest)
		}
		if err := workidentity.ValidateStableOpaqueID(order.OrderID); err != nil {
			return fmt.Errorf("%w: order id: %v", authoritystore.ErrInvalidRequest, err)
		}
		if _, dup := seenOrders[order.OrderID]; dup {
			return fmt.Errorf("%w: duplicate order", authoritystore.ErrInvalidRequest)
		}
		seenOrders[order.OrderID] = struct{}{}
		if len(order.Payload) > s.opts.MaxOrderPayloadBytes || len(order.Payload) > maxOrderPayloadHard {
			return authoritystore.ErrCapacityExceeded
		}
	}
	seenSchedules := make(map[string]struct{}, len(req.AttachedSchedules))
	for _, schedule := range req.AttachedSchedules {
		if schedule.ScheduleID == "" || schedule.ScheduleType == "" || schedule.Generation == 0 || schedule.DueAtMs < 0 {
			return fmt.Errorf("%w: attached schedule", authoritystore.ErrInvalidRequest)
		}
		key := schedule.ScheduleType + "\x00" + schedule.ScheduleID
		if _, dup := seenSchedules[key]; dup {
			return fmt.Errorf("%w: duplicate schedule", authoritystore.ErrInvalidRequest)
		}
		seenSchedules[key] = struct{}{}
		if len(schedule.Payload) > s.opts.MaxSchedulePayloadBytes || len(schedule.Payload) > maxOrderPayloadHard {
			return authoritystore.ErrCapacityExceeded
		}
	}
	// Receipt value hard cap (64 hash prefix + LP framing of the full key
	// identity, receipt id, terminal outcome and first-seen timestamp).
	receiptValueLen, err := redisReceiptValueBytes(req)
	if err != nil {
		return err
	}
	caps, err := s.PhysicalCapabilities()
	if err != nil {
		return err
	}
	if receiptValueLen > caps.MaxSingleReceiptBytes || redisCommitArgvBytesForMode(req, mutation, auditedClaim) > caps.MaxCommitBytes {
		return authoritystore.ErrCapacityExceeded
	}
	return nil
}

func redisReceiptValueBytes(req authoritystore.AuthorityCommitRequest) (int, error) {
	keyIdentity, err := keyIdentityLP(req.IdempotencyKey)
	if err != nil {
		return 0, err
	}
	// Lua uses six lp1 fields. Reserve the same maximum status/timestamp widths
	// as the existing Go pre-check; all remaining components are exact.
	return 64 + 6*4 + len(keyIdentity) + len(req.Receipt.ReceiptID) +
		16 + len(req.OperationResult.ErrorCode) + len(req.OperationResult.Payload) + 20, nil
}

func redisCommitArgvBytes(req authoritystore.AuthorityCommitRequest, mutation *authoritystore.ActorModelMutation) int {
	return redisCommitArgvBytesForMode(req, mutation, false)
}

func redisCommitArgvBytesForMode(req authoritystore.AuthorityCommitRequest, mutation *authoritystore.ActorModelMutation, auditedClaim bool) int {
	total := len(req.OperationResult.Payload)
	// Generic commits append every CandidateState value after the sparse V12
	// envelope. V15 audited claims keep the same positional envelope but send
	// an empty placeholder for unchanged fields; only upserted candidate values
	// are physically repeated and charged twice.
	for name, value := range req.CandidateState {
		if auditedClaim && mutation != nil {
			if _, changed := mutation.ModelUpserts[name]; !changed {
				continue
			}
		}
		total += len(value)
	}
	if mutation != nil {
		for name, value := range mutation.ModelUpserts {
			total += len(name) + len(value)
		}
		for _, name := range mutation.ModelDeletes {
			total += len(name)
		}
		for _, name := range mutation.ResultingManifest {
			total += len(name)
		}
	}
	for _, order := range req.AttachedOrders {
		total += len(order.OrderID) + len(order.OrderType) + len(order.Payload) + len(order.PayloadHash)
	}
	for _, schedule := range req.AttachedSchedules {
		total += authoritystore.AttachedScheduleSerializedBytes(schedule)
	}
	receiptBytes, err := redisReceiptValueBytes(req)
	if err != nil {
		return total
	}
	return total + receiptBytes
}

func (s *Store) commitTransportError(err error) (authoritystore.CommitResult, error) {
	if isContractViolation(err) {
		return authoritystore.CommitResult{}, fmt.Errorf("%w: %v", errScriptContract, err)
	}
	if classifyWriteError(err) == fateNotSent {
		return authoritystore.CommitResult{Status: authoritystore.CommitUnavailableNotSent}, nil
	}
	return authoritystore.CommitResult{Status: authoritystore.CommitTimeoutUnknown}, nil
}

func (s *Store) decodeCommitReply(ctx context.Context, bucket rediskey.AuthorityBucketRef, stampRequest *commitstamp.CapturedRequest, req authoritystore.AuthorityCommitRequest, mutation *authoritystore.ActorModelMutation, reply []string, omitFreshModelState bool) (authoritystore.CommitResult, error) {
	if len(reply) == 0 {
		return authoritystore.CommitResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "COMMITTED":
		if len(reply) < 5 {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: short committed reply", errScriptContract)
		}
		newVersion, err := strconv.ParseUint(reply[1], 10, 64)
		if err != nil || newVersion != req.ExpectedRecordVersion+1 {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: commit version", errScriptContract)
		}
		usage, err := decodePhysicalUsage(reply[2], reply[3], reply[4], newVersion)
		if err != nil || usage.ModelBytes != mutation.ResultingTotalBytes ||
			usage.ReceiptItems > s.opts.Limits.MaxReceipts || usage.ReceiptBytes > s.opts.MaxReceiptTotalBytes {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: committed physical usage", errScriptContract)
		}
		var stamp authoritystore.CommitStamp
		if stampRequest != nil {
			if len(reply) < 6 {
				return authoritystore.CommitResult{}, fmt.Errorf("%w: exact commit time missing", errScriptContract)
			}
			committedAtMs, parseErr := strconv.ParseInt(reply[5], 10, 64)
			if parseErr != nil || committedAtMs <= 0 {
				return authoritystore.CommitResult{}, fmt.Errorf("%w: committed authority time", errScriptContract)
			}
			stamp = commitstamp.IssueFirstCommitted(*stampRequest, newVersion, committedAtMs)
			if stamp == nil {
				return authoritystore.CommitResult{}, fmt.Errorf("%w: invalid first-commit stamp output", errScriptContract)
			}
		}
		var committedModelFields authoritystore.ModelState
		if !omitFreshModelState {
			committedModelFields = cloneModelState(req.CandidateState)
		}
		return authoritystore.CommitResult{
			Status: authoritystore.CommitCommitted,
			Snapshot: authoritystore.AuthoritySnapshot{
				ActorAddress:        req.ActorAddress,
				WriterEpoch:         req.ShardWriteFence.ExpectedWriterEpoch,
				WriterClaimID:       req.ExpectedWriterClaimID,
				RecordVersion:       newVersion,
				ModelLayout:         mutation.ResultingModelLayout,
				BusinessStateDigest: mutation.ResultingBusinessStateDigest,
				ModelFields:         committedModelFields,
				PhysicalUsage:       usage,
			},
			Outcome:     cloneOutcome(req.OperationResult),
			CommitStamp: stamp,
		}, nil
	case "ALREADY_COMMITTED":
		if len(reply) < 2 {
			return authoritystore.CommitResult{}, fmt.Errorf("%w: short already-committed reply", errScriptContract)
		}
		receipt, err := parseReceiptValue(reply[1])
		if err != nil {
			return authoritystore.CommitResult{}, err
		}
		snapshot, err := s.loadCurrentSnapshot(ctx, bucket, req.ActorAddress)
		if err != nil {
			return authoritystore.CommitResult{}, err
		}
		return authoritystore.CommitResult{Status: authoritystore.CommitAlreadyCommitted, Snapshot: snapshot, Outcome: receipt.outcome()}, nil
	case "IDEMPOTENCY_CONFLICT", "RECORD_VERSION_CONFLICT", "WRITER_CLAIM_MISMATCH":
		snapshot, err := s.loadCurrentSnapshot(ctx, bucket, req.ActorAddress)
		if err != nil {
			return authoritystore.CommitResult{}, err
		}
		status := map[string]authoritystore.AuthorityCommitStatus{
			"IDEMPOTENCY_CONFLICT":    authoritystore.CommitIdempotencyConflict,
			"RECORD_VERSION_CONFLICT": authoritystore.CommitRecordVersionConflict,
			"WRITER_CLAIM_MISMATCH":   authoritystore.CommitWriterClaimMismatch,
		}[reply[0]]
		return authoritystore.CommitResult{Status: status, Snapshot: snapshot}, nil
	case "WRITER_EPOCH_MISMATCH":
		return authoritystore.CommitResult{Status: authoritystore.CommitWriterEpochMismatch}, nil
	case "ACTOR_NOT_FOUND":
		return authoritystore.CommitResult{}, authoritystore.ErrActorNotFound
	case "CAPACITY_EXCEEDED":
		return authoritystore.CommitResult{}, authoritystore.ErrCapacityExceeded
	case "MALFORMED_RECORD":
		return authoritystore.CommitResult{}, fmt.Errorf("%w: actor record metadata", errMalformedBlob)
	case "WORK_CONFLICT":
		return authoritystore.CommitResult{}, authoritystore.ErrWorkConflict
	case "SCHEDULE_CONFLICT":
		return authoritystore.CommitResult{}, authoritystore.ErrScheduleConflict
	case "LAYOUT_MISMATCH":
		return authoritystore.CommitResult{}, authoritystore.ErrAuthorityAddressMismatch
	default:
		return authoritystore.CommitResult{}, fmt.Errorf("%w: commit status %q", errScriptContract, reply[0])
	}
}

// ── FenceAndClaim ───────────────────────────────────────────────

func (s *Store) FenceAndClaim(ctx context.Context, req authoritystore.FenceAndClaimRequest) (authoritystore.FenceAndClaimResult, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}
	bucket, err := s.bucketFor(req.ActorAddress)
	if err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}
	if err := s.checkLayout(req.AuthorityLayoutVersion); err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}
	if err := assertGroupDerived(bucket, req.ActorAddress, req.AssertedAuthorityGroup); err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}
	for name, value := range map[string]string{
		"resolve_operation_id": req.ResolveOperationID,
		"old_writer_claim_id":  req.OldWriterClaimID,
		"new_writer_claim_id":  req.NewWriterClaimID,
	} {
		if err := validateStableID(name, value); err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
	}
	if req.OldWriterClaimID == req.NewWriterClaimID || req.BeforeRecordVersion == 0 {
		return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: fence identity/version", authoritystore.ErrInvalidRequest)
	}
	digest, err := req.OriginalIdempotencyKey.OperationDigest()
	if err != nil {
		return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: original idempotency key: %v", authoritystore.ErrInvalidRequest, err)
	}
	keyIdentity, err := keyIdentityLP(req.OriginalIdempotencyKey)
	if err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}

	tag, err := s.keys.HashTag(bucket)
	if err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}
	shardKey, _ := s.keys.ShardAuthority(bucket)
	actorKey, _ := s.keys.ActorRecord(bucket, req.ActorAddress.ActorID)
	if err := rediskey.AssertSameSlot(shardKey, actorKey); err != nil {
		return authoritystore.FenceAndClaimResult{}, err
	}
	resolveSuffix := sha256Hex("receipt:resolve", req.ResolveOperationID)
	requestDigest := resolutionRequestDigest(req, digest)

	args := []interface{}{
		tag,
		req.ShardWriteFence.ExpectedHolderID, req.ShardWriteFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ShardWriteFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ShardWriteFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ShardWriteFence.ExpectedLeaseEpoch, 10),
		req.ResolveOperationID, req.OldWriterClaimID, req.NewWriterClaimID,
		digest, digestHex(req.CanonicalPayloadHash),
		keyIdentity,
		strconv.FormatUint(req.BeforeRecordVersion, 10),
		digestHex(req.BeforeBusinessStateDigest),
		resolveSuffix, requestDigest,
		strconv.Itoa(s.opts.Limits.MaxResolutionReceipts),
		s.layout,
	}
	reply, err := s.registry.Run(ctx, ScriptFenceAndClaimActor, []string{shardKey.String(), actorKey.String()}, args...)
	if err != nil {
		return s.fenceTransportError(err)
	}
	return s.decodeFenceReply(ctx, bucket, req, digest, reply)
}

func (s *Store) fenceTransportError(err error) (authoritystore.FenceAndClaimResult, error) {
	if isContractViolation(err) {
		return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: %v", errScriptContract, err)
	}
	if classifyWriteError(err) == fateNotSent {
		return authoritystore.FenceAndClaimResult{Status: authoritystore.FenceResolutionUnavailableNotSent}, nil
	}
	return authoritystore.FenceAndClaimResult{Status: authoritystore.FenceResolutionTimeoutUnknown}, nil
}

func (s *Store) decodeFenceReply(ctx context.Context, bucket rediskey.AuthorityBucketRef, req authoritystore.FenceAndClaimRequest, originalDigest string, reply []string) (authoritystore.FenceAndClaimResult, error) {
	if len(reply) == 0 {
		return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "COMMITTED", "NOT_COMMITTED", "RECONCILE_REQUIRED":
		if len(reply) == 1 && reply[0] == "RECONCILE_REQUIRED" {
			// Replay of the same resolve operation id with different
			// parameters: the stored resolution is authoritative, so the
			// adapter only reports the conflict.
			snapshot, err := s.loadCurrentSnapshot(ctx, bucket, req.ActorAddress)
			if err != nil {
				return authoritystore.FenceAndClaimResult{}, err
			}
			return authoritystore.FenceAndClaimResult{Status: authoritystore.FenceResolutionReconcileRequired, Snapshot: snapshot}, nil
		}
		if len(reply) < 7 {
			return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: short fence reply", errScriptContract)
		}
		cutVersion, err := strconv.ParseUint(reply[1], 10, 64)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: cut version", errScriptContract)
		}
		cutDigest, err := parseDigestHex(reply[2])
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		status := map[string]authoritystore.FenceResolutionStatus{
			"COMMITTED":          authoritystore.FenceResolutionCommitted,
			"NOT_COMMITTED":      authoritystore.FenceResolutionNotCommitted,
			"RECONCILE_REQUIRED": authoritystore.FenceResolutionReconcileRequired,
		}[reply[0]]
		resolution := authoritystore.ResolutionReceipt{
			ResolveOperationID:        req.ResolveOperationID,
			ActorAddress:              req.ActorAddress,
			OldWriterClaimID:          req.OldWriterClaimID,
			NewWriterClaimID:          req.NewWriterClaimID,
			OriginalIdempotencyKey:    req.OriginalIdempotencyKey,
			CanonicalPayloadHash:      req.CanonicalPayloadHash,
			BeforeRecordVersion:       req.BeforeRecordVersion,
			BeforeBusinessStateDigest: req.BeforeBusinessStateDigest,
			CutRecordVersion:          cutVersion,
			CutBusinessStateDigest:    cutDigest,
			Status:                    status,
			OriginalReceiptID:         reply[6],
		}
		var outcome authoritystore.StoredOperationOutcome
		if status == authoritystore.FenceResolutionCommitted {
			terminal, err := parseTerminalStatus(reply[3])
			if err != nil {
				return authoritystore.FenceAndClaimResult{}, err
			}
			outcome = authoritystore.StoredOperationOutcome{Status: terminal, ErrorCode: reply[4], Payload: []byte(reply[5])}
		}
		snapshot, err := s.loadCurrentSnapshot(ctx, bucket, req.ActorAddress)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		return authoritystore.FenceAndClaimResult{Status: status, Snapshot: snapshot, Outcome: outcome, Resolution: resolution}, nil
	case "RESOLUTION_REPLAY":
		if len(reply) < 2 {
			return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: short replay reply", errScriptContract)
		}
		decoded, err := parseResolutionValue(reply[1])
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		beforeDigest, err := parseDigestHex(decoded.BeforeDigest)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		cutDigest, err := parseDigestHex(decoded.CutDigest)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		payloadHash, err := parseDigestHex(decoded.OriginalHash)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		resolution := authoritystore.ResolutionReceipt{
			ResolveOperationID:        decoded.ResolveOperationID,
			ActorAddress:              req.ActorAddress,
			OldWriterClaimID:          decoded.OldWriterClaimID,
			NewWriterClaimID:          decoded.NewWriterClaimID,
			OriginalIdempotencyKey:    decoded.OriginalKey,
			CanonicalPayloadHash:      payloadHash,
			BeforeRecordVersion:       decoded.BeforeVersion,
			BeforeBusinessStateDigest: beforeDigest,
			CutRecordVersion:          decoded.CutVersion,
			CutBusinessStateDigest:    cutDigest,
			Status:                    decoded.Status,
			OriginalReceiptID:         decoded.OriginalReceiptID,
		}
		var outcome authoritystore.StoredOperationOutcome
		if decoded.Status == authoritystore.FenceResolutionCommitted {
			terminal, err := parseTerminalStatus(decoded.OutStatus)
			if err != nil {
				return authoritystore.FenceAndClaimResult{}, err
			}
			outcome = authoritystore.StoredOperationOutcome{Status: terminal, ErrorCode: decoded.OutErrorCode, Payload: []byte(decoded.OutPayload)}
		}
		snapshot, err := s.loadCurrentSnapshot(ctx, bucket, req.ActorAddress)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		return authoritystore.FenceAndClaimResult{Status: decoded.Status, Snapshot: snapshot, Outcome: outcome, Resolution: resolution}, nil
	case "WRITER_CLAIM_MISMATCH":
		snapshot, err := s.loadCurrentSnapshot(ctx, bucket, req.ActorAddress)
		if err != nil {
			return authoritystore.FenceAndClaimResult{}, err
		}
		return authoritystore.FenceAndClaimResult{Status: authoritystore.FenceResolutionWriterClaimMismatch, Snapshot: snapshot}, nil
	case "WRITER_EPOCH_MISMATCH":
		return authoritystore.FenceAndClaimResult{Status: authoritystore.FenceResolutionWriterEpochMismatch}, nil
	case "ACTOR_NOT_FOUND":
		return authoritystore.FenceAndClaimResult{}, authoritystore.ErrActorNotFound
	case "CAPACITY_EXCEEDED":
		return authoritystore.FenceAndClaimResult{}, authoritystore.ErrCapacityExceeded
	case "LAYOUT_MISMATCH":
		return authoritystore.FenceAndClaimResult{}, authoritystore.ErrAuthorityAddressMismatch
	default:
		return authoritystore.FenceAndClaimResult{}, fmt.Errorf("%w: fence status %q", errScriptContract, reply[0])
	}
}

// ── LookupIdempotencyOutcome ────────────────────────────────────

func (s *Store) LookupIdempotencyOutcome(ctx context.Context, req authoritystore.IdempotencyLookupRequest) (authoritystore.IdempotencyLookupResult, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.IdempotencyLookupResult{}, err
	}
	bucket, err := s.bucketFor(req.ActorAddress)
	if err != nil {
		return authoritystore.IdempotencyLookupResult{}, err
	}
	digest, err := req.IdempotencyKey.OperationDigest()
	if err != nil {
		return authoritystore.IdempotencyLookupResult{}, fmt.Errorf("%w: idempotency key: %v", authoritystore.ErrInvalidRequest, err)
	}
	actorKey, err := s.keys.ActorRecord(bucket, req.ActorAddress.ActorID)
	if err != nil {
		return authoritystore.IdempotencyLookupResult{}, err
	}
	value, err := s.client.HGet(ctx, actorKey.String(), "receipt:op:"+digest).Result()
	if errors.Is(err, redis.Nil) {
		return authoritystore.IdempotencyLookupResult{Status: authoritystore.IdempotencyNotFound}, nil
	}
	if err != nil {
		// Read-only lookup: never escalate to TIMEOUT_UNKNOWN.
		return authoritystore.IdempotencyLookupResult{}, fmt.Errorf("redisauthority: lookup unavailable: %w", err)
	}
	receipt, err := parseReceiptValue(value)
	if err != nil {
		return authoritystore.IdempotencyLookupResult{}, err
	}
	// The digest only located the receipt; the stored full key and payload
	// hash decide match vs conflict.
	if receipt.Key != req.IdempotencyKey || value[:64] != digestHex(req.CanonicalPayloadHash) {
		return authoritystore.IdempotencyLookupResult{Status: authoritystore.IdempotencyHashConflict, ReceiptRef: receipt.ReceiptID}, nil
	}
	return authoritystore.IdempotencyLookupResult{
		Status:     authoritystore.IdempotencyFoundMatch,
		Outcome:    receipt.outcome(),
		ReceiptRef: receipt.ReceiptID,
	}, nil
}

// ── bounded snapshot reads ──────────────────────────────────────

type snapshotMeta struct {
	recordVersion     string
	digest            string
	modelManifest     string
	writerClaimID     string
	writerEpoch       string
	modelItems        string
	modelBytes        string
	receiptItems      string
	receiptBytes      string
	receiptManifest   string
	resolutionItems   string
	schemaVersion     string
	authorityLayout   string
	limitProfile      string
	modelLayout       string
	preloaded         bool
	preloadedReceipts bool
	preloadedHashLen  int64
	preloadedValues   []interface{}
}

var snapshotMetaFields = []string{
	"_meta:record_version", "_meta:business_state_digest", "_meta:model_manifest",
	"_meta:writer_claim_id", "_meta:writer_epoch", "_meta:model_total_bytes",
	"_meta:model_field_count", "_meta:receipt_count", "_meta:receipt_bytes",
	"_meta:receipt_manifest", "_meta:resolution_count", "_meta:schema_version",
	"_meta:authority_layout_version", "_meta:limit_profile_version",
	"_meta:model_layout_version",
}

const actorRecordFixedMetaFieldsV1Legacy = 16
const actorRecordFixedMetaFields = 17

func (s *Store) loadCurrentSnapshot(ctx context.Context, bucket rediskey.AuthorityBucketRef, address authoritystore.ActorAddress) (authoritystore.AuthoritySnapshot, error) {
	actorKey, err := s.keys.ActorRecord(bucket, address.ActorID)
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	values, err := s.client.HMGet(ctx, actorKey.String(), snapshotMetaFields...).Result()
	if err != nil {
		// A follow-up read failure after a definitive script reply is not a
		// write-unknown; surface it as a plain error so the caller retries
		// the idempotent operation.
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("redisauthority: snapshot read unavailable: %w", err)
	}
	if len(values) != len(snapshotMetaFields) {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: short snapshot metadata", errMalformedBlob)
	}
	meta := snapshotMeta{}
	for i, value := range values {
		if snapshotMetaFields[i] == "_meta:model_layout_version" && value == nil {
			continue
		}
		valueString, ok := asString(value)
		if !ok {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: missing %s", errMalformedBlob, snapshotMetaFields[i])
		}
		switch snapshotMetaFields[i] {
		case "_meta:record_version":
			meta.recordVersion = valueString
		case "_meta:business_state_digest":
			meta.digest = valueString
		case "_meta:model_manifest":
			meta.modelManifest = valueString
		case "_meta:writer_claim_id":
			meta.writerClaimID = valueString
		case "_meta:writer_epoch":
			meta.writerEpoch = valueString
		case "_meta:model_field_count":
			meta.modelItems = valueString
		case "_meta:model_total_bytes":
			meta.modelBytes = valueString
		case "_meta:receipt_count":
			meta.receiptItems = valueString
		case "_meta:receipt_bytes":
			meta.receiptBytes = valueString
		case "_meta:receipt_manifest":
			meta.receiptManifest = valueString
		case "_meta:resolution_count":
			meta.resolutionItems = valueString
		case "_meta:schema_version":
			meta.schemaVersion = valueString
		case "_meta:authority_layout_version":
			meta.authorityLayout = valueString
		case "_meta:limit_profile_version":
			meta.limitProfile = valueString
		case "_meta:model_layout_version":
			meta.modelLayout = valueString
		}
	}
	return s.snapshotFromMeta(ctx, bucket, address, meta)
}

// snapshotFromMeta assembles an AuthoritySnapshot from record meta plus a
// bounded HMGET over the manifest (never HGETALL).
func (s *Store) snapshotFromMeta(ctx context.Context, bucket rediskey.AuthorityBucketRef, address authoritystore.ActorAddress, meta snapshotMeta) (authoritystore.AuthoritySnapshot, error) {
	recordVersion, err := parsePositiveUint(meta.recordVersion, "record version")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	writerEpoch, err := parsePositiveUint(meta.writerEpoch, "writer epoch")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	if meta.writerClaimID == "" || meta.schemaVersion != SchemaVersion ||
		meta.authorityLayout != s.layout || meta.limitProfile == "" {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored actor metadata identity", errMalformedBlob)
	}
	modelLayout, explicitModelLayout, err := parseStoredModelLayout(meta.modelLayout)
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	digest, err := parseDigestHex(meta.digest)
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	maxStoredModelFields := s.opts.Limits.MaxModelFields
	if maxStoredModelFields > maxMutationsPerCommit {
		maxStoredModelFields = maxMutationsPerCommit
	}
	modelItems, err := parseStoredCount(meta.modelItems, maxStoredModelFields, "model item usage")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	modelBytes, err := parseStoredCount(meta.modelBytes, s.opts.Limits.MaxModelBytes, "model byte usage")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	receiptItems, err := parseStoredCount(meta.receiptItems, s.opts.Limits.MaxReceipts, "receipt item usage")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	receiptBytes, err := parseStoredCount(meta.receiptBytes, s.opts.MaxReceiptTotalBytes, "receipt byte usage")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	resolutionItems, err := parseStoredCount(meta.resolutionItems, s.opts.Limits.MaxResolutionReceipts, "resolution item usage")
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	if (receiptItems == 0 && (receiptBytes != 0 || meta.receiptManifest != "")) ||
		(receiptItems > 0 && receiptBytes == 0) {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored receipt counters disagree", errMalformedBlob)
	}

	if len(meta.modelManifest) > maxStoredModelFields*(4+64) ||
		len(meta.receiptManifest) > s.opts.Limits.MaxReceipts*(4+64) {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored manifest exceeds bounded envelope", errMalformedBlob)
	}
	names, err := parseManifest(meta.modelManifest)
	if err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	if len(names) != modelItems {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored model manifest count mismatch", errMalformedBlob)
	}
	if modelLayout == authoritystore.ModelLayoutV2 {
		if err := fieldv2.ValidateExactNames(address.Domain, names); err != nil {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: v2 model manifest", errMalformedBlob)
		}
	}
	receiptDigests, err := parseManifest(meta.receiptManifest)
	if err != nil || len(receiptDigests) != receiptItems {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored receipt manifest count mismatch", errMalformedBlob)
	}

	seenNames := make(map[string]struct{}, len(names))
	fields := make([]string, 0, len(names)+len(receiptDigests))
	for _, name := range names {
		if !validModelFieldName(name) {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored model field %q", errMalformedBlob, name)
		}
		if _, duplicate := seenNames[name]; duplicate {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: duplicate stored model field %q", errMalformedBlob, name)
		}
		seenNames[name] = struct{}{}
		fields = append(fields, "model:"+name)
	}
	seenReceipts := make(map[string]struct{}, len(receiptDigests))
	for _, receiptDigest := range receiptDigests {
		if _, err := parseDigestHex(receiptDigest); err != nil {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: receipt manifest digest", errMalformedBlob)
		}
		if _, duplicate := seenReceipts[receiptDigest]; duplicate {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: duplicate receipt manifest digest", errMalformedBlob)
		}
		seenReceipts[receiptDigest] = struct{}{}
		if !meta.preloaded || meta.preloadedReceipts {
			fields = append(fields, "receipt:op:"+receiptDigest)
		}
	}

	fixedFields := actorRecordFixedMetaFieldsV1Legacy
	if explicitModelLayout {
		fixedFields = actorRecordFixedMetaFields
	}
	expectedHashLen := int64(fixedFields + modelItems + receiptItems + resolutionItems)
	var values []interface{}
	var observedHashLen int64
	if meta.preloaded {
		values = meta.preloadedValues
		observedHashLen = meta.preloadedHashLen
	} else {
		actorKey, keyErr := s.keys.ActorRecord(bucket, address.ActorID)
		if keyErr != nil {
			return authoritystore.AuthoritySnapshot{}, keyErr
		}
		pipe := s.client.Pipeline()
		var valuesCmd *redis.SliceCmd
		if len(fields) > 0 {
			valuesCmd = pipe.HMGet(ctx, actorKey.String(), fields...)
		}
		hashLenCmd := pipe.HLen(ctx, actorKey.String())
		if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("redisauthority: bounded snapshot read unavailable: %w", pipeErr)
		}
		observedHashLen = hashLenCmd.Val()
		if valuesCmd != nil {
			values, err = valuesCmd.Result()
			if err != nil {
				return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: bounded snapshot values", errMalformedBlob)
			}
		}
	}
	if observedHashLen != expectedHashLen {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: actor hash field count mismatch", errMalformedBlob)
	}
	if len(values) != len(fields) {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: bounded snapshot values", errMalformedBlob)
	}

	snapshot := authoritystore.AuthoritySnapshot{
		ActorAddress:        address,
		WriterClaimID:       meta.writerClaimID,
		WriterEpoch:         writerEpoch,
		RecordVersion:       recordVersion,
		ModelLayout:         modelLayout,
		BusinessStateDigest: digest,
		ModelFields:         authoritystore.ModelState{},
	}
	calculatedModelBytes := 0
	for i, name := range names {
		valueString, ok := asString(values[i])
		if !ok {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: manifest field %q missing", errMalformedBlob, name)
		}
		snapshot.ModelFields[name] = []byte(valueString)
		calculatedModelBytes += len(name) + len(valueString)
	}
	if calculatedModelBytes != modelBytes || businessStateDigest(snapshot.ModelFields) != digest {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored model aggregate mismatch", errMalformedBlob)
	}
	if modelLayout == authoritystore.ModelLayoutV2 {
		if err := fieldv2.ValidateExactFields(address.Domain, snapshot.ModelFields); err != nil {
			return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored v2 model fields", errMalformedBlob)
		}
	}
	calculatedReceiptBytes := receiptBytes
	if !meta.preloaded || meta.preloadedReceipts {
		calculatedReceiptBytes = 0
		for i, receiptDigest := range receiptDigests {
			valueString, ok := asString(values[len(names)+i])
			if !ok || len(valueString) > maxReceiptValueBytes {
				return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: receipt manifest field missing or oversized", errMalformedBlob)
			}
			receipt, err := parseReceiptValue(valueString)
			if err != nil {
				return authoritystore.AuthoritySnapshot{}, err
			}
			storedDigest, err := receipt.Key.OperationDigest()
			if err != nil || storedDigest != receiptDigest {
				return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: receipt manifest identity mismatch", errMalformedBlob)
			}
			calculatedReceiptBytes += len(valueString)
		}
	}
	if calculatedReceiptBytes != receiptBytes {
		return authoritystore.AuthoritySnapshot{}, fmt.Errorf("%w: stored receipt byte usage mismatch", errMalformedBlob)
	}
	snapshot.PhysicalUsage = authoritystore.PhysicalUsage{
		Trustworthy:   true,
		RecordVersion: recordVersion,
		ModelBytes:    modelBytes,
		ReceiptItems:  receiptItems,
		ReceiptBytes:  receiptBytes,
	}
	if err := snapshot.PhysicalUsage.Validate(snapshot.RecordVersion); err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	return snapshot, nil
}

func parseStoredModelLayout(value string) (authoritystore.ModelLayoutVersion, bool, error) {
	switch value {
	case "":
		return authoritystore.ModelLayoutV1, false, nil
	case "1":
		return authoritystore.ModelLayoutV1, true, nil
	case "2":
		return authoritystore.ModelLayoutV2, true, nil
	default:
		return authoritystore.ModelLayoutUnknown, false, fmt.Errorf("%w: stored model layout", errMalformedBlob)
	}
}

func exactV2ManifestLP(domain actorrouting.RouteDomain) (string, error) {
	names, err := fieldv2.ExactNames(domain)
	if err != nil {
		return "", fmt.Errorf("%w: v2 schema", authoritystore.ErrInvalidRequest)
	}
	var manifest []byte
	for _, name := range names {
		manifest = appendLP(manifest, name)
	}
	return string(manifest), nil
}

func decodePhysicalUsage(modelBytesValue, receiptItemsValue, receiptBytesValue string, recordVersion uint64) (authoritystore.PhysicalUsage, error) {
	modelBytes, err := parseStoredCount(modelBytesValue, int(^uint(0)>>1), "stored model byte usage")
	if err != nil {
		return authoritystore.PhysicalUsage{}, err
	}
	receiptItems, err := parseStoredCount(receiptItemsValue, int(^uint(0)>>1), "stored receipt item usage")
	if err != nil {
		return authoritystore.PhysicalUsage{}, err
	}
	receiptBytes, err := parseStoredCount(receiptBytesValue, int(^uint(0)>>1), "stored receipt byte usage")
	if err != nil {
		return authoritystore.PhysicalUsage{}, err
	}
	usage := authoritystore.PhysicalUsage{
		Trustworthy:   true,
		RecordVersion: recordVersion,
		ModelBytes:    modelBytes,
		ReceiptItems:  receiptItems,
		ReceiptBytes:  receiptBytes,
	}
	return usage, usage.Validate(recordVersion)
}

func parsePositiveUint(value, name string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%w: stored %s", errMalformedBlob, name)
	}
	return parsed, nil
}

func parseStoredCount(value string, maximum int, name string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 || parsed > maximum {
		return 0, fmt.Errorf("%w: %s", errMalformedBlob, name)
	}
	return parsed, nil
}

func modelStatePhysicalBytes(state authoritystore.ModelState) int {
	total := 0
	for name, value := range state {
		total += len(name) + len(value)
	}
	return total
}

func sortedModelNames(state authoritystore.ModelState) []string {
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sortStrings(names)
	return names
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
