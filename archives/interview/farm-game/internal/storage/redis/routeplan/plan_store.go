package routeplan

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	model "farmgame/internal/routeplan"
)

const (
	planFieldSchema         = "schema"
	planFieldID             = "plan_id"
	planFieldRevision       = "revision"
	planFieldJSON           = "canonical_json"
	planFieldChecksum       = "checksum"
	planFieldUpdated        = "updated_at_ms"
	planFieldExpires        = "expires_at_ms"
	planFieldPhase          = "phase"
	planFieldAttempt        = "attempt"
	planFieldStarted        = "started_at_ms"
	planFieldComplete       = "completed_at_ms"
	metaSchema              = "route-plan-meta-v1"
	planPhysicalV1          = "route-plan-v1"
	planPhysicalV2          = "route-plan-v2"
	stopAuditSchema         = "route-plan-stop-audit-v1"
	stopAuditFieldSchema    = "schema"
	stopAuditFieldOwner     = "owner_scale_plan_id"
	stopAuditFieldOperation = "operation_id"
	stopAuditFieldEpoch     = "fence_epoch"
)

type PlanStore struct {
	client        redis.UniversalClient
	registry      *scriptRegistry
	keys          KeyBuilder
	options       Options
	reconcileGate chan struct{}
}

type reconcilePlan struct {
	member         string
	key            string
	record         model.PlanRecord
	recordBytes    int
	physicalExpiry int64
	restart        bool
}

type reconcileExpired struct {
	member         string
	key            string
	physicalExpiry int64
	record         model.PlanRecord
	recordBytes    int
	present        bool
}

type reconcileBatch struct {
	live    []reconcilePlan
	expired []reconcileExpired
}

type reconcileIndexMember struct {
	member         string
	physicalExpiry int64
}

var (
	_ model.PlanStore        = (*PlanStore)(nil)
	_ model.DurableStopStore = (*PlanStore)(nil)
)

// StopOwnedPlanForScaleTermination terminalizes one owned child with one Lua
// mutation. The pre-read supplies the canonical candidate, while the script
// rechecks the exact current envelope and shared references before writing.
func (s *PlanStore) StopOwnedPlanForScaleTermination(ctx context.Context, command model.DurableStopCommand) (model.DurableStopResult, error) {
	unsafe := func(record model.PlanRecord, err error) (model.DurableStopResult, error) {
		return model.DurableStopResult{Status: model.DurableStopUnsafe, Record: record}, err
	}
	if s == nil || s.registry == nil {
		return unsafe(model.PlanRecord{}, ErrInvalidKey)
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return unsafe(model.PlanRecord{}, err)
	}
	defer cancel()
	if err := command.Validate(); err != nil {
		return unsafe(model.PlanRecord{}, err)
	}
	planKey, err := s.keys.Plan(command.PlanID)
	if err != nil {
		return unsafe(model.PlanRecord{}, err)
	}
	current, found, err := s.loadKey(opCtx, command.PlanID, planKey)
	if err != nil {
		return unsafe(model.PlanRecord{}, err)
	}
	if !found {
		return model.DurableStopResult{Status: model.DurableStopNotFound}, nil
	}
	if current.Plan.OwnerScalePlanID != command.OwnerScalePlanID {
		return model.DurableStopResult{Status: model.DurableStopNotOwned, Record: current}, nil
	}

	candidate := current
	terminalStatus := ""
	if current.Plan.Phase.Terminal() {
		if current.Plan.Phase == model.PhasePaused && current.Plan.ErrorCode == model.DurableStopErrorCode {
			terminalStatus = "ALREADY_STOPPED"
		} else {
			terminalStatus = "ALREADY_TERMINAL"
		}
	} else {
		if current.Plan.Revision != command.ExpectedRevision ||
			(command.ExpectedPhase != model.PhaseUnspecified && current.Plan.Phase != command.ExpectedPhase) {
			return model.DurableStopResult{Status: model.DurableStopRevisionConflict, Record: current}, nil
		}
		if current.Plan.Revision == ^uint64(0) || current.Plan.Revision >= uint64(1<<53-1) {
			return unsafe(current, model.ErrPlanConflict)
		}
		candidate.Plan = current.Plan.Clone()
		if !model.CanAdvancePhase(candidate.Plan.Phase, model.PhasePaused) {
			return unsafe(current, model.ErrPlanPhase)
		}
		candidate.Plan.Phase = model.PhasePaused
		candidate.Plan.ErrorCode = model.DurableStopErrorCode
		candidate.Plan.Revision++
		candidate.Plan.UpdatedAtMs = command.NowMs
		candidate.Plan.CompletedAtMs = command.NowMs
		if err := candidate.Plan.Validate(); err != nil {
			return unsafe(current, err)
		}
		candidate.Envelope, err = model.EncodePlan(candidate.Plan)
		if err != nil {
			return unsafe(current, err)
		}
	}

	member, err := s.keys.EncodedPlanID(command.PlanID)
	if err != nil {
		return unsafe(current, err)
	}
	if err := assertStringKeysSameSlot([]rediskeyLike{
		rediskeyLike(s.keys.Meta().String()), rediskeyLike(s.keys.Index().String()),
		rediskeyLike(s.keys.Restart().String()), rediskeyLike(s.keys.Accounting().String()),
		rediskeyLike(planKey.String()), rediskeyLike(s.keys.key("stop-audit:" + member).String()),
	}); err != nil {
		return unsafe(current, err)
	}
	auditKey := s.keys.key("stop-audit:" + member)
	expectedPhase := ""
	if command.ExpectedPhase != model.PhaseUnspecified {
		expectedPhase = command.ExpectedPhase.String()
	}
	args := []any{
		s.keys.HashTag(), member, strconv.FormatUint(command.ExpectedRevision, 10), expectedPhase,
		strconv.FormatInt(command.NowMs, 10), strconv.Itoa(s.options.MaxItems), strconv.Itoa(s.options.MaxBytes),
		strconv.Itoa(s.options.MaxRecordBytes), strconv.FormatInt(s.options.AuditRetention.Milliseconds(), 10),
		current.Envelope.SchemaVersion, current.Plan.PlanID, strconv.FormatUint(current.Plan.Revision, 10),
		string(current.Envelope.CanonicalJSON), current.Envelope.Checksum,
		strconv.FormatInt(current.Envelope.UpdatedAtMs, 10), strconv.FormatInt(current.Envelope.ExpiresAtMs, 10),
		current.Plan.Phase.String(), strconv.FormatUint(current.Plan.Attempt, 10),
		strconv.FormatInt(current.Plan.StartedAtMs, 10), strconv.FormatInt(current.Plan.CompletedAtMs, 10),
		candidate.Envelope.SchemaVersion, candidate.Plan.PlanID, strconv.FormatUint(candidate.Plan.Revision, 10),
		string(candidate.Envelope.CanonicalJSON), candidate.Envelope.Checksum,
		strconv.FormatInt(candidate.Envelope.UpdatedAtMs, 10), strconv.FormatInt(candidate.Envelope.ExpiresAtMs, 10),
		candidate.Plan.Phase.String(), strconv.FormatUint(candidate.Plan.Attempt, 10),
		strconv.FormatInt(candidate.Plan.StartedAtMs, 10), strconv.FormatInt(candidate.Plan.CompletedAtMs, 10),
		terminalStatus, command.OwnerScalePlanID, command.Fence.OperationID(), strconv.FormatUint(command.Fence.Epoch(), 10),
	}
	reply, err := s.registry.runNamed(opCtx, scriptStopOwnedPlan,
		[]string{s.keys.Meta().String(), s.keys.Index().String(), s.keys.Restart().String(), s.keys.Accounting().String(), planKey.String(), auditKey.String()}, args...)
	if err != nil {
		return unsafe(current, err)
	}
	if len(reply) == 0 {
		return unsafe(current, fmt.Errorf("%w: empty durable stop reply", model.ErrPlanCorrupt))
	}
	switch reply[0] {
	case "COMMITTED":
		return model.DurableStopResult{Status: model.DurableStopStopped, Record: candidate}, nil
	case "ALREADY_STOPPED":
		return model.DurableStopResult{Status: model.DurableStopAlreadyStopped, Record: current}, nil
	case "ALREADY_TERMINAL":
		return model.DurableStopResult{Status: model.DurableStopAlreadyTerminal, Record: current}, nil
	case "REVISION_CONFLICT":
		return s.classifyDurableStopConflict(opCtx, command, current)
	case "CAPACITY_EXCEEDED":
		return unsafe(current, model.ErrPlanCapacity)
	case "CORRUPT":
		return unsafe(current, fmt.Errorf("%w: durable stop %s", model.ErrPlanCorrupt, strings.Join(reply[1:], ",")))
	case "CROSS_SLOT":
		return unsafe(current, fmt.Errorf("%w: durable stop CROSS_SLOT", model.ErrPlanCorrupt))
	default:
		return unsafe(current, fmt.Errorf("%w: durable stop status=%q", model.ErrPlanCorrupt, reply[0]))
	}
}

func (s *PlanStore) classifyDurableStopConflict(ctx context.Context, command model.DurableStopCommand, fallback model.PlanRecord) (model.DurableStopResult, error) {
	key, err := s.keys.Plan(command.PlanID)
	if err != nil {
		return model.DurableStopResult{Status: model.DurableStopUnsafe, Record: fallback}, err
	}
	current, found, err := s.loadKey(ctx, command.PlanID, key)
	if err != nil {
		return model.DurableStopResult{Status: model.DurableStopUnsafe, Record: fallback}, err
	}
	if !found {
		return model.DurableStopResult{Status: model.DurableStopNotFound}, nil
	}
	if current.Plan.OwnerScalePlanID != command.OwnerScalePlanID {
		return model.DurableStopResult{Status: model.DurableStopNotOwned, Record: current}, nil
	}
	if current.Plan.Phase.Terminal() {
		status := model.DurableStopAlreadyTerminal
		if current.Plan.Phase == model.PhasePaused && current.Plan.ErrorCode == model.DurableStopErrorCode {
			status = model.DurableStopAlreadyStopped
		}
		return model.DurableStopResult{Status: status, Record: current}, nil
	}
	return model.DurableStopResult{Status: model.DurableStopRevisionConflict, Record: current}, nil
}

func NewPlanStore(client redis.UniversalClient, options Options) (*PlanStore, error) {
	if client == nil {
		return nil, ErrInvalidKey
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	keys, err := NewKeyBuilder(normalized.Namespace)
	if err != nil {
		return nil, err
	}
	registry, err := newScriptRegistry(client)
	if err != nil {
		return nil, err
	}
	return &PlanStore{client: client, registry: registry, keys: keys, options: normalized, reconcileGate: make(chan struct{}, 1)}, nil
}

func NewRedisPlanStore(client redis.UniversalClient, options Options) (*PlanStore, error) {
	return NewPlanStore(client, options)
}

func (s *PlanStore) Client() redis.UniversalClient {
	if s == nil {
		return nil
	}
	return s.client
}

func (s *PlanStore) Keys() KeyBuilder {
	if s == nil {
		return KeyBuilder{}
	}
	return s.keys
}

func (s *PlanStore) ScriptSHA(name string) (string, bool) {
	if s == nil {
		return "", false
	}
	return s.registry.scriptSHA(name)
}

func (s *PlanStore) EnsureLoaded(ctx context.Context) error {
	if s == nil || s.registry == nil {
		return ErrInvalidKey
	}
	op, cancel, err := s.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return s.registry.load(op)
}

func (s *PlanStore) Load(ctx context.Context, id string) (model.PlanRecord, bool, error) {
	if s == nil {
		return model.PlanRecord{}, false, ErrInvalidKey
	}
	if err := model.ValidatePlanID(id); err != nil {
		return model.PlanRecord{}, false, err
	}
	key, err := s.keys.Plan(id)
	if err != nil {
		return model.PlanRecord{}, false, err
	}
	op, cancel, err := s.operationContext(ctx)
	if err != nil {
		return model.PlanRecord{}, false, err
	}
	defer cancel()
	return s.loadKey(op, id, key)
}

func (s *PlanStore) loadKey(ctx context.Context, id string, key interface{ String() string }) (model.PlanRecord, bool, error) {
	keyName := key.String()
	member, err := s.keys.EncodedPlanID(id)
	if err != nil {
		return model.PlanRecord{}, false, err
	}
	reply, err := s.registry.runNamed(ctx, scriptLoadPlan,
		[]string{keyName, s.keys.Index().String(), s.keys.Accounting().String()},
		strconv.Itoa(model.MaxCanonicalPlanBytes), strconv.Itoa(model.MaxPlanEnvelopeBytes), member,
		strconv.FormatInt(s.options.AuditRetention.Milliseconds(), 10))
	if err != nil {
		return model.PlanRecord{}, false, err
	}
	if len(reply) == 1 && reply[0] == "NOT_FOUND" {
		return model.PlanRecord{}, false, nil
	}
	if len(reply) > 0 && reply[0] == "CORRUPT" {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan atomic read %s", model.ErrPlanCorrupt, strings.Join(reply[1:], ","))
	}
	legacy := len(reply) == 9 && reply[0] == "FOUND_V1"
	current := len(reply) == 12 && reply[0] == "FOUND"
	if !legacy && !current {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan atomic read reply", model.ErrPlanCorrupt)
	}
	values := reply[1:]
	physicalSchema := planPhysicalV2
	if legacy {
		physicalSchema = planPhysicalV1
	}
	if values[0] != physicalSchema || values[1] != id {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan identity", model.ErrPlanCorrupt)
	}
	revision, err := strconv.ParseUint(values[2], 10, 64)
	if err != nil || strconv.FormatUint(revision, 10) != values[2] {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan revision", model.ErrPlanCorrupt)
	}
	updated, err := strconv.ParseInt(values[5], 10, 64)
	if err != nil || updated < 0 || strconv.FormatInt(updated, 10) != values[5] {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan updated timestamp", model.ErrPlanCorrupt)
	}
	expires, err := strconv.ParseInt(values[6], 10, 64)
	if err != nil || expires < 0 || strconv.FormatInt(expires, 10) != values[6] {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan expiry", model.ErrPlanCorrupt)
	}
	canonicalSchema := model.PlanSchemaVersion
	if strings.HasPrefix(values[3], `{"schema_version":"`+model.LegacyPlanSchemaVersion+`",`) {
		canonicalSchema = model.LegacyPlanSchemaVersion
	}
	plan, err := model.DecodePlan(model.PlanEnvelope{SchemaVersion: canonicalSchema, PlanID: values[1], Revision: revision,
		CanonicalJSON: []byte(values[3]), Checksum: values[4], UpdatedAtMs: updated, ExpiresAtMs: expires})
	if err != nil {
		return model.PlanRecord{}, true, err
	}
	if !s.planLifetimeWithinLimit(plan) {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan lifetime exceeds configured TTL", model.ErrPlanCorrupt)
	}
	if plan.Phase.String() != values[7] {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan phase tuple", model.ErrPlanCorrupt)
	}
	if legacy {
		return model.PlanRecord{Plan: plan, Envelope: model.PlanEnvelope{SchemaVersion: canonicalSchema, PlanID: plan.PlanID,
			Revision: plan.Revision, CanonicalJSON: []byte(values[3]), Checksum: values[4], UpdatedAtMs: updated, ExpiresAtMs: expires}}, true, nil
	}
	attempt, err := strconv.ParseUint(values[8], 10, 64)
	if err != nil || strconv.FormatUint(attempt, 10) != values[8] || plan.Attempt != attempt {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan attempt tuple", model.ErrPlanCorrupt)
	}
	started, err := strconv.ParseInt(values[9], 10, 64)
	if err != nil || started < 0 || strconv.FormatInt(started, 10) != values[9] || plan.StartedAtMs != started {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan started tuple", model.ErrPlanCorrupt)
	}
	completed, err := strconv.ParseInt(values[10], 10, 64)
	if err != nil || completed < 0 || strconv.FormatInt(completed, 10) != values[10] || plan.CompletedAtMs != completed {
		return model.PlanRecord{}, true, fmt.Errorf("%w: plan completed tuple", model.ErrPlanCorrupt)
	}
	return model.PlanRecord{Plan: plan, Envelope: model.PlanEnvelope{SchemaVersion: canonicalSchema, PlanID: plan.PlanID,
		Revision: plan.Revision, CanonicalJSON: []byte(values[3]), Checksum: values[4], UpdatedAtMs: updated, ExpiresAtMs: expires}}, true, nil
}

func (s *PlanStore) CompareAndSwap(ctx context.Context, expectedRevision uint64, plan model.Plan) (model.CASResult, error) {
	if s == nil || s.registry == nil {
		return model.CASResult{}, ErrInvalidKey
	}
	envelope, err := model.EncodePlan(plan)
	if err != nil {
		return model.CASResult{}, err
	}
	if !s.planLifetimeWithinLimit(plan) {
		return model.CASResult{}, fmt.Errorf("%w: logical lifetime exceeds configured TTL", model.ErrInvalidPlan)
	}
	if _, ok := s.physicalRetainUntil(plan); !ok {
		return model.CASResult{}, fmt.Errorf("%w: owner retention exceeds configured audit horizon", model.ErrInvalidPlan)
	}
	if expectedRevision == 0 && plan.Revision == 1 {
		if err := model.ValidateInitialPlan(plan); err != nil {
			return model.CASResult{Status: model.CASRevisionConflict}, err
		}
	}
	if len(envelope.CanonicalJSON) > s.options.MaxRecordBytes {
		return model.CASResult{Status: model.CASCapacityExceeded}, model.ErrPlanCapacity
	}
	planKey, err := s.keys.Plan(plan.PlanID)
	if err != nil {
		return model.CASResult{}, err
	}
	encodedID, err := s.keys.EncodedPlanID(plan.PlanID)
	if err != nil {
		return model.CASResult{}, err
	}
	op, cancel, err := s.operationContext(ctx)
	if err != nil {
		return model.CASResult{}, err
	}
	defer cancel()
	if expectedRevision > 0 {
		return s.compareAndSwapExisting(op, planKey, encodedID, expectedRevision, plan, envelope)
	}
	result, needsFullProof, err := s.compareAndSwapCreate(op, planKey, encodedID, plan, envelope)
	if err != nil || !needsFullProof {
		return result, err
	}
	return s.compareAndSwapCreateFull(op, planKey, encodedID, plan, envelope)
}

// compareAndSwapCreate validates the maintained aggregate in O(1) and creates
// one initial record without materializing every existing Plan. It deliberately
// falls back to the bounded full proof when the shared retention tail must grow:
// the shared-tail schema requires every retained Plan hash to receive that same
// absolute TTL. Foreground batches freeze CreatedAtMs, so only the first member
// of a batch can require that fallback.
func (s *PlanStore) compareAndSwapCreate(ctx context.Context, planKey interface{ String() string }, member string,
	candidate model.Plan, envelope model.PlanEnvelope) (model.CASResult, bool, error) {
	keys := []string{s.keys.Meta().String(), s.keys.Index().String(), s.keys.Restart().String(),
		s.keys.Accounting().String(), planKey.String()}
	redisKeys := make([]rediskeyLike, 0, len(keys))
	for _, key := range keys {
		redisKeys = append(redisKeys, rediskeyLike(key))
	}
	if err := assertStringKeysSameSlot(redisKeys); err != nil {
		return model.CASResult{}, false, err
	}
	active := "0"
	if !candidate.Phase.Terminal() {
		active = "1"
	}
	args := []any{
		s.keys.HashTag(), strconv.FormatInt(s.options.Now().UnixMilli(), 10), strconv.Itoa(s.options.MaxItems),
		strconv.Itoa(s.options.MaxBytes), strconv.Itoa(s.options.MaxRecordBytes), strconv.FormatInt(s.candidateRetention(candidate), 10),
		candidate.PlanID, member, planPhysicalV2, strconv.FormatUint(candidate.Revision, 10),
		string(envelope.CanonicalJSON), envelope.Checksum, strconv.FormatInt(envelope.UpdatedAtMs, 10),
		strconv.FormatInt(envelope.ExpiresAtMs, 10), candidate.Phase.String(), active,
		strconv.FormatUint(candidate.Attempt, 10), strconv.FormatInt(candidate.StartedAtMs, 10), strconv.FormatInt(candidate.CompletedAtMs, 10),
	}
	reply, err := s.registry.runNamedMode(ctx, scriptCompareAndSwapCreatePlan, keys, true, args...)
	if err != nil {
		return model.CASResult{}, false, err
	}
	if len(reply) == 0 {
		return model.CASResult{}, false, model.ErrPlanCorrupt
	}
	record := model.PlanRecord{Plan: candidate, Envelope: envelope}
	switch reply[0] {
	case "COMMITTED":
		return model.CASResult{Status: model.CASCommitted, Record: record}, false, nil
	case "ALREADY_COMMITTED":
		return model.CASResult{Status: model.CASAlreadyCommitted, Record: record}, false, nil
	case "TAIL_EXTENSION_REQUIRED":
		return model.CASResult{}, true, nil
	case "REVISION_CONFLICT":
		return model.CASResult{Status: model.CASRevisionConflict}, false, model.ErrPlanConflict
	case "CAPACITY_EXCEEDED":
		return model.CASResult{Status: model.CASCapacityExceeded}, false, model.ErrPlanCapacity
	case "MALFORMED", "CROSS_SLOT":
		return model.CASResult{}, false, fmt.Errorf("%w: create CAS %s", model.ErrPlanCorrupt, strings.Join(reply, ","))
	default:
		return model.CASResult{}, false, fmt.Errorf("%w: create CAS status=%q", model.ErrPlanCorrupt, reply[0])
	}
}

func (s *PlanStore) compareAndSwapCreateFull(ctx context.Context, planKey interface{ String() string }, encodedID string,
	plan model.Plan, envelope model.PlanEnvelope) (model.CASResult, error) {
	if err := s.acquireReconcile(ctx); err != nil {
		return model.CASResult{}, err
	}
	defer s.releaseReconcile()
	for {
		batch, err := s.prepareReconcile(ctx)
		if err != nil {
			return model.CASResult{}, err
		}
		for _, current := range batch.expired {
			if !current.present || current.record.Plan.PlanID != plan.PlanID {
				continue
			}
			// A physically expired hash is retained only as immutable audit
			// evidence. It may be replayed byte-for-byte, but it can never be
			// advanced or replaced under the same Plan ID.
			if current.record.Plan.Revision != plan.Revision ||
				string(current.record.Envelope.CanonicalJSON) != string(envelope.CanonicalJSON) ||
				current.record.Envelope.Checksum != envelope.Checksum ||
				current.record.Envelope.UpdatedAtMs != envelope.UpdatedAtMs ||
				current.record.Envelope.ExpiresAtMs != envelope.ExpiresAtMs {
				return model.CASResult{Status: model.CASRevisionConflict, Record: current.record}, model.ErrPlanConflict
			}
			break
		}
		for _, current := range batch.live {
			if current.record.Plan.PlanID != plan.PlanID {
				continue
			}
			break
		}
		keys, args, err := s.reconcileInvocation(batch, planKey.String(), "CAS", plan.PlanID, encodedID, 0, envelope, plan.Phase, !plan.Phase.Terminal(), plan)
		if err != nil {
			return model.CASResult{}, err
		}
		reply, err := s.registry.run(ctx, keys, args...)
		if err != nil {
			return model.CASResult{}, err
		}
		if len(reply) == 0 {
			return model.CASResult{}, model.ErrPlanCorrupt
		}
		switch reply[0] {
		case "COMMITTED":
			return model.CASResult{Status: model.CASCommitted, Record: model.PlanRecord{Plan: plan, Envelope: envelope}}, nil
		case "ALREADY_COMMITTED":
			return model.CASResult{Status: model.CASAlreadyCommitted, Record: model.PlanRecord{Plan: plan, Envelope: envelope}}, nil
		case "REVISION_CONFLICT":
			return model.CASResult{Status: model.CASRevisionConflict}, model.ErrPlanConflict
		case "CAPACITY_EXCEEDED":
			return model.CASResult{Status: model.CASCapacityExceeded}, model.ErrPlanCapacity
		case "MALFORMED":
			if len(reply) == 2 && reply[1] == "LIVE_PLAN" {
				// Existing-plan updates deliberately bypass reconcileGate. If one
				// advances after prepareReconcile read the bounded index, the Lua
				// proof reports LIVE_PLAN before making any write. Re-read and retry
				// within the already bounded operation instead of treating normal
				// per-shard progress as durable corruption.
				if err := ctx.Err(); err != nil {
					return model.CASResult{}, err
				}
				continue
			}
			return model.CASResult{}, fmt.Errorf("%w: %s", model.ErrPlanCorrupt, strings.Join(reply[1:], ","))
		default:
			return model.CASResult{}, fmt.Errorf("%w: status=%s", model.ErrPlanCorrupt, reply[0])
		}
	}
}

// compareAndSwapExisting is the per-RoutePlan execution hot path. Loading one
// target preserves Go's complete semantic validation, while the dedicated Lua
// mutation rechecks that exact envelope and its Redis references atomically.
// Creation remains on the full bounded reconciliation path because it must
// prove global capacity before adding a new member.
func (s *PlanStore) compareAndSwapExisting(ctx context.Context, planKey interface{ String() string }, member string,
	expectedRevision uint64, candidate model.Plan, envelope model.PlanEnvelope) (model.CASResult, error) {
	current, found, err := s.loadKey(ctx, candidate.PlanID, planKey)
	if err != nil {
		return model.CASResult{}, err
	}
	if !found {
		return model.CASResult{Status: model.CASRevisionConflict}, model.ErrPlanConflict
	}
	replay := current.Plan.Revision == candidate.Revision &&
		string(current.Envelope.CanonicalJSON) == string(envelope.CanonicalJSON) &&
		current.Envelope.Checksum == envelope.Checksum &&
		current.Envelope.UpdatedAtMs == envelope.UpdatedAtMs &&
		current.Envelope.ExpiresAtMs == envelope.ExpiresAtMs
	if !replay {
		if current.Plan.Revision != expectedRevision || candidate.Revision != expectedRevision+1 {
			return model.CASResult{Status: model.CASRevisionConflict}, model.ErrPlanConflict
		}
		if err := model.ValidatePlanUpdate(current.Plan, candidate); err != nil {
			return model.CASResult{Status: model.CASRevisionConflict, Record: current}, err
		}
	}
	physicalSchema := planPhysicalV2
	if current.Envelope.SchemaVersion == model.LegacyPlanSchemaVersion {
		physicalSchema = planPhysicalV1
	}
	active := "0"
	if !candidate.Phase.Terminal() {
		active = "1"
	}
	keys := []string{s.keys.Meta().String(), s.keys.Index().String(), s.keys.Restart().String(),
		s.keys.Accounting().String(), planKey.String()}
	redisKeys := make([]rediskeyLike, 0, len(keys))
	for _, key := range keys {
		redisKeys = append(redisKeys, rediskeyLike(key))
	}
	if err := assertStringKeysSameSlot(redisKeys); err != nil {
		return model.CASResult{}, err
	}
	args := []any{
		s.keys.HashTag(), strconv.FormatInt(s.options.Now().UnixMilli(), 10), strconv.Itoa(s.options.MaxItems),
		strconv.Itoa(s.options.MaxBytes), strconv.Itoa(s.options.MaxRecordBytes), strconv.FormatInt(s.options.AuditRetention.Milliseconds(), 10),
		candidate.PlanID, member, strconv.FormatUint(expectedRevision, 10),
		physicalSchema, current.Plan.PlanID, strconv.FormatUint(current.Plan.Revision, 10),
		string(current.Envelope.CanonicalJSON), current.Envelope.Checksum,
		strconv.FormatInt(current.Envelope.UpdatedAtMs, 10), strconv.FormatInt(current.Envelope.ExpiresAtMs, 10), current.Plan.Phase.String(),
		strconv.FormatUint(current.Plan.Attempt, 10), strconv.FormatInt(current.Plan.StartedAtMs, 10), strconv.FormatInt(current.Plan.CompletedAtMs, 10),
		planPhysicalV2, candidate.PlanID, strconv.FormatUint(candidate.Revision, 10), string(envelope.CanonicalJSON), envelope.Checksum,
		strconv.FormatInt(envelope.UpdatedAtMs, 10), strconv.FormatInt(envelope.ExpiresAtMs, 10), candidate.Phase.String(), active,
		strconv.FormatUint(candidate.Attempt, 10), strconv.FormatInt(candidate.StartedAtMs, 10), strconv.FormatInt(candidate.CompletedAtMs, 10),
	}
	reply, err := s.registry.runNamedMode(ctx, scriptCompareAndSwapExistingPlan, keys, true, args...)
	if err != nil {
		return model.CASResult{}, err
	}
	if len(reply) == 0 {
		return model.CASResult{}, model.ErrPlanCorrupt
	}
	result := model.PlanRecord{Plan: candidate, Envelope: envelope}
	switch reply[0] {
	case "COMMITTED":
		return model.CASResult{Status: model.CASCommitted, Record: result}, nil
	case "ALREADY_COMMITTED":
		return model.CASResult{Status: model.CASAlreadyCommitted, Record: result}, nil
	case "REVISION_CONFLICT":
		return model.CASResult{Status: model.CASRevisionConflict}, model.ErrPlanConflict
	case "CAPACITY_EXCEEDED":
		return model.CASResult{Status: model.CASCapacityExceeded}, model.ErrPlanCapacity
	case "MALFORMED", "CROSS_SLOT":
		return model.CASResult{}, fmt.Errorf("%w: existing CAS %s", model.ErrPlanCorrupt, strings.Join(reply, ","))
	default:
		return model.CASResult{}, fmt.Errorf("%w: existing CAS status=%q", model.ErrPlanCorrupt, reply[0])
	}
}

func (s *PlanStore) ListIncomplete(ctx context.Context) ([]model.PlanRecord, error) {
	if s == nil {
		return nil, ErrInvalidKey
	}
	op, cancel, err := s.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.acquireReconcile(op); err != nil {
		return nil, err
	}
	defer s.releaseReconcile()
	var batch reconcileBatch
	for {
		batch, err = s.prepareReconcile(op)
		if err != nil {
			return nil, err
		}
		keys, args, invocationErr := s.reconcileInvocation(batch, s.keys.Meta().String(), "READ", "", "", 0, model.PlanEnvelope{}, model.PhaseUnspecified, false, model.Plan{})
		if invocationErr != nil {
			return nil, invocationErr
		}
		reply, runErr := s.registry.run(op, keys, args...)
		if runErr != nil {
			return nil, runErr
		}
		if len(reply) == 1 && reply[0] == "OK" {
			break
		}
		// Existing-plan progress intentionally bypasses reconcileGate. A child
		// can therefore advance after the bounded pre-read but before the READ
		// proof executes. The Lua script proves that exact race without writing;
		// repeat the complete bounded read instead of misclassifying progress as
		// durable corruption. Every other mismatch remains fail-closed.
		if len(reply) == 2 && reply[0] == "MALFORMED" && reply[1] == "LIVE_PLAN" {
			if err := op.Err(); err != nil {
				return nil, err
			}
			continue
		}
		return nil, fmt.Errorf("%w: list reconcile %s", model.ErrPlanCorrupt, strings.Join(reply, ","))
	}
	result := make([]model.PlanRecord, 0, len(batch.live))
	for _, entry := range batch.live {
		plan := entry.record.Plan
		// A ScalePlan-owned PAUSED child remains visible to recovery. New
		// children replay independently per shard; older OwnerWave records
		// continue through the bounded wave compatibility path.
		if !plan.Phase.Terminal() || recoverablePausedPlan(plan) {
			result = append(result, entry.record)
		}
	}
	if len(result) > s.options.MaxRestartRead {
		return nil, fmt.Errorf("%w: restart bound", model.ErrPlanCorrupt)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Plan.UpdatedAtMs == result[j].Plan.UpdatedAtMs {
			return result[i].Plan.PlanID < result[j].Plan.PlanID
		}
		return result[i].Plan.UpdatedAtMs < result[j].Plan.UpdatedAtMs
	})
	return result, nil
}

func recoverablePausedPlan(plan model.Plan) bool {
	if plan.Phase != model.PhasePaused {
		return false
	}
	if plan.Operation == model.OperationCrashTakeover && plan.OwnerScalePlanID == "" {
		switch plan.ErrorCode {
		case "CANCELED", "TIMEOUT", "PROJECTION_RECOVERY", "EXECUTION_FAILED":
			return true
		default:
			return false
		}
	}
	if plan.OwnerScalePlanID == "" {
		return false
	}
	switch plan.ErrorCode {
	case "CANCELED", "TIMEOUT", "PROJECTION_RECOVERY", model.ErrorAuthorityCommittedSnapshotPending:
		return true
	default:
		return false
	}
}

func (s *PlanStore) prepareReconcile(ctx context.Context) (reconcileBatch, error) {
	members, err := s.readBoundedIndex(ctx)
	if err != nil {
		return reconcileBatch{}, err
	}
	nowMs := s.options.Now().UnixMilli()
	seen := make(map[string]struct{}, len(members))
	batch := reconcileBatch{live: make([]reconcilePlan, 0, len(members)), expired: make([]reconcileExpired, 0)}
	for _, item := range members {
		member := item.member
		if _, exists := seen[member]; exists {
			return reconcileBatch{}, fmt.Errorf("%w: duplicate index member", model.ErrPlanCorrupt)
		}
		seen[member] = struct{}{}
		id, err := s.keys.DecodePlanMember(member)
		if err != nil {
			return reconcileBatch{}, fmt.Errorf("%w: index identity", model.ErrPlanCorrupt)
		}
		key, err := s.keys.Plan(id)
		if err != nil {
			return reconcileBatch{}, err
		}
		physical := item.physicalExpiry
		if physical <= nowMs {
			record, found, err := s.loadKey(ctx, id, key)
			if err != nil {
				return reconcileBatch{}, err
			}
			if !found {
				return reconcileBatch{}, fmt.Errorf("%w: indexed expired plan hash missing", model.ErrPlanCorrupt)
			}
			entry := reconcileExpired{member: member, key: key.String(), physicalExpiry: physical, record: record, present: true}
			if !s.planLifetimeWithinLimit(record.Plan) {
				return reconcileBatch{}, fmt.Errorf("%w: expired plan lifetime", model.ErrPlanCorrupt)
			}
			if !s.physicalRetentionWithinLimit(record.Plan, physical) {
				return reconcileBatch{}, fmt.Errorf("%w: expired expiry score", model.ErrPlanCorrupt)
			}
			entry.recordBytes = modelEnvelopeSize(record.Envelope)
			batch.expired = append(batch.expired, entry)
			continue
		}
		record, found, err := s.loadKey(ctx, id, key)
		if err != nil {
			return reconcileBatch{}, err
		}
		if !found {
			return reconcileBatch{}, fmt.Errorf("%w: indexed plan hash missing", model.ErrPlanCorrupt)
		}
		if !s.planLifetimeWithinLimit(record.Plan) {
			return reconcileBatch{}, fmt.Errorf("%w: live plan lifetime", model.ErrPlanCorrupt)
		}
		if !s.physicalRetentionWithinLimit(record.Plan, physical) {
			return reconcileBatch{}, fmt.Errorf("%w: expiry score", model.ErrPlanCorrupt)
		}
		batch.live = append(batch.live, reconcilePlan{member: member, key: key.String(), record: record,
			recordBytes: modelEnvelopeSize(record.Envelope), physicalExpiry: physical,
			restart: !record.Plan.Phase.Terminal() && record.Plan.ExpiresAtMs > nowMs})
	}
	return batch, nil
}

func (s *PlanStore) readBoundedIndex(ctx context.Context) ([]reconcileIndexMember, error) {
	reply, err := s.registry.runNamed(ctx, scriptReadPlanIndex, []string{s.keys.Index().String()},
		strconv.Itoa(s.options.MaxItems), strconv.Itoa(s.options.MaxBytes), strconv.Itoa(s.options.MaxRecordBytes))
	if err != nil {
		return nil, err
	}
	if len(reply) < 2 || reply[0] != "OK" {
		return nil, fmt.Errorf("%w: bounded index %s", model.ErrPlanCorrupt, strings.Join(reply, ","))
	}
	count, err := strconv.Atoi(reply[1])
	if err != nil || count < 0 || count > s.options.MaxItems || strconv.Itoa(count) != reply[1] || len(reply) != 2+count*2 {
		return nil, fmt.Errorf("%w: bounded index reply", model.ErrPlanCorrupt)
	}
	members := make([]reconcileIndexMember, 0, count)
	for index := 0; index < count; index++ {
		member := reply[2+index*2]
		physical, parseErr := strconv.ParseInt(reply[3+index*2], 10, 64)
		if parseErr != nil || physical < 0 || physical > 1<<53-1 || strconv.FormatInt(physical, 10) != reply[3+index*2] {
			return nil, fmt.Errorf("%w: bounded index member", model.ErrPlanCorrupt)
		}
		members = append(members, reconcileIndexMember{member: member, physicalExpiry: physical})
	}
	return members, nil
}

func (s *PlanStore) planLifetimeWithinLimit(plan model.Plan) bool {
	if s == nil {
		return false
	}
	limitMs := s.options.PlanTTL.Milliseconds()
	if limitMs <= 0 || plan.CreatedAtMs < 0 || plan.ExpiresAtMs < plan.CreatedAtMs || plan.CreatedAtMs > int64(^uint64(0)>>1)-limitMs {
		return false
	}
	return plan.ExpiresAtMs <= plan.CreatedAtMs+limitMs
}

func (s *PlanStore) reconcileInvocation(batch reconcileBatch, candidateKey, mode, planID, member string, expected uint64,
	envelope model.PlanEnvelope, phase model.Phase, active bool, candidate model.Plan) ([]string, []any, error) {
	if mode == "CAS" {
		if _, ok := s.physicalRetainUntil(candidate); !ok {
			return nil, nil, model.ErrPlanCorrupt
		}
	}
	keys := []string{s.keys.Meta().String(), s.keys.Index().String(), s.keys.Restart().String(), s.keys.Accounting().String(), candidateKey}
	for _, entry := range batch.live {
		keys = append(keys, entry.key)
	}
	for _, entry := range batch.expired {
		keys = append(keys, entry.key)
	}
	redisKeys := make([]rediskeyLike, 0, len(keys))
	for _, key := range keys {
		redisKeys = append(redisKeys, rediskeyLike(key))
	}
	if err := assertStringKeysSameSlot(redisKeys); err != nil {
		return nil, nil, err
	}
	activeText := "0"
	if active {
		activeText = "1"
	}
	candidateRetentionMs := s.options.AuditRetention.Milliseconds()
	if mode == "CAS" {
		candidateRetentionMs = s.candidateRetention(candidate)
	}
	args := []any{s.keys.HashTag(), mode, strconv.FormatInt(s.options.Now().UnixMilli(), 10), strconv.Itoa(s.options.MaxItems),
		strconv.Itoa(s.options.MaxBytes), strconv.Itoa(s.options.MaxRecordBytes), strconv.FormatInt(candidateRetentionMs, 10),
		planID, member, strconv.FormatUint(expected, 10), strconv.FormatUint(envelope.Revision, 10), string(envelope.CanonicalJSON), envelope.Checksum,
		strconv.FormatInt(envelope.UpdatedAtMs, 10), strconv.FormatInt(envelope.ExpiresAtMs, 10), phase.String(), activeText,
		strconv.FormatUint(candidate.Attempt, 10), strconv.FormatInt(candidate.StartedAtMs, 10), strconv.FormatInt(candidate.CompletedAtMs, 10),
		strconv.Itoa(len(batch.live)), strconv.Itoa(len(batch.expired))}
	for _, entry := range batch.live {
		restart := "0"
		if entry.restart {
			restart = "1"
		}
		args = append(args, entry.member, entry.record.Plan.PlanID, strconv.FormatUint(entry.record.Plan.Revision, 10),
			string(entry.record.Envelope.CanonicalJSON), entry.record.Envelope.Checksum,
			strconv.FormatInt(entry.record.Plan.UpdatedAtMs, 10), strconv.FormatInt(entry.record.Plan.ExpiresAtMs, 10),
			entry.record.Plan.Phase.String(), strconv.FormatUint(entry.record.Plan.Attempt, 10), strconv.FormatInt(entry.record.Plan.StartedAtMs, 10), strconv.FormatInt(entry.record.Plan.CompletedAtMs, 10),
			strconv.Itoa(entry.recordBytes), strconv.FormatInt(entry.physicalExpiry, 10), restart)
	}
	for _, entry := range batch.expired {
		present := "0"
		if entry.present {
			present = "1"
		}
		args = append(args, entry.member, strconv.FormatInt(entry.physicalExpiry, 10), present,
			entry.record.Plan.PlanID, strconv.FormatUint(entry.record.Plan.Revision, 10),
			string(entry.record.Envelope.CanonicalJSON), entry.record.Envelope.Checksum,
			strconv.FormatInt(entry.record.Plan.UpdatedAtMs, 10), strconv.FormatInt(entry.record.Plan.ExpiresAtMs, 10),
			entry.record.Plan.Phase.String(), strconv.FormatUint(entry.record.Plan.Attempt, 10), strconv.FormatInt(entry.record.Plan.StartedAtMs, 10), strconv.FormatInt(entry.record.Plan.CompletedAtMs, 10),
			strconv.Itoa(entry.recordBytes))
	}
	return keys, args, nil
}

type rediskeyLike string

func (k rediskeyLike) String() string { return string(k) }

func assertStringKeysSameSlot(keys []rediskeyLike) error {
	if len(keys) == 0 {
		return model.ErrPlanCorrupt
	}
	tag := ""
	for _, key := range keys {
		value := key.String()
		start := strings.IndexByte(value, '{')
		end := strings.IndexByte(value, '}')
		if start < 0 || end <= start+1 {
			return model.ErrPlanCorrupt
		}
		if tag == "" {
			tag = value[start+1 : end]
		} else if tag != value[start+1:end] {
			return model.ErrPlanCorrupt
		}
	}
	return nil
}

func (s *PlanStore) candidateRetention(plan model.Plan) int64 {
	retentionMs := s.options.AuditRetention.Milliseconds()
	if plan.OwnerRetainUntilMs > plan.ExpiresAtMs {
		requested := plan.OwnerRetainUntilMs - plan.ExpiresAtMs
		if requested > retentionMs {
			retentionMs = requested
		}
	}
	return retentionMs
}

func (s *PlanStore) physicalRetentionWithinLimit(plan model.Plan, physical int64) bool {
	return physical > plan.ExpiresAtMs && physical-plan.ExpiresAtMs <= s.options.AuditRetention.Milliseconds() &&
		(plan.OwnerRetainUntilMs == 0 || physical >= plan.OwnerRetainUntilMs)
}

func (s *PlanStore) physicalRetainUntil(plan model.Plan) (int64, bool) {
	retentionMs := s.candidateRetention(plan)
	if retentionMs <= 0 || retentionMs > s.options.AuditRetention.Milliseconds() {
		return 0, false
	}
	return addRetention(plan.ExpiresAtMs, time.Duration(retentionMs)*time.Millisecond)
}

func addRetention(expiresAtMs int64, retention time.Duration) (int64, bool) {
	retentionMs := retention.Milliseconds()
	if retentionMs <= 0 || expiresAtMs > int64(^uint64(0)>>1)-retentionMs {
		return 0, false
	}
	return expiresAtMs + retentionMs, true
}

func modelEnvelopeSize(envelope model.PlanEnvelope) int {
	return len(envelope.CanonicalJSON) + len(envelope.Checksum) + 64
}

func (s *PlanStore) acquireReconcile(ctx context.Context) error {
	if s == nil || s.reconcileGate == nil {
		return ErrInvalidKey
	}
	select {
	case s.reconcileGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *PlanStore) releaseReconcile() {
	if s == nil || s.reconcileGate == nil {
		return
	}
	<-s.reconcileGate
}

func (s *PlanStore) operationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("redisrouteplan: context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, s.options.OperationTimeout)
	return bounded, cancel, nil
}
