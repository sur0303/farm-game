package sourceorder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/holderkey"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

// DurableCommitEvidence is constructible only from the exact Authority commit
// request and its opaque first-COMMITTED output stamp. It binds the selected
// AttachedOrder to the actor, layout, source fence, authoritative commit time,
// and resulting record version. It is runtime proof material, not a new
// persisted receipt or SourceOrder field.
type DurableCommitEvidence struct {
	sourceRecordVersion uint64
	committedAtMs       int64
	sourceFence         authoritystore.ShardWriteFence
	actorAddress        authoritystore.ActorAddress
	authorityLayout     string
	attachedOrder       authoritystore.AttachedOrder
}

func NewDurableCommitEvidence(request authoritystore.AuthorityCommitRequest, result authoritystore.CommitResult, orderID string) (DurableCommitEvidence, error) {
	commit, explicit := result.ExplicitCommit(request)
	if !explicit || !validProofToken(request.AuthorityLayoutVersion, 64) {
		return DurableCommitEvidence{}, fmt.Errorf("%w: Authority commit is not an exact explicit COMMITTED output", ErrCommitProofInvalid)
	}
	fence := request.ShardWriteFence
	if !validProofToken(fence.ExpectedHolderID, 128) || !validProofToken(fence.ExpectedHolderBootID, 128) ||
		fence.ExpectedAssignmentRevision == 0 || fence.ExpectedWriterEpoch == 0 || fence.ExpectedLeaseEpoch == 0 {
		return DurableCommitEvidence{}, fmt.Errorf("%w: Authority commit fence", ErrCommitProofInvalid)
	}
	var selected *authoritystore.AttachedOrder
	for index := range request.AttachedOrders {
		order := &request.AttachedOrders[index]
		if order.OrderID != orderID {
			continue
		}
		if selected != nil {
			return DurableCommitEvidence{}, fmt.Errorf("%w: duplicate AttachedOrder", ErrCommitProofInvalid)
		}
		selected = order
	}
	if selected == nil || !validProofToken(selected.OrderID, 128) || !validProofToken(selected.OrderType, 64) ||
		!validProofToken(selected.OperationID, 128) || selected.SubjectPlayerID == 0 || selected.DueAtMs <= 0 || len(selected.Payload) == 0 ||
		selected.PayloadHash == (authoritystore.PayloadHash{}) {
		return DurableCommitEvidence{}, fmt.Errorf("%w: exact AttachedOrder", ErrCommitProofInvalid)
	}
	semanticHash, err := canonicalCommitProofOrderHash(*selected)
	if err != nil || authoritystore.PayloadHash(semanticHash) != selected.PayloadHash {
		return DurableCommitEvidence{}, fmt.Errorf("%w: AttachedOrder semantic Participant hash", ErrCommitProofInvalid)
	}
	order := *selected
	order.Payload = append([]byte(nil), selected.Payload...)
	return DurableCommitEvidence{
		sourceRecordVersion: commit.RecordVersion(), committedAtMs: commit.CommittedAtMs(),
		sourceFence: fence, actorAddress: request.ActorAddress,
		authorityLayout: request.AuthorityLayoutVersion, attachedOrder: order,
	}, nil
}

// NewDurableCommitEvidenceFromExact reopens the process-local CommitStamp
// carrier at the Direct boundary. A copied/zero carrier, timeout-unknown
// result, or order not present in the exact source actor commit fails closed.
func NewDurableCommitEvidenceFromExact(exact *authoritystore.ExactCommitEvidence, orderID string) (DurableCommitEvidence, error) {
	if exact == nil {
		return DurableCommitEvidence{}, fmt.Errorf("%w: exact Authority commit evidence is missing", ErrCommitProofInvalid)
	}
	request, result, ok := exact.Open()
	if !ok {
		return DurableCommitEvidence{}, fmt.Errorf("%w: exact Authority commit evidence is invalid", ErrCommitProofInvalid)
	}
	return NewDurableCommitEvidence(request, result, orderID)
}

func canonicalCommitProofOrderHash(order authoritystore.AttachedOrder) ([sha256.Size]byte, error) {
	var command contractspb.ParticipantCommandType
	switch order.OrderType {
	case string(identity.WorkTypeCareReward):
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD
	case string(identity.WorkTypeHarvest):
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD
	default:
		return [sha256.Size]byte{}, fmt.Errorf("%w: unsupported proof order type", ErrCommitProofInvalid)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if len(order.Payload) == 0 || len(order.Payload) > spec.MaxPayloadBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: proof order payload size", ErrCommitProofInvalid)
	}
	payload := spec.NewPayload()
	if err := proto.Unmarshal(order.Payload, payload); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: proof order payload decode", ErrCommitProofInvalid)
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, order.Payload) {
		return [sha256.Size]byte{}, fmt.Errorf("%w: proof order payload encoding", ErrCommitProofInvalid)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: proof order canonical payload: %v", ErrCommitProofInvalid, err)
	}
	return hash, nil
}

func (e DurableCommitEvidence) valid() bool {
	return e.sourceRecordVersion != 0 && e.committedAtMs > 0 && e.actorAddress.ActorID != 0 &&
		validProofToken(e.authorityLayout, 64) && e.attachedOrder.OrderID != "" &&
		e.sourceFence.ExpectedHolderID != "" && e.sourceFence.ExpectedHolderBootID != "" &&
		e.sourceFence.ExpectedAssignmentRevision != 0 && e.sourceFence.ExpectedWriterEpoch != 0 &&
		e.sourceFence.ExpectedLeaseEpoch != 0
}

type CommitProofMintRequest struct {
	Work                    workidentity.WorkAddress
	WorkVersion             uint64
	OperationID             string
	SubjectPlayerID         uint64
	PayloadHash             [sha256.Size]byte
	DueAtMs                 int64
	AuthorityLayoutVersion  string
	DestinationRouteDomain  workidentity.RouteDomain
	DestinationActorID      uint64
	DestinationStep         string
	DestinationInvocationID string
	Commit                  DurableCommitEvidence
}

type CommitProofIssuer struct {
	signer       *CommitProofSigner
	resolver     actorrouting.Resolver
	realm        string
	ttl          time.Duration
	safetyMargin time.Duration
	now          func() time.Time

	// publishedKey is non-empty only for the production holder-boot path. Mint
	// then resolves route, fence, and public-key publication from one atomic
	// RouteSnapshot table before signing.
	publishedKey holderkey.Publication
}

// CommitProofRouteLookup returns route, fence, holder generation, and public
// key from one atomically installed snapshot.
type CommitProofRouteLookup interface {
	LookupRoute(context.Context, actorrouting.Address) (actorrouting.ShardRouteSnapshot, error)
}

func NewCommitProofIssuer(signer *CommitProofSigner, resolver actorrouting.Resolver, realm string, ttl, safetyMargin time.Duration, now func() time.Time) (*CommitProofIssuer, error) {
	if signer == nil || resolver == nil || !validProofToken(realm, 128) || ttl < time.Millisecond || ttl > time.Minute ||
		ttl%time.Millisecond != 0 || safetyMargin < 0 || safetyMargin >= ttl || safetyMargin%time.Millisecond != 0 {
		return nil, fmt.Errorf("%w: issuer configuration", ErrCommitProofInvalid)
	}
	if now == nil {
		now = time.Now
	}
	return &CommitProofIssuer{signer: signer, resolver: resolver, realm: realm, ttl: ttl, safetyMargin: safetyMargin, now: now}, nil
}

// NewPublishedCommitProofIssuer is the production constructor. It rejects a
// signer whose public half is not the exact HolderControl publication and a
// resolver that cannot return route+holder from one atomic snapshot.
func NewPublishedCommitProofIssuer(signer *CommitProofSigner, publication holderkey.Publication, resolver actorrouting.Resolver, realm string, ttl, safetyMargin time.Duration, now func() time.Time) (*CommitProofIssuer, error) {
	issuer, err := NewCommitProofIssuer(signer, resolver, realm, ttl, safetyMargin, now)
	if err != nil {
		return nil, err
	}
	if !signer.MatchesPublication(publication) {
		return nil, fmt.Errorf("%w: signer publication mismatch", ErrCommitProofInvalid)
	}
	if _, ok := resolver.(CommitProofRouteLookup); !ok {
		return nil, fmt.Errorf("%w: atomic route key lookup unavailable", ErrCommitProofInvalid)
	}
	issuer.publishedKey = publication.Clone()
	return issuer, nil
}

func (i *CommitProofIssuer) Mint(ctx context.Context, request CommitProofMintRequest) (*CommitProof, error) {
	if i == nil || i.signer == nil || i.resolver == nil || ctx == nil || !request.Commit.valid() {
		return nil, fmt.Errorf("%w: issuer/evidence unavailable", ErrCommitProofInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := AcceptsDirect(request.Work); err != nil {
		return nil, fmt.Errorf("%w: work: %v", ErrCommitProofInvalid, err)
	}
	evidence := request.Commit
	order := evidence.attachedOrder
	if evidence.actorAddress.Domain != actorrouting.DomainFarm || evidence.actorAddress.ActorID != request.Work.SourceActorID ||
		evidence.authorityLayout != request.AuthorityLayoutVersion || order.OrderID != request.Work.WorkID ||
		order.OrderType != request.Work.WorkType || order.OperationID != request.OperationID ||
		order.SubjectPlayerID != request.SubjectPlayerID || order.PayloadHash != request.PayloadHash || order.DueAtMs != request.DueAtMs {
		return nil, fmt.Errorf("%w: durable AttachedOrder binding", ErrCommitProofInvalid)
	}
	domain, err := routeDomainToActor(request.Work.SourceRouteDomain)
	if err != nil {
		return nil, err
	}
	address := actorrouting.Address{Realm: i.realm, Kind: domain, ID: request.Work.SourceActorID}
	var assignment actorrouting.Assignment
	if i.publishedKey.KeyID != "" {
		lookup, ok := i.resolver.(CommitProofRouteLookup)
		if !ok {
			return nil, ErrCommitProofRoute
		}
		view, lookupErr := lookup.LookupRoute(ctx, address)
		if lookupErr != nil {
			return nil, errors.Join(ErrCommitProofRoute, lookupErr)
		}
		publication, found, publicationErr := view.Holder.CommitProofPublication()
		if publicationErr != nil || !found || !publication.Equal(i.publishedKey) {
			return nil, ErrCommitProofRoute
		}
		assignment = actorrouting.Assignment{
			Kind: domain, ShardID: view.Shard, HolderPod: view.Holder.InstanceID,
			HolderBootID: view.Holder.BootID, Endpoint: view.Holder.Endpoint,
			ShardRevision: view.Route.ShardRevision, AssignmentRevision: view.Route.ShardRevision,
			WriterEpoch: view.Route.ExpectedWriterEpoch, LeaseEpoch: view.Route.ExpectedLeaseEpoch,
			LeaseUntilMs: view.Route.LeaseUntilMs, SnapshotRevision: view.GlobalRevision,
			HolderState: view.Holder.State, ShardState: view.Route.State,
		}
	} else {
		var resolveErr error
		assignment, resolveErr = i.resolver.Resolve(ctx, address)
		if resolveErr != nil {
			return nil, errors.Join(ErrCommitProofRoute, resolveErr)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fence := evidence.sourceFence
	if assignment.Kind != domain || assignment.HolderState != actorrouting.HolderStateActive || assignment.ShardState != actorrouting.ShardStateActive ||
		assignment.ShardID != request.Work.BucketID || assignment.HolderPod != fence.ExpectedHolderID ||
		assignment.HolderBootID != fence.ExpectedHolderBootID || assignment.ShardRevision != fence.ExpectedAssignmentRevision ||
		assignment.AssignmentRevision != fence.ExpectedAssignmentRevision || assignment.WriterEpoch != fence.ExpectedWriterEpoch ||
		assignment.LeaseEpoch != fence.ExpectedLeaseEpoch || assignment.LeaseUntilMs <= 0 {
		return nil, ErrCommitProofRoute
	}
	now := i.now()
	nowMs := now.UnixMilli()
	if now.IsZero() || evidence.committedAtMs > nowMs || request.DueAtMs > nowMs {
		return nil, ErrCommitProofStale
	}
	expiresAtMs := now.Add(i.ttl).UnixMilli()
	commitDeadline := evidence.committedAtMs + i.ttl.Milliseconds()
	if commitDeadline < expiresAtMs {
		expiresAtMs = commitDeadline
	}
	leaseDeadline := assignment.LeaseUntilMs - i.safetyMargin.Milliseconds()
	if leaseDeadline < expiresAtMs {
		expiresAtMs = leaseDeadline
	}
	if expiresAtMs <= nowMs || expiresAtMs <= evidence.committedAtMs {
		return nil, ErrCommitProofStale
	}
	statement := CommitProofStatement{
		Work: request.Work, WorkVersion: request.WorkVersion, OperationID: request.OperationID,
		SubjectPlayerID: request.SubjectPlayerID, PayloadHash: request.PayloadHash, DueAtMs: request.DueAtMs,
		SourceRealm: i.realm, AuthorityLayoutVersion: request.AuthorityLayoutVersion,
		SourceHolderID: fence.ExpectedHolderID, SourceHolderBootID: fence.ExpectedHolderBootID,
		AssignmentRevision: fence.ExpectedAssignmentRevision, WriterEpoch: fence.ExpectedWriterEpoch,
		LeaseEpoch: fence.ExpectedLeaseEpoch, LeaseUntilMs: assignment.LeaseUntilMs,
		SourceRecordVersion: evidence.sourceRecordVersion, CommittedAtMs: evidence.committedAtMs,
		ExpiresAtMs: expiresAtMs, DestinationRouteDomain: request.DestinationRouteDomain,
		DestinationActorID: request.DestinationActorID, DestinationStep: request.DestinationStep,
		DestinationInvocationID: request.DestinationInvocationID,
	}
	proof, err := i.signer.Sign(statement)
	if err != nil {
		return nil, err
	}
	return &proof, nil
}
