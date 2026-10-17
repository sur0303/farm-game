package holdercontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/storage/authoritystore"
)

var (
	ErrDrainControllerInvalid       = errors.New("holdercontrol: invalid remote drain controller")
	ErrDrainControllerIdentity      = errors.New("holdercontrol: holder identity mismatch")
	ErrDrainControllerStatus        = errors.New("holdercontrol: holder status mismatch")
	ErrDrainControllerGeneration    = errors.New("holdercontrol: authority generation mismatch")
	ErrDrainControllerCallbackInput = errors.New("holdercontrol: manager callback target mismatch")
)

type HolderPeer struct {
	Client                *HolderControlClient
	Domain                actorrouting.RouteDomain
	HolderID              string
	BootID                string
	LogicEndpoint         string
	HolderControlEndpoint string
	TLSDNSIdentity        string
}

type RemoteDrainControllerOptions struct {
	Source           HolderPeer
	Target           HolderPeer
	Domain           actorrouting.RouteDomain
	VirtualShard     uint32
	OperationTimeout time.Duration
	VerifyStatus     bool
	Now              func() time.Time
}

// RemoteDrainController is a fixed source/target adapter for the existing
// authority.RouteAssignmentManager. Manager callback arguments are treated as
// assertions, never as a way to swap the constructed target.
type RemoteDrainController struct {
	source           HolderPeer
	target           HolderPeer
	domain           actorrouting.RouteDomain
	virtualShard     uint32
	operationTimeout time.Duration
	verifyStatus     bool
	now              func() time.Time
}

var _ authority.DrainController = (*RemoteDrainController)(nil)

func NewRemoteDrainController(opts RemoteDrainControllerOptions) (*RemoteDrainController, error) {
	if err := validateHolderPeer(opts.Source); err != nil {
		return nil, fmt.Errorf("%w: source: %w", ErrDrainControllerInvalid, err)
	}
	if err := validateHolderPeer(opts.Target); err != nil {
		return nil, fmt.Errorf("%w: target: %w", ErrDrainControllerInvalid, err)
	}
	if opts.Domain != actorrouting.DomainPlayer && opts.Domain != actorrouting.DomainFarm {
		return nil, fmt.Errorf("%w: domain", ErrDrainControllerInvalid)
	}
	if opts.Source.Domain != opts.Domain || opts.Target.Domain != opts.Domain {
		return nil, fmt.Errorf("%w: peer domain", ErrDrainControllerInvalid)
	}
	if opts.Source.HolderID == opts.Target.HolderID && opts.Source.BootID == opts.Target.BootID {
		return nil, fmt.Errorf("%w: source and target are identical", ErrDrainControllerInvalid)
	}
	if opts.OperationTimeout < 0 {
		return nil, fmt.Errorf("%w: operation timeout", ErrDrainControllerInvalid)
	}
	if opts.OperationTimeout == 0 {
		opts.OperationTimeout = defaultHolderControlRPCTimeout
	}
	if err := validateHolderControlTimeout("drain operation timeout", opts.OperationTimeout); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDrainControllerInvalid, err)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &RemoteDrainController{
		source: opts.Source, target: opts.Target, domain: opts.Domain,
		virtualShard: opts.VirtualShard, operationTimeout: opts.OperationTimeout,
		verifyStatus: opts.VerifyStatus, now: opts.Now,
	}, nil
}

func (c *RemoteDrainController) StopNewActivationAndTurns(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) error {
	if err := c.checkCallback(domain, shard); err != nil {
		return err
	}
	req := &holdercontrolpb.StopNewActivationAndTurnsRequest{Domain: string(c.domain), Shard: c.virtualShard}
	response, err := c.call(ctx, func(callCtx context.Context) (*holdercontrolpb.HolderControlResponse, error) {
		return c.source.Client.StopNewActivationAndTurns(callCtx, req)
	})
	if err != nil {
		return err
	}
	if _, err := validateResponse(response, c.source, c.virtualShard, c.now(), []holdercontrolpb.DrainPhase{
		holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
	}); err != nil {
		return err
	}
	if c.verifyStatus {
		return c.verifySourceStatus(ctx, []holdercontrolpb.DrainPhase{
			holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED,
			holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
			holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
		})
	}
	return nil
}

func (c *RemoteDrainController) WaitCurrentTurns(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) error {
	if err := c.checkCallback(domain, shard); err != nil {
		return err
	}
	req := &holdercontrolpb.WaitCurrentTurnsRequest{Domain: string(c.domain), Shard: c.virtualShard}
	response, err := c.call(ctx, func(callCtx context.Context) (*holdercontrolpb.HolderControlResponse, error) {
		return c.source.Client.WaitCurrentTurns(callCtx, req)
	})
	if err != nil {
		return err
	}
	if _, err := validateResponse(response, c.source, c.virtualShard, c.now(), []holdercontrolpb.DrainPhase{
		holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
	}); err != nil {
		return err
	}
	if c.verifyStatus {
		return c.verifySourceStatus(ctx, []holdercontrolpb.DrainPhase{
			holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
			holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
		})
	}
	return nil
}

func (c *RemoteDrainController) ConfirmFactsCommitted(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) error {
	if err := c.checkCallback(domain, shard); err != nil {
		return err
	}
	req := &holdercontrolpb.ConfirmFactsCommittedRequest{Domain: string(c.domain), Shard: c.virtualShard}
	response, err := c.call(ctx, func(callCtx context.Context) (*holdercontrolpb.HolderControlResponse, error) {
		return c.source.Client.ConfirmFactsCommitted(callCtx, req)
	})
	if err != nil {
		return err
	}
	if _, err := validateResponse(response, c.source, c.virtualShard, c.now(), []holdercontrolpb.DrainPhase{
		holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
	}); err != nil {
		return err
	}
	if c.verifyStatus {
		return c.verifySourceStatus(ctx, []holdercontrolpb.DrainPhase{
			holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
		})
	}
	return nil
}

func (c *RemoteDrainController) ColdLoadNewHolder(ctx context.Context, domain actorrouting.RouteDomain, shard uint32, value authoritystore.ShardWriteAuthority) error {
	if err := c.checkCallback(domain, shard); err != nil {
		return err
	}
	if value.State != authoritystore.ShardAuthorityActive ||
		value.Domain != c.domain || value.VirtualShard != c.virtualShard ||
		value.HolderID != c.target.HolderID || value.HolderBootID != c.target.BootID {
		return ErrDrainControllerGeneration
	}
	generation := AuthorityGeneration{
		HolderID: value.HolderID, HolderBootID: value.HolderBootID,
		AssignmentRevision: value.AssignmentRevision, WriterEpoch: value.WriterEpoch,
		LeaseEpoch: value.LeaseEpoch, LeaseUntilMs: value.LeaseUntilMs,
	}
	if err := generation.ValidateAt(c.now()); err != nil {
		return fmt.Errorf("%w: %v", ErrDrainControllerGeneration, err)
	}
	reqGeneration, err := EncodeAuthorityGeneration(generation)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDrainControllerGeneration, err)
	}
	req := &holdercontrolpb.PrepareShardGenerationRequest{
		Domain: string(c.domain), Shard: c.virtualShard, Generation: reqGeneration,
	}
	response, err := c.call(ctx, func(callCtx context.Context) (*holdercontrolpb.HolderControlResponse, error) {
		return c.target.Client.PrepareShardGeneration(callCtx, req)
	})
	if err != nil {
		return err
	}
	status, err := validateResponse(response, c.target, c.virtualShard, c.now(), []holdercontrolpb.DrainPhase{
		holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN,
	})
	if err != nil {
		return err
	}
	if !status.Prepared || status.PreparedGeneration == nil {
		return fmt.Errorf("%w: target did not confirm Prepare", ErrDrainControllerGeneration)
	}
	prepared, err := DecodeAuthorityGeneration(status.PreparedGeneration)
	if err != nil {
		return fmt.Errorf("%w: prepared generation is invalid", ErrDrainControllerGeneration)
	}
	// Prepare proves the exact generation fence but its holder-authorized
	// Renew may extend the durable deadline. Accept only that monotonic deadline
	// refresh; Manager reconciles the exact durable Authority before marking
	// cold-load complete and publishing ACTIVE.
	if err := prepared.ValidateAt(c.now()); err != nil ||
		!sameAuthorityGenerationIdentity(prepared, generation) ||
		prepared.LeaseUntilMs < generation.LeaseUntilMs {
		return fmt.Errorf("%w: prepared generation differs", ErrDrainControllerGeneration)
	}
	if c.verifyStatus {
		return c.verifyTargetStatus(ctx, generation)
	}
	return nil
}

func (c *RemoteDrainController) GetStatus(ctx context.Context, target bool) (*holdercontrolpb.ShardStatus, error) {
	if c == nil {
		return nil, ErrDrainControllerInvalid
	}
	peer := c.source
	if target {
		peer = c.target
	}
	req := &holdercontrolpb.GetShardStatusRequest{Domain: string(c.domain), Shard: c.virtualShard}
	response, err := c.callStatus(ctx, peer, req)
	if err != nil {
		return nil, err
	}
	if response == nil || response.Status == nil {
		return nil, ErrDrainControllerStatus
	}
	if err := validateShardStatus(response.Status, peer, c.virtualShard, c.now()); err != nil {
		return nil, err
	}
	if target && response.Status.Phase != holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN {
		return nil, ErrDrainControllerStatus
	}
	return response.Status, nil
}

func (c *RemoteDrainController) checkCallback(domain actorrouting.RouteDomain, shard uint32) error {
	if c == nil || c.source.Client == nil || c.target.Client == nil {
		return ErrDrainControllerInvalid
	}
	if domain != c.domain || shard != c.virtualShard {
		return ErrDrainControllerCallbackInput
	}
	return nil
}

func (c *RemoteDrainController) call(ctx context.Context, fn func(context.Context) (*holdercontrolpb.HolderControlResponse, error)) (*holdercontrolpb.HolderControlResponse, error) {
	if c == nil || ctx == nil || fn == nil {
		return nil, ErrDrainControllerInvalid
	}
	callCtx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := fn(callCtx)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *RemoteDrainController) callStatus(ctx context.Context, peer HolderPeer, req *holdercontrolpb.GetShardStatusRequest) (*holdercontrolpb.GetShardStatusResponse, error) {
	if c == nil || ctx == nil || peer.Client == nil || req == nil {
		return nil, ErrDrainControllerInvalid
	}
	callCtx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := peer.Client.GetShardStatus(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *RemoteDrainController) operationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := validateHolderControlTimeout("drain operation timeout", c.operationTimeout); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrDrainControllerInvalid, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	return callCtx, cancel, nil
}

func (c *RemoteDrainController) verifySourceStatus(ctx context.Context, allowed []holdercontrolpb.DrainPhase) error {
	status, err := c.GetStatus(ctx, false)
	if err != nil {
		return err
	}
	if !phaseAllowed(status.Phase, allowed) {
		return ErrDrainControllerStatus
	}
	return nil
}

func (c *RemoteDrainController) verifyTargetStatus(ctx context.Context, generation AuthorityGeneration) error {
	status, err := c.GetStatus(ctx, true)
	if err != nil {
		return err
	}
	if !status.Prepared || status.PreparedGeneration == nil {
		return ErrDrainControllerGeneration
	}
	prepared, err := DecodeAuthorityGeneration(status.PreparedGeneration)
	if err != nil || prepared.ValidateAt(c.now()) != nil ||
		!sameAuthorityGenerationIdentity(prepared, generation) ||
		prepared.LeaseUntilMs < generation.LeaseUntilMs {
		return ErrDrainControllerGeneration
	}
	return nil
}

func sameAuthorityGenerationIdentity(left, right AuthorityGeneration) bool {
	return left.HolderID == right.HolderID &&
		left.HolderBootID == right.HolderBootID &&
		left.AssignmentRevision == right.AssignmentRevision &&
		left.WriterEpoch == right.WriterEpoch &&
		left.LeaseEpoch == right.LeaseEpoch
}

func validateHolderPeer(peer HolderPeer) error {
	if peer.Client == nil || peer.Domain == "" || peer.LogicEndpoint == "" || peer.HolderControlEndpoint == "" {
		return ErrDrainControllerIdentity
	}
	if peer.Domain != actorrouting.DomainPlayer && peer.Domain != actorrouting.DomainFarm {
		return ErrDrainControllerIdentity
	}
	if strings.TrimSpace(peer.LogicEndpoint) != peer.LogicEndpoint {
		return ErrDrainControllerIdentity
	}
	if strings.TrimSpace(peer.HolderControlEndpoint) != peer.HolderControlEndpoint {
		return ErrDrainControllerIdentity
	}
	if err := validateHolderIdentity(peer.HolderID, peer.BootID); err != nil {
		return fmt.Errorf("%w: holder identity", ErrDrainControllerIdentity)
	}
	if _, err := canonicalHolderDNSIdentity(peer.TLSDNSIdentity); err != nil {
		return fmt.Errorf("%w: TLS identity", ErrDrainControllerIdentity)
	}
	if peer.Client.Identity() != peer.TLSDNSIdentity {
		return fmt.Errorf("%w: TLS identity", ErrDrainControllerIdentity)
	}
	return nil
}

func validateResponse(response *holdercontrolpb.HolderControlResponse, peer HolderPeer, shard uint32, now time.Time, allowed []holdercontrolpb.DrainPhase) (*holdercontrolpb.ShardStatus, error) {
	if response == nil || response.Status == nil {
		return nil, ErrDrainControllerStatus
	}
	if err := validateShardStatus(response.Status, peer, shard, now); err != nil {
		return nil, err
	}
	if !phaseAllowed(response.Status.Phase, allowed) {
		return nil, ErrDrainControllerStatus
	}
	return response.Status, nil
}

func validateShardStatus(status *holdercontrolpb.ShardStatus, peer HolderPeer, shard uint32, now time.Time) error {
	return validateShardStatusFields(status, string(peer.Domain), shard, peer.HolderID, peer.BootID, now)
}

func validateShardStatusFields(status *holdercontrolpb.ShardStatus, domain string, shard uint32, holderID, bootID string, now time.Time) error {
	if status == nil {
		return ErrDrainControllerStatus
	}
	if status.Domain != domain || status.Shard != shard ||
		status.HolderId != holderID || status.HolderBootId != bootID {
		return ErrDrainControllerStatus
	}
	switch status.Phase {
	case holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN:
		if !status.Admitting || status.WaitComplete {
			return ErrDrainControllerStatus
		}
	case holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED:
		if status.Admitting || status.WaitComplete {
			return ErrDrainControllerStatus
		}
	case holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED:
		if status.Admitting || !status.WaitComplete || status.AcceptedUserJobs != 0 {
			return ErrDrainControllerStatus
		}
	default:
		return ErrDrainControllerStatus
	}
	if !status.Prepared {
		if status.PreparedGeneration != nil {
			return ErrDrainControllerGeneration
		}
		return nil
	}
	if status.PreparedGeneration == nil {
		return ErrDrainControllerGeneration
	}
	generation, err := DecodeAuthorityGeneration(status.PreparedGeneration)
	if err != nil || generation.ValidateAt(now) != nil ||
		generation.HolderID != holderID || generation.HolderBootID != bootID {
		return ErrDrainControllerGeneration
	}
	return nil
}

func phaseAllowed(value holdercontrolpb.DrainPhase, allowed []holdercontrolpb.DrainPhase) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
