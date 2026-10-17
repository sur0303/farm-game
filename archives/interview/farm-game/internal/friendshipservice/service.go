package friendshipservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"
	"farmgame/internal/publicprofile"
)

type ServiceOptions struct {
	Shares              ShareProjectionStore
	Domain              *friend.DomainService
	Index               friend.FriendIndexRepository
	Read                *friend.ReadAPI
	RepairQueue         friend.FriendIndexRepairQueue
	Navigation          friend.NavigationVisitedRepository
	Navigator           *friend.Navigator
	NavigationState     friend.NavigationRepository
	NavigationV3Enabled bool
	// NavigationWindowMode selects full-tail (default) or top-k. Empty/unknown
	// values fail closed to full-tail so rollback stays immediate.
	NavigationWindowMode friend.NavigationWindowMode
	NavigationWindowSize int
	Profiles             publicprofile.Reader
	FriendLimit          int
	Now                  func() time.Time
}

// Service is the stateless FriendshipDomain application boundary. Durable
// state lives behind the injected repositories; this object is never placed
// in PlayerActor's registry or mailbox.
type Service struct {
	shares              ShareProjectionStore
	domain              *friend.DomainService
	index               friend.FriendIndexRepository
	read                *friend.ReadAPI
	repairQueue         friend.FriendIndexRepairQueue
	navigation          friend.NavigationVisitedRepository
	navigator            *friend.Navigator
	navigationState      friend.NavigationRepository
	navigationV3Enabled  bool
	navigationWindowMode friend.NavigationWindowMode
	navigationWindowSize int
	friendLimit          int
	profiles             publicprofile.Reader
	now                  func() time.Time
	visit                *friend.VisitAuthorizer
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Shares == nil || options.Domain == nil ||
		options.Index == nil || options.Read == nil || options.RepairQueue == nil ||
		options.Navigation == nil || options.Profiles == nil || options.FriendLimit <= 0 {
		return nil, fmt.Errorf("%w: friendship application dependencies", ErrInvalidArgument)
	}
	if options.NavigationV3Enabled && (options.Navigator == nil || options.NavigationState == nil) {
		return nil, fmt.Errorf("%w: friendship navigator v3 dependencies", ErrInvalidArgument)
	}
	visit, err := friend.NewVisitAuthorizer(options.Domain.Pairs)
	if err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	windowMode := friend.ParseNavigationWindowMode(string(options.NavigationWindowMode))
	windowSize := options.NavigationWindowSize
	if windowMode == friend.NavigationWindowModeTopK &&
		(windowSize <= 0 || windowSize > friend.MaxTopKWindowSize) {
		windowSize = friend.DefaultTopKWindowSize
	}
	return &Service{
		shares: options.Shares, domain: options.Domain,
		index: options.Index, read: options.Read, repairQueue: options.RepairQueue,
		navigation: options.Navigation, navigator: options.Navigator,
		navigationState: options.NavigationState, navigationV3Enabled: options.NavigationV3Enabled,
		navigationWindowMode: windowMode, navigationWindowSize: windowSize,
		friendLimit: options.FriendLimit, profiles: options.Profiles,
		now: options.Now, visit: visit,
	}, nil
}

func outcomeFromDomain(value friend.Outcome) Outcome {
	return Outcome{Status: value.Status, Reason: value.Reason, Identity: value.Identity,
		CommittedEpoch: value.CommittedEpoch, CommittedAtMs: value.CommittedAtMs,
		Detail: value.Detail, PayloadHash: append([]byte(nil), value.PayloadHash...)}
}

func pairState(snapshot friend.PairSnapshot, changed bool) PairState {
	return PairState{RelationID: snapshot.Relation.RelationID, RelationEpoch: snapshot.Relation.RelationEpoch,
		Status: snapshot.Relation.Status, Changed: changed}
}

func applicationPayload(parts ...string) []byte {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	return hash.Sum(nil)
}

func (s *Service) CreateShare(ctx context.Context, input CreateShareInput) (CreateShareResult, error) {
	if s == nil || input.SubjectPlayerID == 0 {
		return CreateShareResult{}, fmt.Errorf("%w: create share", ErrInvalidArgument)
	}
	if input.ObservationOnly {
		issuer, err := s.domain.Shares.GetIssuer(ctx, input.SubjectPlayerID)
		if err != nil {
			return CreateShareResult{}, err
		}
		if issuer.Current.TokenID == "" || issuer.Current.TokenGeneration == 0 || issuer.Current.Status != friend.TokenStatusActive {
			return CreateShareResult{}, fmt.Errorf("%w: observed issuer has no active token", ErrMalformedResponse)
		}
		return CreateShareResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: issuer.Current.TokenGeneration},
			ShareToken: issuer.Current.TokenID, AcceptMode: ShareAcceptModeMulti,
			TokenGeneration: issuer.Current.TokenGeneration, IssuedAtMs: issuer.Current.IssuedAtMs,
			ExpiresAtMs: issuer.Current.ExpiresAtMs}, nil
	}

	candidate, err := s.shares.Create(ctx, input.SubjectPlayerID)
	if err != nil {
		return CreateShareResult{}, err
	}
	now := s.now()
	payloadHash := applicationPayload("GENERATE_SHARE_TOKEN", strconv.FormatUint(input.SubjectPlayerID, 10), candidate)
	operationID := strings.TrimSpace(input.OperationID)
	if operationID == "" {
		operationID = "https-share-generate:" + candidate
	}
	identity := friend.CommitIdentity{SubjectPlayerID: input.SubjectPlayerID, CommandType: "GENERATE_SHARE_TOKEN", OperationID: operationID}
	token, outcome, err := s.domain.Shares.GenerateShareToken(ctx, input.SubjectPlayerID, candidate, identity, payloadHash, now.UnixMilli())
	if err != nil {
		return CreateShareResult{}, err
	}
	if (outcome.Status != friend.StatusSucceeded && outcome.Status != friend.StatusNoChange) ||
		token.TokenID == "" || token.Status != friend.TokenStatusActive {
		return CreateShareResult{}, fmt.Errorf("%w: create share authority status", ErrMalformedResponse)
	}
	selected := token
	if token.TokenID != candidate {
		if _, resolveErr := s.shares.ResolveForAdmission(ctx, token.TokenID); resolveErr != nil {
			rotateHash := applicationPayload("ROTATE_SHARE_TOKEN", strconv.FormatUint(input.SubjectPlayerID, 10), candidate)
			rotateIdentity := friend.CommitIdentity{SubjectPlayerID: input.SubjectPlayerID, CommandType: "ROTATE_SHARE_TOKEN", OperationID: "https-share-rotate:" + candidate}
			selected, outcome, err = s.domain.Shares.RotateShareToken(ctx, input.SubjectPlayerID, candidate, rotateIdentity, rotateHash, now.UnixMilli())
			if err != nil || outcome.Status != friend.StatusSucceeded || selected.TokenID != candidate {
				if err == nil {
					err = fmt.Errorf("%w: rotate share authority status", ErrMalformedResponse)
				}
				return CreateShareResult{}, err
			}
		}
	}
	if err := s.shares.AlignAuthorityToken(ctx, selected.TokenID, selected.IssuedAtMs, selected.ExpiresAtMs); err != nil {
		return CreateShareResult{}, err
	}
	return CreateShareResult{Outcome: outcomeFromDomain(outcome), ShareToken: selected.TokenID,
		AcceptMode: ShareAcceptModeMulti, TokenGeneration: selected.TokenGeneration,
		IssuedAtMs: selected.IssuedAtMs, ExpiresAtMs: selected.ExpiresAtMs}, nil
}

func (s *Service) ResolveShare(ctx context.Context, input ResolveShareInput) (ResolveShareResult, error) {
	if s == nil || strings.TrimSpace(input.ShareToken) == "" {
		return ResolveShareResult{}, fmt.Errorf("%w: resolve share", ErrInvalidArgument)
	}
	resolved, err := s.shares.Resolve(ctx, input.ShareToken)
	if err != nil {
		return ResolveShareResult{}, err
	}
	profile, err := s.readProfile(ctx, resolved.InviterPlayerID)
	if err != nil {
		return ResolveShareResult{}, err
	}
	return ResolveShareResult{InviterPlayerID: resolved.InviterPlayerID, InviterProfile: profile,
		AcceptMode: resolved.AcceptMode}, nil
}

func (s *Service) RedeemShare(ctx context.Context, input RedeemShareInput) (RedeemShareResult, error) {
	if s == nil || input.RedeemerPlayerID == 0 || strings.TrimSpace(input.ShareToken) == "" {
		return RedeemShareResult{}, fmt.Errorf("%w: redeem share", ErrInvalidArgument)
	}
	resolved, err := s.shares.ResolveForAdmission(ctx, input.ShareToken)
	if err != nil {
		return RedeemShareResult{}, err
	}
	if resolved.InviterPlayerID == input.RedeemerPlayerID {
		return RedeemShareResult{}, ErrShareSelfInvite
	}
	profile, err := s.readProfile(ctx, resolved.InviterPlayerID)
	if err != nil {
		return RedeemShareResult{}, err
	}
	if input.ObservationOnly {
		pair, err := s.domain.Pairs.GetPair(ctx, input.RedeemerPlayerID, resolved.InviterPlayerID)
		if err != nil {
			return RedeemShareResult{}, err
		}
		if pair.Relation.Status != friend.RelationStatusActive || pair.HasActiveBlock() {
			return RedeemShareResult{Outcome: Outcome{Status: friend.StatusRejected, Reason: friend.ReasonVisitNotPermitted},
				Pair: pairState(pair, false), InviterPlayerID: resolved.InviterPlayerID, InviterProfile: profile}, nil
		}
		return RedeemShareResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: pair.Relation.RelationEpoch},
			Pair: pairState(pair, false), InviterPlayerID: resolved.InviterPlayerID, InviterProfile: profile}, nil
	}

	operationSource := strings.TrimSpace(input.OperationID)
	if operationSource == "" {
		operationSource = input.ShareToken
	}
	operationID, payloadHash, err := friend.PairMutationIdentity(friend.PairMutationIdentityRedeemShare,
		input.RedeemerPlayerID, resolved.InviterPlayerID, operationSource, 0)
	if err != nil {
		return RedeemShareResult{}, fmt.Errorf("%w: redeem identity", ErrInvalidArgument)
	}
	if err := s.enqueueRepair(ctx, input.RedeemerPlayerID, resolved.InviterPlayerID); err != nil {
		return RedeemShareResult{}, err
	}
	issuer, err := s.domain.Shares.GetIssuer(ctx, resolved.InviterPlayerID)
	if err != nil {
		return RedeemShareResult{}, err
	}
	if issuer.Current.TokenID != strings.TrimSpace(input.ShareToken) || issuer.Current.Status != friend.TokenStatusActive ||
		issuer.Current.TokenGeneration == 0 {
		return RedeemShareResult{}, ErrShareLinkInvalid
	}
	result, err := s.domain.RedeemShare(ctx, friend.RedeemShareCommand{
		RedeemerPlayerID: input.RedeemerPlayerID, InviterPlayerID: resolved.InviterPlayerID,
		TokenID: issuer.Current.TokenID, TokenGeneration: issuer.Current.TokenGeneration,
		OperationID: operationID, PayloadHash: payloadHash, NowMs: s.now().UnixMilli(),
	})
	if err != nil {
		return RedeemShareResult{}, err
	}
	if result.Outcome.Status == friend.StatusRejected {
		return RedeemShareResult{Outcome: outcomeFromDomain(result.Outcome), InviterPlayerID: resolved.InviterPlayerID,
			InviterProfile: profile}, nil
	}
	if result.Outcome.Status != friend.StatusSucceeded && result.Outcome.Status != friend.StatusNoChange {
		return RedeemShareResult{}, fmt.Errorf("%w: redeem outcome status", ErrMalformedResponse)
	}
	pair, err := s.domain.Pairs.GetPair(ctx, input.RedeemerPlayerID, resolved.InviterPlayerID)
	if err != nil {
		return RedeemShareResult{}, err
	}
	// FriendIndex is acceleration only. Durable repair was enqueued before the
	// Pair commit, so a direct projection failure must not rewrite or fail the
	// canonical business result.
	_ = s.projectActivePair(ctx, pair)
	state := pairState(pair, result.Outcome.Status == friend.StatusSucceeded)
	return RedeemShareResult{Outcome: outcomeFromDomain(result.Outcome), Pair: state,
		InviterPlayerID: resolved.InviterPlayerID, InviterProfile: profile}, nil
}

func (s *Service) GetFriendList(ctx context.Context, input GetFriendListInput) (GetFriendListResult, error) {
	if s == nil || input.ViewerPlayerID == 0 || input.PageSize <= 0 || input.PageSize > friend.MaxPageSize {
		return GetFriendListResult{}, fmt.Errorf("%w: friend list", ErrInvalidArgument)
	}
	page, err := s.read.GetFriendList(ctx, input.ViewerPlayerID, strings.TrimSpace(input.Cursor), input.PageSize)
	return GetFriendListResult{Page: page}, err
}

func (s *Service) DeleteFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	if err := validatePairMutationInput(input); err != nil {
		return PairMutationResult{}, err
	}
	if input.ObservationOnly {
		return s.observePair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	}
	before, err := s.domain.Pairs.GetPair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	if err != nil {
		return PairMutationResult{}, err
	}
	source := strings.TrimSpace(input.OperationID)
	if source == "" {
		source = fmt.Sprintf("revoke-%d-%d", input.CallerPlayerID, before.Relation.RelationEpoch)
	}
	operationID, payloadHash, err := friend.PairMutationIdentity(friend.PairMutationIdentityDeleteFriend,
		input.CallerPlayerID, input.PeerPlayerID, source, before.Relation.RelationEpoch)
	if err != nil {
		return PairMutationResult{}, fmt.Errorf("%w: delete identity", ErrInvalidArgument)
	}
	if err := s.enqueueRepair(ctx, input.CallerPlayerID, input.PeerPlayerID); err != nil {
		return PairMutationResult{}, err
	}
	outcome, err := s.domain.DeleteFriend(ctx, input.CallerPlayerID, input.PeerPlayerID, operationID, payloadHash, s.now().UnixMilli())
	if err != nil {
		return PairMutationResult{}, err
	}
	if outcome.Status != friend.StatusSucceeded && outcome.Status != friend.StatusNoChange {
		return PairMutationResult{}, fmt.Errorf("%w: delete outcome status", ErrMalformedResponse)
	}
	pair, err := s.domain.Pairs.GetPair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	if err != nil {
		return PairMutationResult{}, err
	}
	_ = s.removeProjectedPair(ctx, pair)
	state := pairState(pair, outcome.Status == friend.StatusSucceeded)
	return PairMutationResult{Outcome: outcomeFromDomain(outcome), Pair: state}, nil
}

func (s *Service) BlockFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	if err := validatePairMutationInput(input); err != nil {
		return PairMutationResult{}, err
	}
	if input.ObservationOnly {
		return s.observePair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	}
	payloadHash := applicationPayload("BLOCK_PLAYER", strconv.FormatUint(input.CallerPlayerID, 10), strconv.FormatUint(input.PeerPlayerID, 10))
	operationID := strings.TrimSpace(input.OperationID)
	if operationID == "" {
		operationID = "internal-block:" + strconv.FormatInt(s.now().UnixNano(), 10)
	}
	if err := s.enqueueRepair(ctx, input.CallerPlayerID, input.PeerPlayerID); err != nil {
		return PairMutationResult{}, err
	}
	outcome, err := s.domain.BlockPlayer(ctx, input.CallerPlayerID, input.PeerPlayerID, operationID, payloadHash, s.now().UnixMilli())
	if err != nil {
		return PairMutationResult{}, err
	}
	if outcome.Status != friend.StatusSucceeded && outcome.Status != friend.StatusNoChange {
		return PairMutationResult{}, fmt.Errorf("%w: block outcome status", ErrMalformedResponse)
	}
	pair, err := s.domain.Pairs.GetPair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	if err != nil {
		return PairMutationResult{}, err
	}
	if pair.Relation.RelationID != "" {
		_ = s.removeProjectedPair(ctx, pair)
	}
	return PairMutationResult{Outcome: outcomeFromDomain(outcome), Pair: pairState(pair, outcome.Status == friend.StatusSucceeded)}, nil
}

func (s *Service) UnblockFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	if err := validatePairMutationInput(input); err != nil {
		return PairMutationResult{}, err
	}
	if input.ObservationOnly {
		return s.observePair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	}
	payloadHash := applicationPayload("UNBLOCK_PLAYER", strconv.FormatUint(input.CallerPlayerID, 10), strconv.FormatUint(input.PeerPlayerID, 10))
	operationID := strings.TrimSpace(input.OperationID)
	if operationID == "" {
		operationID = "internal-unblock:" + strconv.FormatInt(s.now().UnixNano(), 10)
	}
	if err := s.enqueueRepair(ctx, input.CallerPlayerID, input.PeerPlayerID); err != nil {
		return PairMutationResult{}, err
	}
	outcome, err := s.domain.UnblockPlayer(ctx, input.CallerPlayerID, input.PeerPlayerID, operationID, payloadHash, s.now().UnixMilli())
	if err != nil {
		return PairMutationResult{}, err
	}
	if outcome.Status != friend.StatusSucceeded && outcome.Status != friend.StatusNoChange {
		return PairMutationResult{}, fmt.Errorf("%w: unblock outcome status", ErrMalformedResponse)
	}
	pair, err := s.domain.Pairs.GetPair(ctx, input.CallerPlayerID, input.PeerPlayerID)
	if err != nil {
		return PairMutationResult{}, err
	}
	if pair.Relation.Status == friend.RelationStatusActive && !pair.HasActiveBlock() {
		_ = s.projectActivePair(ctx, pair)
	}
	return PairMutationResult{Outcome: outcomeFromDomain(outcome), Pair: pairState(pair, outcome.Status == friend.StatusSucceeded)}, nil
}

func (s *Service) GetNextFriendOpportunity(ctx context.Context, input GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	if s == nil || input.ViewerPlayerID == 0 {
		return GetNextFriendOpportunityResult{}, fmt.Errorf("%w: next opportunity", ErrInvalidArgument)
	}
	hintedTargets, err := normalizeFriendOpportunityHintedTargets(input.ViewerPlayerID, input.HintedTargets)
	if err != nil {
		return GetNextFriendOpportunityResult{}, err
	}
	input.HintedTargets = hintedTargets
	if s.navigationV3Enabled {
		return s.getNextFriendOpportunityV3(ctx, input)
	}
	visited, err := s.navigation.ListVisited(ctx, input.ViewerPlayerID)
	if err != nil {
		return GetNextFriendOpportunityResult{}, err
	}
	cursor := ""
	unknown := false
	for {
		page, err := s.read.GetFriendList(ctx, input.ViewerPlayerID, cursor, friend.MaxPageSize)
		if err != nil {
			return GetNextFriendOpportunityResult{}, err
		}
		for index := range page.Friends {
			card := page.Friends[index]
			if _, seen := visited[card.FriendPlayerID]; seen {
				continue
			}
			if card.OpportunityStatus == friend.OpportunityReadUnknown {
				unknown = true
				continue
			}
			if card.Action.Actionable() {
				copyCard := card
				return GetNextFriendOpportunityResult{Status: NextOpportunityFound, Friend: &copyCard}, nil
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if unknown {
		return GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}, nil
	}
	return GetNextFriendOpportunityResult{Status: NextOpportunityNone}, nil
}

func normalizeFriendOpportunityHintedTargets(viewer uint64, input []FriendOpportunityHintedTarget) ([]FriendOpportunityHintedTarget, error) {
	if len(input) > MaxFriendOpportunityHintedTargets {
		return nil, fmt.Errorf("%w: too many hinted opportunity targets", ErrInvalidArgument)
	}
	deduplicated := make(map[uint64]uint64, len(input))
	for _, target := range input {
		if target.OwnerID == 0 || target.OwnerID == viewer || target.OpportunityRevision == 0 {
			return nil, fmt.Errorf("%w: hinted opportunity target", ErrInvalidArgument)
		}
		if previous := deduplicated[target.OwnerID]; target.OpportunityRevision > previous {
			deduplicated[target.OwnerID] = target.OpportunityRevision
		}
	}
	result := make([]FriendOpportunityHintedTarget, 0, len(deduplicated))
	for ownerID, revision := range deduplicated {
		result = append(result, FriendOpportunityHintedTarget{OwnerID: ownerID, OpportunityRevision: revision})
	}
	return result, nil
}

const (
	friendNavigationTTL        = 30 * time.Minute
	friendNavigationCASRetries = 3
	friendNavigationRechecks   = friend.CandidateWindowSize
)

// getNextFriendOpportunityV3 performs one bounded global Header sort and an
// exact Top-K evaluation, then persists the lossy CandidateWindow with CAS.
// FriendIndex rows are Header-sorted before Pair Authority is consulted: only
// the Top-K exact window is batch-validated against canonical Pair, then
// Facts. The selected target is revalidated against FriendIndex + Pair and
// assembled into a full card only after Navigator chooses it.
func (s *Service) getNextFriendOpportunityV3(ctx context.Context, input GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	viewer := input.ViewerPlayerID
	for attempt := 0; attempt < friendNavigationCASRetries; attempt++ {
		snapshot, loadErr := s.navigationState.Load(ctx, viewer)
		if loadErr != nil {
			return GetNextFriendOpportunityResult{}, loadErr
		}
		nowMs := s.now().UnixMilli()
		rosterRevision := uint64(0)
		if snapshot.Navigation != nil {
			rosterRevision = snapshot.Navigation.RosterRevision
		}
		navigation := s.navigationForRoster(snapshot.Navigation, viewer, rosterRevision, nowMs)
		snapshot.Navigation = navigation
		if len(input.HintedTargets) > 0 {
			hintedAnswer, decided, hintedErr := s.applyHintedOpportunityTargets(ctx, navigation, input.HintedTargets, nowMs)
			if hintedErr != nil {
				return GetNextFriendOpportunityResult{}, hintedErr
			}
			if decided {
				if saveErr := s.navigationState.Save(ctx, snapshot); saveErr != nil {
					if errors.Is(saveErr, friend.ErrNavigationConflict) {
						continue
					}
					return GetNextFriendOpportunityResult{}, saveErr
				}
				return hintedAnswer, nil
			}
		}
		needScan := navigation.NeedsRosterScan(navigation.RosterRevision)
		if !needScan {
			currentRevision, revisionErr := s.read.GetFriendRosterRevision(ctx, viewer)
			if revisionErr != nil {
				return GetNextFriendOpportunityResult{}, revisionErr
			}
			rosterRevision = currentRevision
			needScan = navigation.NeedsRosterScan(rosterRevision)
		}
		var rosterFarmIDs []uint64
		if needScan {
			roster, rosterErr := s.read.ScanFriendIndexRoster(ctx, viewer, s.friendLimit)
			if rosterErr != nil {
				return GetNextFriendOpportunityResult{}, rosterErr
			}
			rosterRevision = roster.RosterRevision
			rosterFarmIDs = roster.FarmIDs
			navigation = s.navigationForRoster(navigation, viewer, rosterRevision, nowMs)
			snapshot.Navigation = navigation
		}

		var answer GetNextFriendOpportunityResult
		for recheck := 0; recheck < friendNavigationRechecks; recheck++ {
			next, nextErr := s.navigator.Next(ctx, navigation, rosterFarmIDs, rosterRevision, nowMs)
			if nextErr != nil {
				return GetNextFriendOpportunityResult{}, nextErr
			}
			switch next.Status {
			case friend.NavigationResultFound:
				card, found, cardErr := s.assembleFoundFriendCard(ctx, viewer, next)
				if cardErr != nil {
					return GetNextFriendOpportunityResult{}, cardErr
				}
				if !found || (card.OpportunityStatus == friend.OpportunityReadReady && !card.Action.Actionable()) {
					// Relationship or farm facts changed between Top-K evaluation
					// and card assembly. Navigator already consumed this target;
					// continue with the remaining bounded window.
					navigation.DiscardCandidate(next.FarmID)
					continue
				}
				if card.OpportunityStatus != friend.OpportunityReadReady {
					answer = GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}
				} else {
					copyCard := card
					answer = GetNextFriendOpportunityResult{Status: NextOpportunityFound, Friend: &copyCard}
				}
			case friend.NavigationResultUnknown:
				answer = GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}
			case friend.NavigationResultNone:
				answer = GetNextFriendOpportunityResult{Status: NextOpportunityNone}
				// Persist an invalidated empty window: this answer is correct for
				// now, while a later maturity or login can trigger a fresh scan.
				navigation.InvalidateCandidates()
			default:
				return GetNextFriendOpportunityResult{}, fmt.Errorf("%w: navigator result", ErrMalformedResponse)
			}
			break
		}
		if answer.Status == NextOpportunityUnspecified {
			// Too many candidates changed while being assembled. Preserve the
			// remaining window and ask the client to retry; never claim NONE.
			answer.Status = NextOpportunityUnknown
		}
		if saveErr := s.navigationState.Save(ctx, snapshot); saveErr != nil {
			if errors.Is(saveErr, friend.ErrNavigationConflict) {
				continue
			}
			return GetNextFriendOpportunityResult{}, saveErr
		}
		return answer, nil
	}
	return GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}, nil
}

func (s *Service) assembleFoundFriendCard(ctx context.Context, viewer uint64, next friend.NavigationResult) (friend.FriendCard, bool, error) {
	if next.IndexEntry.FriendPlayerID == next.FarmID && next.IndexEntry.RelationID != "" &&
		friend.ReusableFriendOpportunityEvaluation(next.Evaluation) {
		card, err := s.read.AssembleFriendCard(ctx, viewer, next.IndexEntry, next.Evaluation)
		if err != nil {
			return friend.FriendCard{}, false, err
		}
		return card, true, nil
	}
	return s.read.GetValidatedFriendCard(ctx, viewer, next.FarmID)
}

// applyHintedOpportunityTargets validates the small server-originated Hint
// batch against current relationship/profile/farm authority. A Hint is only
// an invalidation edge: it never grants an action by itself. Valid answers are
// merged into Navigation before the enclosing repository CAS is saved.
func (s *Service) applyHintedOpportunityTargets(
	ctx context.Context,
	navigation *friend.Navigation,
	targets []FriendOpportunityHintedTarget,
	nowMs int64,
) (GetNextFriendOpportunityResult, bool, error) {
	ids := make([]uint64, 0, len(targets))
	for _, target := range targets {
		if !navigation.HasVisited(target.OwnerID) {
			ids = append(ids, target.OwnerID)
		}
	}
	coarse := make(map[uint64]friend.FarmOpportunityCandidate)
	if len(ids) > 0 {
		var err error
		coarse, err = s.navigator.Candidates.BatchGetCandidates(ctx, ids, nowMs)
		if err != nil {
			return GetNextFriendOpportunityResult{}, false, err
		}
	}

	unknownSeen := false
	var bestCard *friend.FriendCard
	bestActionableAt := int64(0)
	for _, target := range targets {
		card, found, err := s.read.GetValidatedFriendCard(ctx, navigation.ViewerPlayerID, target.OwnerID)
		if err != nil {
			return GetNextFriendOpportunityResult{}, false, err
		}
		if !found {
			navigation.DiscardCandidate(target.OwnerID)
			continue
		}
		if card.OpportunityStatus != friend.OpportunityReadReady || card.OpportunityRevision < target.OpportunityRevision {
			unknownSeen = true
			continue
		}
		if !card.Action.Actionable() {
			navigation.DiscardCandidate(target.OwnerID)
			continue
		}
		// Share accept / WATER visit marks the farm visited. THEFT at mature is
		// a new boundary and must be allowed to occupy the home popup again.
		navigation.ClearVisited(target.OwnerID)
		candidate := coarse[target.OwnerID]
		actionableAt := candidate.ActionableAtMs
		if actionableAt <= 0 {
			actionableAt = nowMs
		}
		entry := friend.CandidateWindowEntry{
			FarmID: target.OwnerID, SourceFarmVersion: candidate.SourceFarmVersion,
			ActionableAtMs: actionableAt, ActionMask: card.Action, ValidatedAtMs: card.ValidatedAtMs,
		}
		if !navigation.UpsertValidatedCandidate(entry) {
			continue
		}
		if bestCard == nil || actionableAt > bestActionableAt ||
			(actionableAt == bestActionableAt && target.OwnerID < bestCard.FriendPlayerID) {
			copyCard := card
			bestCard = &copyCard
			bestActionableAt = actionableAt
		}
	}
	if bestCard != nil {
		return GetNextFriendOpportunityResult{Status: NextOpportunityFound, Friend: bestCard}, true, nil
	}
	if unknownSeen {
		return GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}, true, nil
	}
	return GetNextFriendOpportunityResult{}, false, nil
}

func (s *Service) navigationForRoster(current *friend.Navigation, viewer, rosterRevision uint64, nowMs int64) *friend.Navigation {
	if current == nil {
		return s.newNavigation(viewer, rosterRevision, nowMs)
	}
	// A state containing only Redis-side visited marks has no expiry/roster
	// baseline yet. Preserve those marks when initializing the first window.
	if current.ExpireAtMs == 0 {
		fresh := s.newNavigation(viewer, rosterRevision, nowMs)
		for farmID, ordinal := range current.Visited {
			fresh.Visited[farmID] = ordinal
		}
		return fresh
	}
	if current.ExpireAtMs <= nowMs {
		return s.newNavigation(viewer, rosterRevision, nowMs)
	}
	// Reconcile the persisted tail with the active configuration on every
	// load. A persisted top-k tail served unchanged while the active config
	// switched to empty/unknown/full-tail would answer NONE from a truncated
	// payload; a full-tail switch must rebuild a complete tail before serving.
	return current.ReconcileWindowMode(s.navigationWindowMode, s.navigationWindowSize)
}

func (s *Service) newNavigation(viewer, rosterRevision uint64, nowMs int64) *friend.Navigation {
	navigation := friend.NewNavigation(viewer, rosterRevision, friendNavigationTTL.Milliseconds(), nowMs)
	if s == nil {
		return navigation
	}
	navigation.WindowMode = s.navigationWindowMode
	navigation.WindowSize = s.navigationWindowSize
	return navigation
}

func (s *Service) AuthorizeVisit(ctx context.Context, input AuthorizeVisitInput) (AuthorizeVisitResult, error) {
	if s == nil {
		return AuthorizeVisitResult{}, fmt.Errorf("%w: authorize visit", ErrInvalidArgument)
	}
	permit, outcome, err := s.visit.AuthorizeVisit(ctx, input.ViewerPlayerID, input.OwnerPlayerID, input.TargetFarmID,
		input.JoinOperationID, input.JoinAttemptID, input.NowMs, input.TTLMs)
	return AuthorizeVisitResult{Outcome: outcomeFromDomain(outcome), Permit: permit}, err
}

func (s *Service) CommitFriendActionAdmission(ctx context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	if s == nil || s.domain == nil || s.domain.Pairs == nil {
		return friend.ActionAdmissionOutcome{}, fmt.Errorf("%w: action admission", ErrUnavailable)
	}
	result, err := s.domain.Pairs.CommitFriendActionAdmission(ctx, command)
	return bindAdmissionOutcome(result, command), err
}

func (s *Service) GetFriendActionAdmission(ctx context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	if s == nil || s.domain == nil || s.domain.Pairs == nil {
		return friend.ActionAdmissionOutcome{}, fmt.Errorf("%w: action admission", ErrUnavailable)
	}
	result, err := s.domain.Pairs.GetFriendActionAdmission(ctx, command)
	return bindAdmissionOutcome(result, command), err
}

func bindAdmissionOutcome(result friend.ActionAdmissionOutcome, command friend.ActionAdmissionCommand) friend.ActionAdmissionOutcome {
	if result.Outcome.Status == friend.StatusUnspecified {
		return result
	}
	if !result.Outcome.Identity.Valid() {
		result.Outcome.Identity = command.Identity
	}
	if len(result.Outcome.PayloadHash) == 0 {
		result.Outcome.PayloadHash = append([]byte(nil), command.PayloadHash...)
	}
	return result
}

func validatePairMutationInput(input PairMutationInput) error {
	if !friend.ValidPair(input.CallerPlayerID, input.PeerPlayerID) {
		return fmt.Errorf("%w: friend pair", ErrInvalidArgument)
	}
	return nil
}

func (s *Service) observePair(ctx context.Context, caller, peer uint64) (PairMutationResult, error) {
	pair, err := s.domain.Pairs.GetPair(ctx, caller, peer)
	if err != nil {
		return PairMutationResult{}, err
	}
	return PairMutationResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: pair.Relation.RelationEpoch},
		Pair: pairState(pair, false)}, nil
}

func (s *Service) enqueueRepair(ctx context.Context, left, right uint64) error {
	due := s.now().UnixMilli()
	if err := s.repairQueue.Enqueue(ctx, left, due); err != nil {
		return err
	}
	return s.repairQueue.Enqueue(ctx, right, due)
}

func (s *Service) projectActivePair(ctx context.Context, pair friend.PairSnapshot) error {
	if pair.Relation.Status != friend.RelationStatusActive || pair.HasActiveBlock() || pair.LowPlayerID == 0 ||
		pair.HighPlayerID == 0 || pair.Relation.RelationID == "" || pair.Relation.RelationEpoch == 0 ||
		pair.Relation.ActivatedAtMs <= 0 {
		return fmt.Errorf("%w: canonical active Pair", ErrMalformedResponse)
	}
	left := friend.FriendIndexEntry{FriendPlayerID: pair.HighPlayerID, RelationID: pair.Relation.RelationID,
		RelationEpoch: pair.Relation.RelationEpoch, ActivatedAtMs: pair.Relation.ActivatedAtMs}
	right := friend.FriendIndexEntry{FriendPlayerID: pair.LowPlayerID, RelationID: pair.Relation.RelationID,
		RelationEpoch: pair.Relation.RelationEpoch, ActivatedAtMs: pair.Relation.ActivatedAtMs}
	if _, err := s.index.Apply(ctx, pair.LowPlayerID, left); err != nil {
		return err
	}
	_, err := s.index.Apply(ctx, pair.HighPlayerID, right)
	return err
}

func (s *Service) removeProjectedPair(ctx context.Context, pair friend.PairSnapshot) error {
	if pair.LowPlayerID == 0 || pair.HighPlayerID == 0 || pair.Relation.RelationID == "" || pair.Relation.RelationEpoch == 0 {
		return fmt.Errorf("%w: canonical deleted Pair", ErrMalformedResponse)
	}
	terminatedEpoch := pair.Relation.RelationEpoch
	if pair.Relation.Status == friend.RelationStatusDeleted && terminatedEpoch > 1 {
		terminatedEpoch--
	}
	if _, err := s.index.Remove(ctx, pair.LowPlayerID, pair.HighPlayerID, pair.Relation.RelationID, terminatedEpoch); err != nil {
		return err
	}
	_, err := s.index.Remove(ctx, pair.HighPlayerID, pair.LowPlayerID, pair.Relation.RelationID, terminatedEpoch)
	return err
}

func (s *Service) readProfile(ctx context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	if playerID == 0 {
		return nil, fmt.Errorf("%w: public profile player", ErrInvalidArgument)
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	profile, err := s.profiles.Read(readCtx, playerID)
	if err != nil {
		return nil, err
	}
	if err := publicprofile.Validate(profile, playerID); err != nil {
		return nil, err
	}
	return profile, nil
}
