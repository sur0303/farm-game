package pet

// ItemConsumption is an inventory deduction requirement emitted by a
// successful feed decision. The PlayerActor Authority layer consumes the dog
// food inside the same Authority Commit as the CandidateState; this package
// never writes inventory (docs/design/36 §4.3).
type ItemConsumption struct {
	ItemID   int32
	Quantity int64
}

// FeedReceipt is the business receipt payload of a feed
// (docs/design/36 §4.3 PetFeedReceipt). operation_id is stamped by the
// Actor layer via the request.
type FeedReceipt struct {
	OperationID           string
	Quantity              int64
	ConsumedItemID        int32
	FeedRuleVersion       uint64
	DurationPerFoodMs     int64
	PreviousGuardUntilMs  int64
	ResultingGuardUntilMs int64
}

// FeedRequest mirrors the public PET_FEED logical fields
// (docs/design/36 §4.2). The client submits only a quantity and expected
// versions; item_id, durations, the final deadline and guard_version are all
// decided by server config and current state.
type FeedRequest struct {
	OperationID                    string
	Quantity                       int64
	ExpectedFeedRuleVersion        uint64
	ExpectedActivePetConfigVersion uint64
	ExpectedGuardRuleVersion       uint64
}

// FeedResult is the deterministic outcome of a feed attempt. Candidate,
// Consumption and GuardSync are valid only when Status == StatusSucceeded.
type FeedResult struct {
	Status      ActionStatus
	ErrorCode   ErrorCode
	Candidate   State
	Consumption ItemConsumption
	Receipt     FeedReceipt
	GuardSync   GuardSyncInput
}

// Feed consumes dog food to extend the single guard slot
// (docs/design/36 §4 Q36-03). Frozen P0 rule:
//
//	base_time            = max(trusted_now_ms, old_guard_until_ms)
//	candidate_guard_until = base_time + quantity * duration_per_food_ms   (checked)
//	require: candidate_guard_until <= trusted_now_ms + max_guard_horizon_ms
//
// No partial consumption, truncation or refund: if the candidate exceeds the
// horizon the whole command is REJECTED/PET_FEED_LIMIT_EXCEEDED and not a
// single dog food is deducted. trusted_now_ms is captured once by the caller
// (PlayerActor turn) and passed in; client clocks never participate.
//
// Expiry is lazy: after expiry base_time = trusted_now_ms, so expired gaps
// are never backfilled (§4.5).
func Feed(s State, cat CatalogSnapshot, req FeedRequest, trustedNowMs int64, dogFoodBalance int64) FeedResult {
	if !s.HasActiveGuardPet() {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetNotAvailable}
	}
	def, ok := cat.Find(s.ActiveGuardPetID)
	if !ok {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetNotFound}
	}
	fr := cat.FeedRule
	if fr.FeedRuleVersion != req.ExpectedFeedRuleVersion {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedRuleChanged}
	}
	if def.PetConfigVersion != req.ExpectedActivePetConfigVersion {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetConfigVersionChanged}
	}
	if def.GuardRule.GuardRuleVersion != req.ExpectedGuardRuleVersion {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetGuardRuleChanged}
	}
	if req.Quantity <= 0 || req.Quantity > fr.MaxQuantityPerCommand {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedLimitExceeded}
	}
	if dogFoodBalance < req.Quantity {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrInsufficientDogFood}
	}

	base := trustedNowMs
	if s.GuardUntilMs > base {
		base = s.GuardUntilMs
	}
	added, ok := checkedMul(req.Quantity, fr.DurationPerFoodMs)
	if !ok {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedLimitExceeded}
	}
	candidateUntil, ok := checkedAdd(base, added)
	if !ok {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedLimitExceeded}
	}
	limit, ok := checkedAdd(trustedNowMs, fr.MaxGuardHorizonMs)
	if !ok {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedLimitExceeded}
	}
	if candidateUntil > limit {
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedLimitExceeded}
	}

	frozen := freezeGuardRule(def)
	candidate := s.Clone()
	candidate.GuardUntilMs = candidateUntil
	candidate.GuardRuleSnapshot = &frozen
	candidate.PetStateVersion++
	candidate.GuardVersion++

	sync, ok := BuildGuardSyncInput(candidate)
	if !ok {
		// Unreachable after GuardVersion++; fail closed rather than emitting
		// a feed without its sync obligation (§6.3 BUSINESS_REQUIRED).
		return FeedResult{Status: StatusRejected, ErrorCode: ErrPetFeedLimitExceeded}
	}
	return FeedResult{
		Status:      StatusSucceeded,
		Candidate:   candidate,
		Consumption: ItemConsumption{ItemID: fr.DogFoodItemID, Quantity: req.Quantity},
		Receipt: FeedReceipt{
			OperationID:           req.OperationID,
			Quantity:              req.Quantity,
			ConsumedItemID:        fr.DogFoodItemID,
			FeedRuleVersion:       fr.FeedRuleVersion,
			DurationPerFoodMs:     fr.DurationPerFoodMs,
			PreviousGuardUntilMs:  s.GuardUntilMs,
			ResultingGuardUntilMs: candidateUntil,
		},
		GuardSync: sync,
	}
}
