package pet

// CoinFee is a wallet deduction requirement emitted by a successful
// domain decision. The PlayerActor Authority layer deducts it from
// Wallet.normal_coin inside the same Authority Commit as the CandidateState;
// this package never touches the wallet itself (docs/design/36 §3.2).
type CoinFee struct {
	Amount       int64
	PriceVersion uint64
}

// UnlockReceipt is the business receipt payload of a coin unlock. The
// Authority layer persists it via the bounded Operation Receipt/Outcome
// mechanism of docs/design/30-31; it is never appended to PetState.
type UnlockReceipt struct {
	OperationID      string
	PetID            int32
	CoinPrice        int64
	PriceVersion     uint64
	PetConfigVersion uint64
}

// UnlockByCoinRequest mirrors the public PET_UNLOCK_BY_COIN logical fields
// (docs/design/36 §3.2, 28 §PetUnlockByCoinCanonicalPayload). The client
// cannot submit a price; player_id and operation identity come from the
// Session/Envelope at the Actor layer.
type UnlockByCoinRequest struct {
	OperationID              string
	PetID                    int32
	ExpectedPetConfigVersion uint64
	ExpectedPriceVersion     uint64
}

// UnlockResult is the deterministic outcome of an unlock attempt.
// Candidate is non-nil only when Status == StatusSucceeded.
type UnlockResult struct {
	Status    ActionStatus
	ErrorCode ErrorCode
	Candidate State
	Fee       CoinFee // valid only on success
	Receipt   UnlockReceipt
}

// UnlockByCoin unlocks a pet via the normal-coin offer
// (docs/design/36 §3.2). It is a pure function: no wallet mutation, no
// GuardSyncOrder (unlock without selection never syncs, §3.2), no version
// bump on NO_CHANGE/REJECTED.
//
// Idempotency: an already-owned pet returns NO_CHANGE/PET_ALREADY_UNLOCKED
// with a zero fee, so a weak-network replay of the same operation can never
// charge twice; the Actor layer additionally replays the stored outcome for
// the same operation_id.
func UnlockByCoin(s State, cat CatalogSnapshot, req UnlockByCoinRequest, normalCoinBalance int64) UnlockResult {
	// Set membership is the idempotency truth. Check it before consulting
	// mutable catalog availability so a previously successful purchase stays
	// NO_CHANGE after a normal storefront delist or catalog removal.
	if s.Owns(req.PetID) {
		return UnlockResult{Status: StatusNoChange, ErrorCode: ErrPetAlreadyUnlocked}
	}
	def, ok := cat.Find(req.PetID)
	if !ok {
		return UnlockResult{Status: StatusRejected, ErrorCode: ErrPetNotFound}
	}
	if def.PetConfigVersion != req.ExpectedPetConfigVersion {
		return UnlockResult{Status: StatusRejected, ErrorCode: ErrPetConfigVersionChanged}
	}
	if !def.UnlockOffer.CoinUnlockEnabled {
		// 下架保留既有: new coin purchases are refused, owned pets unaffected.
		return UnlockResult{Status: StatusRejected, ErrorCode: ErrPetNotAvailable}
	}
	if def.UnlockOffer.PriceVersion != req.ExpectedPriceVersion {
		return UnlockResult{Status: StatusRejected, ErrorCode: ErrPetPriceChanged}
	}
	if len(s.OwnedPetIDs) >= MaxOwnedPets {
		return UnlockResult{Status: StatusRejected, ErrorCode: ErrPetNotAvailable}
	}
	if normalCoinBalance < def.UnlockOffer.CoinPrice {
		return UnlockResult{Status: StatusRejected, ErrorCode: ErrInsufficientCoin}
	}

	candidate := s.Clone()
	candidate.OwnedPetIDs = insertOwned(candidate.OwnedPetIDs, req.PetID)
	candidate.PetStateVersion++
	// GuardVersion intentionally unchanged: unlock without selection does
	// not change the guard slot and must not create a GuardSyncOrder.
	return UnlockResult{
		Status:    StatusSucceeded,
		Candidate: candidate,
		Fee:       CoinFee{Amount: def.UnlockOffer.CoinPrice, PriceVersion: def.UnlockOffer.PriceVersion},
		Receipt: UnlockReceipt{
			OperationID:      req.OperationID,
			PetID:            req.PetID,
			CoinPrice:        def.UnlockOffer.CoinPrice,
			PriceVersion:     def.UnlockOffer.PriceVersion,
			PetConfigVersion: def.PetConfigVersion,
		},
	}
}

// TaskUnlockGrant is the frozen pet grant inside a task RewardBundle
// (docs/design/36 §3.3, docs/design/34 PetUnlockGrant). There is no public
// PET_UNLOCK_BY_TASK command; the grant is applied inside the task-claim
// PlayerActor turn.
type TaskUnlockGrant struct {
	PetID                  int32
	PetConfigVersion       uint64
	GrantDefinitionVersion uint64
}

// TaskUnlockResult reports whether the owned set actually changed. A grant
// for an already-owned pet is set-idempotent: Changed=false, no version
// bump, and the surrounding task claim still succeeds normally.
type TaskUnlockResult struct {
	Status    ActionStatus
	ErrorCode ErrorCode
	Changed   bool
	Candidate State
}

// ApplyTaskUnlockGrant applies a frozen task reward grant to the owned set.
// It never charges the wallet, never touches the guard slot, and never
// creates a GuardSyncOrder.
func ApplyTaskUnlockGrant(s State, provider Provider, grant TaskUnlockGrant) TaskUnlockResult {
	if s.Owns(grant.PetID) {
		return TaskUnlockResult{Status: StatusNoChange, ErrorCode: ErrPetAlreadyUnlocked, Changed: false, Candidate: s.Clone()}
	}
	if provider == nil {
		return TaskUnlockResult{Status: StatusRejected, ErrorCode: ErrPetNotFound}
	}
	_, ok := provider.DefinitionAt(grant.PetID, grant.PetConfigVersion)
	if !ok {
		current := provider.Current()
		if _, present := current.Find(grant.PetID); present {
			return TaskUnlockResult{Status: StatusRejected, ErrorCode: ErrPetConfigVersionChanged}
		}
		return TaskUnlockResult{Status: StatusRejected, ErrorCode: ErrPetNotFound}
	}
	if len(s.OwnedPetIDs) >= MaxOwnedPets {
		return TaskUnlockResult{Status: StatusRejected, ErrorCode: ErrPetNotAvailable}
	}

	candidate := s.Clone()
	candidate.OwnedPetIDs = insertOwned(candidate.OwnedPetIDs, grant.PetID)
	candidate.PetStateVersion++
	return TaskUnlockResult{Status: StatusSucceeded, Changed: true, Candidate: candidate}
}
