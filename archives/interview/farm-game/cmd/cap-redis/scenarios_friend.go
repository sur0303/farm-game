package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"farmgame/internal/friend"
)

func friendIdentity(subject uint64, command string) friend.CommitIdentity {
	return friend.CommitIdentity{SubjectPlayerID: subject, CommandType: command,
		OperationID: fmt.Sprintf("cap-friend-operation-%d", nextID())}
}

func friendStatus(status friend.StatusClass) string {
	switch status {
	case friend.StatusSucceeded:
		return "SUCCEEDED"
	case friend.StatusNoChange:
		return "NO_CHANGE"
	case friend.StatusRejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}

type authorityDiagnosticLocator struct {
	kind              friend.FriendshipAuthorityKind
	authorityID       string
	bucketID          uint32
	recordKey         string
	receiptKey        string
	receiptField      string
	attemptKey        string
	attemptStateField string
}

type pairActivation struct {
	inviter, redeemer uint64
	pairID            string
	command           friend.ActivateCommand
	identity          friend.CommitIdentity
	hash              []byte
	diagnostic        authorityDiagnosticLocator
}

func appendFriendDiagnosticLP(dst []byte, value string) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	dst = append(dst, size[:]...)
	return append(dst, value...)
}

// participantDiagnosticDigest follows the frozen Friend participant locator
// contract. It is used only to construct exact, bounded diagnostic keys; the
// raw digest and keys remain in memory and are never emitted.
func participantDiagnosticDigest(invocationID string) [sha256.Size]byte {
	material := appendFriendDiagnosticLP(nil, "friend-participant-receipt-v1")
	material = appendFriendDiagnosticLP(material, invocationID)
	return sha256.Sum256(material)
}

func publicDiagnosticDigest(identity friend.CommitIdentity, kind friend.FriendshipAuthorityKind, authorityID string) [sha256.Size]byte {
	material := appendFriendDiagnosticLP(nil, strconv.FormatUint(identity.SubjectPlayerID, 10))
	material = appendFriendDiagnosticLP(material, identity.CommandType)
	material = appendFriendDiagnosticLP(material, identity.OperationID)
	material = appendFriendDiagnosticLP(material, string(kind))
	material = appendFriendDiagnosticLP(material, authorityID)
	return sha256.Sum256(material)
}

func newAuthorityDiagnosticLocator(e *benchmarkEnvironment, kind friend.FriendshipAuthorityKind, authorityID string,
	identity friend.CommitIdentity) (authorityDiagnosticLocator, error) {
	ref, err := e.friendKeys.BucketFor(kind, authorityID)
	if err != nil {
		return authorityDiagnosticLocator{}, err
	}
	var digest [sha256.Size]byte
	fieldPrefix := "participant:"
	if identity.IsPublic() {
		digest = publicDiagnosticDigest(identity, kind, authorityID)
		fieldPrefix = "op:"
	} else {
		digest = participantDiagnosticDigest(identity.ParticipantInvocationID)
	}
	digestHex := hex.EncodeToString(digest[:])
	recordKey, err := e.friendKeys.Record(ref, authorityID)
	if err != nil {
		return authorityDiagnosticLocator{}, err
	}
	receiptKey, err := e.friendKeys.Receipt(ref, authorityID, digest)
	if err != nil {
		return authorityDiagnosticLocator{}, err
	}
	attemptKey, err := e.friendKeys.CommitAttempt(ref, authorityID, digest)
	if err != nil {
		return authorityDiagnosticLocator{}, err
	}
	return authorityDiagnosticLocator{kind: kind, authorityID: authorityID, bucketID: ref.BucketID,
		recordKey: string(recordKey), receiptKey: string(receiptKey), receiptField: fieldPrefix + digestHex,
		attemptKey: string(attemptKey), attemptStateField: "cursor:" + digestHex + ":state"}, nil
}

func diagnosticInput(operationID string, locator authorityDiagnosticLocator, presentBefore bool, versionBefore uint64) *operationDiagnosticInput {
	return &operationDiagnosticInput{OperationID: operationID, AuthorityKind: locator.kind, AuthorityID: locator.authorityID,
		AuthorityBucketID: locator.bucketID, AuthorityRecordPresentBefore: presentBefore, AuthorityRecordVersionBefore: versionBefore,
		recordKey: locator.recordKey, receiptKey: locator.receiptKey, receiptField: locator.receiptField,
		attemptKey: locator.attemptKey, attemptStateField: locator.attemptStateField}
}

func newPairActivation(e *benchmarkEnvironment) (*pairActivation, error) {
	inviter, redeemer := uint64(40_000_000)+nextID(), uint64(50_000_000)+nextID()
	pairID, err := friend.RelationIDFor(inviter, redeemer)
	if err != nil {
		return nil, err
	}
	identity := friend.CommitIdentity{WorkAddressID: pairID,
		ParticipantInvocationID: fmt.Sprintf("cap-activate-invocation-%d", nextID()), ParticipantStep: "ACTIVATE"}
	locator, err := newAuthorityDiagnosticLocator(e, friend.FriendPairAuthority, pairID, identity)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(identity.ParticipantInvocationID))
	return &pairActivation{inviter: inviter, redeemer: redeemer, pairID: pairID, diagnostic: locator,
		command: friend.ActivateCommand{InviterPlayerID: inviter, RedeemerPlayerID: redeemer,
			TargetRelationEpoch: 1, TokenID: fmt.Sprintf("cap-token-%d", nextID()), TokenGeneration: 1,
			OperationID: fmt.Sprintf("cap-activate-%d", nextID()), NowMs: time.Now().UnixMilli()},
		identity: identity, hash: hash[:]}, nil
}

func preparePairActivate(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	operations := make([]measuredOperation, total)
	var hot *pairActivation
	var hotRecordPresentBefore bool
	var hotRecordVersionBefore uint64
	var err error
	if distribution == "hot" {
		hot, err = newPairActivation(e)
		if err != nil {
			return nil, err
		}
		outcome, activateErr := e.friends.Pair.ActivateFriendRelation(ctx, hot.command, hot.identity, hot.hash)
		if activateErr != nil || outcome.Status != friend.StatusSucceeded {
			return nil, fmt.Errorf("prime hot Pair replay fixture=%s err=%v", friendStatus(outcome.Status), activateErr)
		}
		snapshot, snapshotErr := e.friends.Pair.GetPair(ctx, hot.inviter, hot.redeemer)
		if snapshotErr != nil || snapshot.Relation.Status != friend.RelationStatusActive {
			return nil, fmt.Errorf("observe primed hot Pair fixture err=%v", snapshotErr)
		}
		hotRecordPresentBefore = true
		hotRecordVersionBefore = snapshot.RecordVersion
	}
	for i := range operations {
		activation := hot
		if activation == nil {
			activation, err = newPairActivation(e)
			if err != nil {
				return nil, err
			}
		}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			diagnostic := diagnosticInput(activation.command.OperationID, activation.diagnostic, hotRecordPresentBefore, hotRecordVersionBefore)
			diagnostic.PairLowPlayerID, diagnostic.PairHighPlayerID = activation.inviter, activation.redeemer
			start := time.Now()
			logical := len(activation.command.TokenID) + len(activation.command.OperationID) + len(activation.identity.ParticipantInvocationID) + len(activation.hash) + 64
			serialization := time.Since(start)
			outcome, err := e.friends.Pair.ActivateFriendRelation(ctx, activation.command, activation.identity, activation.hash)
			if err != nil {
				return sampleResult{Diagnostic: diagnostic}, err
			}
			if outcome.Status != friend.StatusSucceeded {
				return sampleResult{Diagnostic: diagnostic}, fmt.Errorf("pair activate=%s", friendStatus(outcome.Status))
			}
			return sampleResult{Outcome: friendStatus(outcome.Status), LogicalRequestBytes: logical,
				LogicalResponseBytes: 48, PayloadBytes: len(activation.hash), SerializationTime: serialization,
				Diagnostic: diagnostic}, nil
		}
	}
	label := "uniform_pair_keys"
	var notes []string
	if distribution == "hot" {
		label = "hot_pair_key_idempotent_replay"
		notes = append(notes, "hot Pair operation is committed during fixture setup; the measured window contains only concurrent idempotent replays")
	}
	return &preparedScenario{operations: operations, keyDistribution: label, notes: notes, diagnoseFailures: e.diagnoseFriendAuthorityFailures}, nil
}

func prepareQuotaReserve(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	operations := make([]measuredOperation, total)
	type fixture struct {
		player      uint64
		reservation friend.QuotaReservation
		identity    friend.CommitIdentity
		hash        []byte
		diagnostic  authorityDiagnosticLocator
	}
	newFixture := func() (*fixture, error) {
		player, peer := uint64(60_000_000)+nextID(), uint64(70_000_000)+nextID()
		relation, err := friend.RelationIDFor(player, peer)
		if err != nil {
			return nil, err
		}
		identity := friendIdentity(player, "RESERVE_FRIEND_SLOT")
		locator, err := newAuthorityDiagnosticLocator(e, friend.FriendQuotaAuthority, strconv.FormatUint(player, 10), identity)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256([]byte(identity.OperationID))
		return &fixture{player: player, reservation: friend.QuotaReservation{ReservationID: "reservation-" + identity.OperationID,
			WorkflowID: "workflow-" + identity.OperationID, RelationID: relation, TargetRelationEpoch: 1}, identity: identity,
			hash: hash[:], diagnostic: locator}, nil
	}
	var hot *fixture
	var hotRecordPresentBefore bool
	var hotRecordVersionBefore uint64
	var err error
	if distribution == "hot" {
		hot, err = newFixture()
		if err != nil {
			return nil, err
		}
		outcome, reserveErr := e.friends.Quota.ReserveFriendSlot(ctx, hot.player, hot.reservation, hot.identity, hot.hash, time.Now().UnixMilli())
		if reserveErr != nil || outcome.Status != friend.StatusSucceeded {
			return nil, fmt.Errorf("prime hot Quota replay fixture=%s err=%v", friendStatus(outcome.Status), reserveErr)
		}
		snapshot, snapshotErr := e.friends.Quota.GetQuota(ctx, hot.player)
		if snapshotErr != nil || snapshot.OpenReservations != 1 || snapshot.RecordVersion == 0 {
			return nil, fmt.Errorf("observe primed hot Quota fixture reservations=%d version=%d err=%v",
				snapshot.OpenReservations, snapshot.RecordVersion, snapshotErr)
		}
		hotRecordPresentBefore = true
		hotRecordVersionBefore = snapshot.RecordVersion
	}
	for i := range operations {
		item := hot
		if item == nil {
			item, err = newFixture()
			if err != nil {
				return nil, err
			}
		}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			diagnostic := diagnosticInput(item.identity.OperationID, item.diagnostic, hotRecordPresentBefore, hotRecordVersionBefore)
			start := time.Now()
			logical := len(item.reservation.ReservationID) + len(item.reservation.WorkflowID) + len(item.reservation.RelationID) + len(item.hash) + 64
			serialization := time.Since(start)
			outcome, err := e.friends.Quota.ReserveFriendSlot(ctx, item.player, item.reservation, item.identity, item.hash, time.Now().UnixMilli())
			if err != nil {
				return sampleResult{Diagnostic: diagnostic}, err
			}
			if outcome.Status != friend.StatusSucceeded {
				return sampleResult{Diagnostic: diagnostic}, fmt.Errorf("quota reserve=%s", friendStatus(outcome.Status))
			}
			return sampleResult{Outcome: friendStatus(outcome.Status), LogicalRequestBytes: logical,
				LogicalResponseBytes: 48, PayloadBytes: len(item.hash), SerializationTime: serialization, Diagnostic: diagnostic}, nil
		}
	}
	label := "uniform_quota_keys"
	var notes []string
	if distribution == "hot" {
		label = "hot_quota_key_idempotent_replay"
		notes = append(notes, "hot Quota operation is committed during fixture setup; the measured window contains only concurrent idempotent replays")
	}
	return &preparedScenario{operations: operations, keyDistribution: label, notes: notes,
		diagnoseFailures: e.diagnoseFriendAuthorityFailures}, nil
}

func prepareShareGenerate(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	operations := make([]measuredOperation, total)
	type fixture struct {
		inviter    uint64
		token      string
		identity   friend.CommitIdentity
		hash       []byte
		diagnostic authorityDiagnosticLocator
	}
	newFixture := func() (*fixture, error) {
		inviter := uint64(80_000_000) + nextID()
		identity := friendIdentity(inviter, "GENERATE_SHARE")
		locator, err := newAuthorityDiagnosticLocator(e, friend.FriendShareIssuerAuthority, strconv.FormatUint(inviter, 10), identity)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256([]byte(identity.OperationID))
		return &fixture{inviter: inviter, token: "share-token-" + identity.OperationID, identity: identity,
			hash: hash[:], diagnostic: locator}, nil
	}
	var hot *fixture
	var hotRecordPresentBefore bool
	var hotRecordVersionBefore uint64
	var err error
	if distribution == "hot" {
		hot, err = newFixture()
		if err != nil {
			return nil, err
		}
		token, outcome, generateErr := e.friends.Share.GenerateShareToken(ctx, hot.inviter, hot.token, hot.identity, hot.hash, time.Now().UnixMilli())
		if generateErr != nil || outcome.Status != friend.StatusSucceeded || token.TokenGeneration != 1 {
			return nil, fmt.Errorf("prime hot Share replay fixture=%s generation=%d err=%v",
				friendStatus(outcome.Status), token.TokenGeneration, generateErr)
		}
		snapshot, snapshotErr := e.friends.Share.GetIssuer(ctx, hot.inviter)
		if snapshotErr != nil || snapshot.Current.TokenID != hot.token || snapshot.RecordVersion == 0 {
			return nil, fmt.Errorf("observe primed hot Share fixture version=%d err=%v", snapshot.RecordVersion, snapshotErr)
		}
		hotRecordPresentBefore = true
		hotRecordVersionBefore = snapshot.RecordVersion
	}
	for i := range operations {
		item := hot
		if item == nil {
			item, err = newFixture()
			if err != nil {
				return nil, err
			}
		}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			diagnostic := diagnosticInput(item.identity.OperationID, item.diagnostic, hotRecordPresentBefore, hotRecordVersionBefore)
			start := time.Now()
			logical := len(item.token) + len(item.identity.OperationID) + len(item.hash) + 48
			serialization := time.Since(start)
			token, outcome, err := e.friends.Share.GenerateShareToken(ctx, item.inviter, item.token, item.identity, item.hash, time.Now().UnixMilli())
			if err != nil {
				return sampleResult{Diagnostic: diagnostic}, err
			}
			if outcome.Status != friend.StatusSucceeded || token.TokenGeneration != 1 {
				return sampleResult{Diagnostic: diagnostic}, fmt.Errorf("share generate=%s generation=%d", friendStatus(outcome.Status), token.TokenGeneration)
			}
			return sampleResult{Outcome: friendStatus(outcome.Status), LogicalRequestBytes: logical,
				LogicalResponseBytes: len(token.TokenID) + 64, PayloadBytes: len(item.hash), SerializationTime: serialization,
				Diagnostic: diagnostic}, nil
		}
	}
	label := "uniform_share_issuer_keys"
	var notes []string
	if distribution == "hot" {
		label = "hot_share_issuer_key_idempotent_replay"
		notes = append(notes, "hot Share operation is committed during fixture setup; the measured window contains only concurrent idempotent replays")
	}
	return &preparedScenario{operations: operations, keyDistribution: label, notes: notes,
		diagnoseFailures: e.diagnoseFriendAuthorityFailures}, nil
}

func activatePair(ctx context.Context, e *benchmarkEnvironment) (*pairActivation, error) {
	activation, err := newPairActivation(e)
	if err != nil {
		return nil, err
	}
	outcome, err := e.friends.Pair.ActivateFriendRelation(ctx, activation.command, activation.identity, activation.hash)
	if err != nil || outcome.Status != friend.StatusSucceeded {
		return nil, fmt.Errorf("activate pair setup=%s err=%v", friendStatus(outcome.Status), err)
	}
	return activation, nil
}

func preparePairBatchRead(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	resourceCount := concurrency
	if distribution == "hot" {
		resourceCount = 1
	}
	type roster struct {
		viewer        uint64
		peers         []uint64
		uniqueBuckets int
	}
	resources := make([]roster, resourceCount)
	for index := range resources {
		viewer := uint64(90_000_000) + nextID()
		peers := make([]uint64, 20)
		buckets := make(map[uint32]struct{})
		for p := range peers {
			peer := uint64(100_000_000) + nextID()
			pairID, err := friend.RelationIDFor(viewer, peer)
			if err != nil {
				return nil, err
			}
			ref, err := e.friendKeys.BucketFor(friend.FriendPairAuthority, pairID)
			if err != nil {
				return nil, err
			}
			buckets[ref.BucketID] = struct{}{}
			identity := friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: fmt.Sprintf("batch-activate-%d", nextID()), ParticipantStep: "ACTIVATE"}
			hash := sha256.Sum256([]byte(identity.ParticipantInvocationID))
			outcome, err := e.friends.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{InviterPlayerID: viewer,
				RedeemerPlayerID: peer, TargetRelationEpoch: 1, TokenID: fmt.Sprintf("batch-token-%d", nextID()),
				TokenGeneration: 1, OperationID: fmt.Sprintf("batch-op-%d", nextID()), NowMs: time.Now().UnixMilli()}, identity, hash[:])
			if err != nil || outcome.Status != friend.StatusSucceeded {
				return nil, fmt.Errorf("batch setup=%s err=%v", friendStatus(outcome.Status), err)
			}
			peers[p] = peer
		}
		resources[index] = roster{viewer: viewer, peers: peers, uniqueBuckets: len(buckets)}
	}
	var expectedSnapshotCalls uint64
	operations := splitOperations(total, concurrency, func(worker, ordinal int) measuredOperation {
		resource := resources[worker%len(resources)]
		expectedSnapshotCalls += uint64(resource.uniqueBuckets)
		return func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logical := 8 + len(resource.peers)*8
			serialization := time.Since(start)
			pairs, err := e.friends.Pair.BatchGetPairs(ctx, resource.viewer, resource.peers)
			if err != nil {
				return sampleResult{}, err
			}
			if len(pairs) != len(resource.peers) {
				return sampleResult{}, fmt.Errorf("batch pairs=%d", len(pairs))
			}
			for _, pair := range pairs {
				if !pair.Exists() {
					return sampleResult{}, fmt.Errorf("batch returned missing pair")
				}
			}
			return sampleResult{Outcome: "BATCH_READ", LogicalRequestBytes: logical,
				LogicalResponseBytes: len(pairs) * 256, PayloadBytes: len(resource.peers), SerializationTime: serialization}, nil
		}
	})
	label := "uniform_viewer_rosters"
	if distribution == "hot" {
		label = "hot_20_pair_keys_and_bucket_envelopes"
	}
	scriptSHA, ok := e.friends.ScriptSHA("ReadFriendAuthorityCompleteSnapshotV2")
	if !ok || len(scriptSHA) != 40 {
		return nil, fmt.Errorf("CompleteSnapshotV2 script SHA unavailable")
	}
	return &preparedScenario{operations: operations, keyDistribution: label,
		friendSnapshot: &friendSnapshotExpectation{scriptSHA: scriptSHA, expectedCalls: expectedSnapshotCalls,
			observedCalls: func() uint64 { return e.friends.EnvelopeReadMetrics().AtomicReadTotal }},
		notes: []string{"One operation is the bounded 20-pair canonical batch read; one CompleteSnapshotV2 script call per unique physical bucket is required."}}, nil
}

func prepareFriendIndexScan(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	resourceCount := concurrency
	if distribution == "hot" {
		resourceCount = 1
	}
	viewers := make([]uint64, resourceCount)
	for resource := range viewers {
		viewer := uint64(105_000_000) + nextID()
		viewers[resource] = viewer
		for item := 0; item < 20; item++ {
			peer := uint64(106_000_000) + nextID()
			relation, err := friend.RelationIDFor(viewer, peer)
			if err != nil {
				return nil, err
			}
			changed, err := e.friends.Index.Apply(ctx, viewer, friend.FriendIndexEntry{FriendPlayerID: peer,
				RelationID: relation, RelationEpoch: 1, ActivatedAtMs: time.Now().UnixMilli() + int64(item)})
			if err != nil || !changed {
				return nil, fmt.Errorf("index scan setup changed=%v err=%v", changed, err)
			}
		}
	}
	operations := splitOperations(total, concurrency, func(worker, ordinal int) measuredOperation {
		viewer := viewers[worker%len(viewers)]
		return func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logicalRequest := 8 + 4
			serialization := time.Since(start)
			page, err := e.friends.Index.Scan(ctx, viewer, "", 20)
			if err != nil {
				return sampleResult{}, err
			}
			if len(page.Entries) != 20 {
				return sampleResult{}, fmt.Errorf("index scan entries=%d", len(page.Entries))
			}
			return sampleResult{Outcome: "INDEX_BATCH_READ", LogicalRequestBytes: logicalRequest,
				LogicalResponseBytes: len(page.Entries) * 64, PayloadBytes: len(page.Entries), SerializationTime: serialization}, nil
		}
	})
	label := "uniform_friend_index_keys"
	if distribution == "hot" {
		label = "hot_friend_index_key_20_entries"
	}
	return &preparedScenario{operations: operations, keyDistribution: label}, nil
}

func prepareProjectionRepair(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	operations := make([]measuredOperation, total)
	hotViewer := uint64(110_000_001)
	for i := range operations {
		viewer := uint64(110_000_000) + nextID()
		if distribution == "hot" {
			viewer = hotViewer
		}
		peer := uint64(120_000_000) + nextID()
		relation, err := friend.RelationIDFor(viewer, peer)
		if err != nil {
			return nil, err
		}
		entry := friend.FriendIndexEntry{FriendPlayerID: peer, RelationID: relation, RelationEpoch: 1, ActivatedAtMs: time.Now().UnixMilli() + int64(i)}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logical := len(entry.RelationID) + 32
			serialization := time.Since(start)
			changed, err := e.friends.Index.Apply(ctx, viewer, entry)
			if err != nil {
				return sampleResult{}, err
			}
			if !changed {
				return sampleResult{}, fmt.Errorf("projection repair unexpectedly no-change")
			}
			return sampleResult{Outcome: "REPAIRED", LogicalRequestBytes: logical, LogicalResponseBytes: 8,
				PayloadBytes: logical, SerializationTime: serialization}, nil
		}
	}
	label := "uniform_viewer_projection_keys"
	if distribution == "hot" {
		label = "hot_viewer_projection_key_distinct_friend_fields"
	}
	return &preparedScenario{operations: operations, keyDistribution: label}, nil
}

func friendScenarios() []scenarioDefinition {
	return []scenarioDefinition{
		{name: "friend_pair_activate", group: "friend_authority", prepare: preparePairActivate},
		{name: "friend_quota_reserve", group: "friend_authority", prepare: prepareQuotaReserve},
		{name: "friend_share_generate", group: "friend_authority", prepare: prepareShareGenerate},
		{name: "friend_pair_batch_read_20", group: "friend_authority", prepare: preparePairBatchRead},
		{name: "friend_index_scan_20", group: "friend_authority", prepare: prepareFriendIndexScan},
		{name: "friend_projection_repair", group: "friend_authority", prepare: prepareProjectionRepair},
	}
}
