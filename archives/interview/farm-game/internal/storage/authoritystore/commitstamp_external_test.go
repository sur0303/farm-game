package authoritystore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"sort"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"
)

const externalStampHashSeed uint64 = 202608210001

// TestCommitStampCanonicalFingerprintIsExactAndImmutable exercises only the
// public authoritystore boundary. In particular, it obtains a genuine stamp
// from an adapter instead of importing the sealed issuance package.
func TestCommitStampCanonicalFingerprintIsExactAndImmutable(t *testing.T) {
	store, address, fence, group := newExternalStampStore(t)
	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress:           address,
		AuthorityLayoutVersion: "auth-v1",
		ShardWriteFence:        fence,
		ClaimOperationID:       "claim-external-stamp",
		NewWriterClaimID:       "claim-external-stamp",
		InitialModelFields: authoritystore.ModelState{
			"alpha": []byte("old-alpha"),
			"omega": []byte("old-omega"),
		},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}

	request := externalStampRequest(t, address, fence, group, loaded.Snapshot.RecordVersion)
	result, err := store.CommitExact(context.Background(), request)
	if err != nil || result.Status != authoritystore.CommitCommitted || result.CommitStamp == nil {
		t.Fatalf("commit=%+v err=%v", result, err)
	}
	if _, ok := result.ExplicitCommit(request); !ok {
		t.Fatal("genuine stamp did not validate its exact request")
	}

	t.Run("map order is canonical", func(t *testing.T) {
		reordered := cloneExternalStampRequest(request)
		reordered.CandidateState = reverseModelMap(reordered.CandidateState)
		reordered.ModelMutation.ModelUpserts = reverseModelMap(reordered.ModelMutation.ModelUpserts)
		if _, ok := result.ExplicitCommit(reordered); !ok {
			t.Fatal("equivalent maps with different insertion order changed the fingerprint")
		}
	})

	t.Run("every request field is bound", func(t *testing.T) {
		tests := []struct {
			field  string
			mutate func(*authoritystore.AuthorityCommitRequest)
		}{
			{"CommitRequestID", func(r *authoritystore.AuthorityCommitRequest) { r.CommitRequestID += "-other" }},
			{"ActorAddress", func(r *authoritystore.AuthorityCommitRequest) { r.ActorAddress.ActorID++ }},
			{"AuthorityLayoutVersion", func(r *authoritystore.AuthorityCommitRequest) { r.AuthorityLayoutVersion += "-other" }},
			{"AssertedAuthorityGroup", func(r *authoritystore.AuthorityCommitRequest) { r.AssertedAuthorityGroup.VirtualShard++ }},
			{"ShardWriteFence", func(r *authoritystore.AuthorityCommitRequest) { r.ShardWriteFence.ExpectedLeaseEpoch++ }},
			{"ExpectedWriterClaimID", func(r *authoritystore.AuthorityCommitRequest) { r.ExpectedWriterClaimID += "-other" }},
			{"ExpectedRecordVersion", func(r *authoritystore.AuthorityCommitRequest) { r.ExpectedRecordVersion++ }},
			{"IdempotencyKey", func(r *authoritystore.AuthorityCommitRequest) { r.IdempotencyKey.OperationID += "-other" }},
			{"CanonicalPayloadHash", func(r *authoritystore.AuthorityCommitRequest) { r.CanonicalPayloadHash[0] ^= 0xff }},
			{"CandidateState", func(r *authoritystore.AuthorityCommitRequest) { r.CandidateState["omega"][0] ^= 0xff }},
			{"ModelMutation", func(r *authoritystore.AuthorityCommitRequest) { r.ModelMutation.ResultingTotalBytes++ }},
			{"Receipt", func(r *authoritystore.AuthorityCommitRequest) { r.Receipt.ReceiptID += "-other" }},
			{"OperationResult", func(r *authoritystore.AuthorityCommitRequest) { r.OperationResult.ErrorCode += "-other" }},
			{"AttachedOrders", func(r *authoritystore.AuthorityCommitRequest) { r.AttachedOrders[0].DueAtMs++ }},
			{"AttachedSchedules", func(r *authoritystore.AuthorityCommitRequest) { r.AttachedSchedules[0].Generation++ }},
			{"PendingOutbox", func(r *authoritystore.AuthorityCommitRequest) { r.PendingOutbox[0].DueAtMs++ }},
		}
		requestType := reflect.TypeOf(authoritystore.AuthorityCommitRequest{})
		if len(tests) != requestType.NumField() {
			t.Fatalf("binding cases=%d request fields=%d; update exact fingerprint coverage", len(tests), requestType.NumField())
		}
		seen := make(map[string]struct{}, len(tests))
		for _, test := range tests {
			t.Run(test.field, func(t *testing.T) {
				if _, duplicate := seen[test.field]; duplicate {
					t.Fatalf("duplicate field case %s", test.field)
				}
				seen[test.field] = struct{}{}
				if _, found := requestType.FieldByName(test.field); !found {
					t.Fatalf("unknown request field %s", test.field)
				}
				changed := cloneExternalStampRequest(request)
				test.mutate(&changed)
				if _, ok := result.ExplicitCommit(changed); ok {
					t.Fatalf("stamp accepted request with changed %s", test.field)
				}
			})
		}
	})

	t.Run("nil and empty are distinct", func(t *testing.T) {
		changedPointer := cloneExternalStampRequest(request)
		changedPointer.ModelMutation = nil
		if _, ok := result.ExplicitCommit(changedPointer); ok {
			t.Fatal("stamp treated a non-nil pointer as nil")
		}

		if request.ModelMutation.ModelDeletes == nil || len(request.ModelMutation.ModelDeletes) != 0 {
			t.Fatal("test request must carry a non-nil empty ModelDeletes")
		}
		changedSlice := cloneExternalStampRequest(request)
		changedSlice.ModelMutation.ModelDeletes = nil
		if _, ok := result.ExplicitCommit(changedSlice); ok {
			t.Fatal("stamp treated a non-nil empty slice as nil")
		}

		if request.PendingOutbox[0].Payload == nil || len(request.PendingOutbox[0].Payload) != 0 {
			t.Fatal("test request must carry a non-nil empty payload")
		}
		changedBytes := cloneExternalStampRequest(request)
		changedBytes.PendingOutbox[0].Payload = nil
		if _, ok := result.ExplicitCommit(changedBytes); ok {
			t.Fatal("stamp treated a non-nil empty byte slice as nil")
		}
	})

	t.Run("issued state is immutable scalars", func(t *testing.T) {
		stamp := reflect.ValueOf(result.CommitStamp)
		if stamp.Kind() != reflect.Pointer || stamp.IsNil() || stamp.Elem().Kind() != reflect.Struct {
			t.Fatalf("stamp concrete value=%v", stamp.Kind())
		}
		stamp = stamp.Elem()
		want := map[string]reflect.Kind{
			"requestFingerprint": reflect.String,
			"recordVersion":      reflect.Uint64,
			"committedAtMs":      reflect.Int64,
		}
		if stamp.NumField() != len(want) {
			t.Fatalf("issued stamp fields=%d, want immutable fingerprint plus two scalars", stamp.NumField())
		}
		for name, kind := range want {
			field, found := stamp.Type().FieldByName(name)
			if !found || field.Type.Kind() != kind {
				t.Fatalf("issued stamp field %s=%v found=%v, want %s", name, field.Type, found, kind)
			}
		}
	})

	t.Run("safe reflection cannot retarget stamp", func(t *testing.T) {
		requestB := cloneExternalStampRequest(request)
		requestB.CandidateState["alpha"] = []byte("reflection-B")
		if _, ok := result.ExplicitCommit(requestB); ok {
			t.Fatal("stamp unexpectedly started bound to request B")
		}

		// Before fingerprints were immutable strings, issuedStamp retained a
		// deep-copied request behind an unexported interface. reflect.Value.Bytes
		// exposes the backing storage of an unexported []byte without unsafe, so
		// an external package could rewrite request A into request B.
		_ = replaceReflectedBytes(reflect.ValueOf(result.CommitStamp), []byte("reflection-A"), []byte("reflection-B"))
		if _, ok := result.ExplicitCommit(requestB); ok {
			t.Fatal("genuine stamp A validated request B after safe-reflection mutation")
		}
	})
}

func newExternalStampStore(t *testing.T) (*authoritystore.MemoryStore, authoritystore.ActorAddress, authoritystore.ShardWriteFence, authoritystore.AuthorityGroupRef) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator("stamp-test-realm", actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2,
		Seed:    externalStampHashSeed,
	}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(10_000)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "auth-v1",
		Now:                    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 42001}
	shard, err := calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		Domain:             address.Domain,
		VirtualShard:       shard,
		HolderID:           "stamp-holder",
		HolderBootID:       "stamp-boot",
		AssignmentRevision: 11,
		WriterEpoch:        7,
		LeaseEpoch:         3,
		State:              authoritystore.ShardAuthorityActive,
		LeaseUntilMs:       now.Add(time.Minute).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           authority.HolderID,
		ExpectedHolderBootID:       authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision,
		ExpectedWriterEpoch:        authority.WriterEpoch,
		ExpectedLeaseEpoch:         authority.LeaseEpoch,
	}
	group := authoritystore.AuthorityGroupRef{
		AuthorityLayoutVersion: "auth-v1",
		Domain:                 address.Domain,
		ActorID:                address.ActorID,
		VirtualShard:           shard,
	}
	return store, address, fence, group
}

func externalStampRequest(t *testing.T, address authoritystore.ActorAddress, fence authoritystore.ShardWriteFence, group authoritystore.AuthorityGroupRef, version uint64) authoritystore.AuthorityCommitRequest {
	t.Helper()
	candidate := authoritystore.ModelState{
		"proof": []byte("proof-state"),
		"omega": []byte("new-omega"),
		"alpha": []byte("reflection-A"),
	}
	upserts := authoritystore.ModelState{
		"omega": []byte("new-omega"),
		"alpha": []byte("reflection-A"),
		"proof": []byte("proof-state"),
	}
	mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate, authoritystore.ModelLayoutV1, candidate, upserts, nil)
	if err != nil {
		t.Fatal(err)
	}
	mutation.ModelDeletes = make([]string, 0)
	payloadHash := sha256.Sum256([]byte("external-stamp-payload"))
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID:       address.ActorID,
		StableCommandTypeName: "STAMP_TEST",
		OperationID:           "external-stamp-operation",
		Root: authoritystore.OperationRootRef{
			Kind:        authoritystore.RootKindPlayerActor,
			CanonicalID: "42001",
		},
	}
	outcome := authoritystore.StoredOperationOutcome{
		Status:    authoritystore.TerminalSucceeded,
		ErrorCode: "OK",
		Payload:   []byte("external-stamp-outcome"),
	}
	orderPayload := []byte("external-stamp-order")
	schedulePayload := []byte("external-stamp-schedule")
	orderID, err := workidentity.StableOpaqueID("stamp-test-order", "external")
	if err != nil {
		t.Fatal(err)
	}
	scheduleID, err := workidentity.StableOpaqueID("stamp-test-schedule", "external")
	if err != nil {
		t.Fatal(err)
	}
	return authoritystore.AuthorityCommitRequest{
		CommitRequestID:        "commit-external-stamp",
		ActorAddress:           address,
		AuthorityLayoutVersion: "auth-v1",
		AssertedAuthorityGroup: &group,
		ShardWriteFence:        fence,
		ExpectedWriterClaimID:  "claim-external-stamp",
		ExpectedRecordVersion:  version,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   payloadHash,
		CandidateState:         candidate,
		ModelMutation:          mutation,
		Receipt: authoritystore.AuthorityReceipt{
			ReceiptID:            "receipt-external-stamp",
			IdempotencyKey:       key,
			CanonicalPayloadHash: payloadHash,
			Outcome:              outcome,
		},
		OperationResult: outcome,
		AttachedOrders: []authoritystore.AttachedOrder{{
			OrderID:         orderID,
			OrderType:       "CARE_REWARD",
			Payload:         orderPayload,
			PayloadHash:     sha256.Sum256(orderPayload),
			DueAtMs:         12_000,
			OperationID:     "external-stamp-operation",
			SubjectPlayerID: address.ActorID,
		}},
		AttachedSchedules: []authoritystore.AttachedSchedule{{
			ScheduleID:      scheduleID,
			ScheduleType:    "TEST_SCHEDULE",
			Generation:      1,
			DueAtMs:         13_000,
			Payload:         schedulePayload,
			PayloadHash:     sha256.Sum256(schedulePayload),
			SubjectPlayerID: address.ActorID,
		}},
		PendingOutbox: []authoritystore.PendingOutbox{{
			OutboxID:  "outbox-external-stamp",
			EventType: "TEST_EVENT",
			Payload:   make([]byte, 0),
			DueAtMs:   14_000,
		}},
	}
}

func cloneExternalStampRequest(in authoritystore.AuthorityCommitRequest) authoritystore.AuthorityCommitRequest {
	if in.AssertedAuthorityGroup != nil {
		group := *in.AssertedAuthorityGroup
		in.AssertedAuthorityGroup = &group
	}
	in.CandidateState = cloneModelMap(in.CandidateState)
	if in.ModelMutation != nil {
		mutation := *in.ModelMutation
		mutation.ModelUpserts = cloneModelMap(mutation.ModelUpserts)
		mutation.ModelDeletes = cloneStrings(mutation.ModelDeletes)
		mutation.ResultingManifest = cloneStrings(mutation.ResultingManifest)
		in.ModelMutation = &mutation
	}
	in.Receipt.Outcome.Payload = cloneBytes(in.Receipt.Outcome.Payload)
	in.OperationResult.Payload = cloneBytes(in.OperationResult.Payload)
	in.AttachedOrders = cloneOrders(in.AttachedOrders)
	in.AttachedSchedules = cloneSchedules(in.AttachedSchedules)
	in.PendingOutbox = cloneOutbox(in.PendingOutbox)
	return in
}

func cloneModelMap(in authoritystore.ModelState) authoritystore.ModelState {
	if in == nil {
		return nil
	}
	out := make(authoritystore.ModelState, len(in))
	for key, value := range in {
		out[key] = cloneBytes(value)
	}
	return out
}

func reverseModelMap(in authoritystore.ModelState) authoritystore.ModelState {
	if in == nil {
		return nil
	}
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	out := make(authoritystore.ModelState, len(in))
	for _, key := range keys {
		out[key] = cloneBytes(in[key])
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneOrders(in []authoritystore.AttachedOrder) []authoritystore.AttachedOrder {
	if in == nil {
		return nil
	}
	out := make([]authoritystore.AttachedOrder, len(in))
	copy(out, in)
	for index := range out {
		out[index].Payload = cloneBytes(out[index].Payload)
	}
	return out
}

func cloneSchedules(in []authoritystore.AttachedSchedule) []authoritystore.AttachedSchedule {
	if in == nil {
		return nil
	}
	out := make([]authoritystore.AttachedSchedule, len(in))
	copy(out, in)
	for index := range out {
		out[index].Payload = cloneBytes(out[index].Payload)
	}
	return out
}

func cloneOutbox(in []authoritystore.PendingOutbox) []authoritystore.PendingOutbox {
	if in == nil {
		return nil
	}
	out := make([]authoritystore.PendingOutbox, len(in))
	copy(out, in)
	for index := range out {
		out[index].Payload = cloneBytes(out[index].Payload)
	}
	return out
}

// replaceReflectedBytes deliberately uses only the public reflect API. It
// returns whether a matching mutable byte slice was reachable.
func replaceReflectedBytes(value reflect.Value, before, after []byte) bool {
	if !value.IsValid() {
		return false
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return false
		}
		return replaceReflectedBytes(value.Elem(), before, after)
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if replaceReflectedBytes(value.Field(index), before, after) {
				return true
			}
		}
	case reflect.Map:
		if value.IsNil() {
			return false
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if replaceReflectedBytes(iterator.Value(), before, after) {
				return true
			}
		}
	case reflect.Slice:
		if value.IsNil() {
			return false
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			mutable := value.Bytes()
			if len(mutable) == len(after) && bytes.Equal(mutable, before) {
				copy(mutable, after)
				return true
			}
			return false
		}
		for index := 0; index < value.Len(); index++ {
			if replaceReflectedBytes(value.Index(index), before, after) {
				return true
			}
		}
	}
	return false
}
