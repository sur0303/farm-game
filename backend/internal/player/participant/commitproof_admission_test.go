package participant

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/worker/sourceorder"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type participantCommitProofRouteFunc func(context.Context, sourceorder.CommitProofStatement) error

func (f participantCommitProofRouteFunc) VerifyCommitProofRoute(ctx context.Context, statement sourceorder.CommitProofStatement) error {
	return f(ctx, statement)
}

func signedCareCommitProofCall(t *testing.T, now, verifyNow time.Time, routeErr error) (*contracts.ParticipantCall, *sourceorder.CommitProofValidator) {
	t.Helper()
	request := careRequest(t, plantTestPlayerID, plantTestPlayerID, "route-commit-proof", contractspb.CareContext_CARE_CONTEXT_OWNER, uint64(now.Add(-time.Second).UnixMilli()), 7)
	return signedCommitProofCall(t, request, now, verifyNow, routeErr)
}

func signedCommitProofCall(t *testing.T, request *contractspb.ParticipantRequest, now, verifyNow time.Time, routeErr error) (*contracts.ParticipantCall, *sourceorder.CommitProofValidator) {
	t.Helper()
	base, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := sourceorder.CommitProofExpectedFromParticipantCall(base, "proof-realm")
	if err != nil {
		t.Fatal(err)
	}
	statement := sourceorder.CommitProofStatement{
		Work: expected.Work, WorkVersion: expected.WorkVersion, OperationID: expected.OperationID,
		SubjectPlayerID: expected.SubjectPlayerID, PayloadHash: expected.PayloadHash,
		DueAtMs: now.Add(-time.Second).UnixMilli(), SourceRealm: expected.SourceRealm,
		AuthorityLayoutVersion: expected.AuthorityLayoutVersion,
		SourceHolderID:         "farm-holder-0", SourceHolderBootID: "farm-holder-0-boot-a",
		AssignmentRevision: 7, WriterEpoch: 11, LeaseEpoch: 13,
		LeaseUntilMs: now.Add(30 * time.Second).UnixMilli(), SourceRecordVersion: 9,
		CommittedAtMs: now.Add(-1500 * time.Millisecond).UnixMilli(), ExpiresAtMs: now.Add(5 * time.Second).UnixMilli(),
		DestinationRouteDomain: expected.DestinationRouteDomain, DestinationActorID: expected.DestinationActorID,
		DestinationStep: expected.DestinationStep, DestinationInvocationID: expected.DestinationInvocationID,
	}
	seed := sha256.Sum256([]byte("participant-production-commit-proof-key"))
	private := ed25519.NewKeyFromSeed(seed[:])
	public := private.Public().(ed25519.PublicKey)
	signer, err := sourceorder.NewCommitProofSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, "farm-key-a", private)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := signer.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	request.SourceOrderCommitProof, err = sourceorder.CommitProofToProto(proof)
	if err != nil {
		t.Fatal(err)
	}
	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := sourceorder.NewCommitProofKeyring([]sourceorder.CommitProofVerificationKey{{
		SourceRealm: statement.SourceRealm, HolderID: statement.SourceHolderID, HolderBootID: statement.SourceHolderBootID,
		KeyID: proof.KeyID, PublicKey: public, NotBeforeMs: statement.CommittedAtMs - 1, NotAfterMs: statement.LeaseUntilMs + 1,
	}}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	route := participantCommitProofRouteFunc(func(context.Context, sourceorder.CommitProofStatement) error { return routeErr })
	validator, err := sourceorder.NewCommitProofValidator(keyring, route, func() time.Time { return verifyNow })
	if err != nil {
		t.Fatal(err)
	}
	return call, validator
}

func redisCareProof(call *contracts.ParticipantCall, dueAtMs int64) (CareDependencyProof, error) {
	call.TrustedSourceDueAtMs = dueAtMs
	return careDependencyProofFromCommitProof(call)
}

func TestCommitProofShadowMixedVersionAndPreferBypass(t *testing.T) {
	now := time.UnixMilli(1_800_000_100_000)
	call, validator := signedCareCommitProofCall(t, now, now, nil)
	dueAtMs := call.SourceOrderCommitProof.GetStatement().GetDueAtMs()

	redisCalls := 0
	redisVerifier := CareDependencyVerifierFunc(func(_ context.Context, got *contracts.ParticipantCall) (CareDependencyProof, error) {
		redisCalls++
		return redisCareProof(got, dueAtMs)
	})
	shadowMetrics := &CommitProofMetrics{}
	shadow := &Server{careDependency: redisVerifier, commitProofMode: sourceorder.CommitProofModeShadow,
		commitProof: validator, commitProofSourceRealm: "proof-realm", commitProofMetrics: shadowMetrics}
	proof, err := shadow.verifyCareDependency(context.Background(), call)
	if err != nil || validateCareDependencyProof(call, proof) != nil || redisCalls != 1 {
		t.Fatalf("shadow proof=%+v redis_calls=%d err=%v", proof, redisCalls, err)
	}
	if shadowMetrics.decisionCount(commitProofFamilyCare, commitProofDecisionShadowMatch) != 1 {
		t.Fatal("valid shadow proof did not match Redis")
	}

	mixed := *call
	mixed.SourceOrderCommitProof = nil
	if _, err := shadow.verifyCareDependency(context.Background(), &mixed); err != nil || redisCalls != 2 {
		t.Fatalf("mixed-version missing proof did not use Redis: calls=%d err=%v", redisCalls, err)
	}
	if shadowMetrics.validationCount(commitProofFamilyCare, commitProofValidationMissing) != 1 ||
		shadowMetrics.decisionCount(commitProofFamilyCare, commitProofDecisionShadowMismatch) != 1 {
		t.Fatalf("mixed-version metrics missing=%d mismatch=%d",
			shadowMetrics.validationCount(commitProofFamilyCare, commitProofValidationMissing),
			shadowMetrics.decisionCount(commitProofFamilyCare, commitProofDecisionShadowMismatch))
	}

	preferMetrics := &CommitProofMetrics{}
	prefer := &Server{careDependency: redisVerifier, commitProofMode: sourceorder.CommitProofModePrefer,
		commitProof: validator, commitProofSourceRealm: "proof-realm", commitProofMetrics: preferMetrics}
	proof, err = prefer.verifyCareDependency(context.Background(), call)
	if err != nil || validateCareDependencyProof(call, proof) != nil || redisCalls != 2 {
		t.Fatalf("prefer did not bypass source Redis: calls=%d proof=%+v err=%v", redisCalls, proof, err)
	}
	if preferMetrics.decisionCount(commitProofFamilyCare, commitProofDecisionBypass) != 1 {
		t.Fatal("prefer bypass metric missing")
	}
}

func TestHarvestCommitProofPreferBypassesAndShadowComparesRedis(t *testing.T) {
	now := time.UnixMilli(1_800_000_150_000)
	request, _ := harvestRequest(t, harvestWorkID(t, "source-proof"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 5, 10)
	work := request.GetCoordinator().GetWork()
	work.PlacementKind = contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET
	work.BucketId = 3
	farmRoute := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	work.SourceAuthority = &contractspb.SourceAuthorityRef{
		AuthorityKind:        contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR,
		CanonicalAuthorityId: "7001", AuthorityLayoutVersion: plantTestLayout, ActorRouteDomain: &farmRoute,
	}
	call, validator := signedCommitProofCall(t, request, now, now, nil)
	dueAtMs := call.SourceOrderCommitProof.GetStatement().GetDueAtMs()
	redisCalls := 0
	redis := HarvestSourceVerifierFunc(func(_ context.Context, got *contracts.ParticipantCall) error {
		redisCalls++
		got.TrustedSourceDueAtMs = dueAtMs
		return nil
	})
	prefer := &Server{harvestSource: redis, commitProofMode: sourceorder.CommitProofModePrefer,
		commitProof: validator, commitProofSourceRealm: "proof-realm", commitProofMetrics: &CommitProofMetrics{}}
	if err := prefer.verifyHarvestSource(context.Background(), call); err != nil || redisCalls != 0 {
		t.Fatalf("Harvest prefer err=%v redis_calls=%d", err, redisCalls)
	}
	shadowMetrics := &CommitProofMetrics{}
	shadow := &Server{harvestSource: redis, commitProofMode: sourceorder.CommitProofModeShadow,
		commitProof: validator, commitProofSourceRealm: "proof-realm", commitProofMetrics: shadowMetrics}
	if err := shadow.verifyHarvestSource(context.Background(), call); err != nil || redisCalls != 1 {
		t.Fatalf("Harvest shadow err=%v redis_calls=%d", err, redisCalls)
	}
	if shadowMetrics.decisionCount(commitProofFamilyHarvest, commitProofDecisionShadowMatch) != 1 {
		t.Fatal("Harvest shadow comparison metric missing")
	}
}

func TestCommitProofOffUsesRedisAndPrivateRegistryReportsRedisOff(t *testing.T) {
	now := time.UnixMilli(1_800_000_175_000)
	careCall, _ := signedCareCommitProofCall(t, now, now, nil)
	careDue := careCall.SourceOrderCommitProof.GetStatement().GetDueAtMs()

	harvestRequest, _ := harvestRequest(t, harvestWorkID(t, "redis-off"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 5, 10)
	harvestCall, err := contracts.ValidateAndDecode(harvestRequest, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}

	metrics := &CommitProofMetrics{}
	careCalls := 0
	harvestCalls := 0
	server := &Server{
		commitProofMode: sourceorder.CommitProofModeOff, commitProofMetrics: metrics,
		careDependency: CareDependencyVerifierFunc(func(_ context.Context, call *contracts.ParticipantCall) (CareDependencyProof, error) {
			careCalls++
			return redisCareProof(call, careDue)
		}),
		harvestSource: HarvestSourceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			harvestCalls++
			return nil
		}),
	}
	if _, err := server.verifyCareDependency(context.Background(), careCall); err != nil {
		t.Fatal(err)
	}
	if err := server.verifyHarvestSource(context.Background(), harvestCall); err != nil {
		t.Fatal(err)
	}
	if careCalls != 1 || harvestCalls != 1 ||
		metrics.decisionCount(commitProofFamilyCare, commitProofDecisionRedisOff) != 1 ||
		metrics.decisionCount(commitProofFamilyHarvest, commitProofDecisionRedisOff) != 1 {
		t.Fatalf("off decisions care_calls=%d harvest_calls=%d care_metric=%d harvest_metric=%d", careCalls, harvestCalls,
			metrics.decisionCount(commitProofFamilyCare, commitProofDecisionRedisOff),
			metrics.decisionCount(commitProofFamilyHarvest, commitProofDecisionRedisOff))
	}

	registry := prometheus.NewRegistry()
	if err := RegisterCommitProofMetrics(registry, metrics, sourceorder.CommitProofModeOff); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "farm_source_order_commit_proof_admission_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["decision"] == "redis_off" {
				seen[labels["family"]] = metric.GetCounter().GetValue()
			}
		}
	}
	if seen["care"] != 1 || seen["harvest"] != 1 {
		t.Fatalf("gathered redis_off=%v", seen)
	}
}

func TestCommitProofWorkerAlwaysRejectsEnvelopeAndUsesRedis(t *testing.T) {
	request, _ := harvestRequest(t, harvestWorkID(t, "worker-proof"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 5, 10)
	work := request.GetCoordinator().GetWork()
	work.PlacementKind = contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET
	work.BucketId = 3
	farmRoute := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	work.SourceAuthority = &contractspb.SourceAuthorityRef{AuthorityKind: contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR,
		CanonicalAuthorityId: "7001", AuthorityLayoutVersion: plantTestLayout, ActorRouteDomain: &farmRoute}
	coordinator := request.GetCoordinator()
	request.CallerContext = &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
		CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
		CallerInstanceId: "worker-a", CallerBootId: "worker-a-boot", Work: work,
		ExpectedWorkVersion: coordinator.GetExpectedWorkVersion(), ParticipantStep: coordinator.GetParticipantStep(),
		ParticipantInvocationId: coordinator.GetParticipantInvocationId(), LeaseOwnerToken: "lease-token", LeaseEpoch: 1,
		RemainingBudgetMs: coordinator.GetRemainingBudgetMs(),
	}}
	request.SourceOrderCommitProof = &contractspb.SourceOrderCommitProof{SchemaVersion: sourceorder.CommitProofSchemaVersion}
	if _, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerWorkerRecovery); statusCode(err) != codes.InvalidArgument {
		t.Fatalf("Worker accepted commit proof envelope: %v", err)
	}
	request.SourceOrderCommitProof = nil
	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerWorkerRecovery)
	if err != nil {
		t.Fatal(err)
	}
	redisCalls := 0
	server := &Server{commitProofMode: sourceorder.CommitProofModePrefer, commitProofMetrics: &CommitProofMetrics{},
		harvestSource: HarvestSourceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { redisCalls++; return nil })}
	if err := server.verifyHarvestSource(context.Background(), call); err != nil || redisCalls != 1 {
		t.Fatalf("Worker did not verify source Redis: calls=%d err=%v", redisCalls, err)
	}
}

func TestCommitProofPreferInvalidStaleAndMovedFallbackBeforeRedis(t *testing.T) {
	now := time.UnixMilli(1_800_000_200_000)
	validCall, validator := signedCareCommitProofCall(t, now, now, nil)
	redisCalls := 0
	redisVerifier := CareDependencyVerifierFunc(func(_ context.Context, got *contracts.ParticipantCall) (CareDependencyProof, error) {
		redisCalls++
		return redisCareProof(got, got.SourceOrderCommitProof.GetStatement().GetDueAtMs())
	})
	tests := []struct {
		name      string
		call      *contracts.ParticipantCall
		validator *sourceorder.CommitProofValidator
	}{
		{name: "missing", call: func() *contracts.ParticipantCall { copy := *validCall; copy.SourceOrderCommitProof = nil; return &copy }(), validator: validator},
		{name: "invalid", call: func() *contracts.ParticipantCall {
			copy := *validCall
			copy.SourceOrderCommitProof = proto.Clone(copy.SourceOrderCommitProof).(*contractspb.SourceOrderCommitProof)
			copy.SourceOrderCommitProof.Signature[0]++
			return &copy
		}(), validator: validator},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := &CommitProofMetrics{}
			server := &Server{careDependency: redisVerifier, commitProofMode: sourceorder.CommitProofModePrefer,
				commitProof: test.validator, commitProofSourceRealm: "proof-realm", commitProofMetrics: metrics}
			if _, err := server.verifyCareDependency(context.Background(), test.call); statusCode(err) != codes.FailedPrecondition {
				t.Fatalf("fallback err=%v", err)
			}
			if metrics.decisionCount(commitProofFamilyCare, commitProofDecisionRecoveryRequired) != 1 {
				t.Fatal("durable recovery requirement metric missing")
			}
		})
	}

	staleCall, staleValidator := signedCareCommitProofCall(t, now, now.Add(time.Minute), nil)
	staleServer := &Server{careDependency: redisVerifier, commitProofMode: sourceorder.CommitProofModePrefer,
		commitProof: staleValidator, commitProofSourceRealm: "proof-realm", commitProofMetrics: &CommitProofMetrics{}}
	if _, err := staleServer.verifyCareDependency(context.Background(), staleCall); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("stale fallback err=%v", err)
	}

	movedCall, movedValidator := signedCareCommitProofCall(t, now, now, sourceorder.ErrCommitProofRoute)
	movedServer := &Server{careDependency: redisVerifier, commitProofMode: sourceorder.CommitProofModePrefer,
		commitProof: movedValidator, commitProofSourceRealm: "proof-realm", commitProofMetrics: &CommitProofMetrics{}}
	if _, err := movedServer.verifyCareDependency(context.Background(), movedCall); statusCode(err) != codes.FailedPrecondition {
		t.Fatalf("moved fallback err=%v", err)
	}
	if redisCalls != 0 {
		t.Fatalf("invalid/stale/moved prefer proof reached source Redis %d times", redisCalls)
	}
}

func TestCommitProofMetricsHaveFixedCardinality(t *testing.T) {
	metrics := &CommitProofMetrics{}
	registry := prometheus.NewRegistry()
	if err := RegisterCommitProofMetrics(registry, metrics, sourceorder.CommitProofModeOff); err != nil {
		t.Fatal(err)
	}
	// Exercise counters repeatedly; no work, actor, holder, boot, key, or
	// operation identity is accepted as a label dimension.
	now := time.UnixMilli(1_800_000_250_000)
	care, _ := signedCareCommitProofCall(t, now, now, nil)
	harvestRequest, _ := harvestRequest(t, harvestWorkID(t, "metric-cardinality"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 1, 1)
	harvest, err := contracts.ValidateAndDecode(harvestRequest, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		metrics.observeValidation(care, index%commitProofValidationCount)
		metrics.observeDecision(care, index%commitProofDecisionCount)
		metrics.observeValidation(harvest, index%commitProofValidationCount)
		metrics.observeDecision(harvest, index%commitProofDecisionCount)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	cardinality := map[string]int{}
	for _, family := range families {
		cardinality[family.GetName()] = len(family.GetMetric())
	}
	want := map[string]int{
		"farm_source_order_commit_proof_validation_total": 10,
		"farm_source_order_commit_proof_admission_total":  10,
		"farm_source_order_commit_proof_mode":             3,
	}
	if len(cardinality) != len(want) {
		t.Fatalf("metric families=%v", cardinality)
	}
	for name, count := range want {
		if cardinality[name] != count {
			t.Fatalf("metric %s cardinality=%d want=%d (all=%v)", name, cardinality[name], count, cardinality)
		}
	}
}

func statusCode(err error) codes.Code { return status.Code(err) }
