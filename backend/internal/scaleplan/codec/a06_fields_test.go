package codec_test

import (
	"bytes"
	"reflect"
	"testing"

	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/codec"
	"farmgame/internal/testkit/scaleplanstore"
)

func TestA06FieldsRoundTripAndChecksumBinding(t *testing.T) {
	plan := scaleplanstore.ReleasingScaleInPlan(t, "realm-a", "operation-a06", "workload-a06")
	encoded, err := codec.EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"phase":"RELEASING_EXCLUSION"`, `"cleanup_intent"`, `"failure_code":"SCALE_IN_FAILURE"`,
		`"scale_resolution":"NOT_COMMITTED"`, `"release_outcome":"RELEASED"`,
	} {
		if !bytes.Contains(encoded, []byte(want)) {
			t.Fatalf("canonical JSON omitted %s: %s", want, encoded)
		}
	}
	decoded, err := codec.DecodePlan(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan, decoded) {
		t.Fatalf("A0.6 plan changed across round-trip: want=%+v got=%+v", plan, decoded)
	}
	reencoded, err := codec.EncodePlan(decoded)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("A0.6 canonical bytes changed: err=%v", err)
	}
	baseChecksum := codec.Checksum(encoded)
	for _, outcome := range []core.PlacementReleaseOutcome{
		core.PlacementReleaseAlreadyReleased, core.PlacementReleaseBootRollover,
	} {
		candidate := plan.Clone()
		candidate.CleanupIntent.ReleaseOutcome = outcome
		candidateBytes, err := codec.EncodePlan(candidate)
		if err != nil {
			t.Fatalf("outcome %s encode: %v", outcome, err)
		}
		if bytes.Equal(encoded, candidateBytes) || codec.Checksum(candidateBytes) == baseChecksum {
			t.Fatalf("outcome %s did not change canonical identity", outcome)
		}
		candidateDecoded, err := codec.DecodePlan(candidateBytes)
		if err != nil || candidateDecoded.CleanupIntent.ReleaseOutcome != outcome {
			t.Fatalf("outcome %s round-trip: plan=%+v err=%v", outcome, candidateDecoded, err)
		}
	}
	proofPlan := scaleplanstore.SafeTerminatedScaleInPlan(t, "realm-a", "operation-proof", "workload-proof")
	proofBytes, err := codec.EncodePlan(proofPlan)
	if err != nil {
		t.Fatalf("proof encode: %v", err)
	}
	proofDecoded, err := codec.DecodePlan(proofBytes)
	if err != nil || !reflect.DeepEqual(proofPlan, proofDecoded) {
		t.Fatalf("proof A0.6 round-trip: err=%v want=%+v got=%+v", err, proofPlan, proofDecoded)
	}
	for _, want := range []string{`"scale_resolution":"NOT_COMMITTED"`, `"exclusion_release_outcome":"RELEASED"`} {
		if !bytes.Contains(proofBytes, []byte(want)) {
			t.Fatalf("proof JSON omitted %s: %s", want, proofBytes)
		}
	}
	invalid := plan.Clone()
	invalid.CleanupIntent.ReleaseOutcome = core.PlacementReleaseUnknown
	if _, err := codec.EncodePlan(invalid); err == nil {
		t.Fatal("accepted nonterminal release outcome in cleanup intent")
	}
}

func TestDecodeA06RejectsOldSchemaCorruptionAndMissingProofEvidence(t *testing.T) {
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-old", "workload-old")
	encoded, err := codec.EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	legacySchema := bytes.Replace(encoded, []byte(`"schema_version":"scale-plan-v2"`), []byte(`"schema_version":"scale-plan-v1"`), 1)
	if _, err := codec.DecodePlan(legacySchema); err == nil {
		t.Fatal("accepted an old core schema")
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)/2] ^= 1
	if _, err := codec.DecodePlan(corrupt); err == nil {
		t.Fatal("accepted corrupted canonical bytes")
	}
	proofBytes, err := codec.EncodePlan(scaleplanstore.SafeTerminatedScaleInPlan(t, "realm-a", "operation-proof-old", "workload-proof-old"))
	if err != nil {
		t.Fatal(err)
	}
	legacyProof := bytes.Replace(proofBytes, []byte(`,"scale_resolution":"NOT_COMMITTED"`), nil, 1)
	if bytes.Equal(proofBytes, legacyProof) {
		t.Fatal("proof fixture did not contain the new field")
	}
	if _, err := codec.DecodePlan(legacyProof); err == nil {
		t.Fatal("accepted a proof record missing required A0.6 evidence")
	}
}
