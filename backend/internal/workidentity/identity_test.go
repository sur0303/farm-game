package workidentity

import "testing"

func TestStableOpaqueIDGoldenVectors(t *testing.T) {
	workflowID, err := WorkflowID(10001, "PLANT", "op-0001")
	assertIdentity(t, workflowID, err, "sid1_pK6aJIBpMU-PpnlrN1v-fHZrdkGG_fgNJOFHG70Bg4s")

	orderID, err := SourceOrderID("CARE_REWARD", "FARM", 20002, "care-plot-3-cycle-7", 12)
	assertIdentity(t, orderID, err, "sid1_b5wzCfNHotiHpKRT4j50m8cG9Z81yurh409UIDCzGR0")

	invocationID, err := ParticipantInvocationID("PLANT", workflowID, "RESERVE_SEED", 1)
	assertIdentity(t, invocationID, err, "sid1_sj6dghgqP6O6OZwt9H35c70SKGQVldNqQCd5FPAlf8A")

	unlockWorkflowID, err := WorkflowID(10001, "UNLOCK_PLOT", "op-unlock-0001")
	assertIdentity(t, unlockWorkflowID, err, "sid1_SeqFyhVqwC6qXGpFWZ-0yYby_uEBns7oIXvRuui6kHE")
	theftWorkflowID, err := WorkflowID(10001, "THEFT", "op-theft-0001")
	assertIdentity(t, theftWorkflowID, err, "sid1_uI4Mff9effOsklPTi7nTjdRFe8YaXkDuz_k9zILnQQc")

	unlockReservationID, err := UnlockCoinReservationID(unlockWorkflowID)
	assertIdentity(t, unlockReservationID, err, "sid1_1sgMjkkNApGNiIx2uqGziZ7R9f7emBWgavpDU06hqcI")
	seedReservationID, err := SeedReservationID(workflowID)
	assertIdentity(t, seedReservationID, err, "sid1_h0ZFZOBOO7FXtkV8_inlqcTTZOg4bHO1ZKSDJh1NV3E")
	cropCycleID, err := CropCycleID(workflowID)
	assertIdentity(t, cropCycleID, err, "sid1_aViS3BR5js7sFDS5mI1WMwoUri7G4jnll_XnS-_ns2M")
	careIncidentID, err := CareIncidentID(cropCycleID, "WATER", 1)
	assertIdentity(t, careIncidentID, err, "sid1_fIxtzCeWXtEQGCLB2MFLxeVgCPOONqUYXmSTlhNe_BI")
	theftAttemptID, err := TheftAttemptID(20002, cropCycleID, 10001)
	assertIdentity(t, theftAttemptID, err, "sid1_FpmjZt61kuQ5fiyaDDuHOkTC6l9YeygaW9Tghv0K9ng")
	theftSettlementID, err := TheftSettlementID(theftWorkflowID)
	assertIdentity(t, theftSettlementID, err, "sid1_mZzxya9hQ7_FbzDYzgeSn6c2c3bwXVVFnJ8Q2C-8e2k")
}

func TestOperationDigestGoldenVector(t *testing.T) {
	got, err := OperationDigest(10001, "CARE", "op-care-0001")
	assertIdentity(t, got, err, "3bb259228c635917cc44509eb7fed0ba49b3af6e0515a297500dbdd9ce1548ee")
}

func TestStableOpaqueIDRejectsNonCanonicalInputs(t *testing.T) {
	if _, err := WorkflowID(0, "PLANT", "op"); err == nil {
		t.Fatal("zero subject accepted")
	}
	if _, err := WorkflowID(1, "Plant", "op"); err == nil {
		t.Fatal("unstable command name accepted")
	}
	if err := ValidateStableOpaqueID("sid1_short"); err == nil {
		t.Fatal("truncated stable id accepted")
	}
	valid, _ := WorkflowID(1, "PLANT", "op")
	if err := ValidateStableOpaqueID(valid + "="); err == nil {
		t.Fatal("padded stable id accepted")
	}
}

func assertIdentity(t *testing.T, got string, err error, want string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("identity\n got: %s\nwant: %s", got, want)
	}
}
