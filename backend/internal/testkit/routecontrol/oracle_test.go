package routecontrol

import "testing"

func TestOracleRegistryStoresFutureMetadataOnly(t *testing.T) {
	registry := NewOracleRegistry()
	if err := registry.RegisterFuture(OracleSpec{
		ID:                   "route.e3",
		Description:          "future real-process route observation",
		MinimumEvidenceLevel: EvidenceE3,
		RequiresAttestation:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(OracleSpec{
		ID:                   "route.e1",
		Description:          "future local observation",
		MinimumEvidenceLevel: EvidenceE1,
	}); err != nil {
		t.Fatal(err)
	}
	specs := registry.List()
	if len(specs) != 2 || specs[0].ID != "route.e1" || specs[1].ID != "route.e3" {
		t.Fatalf("unexpected sorted specs: %+v", specs)
	}
	if _, ok := registry.Lookup("route.e3"); !ok {
		t.Fatal("registered oracle not found")
	}
}

func TestOracleRegistryRejectsUnsafeHighLevelRegistration(t *testing.T) {
	registry := NewOracleRegistry()
	if err := registry.Register(OracleSpec{
		ID:                   "route-e3",
		Description:          "missing attestation requirement",
		MinimumEvidenceLevel: EvidenceE3,
	}); err == nil {
		t.Fatal("high-level oracle without attestation requirement accepted")
	}
	if err := registry.Register(OracleSpec{
		ID:                   "route.e3",
		Description:          "ok",
		MinimumEvidenceLevel: EvidenceE3,
		RequiresAttestation:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(OracleSpec{
		ID:                   "route.e3",
		Description:          "duplicate",
		MinimumEvidenceLevel: EvidenceE3,
		RequiresAttestation:  true,
	}); err == nil {
		t.Fatal("duplicate oracle accepted")
	}
}
