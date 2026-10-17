package main

import "testing"

func TestFormalLeaseBatchingEvidence(t *testing.T) {
	report, err := runEvidence()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEvidence(&report); err != nil {
		t.Fatalf("formal evidence invalid: %v\nreport=%+v", err, report)
	}
}
