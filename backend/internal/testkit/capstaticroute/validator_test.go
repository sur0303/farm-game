package capstaticroute

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestValidateBundleRejectsDriftAndStaleLease(t *testing.T) {
	rig := newRealRig(t, 4)
	result, err := rig.initializer.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	receiptBytes, err := MarshalReceipt(result.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBundle(result.Bytes, result.LedgerBytes, receiptBytes, time.Now()); err != nil {
		t.Fatal(err)
	}
	withUnknown := append([]byte(nil), receiptBytes...)
	withUnknown = append(withUnknown[:len(withUnknown)-2], []byte(",\n  \"unexpected\": true\n}\n")...)
	if err := ValidateBundle(result.Bytes, result.LedgerBytes, withUnknown, time.Now()); err == nil {
		t.Fatal("unknown receipt field accepted")
	}

	ledger := result.Ledger
	ledger.Records = append([]LedgerRecord(nil), ledger.Records...)
	ledger.Records[0].HolderID = "drifted-holder"
	ledger.Records[0].RecordSHA256, err = ledgerRecordHash(ledger.Records[0])
	if err != nil {
		t.Fatal(err)
	}
	ledgerBytes, _, err := MarshalLedger(ledger)
	if err != nil {
		t.Fatal(err)
	}
	ledgerDigest := sha256.Sum256(ledgerBytes)
	receipt := result.Receipt
	receipt.ExactLedgerSHA256 = hex.EncodeToString(ledgerDigest[:])
	driftReceipt, _ := MarshalReceipt(receipt)
	if err := ValidateBundle(result.Bytes, ledgerBytes, driftReceipt, time.Now()); err == nil {
		t.Fatal("ledger/snapshot identity drift accepted")
	}

	stale := time.UnixMilli(result.Receipt.MinimumLeaseUntilMs).Add(-time.Duration(result.Receipt.MinimumRemainingLeaseMs)*time.Millisecond + time.Millisecond)
	if err := ValidateBundle(result.Bytes, result.LedgerBytes, receiptBytes, stale); err == nil {
		t.Fatal("insufficient remaining lease accepted")
	}
	if err := ValidateBundleEvidence(result.Bytes, result.LedgerBytes, receiptBytes); err != nil {
		t.Fatalf("historical exact evidence rejected after liveness expiry: %v", err)
	}
}
