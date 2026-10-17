package workidentity

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
)

func TestWorkRefV1GoldenAndRoundTrip(t *testing.T) {
	const workID = "sid1_b5wzCfNHotiHpKRT4j50m8cG9Z81yurh409UIDCzGR0"
	const want = "wr1_AAAAC0NBUkVfUkVXQVJEAAAAMHNpZDFfYjV3ekNmTkhvdGlIcEtSVDRqNTBtOGNHOVo4MXl1cmg0MDlVSURDekdSMA"
	got, err := EncodeWorkRefV1("CARE_REWARD", workID)
	assertIdentity(t, got, err, want)
	decoded, err := DecodeWorkRefV1(got)
	if err != nil || decoded.WorkType != "CARE_REWARD" || decoded.WorkID != workID {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}

func TestScheduleRefV1IsDistinctAndCanonical(t *testing.T) {
	workID, err := WorkflowID(1, "FRIEND_OPPORTUNITY", "schedule")
	if err != nil {
		t.Fatal(err)
	}
	workRef, err := EncodeWorkRefV1("FRIEND_OPPORTUNITY", workID)
	if err != nil {
		t.Fatal(err)
	}
	scheduleRef, err := EncodeScheduleRefV1("FRIEND_OPPORTUNITY", workID)
	if err != nil {
		t.Fatal(err)
	}
	if workRef == scheduleRef {
		t.Fatalf("schedule ref aliases order ref: %q", workRef)
	}
	decoded, err := DecodeScheduleRefV1(scheduleRef)
	if err != nil || decoded.ScheduleType != "FRIEND_OPPORTUNITY" || decoded.ScheduleID != workID {
		t.Fatalf("schedule ref decoded=%+v err=%v", decoded, err)
	}
	if _, err := DecodeScheduleRefV1(workRef); err == nil {
		t.Fatal("order reference accepted as schedule reference")
	}
}

func TestReceiptRefV1GoldenAndRoundTrip(t *testing.T) {
	const digest = "3bb259228c635917cc44509eb7fed0ba49b3af6e0515a297500dbdd9ce1548ee"
	const want = "rr1_AAAABTIwMDAyAAAAEUFVVEhPUklUWV9SRUNFSVBUAAAAQDNiYjI1OTIyOGM2MzU5MTdjYzQ0NTA5ZWI3ZmVkMGJhNDliM2FmNmUwNTE1YTI5NzUwMGRiZGQ5Y2UxNTQ4ZWU"
	got, err := EncodeReceiptRefV1(20002, "AUTHORITY_RECEIPT", digest)
	assertIdentity(t, got, err, want)
	decoded, err := DecodeReceiptRefV1(got)
	if err != nil || decoded.ActorID != 20002 || decoded.ReceiptKind != "AUTHORITY_RECEIPT" || decoded.OperationDigest != digest {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}

func TestReferenceDecodersFailClosed(t *testing.T) {
	workID, _ := WorkflowID(1, "PLANT", "op")
	valid, _ := EncodeWorkRefV1("PLANT", workID)
	lowercaseRaw, _ := appendLP(nil, "plant")
	lowercaseRaw, _ = appendLP(lowercaseRaw, workID)
	lowercaseType := workRefPrefix + base64.RawURLEncoding.EncodeToString(lowercaseRaw)
	for _, input := range []string{"", valid + "=", lowercaseType, "wr1_AAAAAQ"} {
		if _, err := DecodeWorkRefV1(input); err == nil {
			t.Fatalf("malformed work ref accepted: %q", input)
		}
	}

	// A syntactically valid LP payload with a non-canonical actor ID must fail.
	raw := make([]byte, 0)
	raw = binary.BigEndian.AppendUint32(raw, 5)
	raw = append(raw, "02002"...)
	raw = binary.BigEndian.AppendUint32(raw, uint32(len("AUTHORITY_RECEIPT")))
	raw = append(raw, "AUTHORITY_RECEIPT"...)
	raw = binary.BigEndian.AppendUint32(raw, 64)
	raw = append(raw, strings.Repeat("a", 64)...)
	if _, err := DecodeReceiptRefV1("rr1_" + base64.RawURLEncoding.EncodeToString(raw)); err == nil {
		t.Fatal("receipt ref with leading-zero actor accepted")
	}
}
