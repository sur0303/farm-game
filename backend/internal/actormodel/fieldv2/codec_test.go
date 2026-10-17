package fieldv2

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"farmgame/internal/actorrouting"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var benchmarkEnvelope []byte

func TestMarshalProtoMatchesOwnedPayloadPath(t *testing.T) {
	message := wrapperspb.String(strings.Repeat("farm-profile-", 128))
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Marshal(actorrouting.DomainPlayer, "profile", payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MarshalProto(actorrouting.DomainPlayer, "profile", message)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("direct protobuf envelope differs from the frozen owned-payload encoding")
	}
}

func TestVisitPayloadBorrowsOnlyForSynchronousCallback(t *testing.T) {
	encoded, err := Marshal(actorrouting.DomainFarm, "plots", []byte("immutable-payload"))
	if err != nil {
		t.Fatal(err)
	}
	borrowed := false
	if err := VisitPayload(actorrouting.DomainFarm, "plots", encoded, func(payload []byte) error {
		borrowed = len(payload) > 0 && &payload[0] == &encoded[envelopeHeaderBytes]
		if string(payload) != "immutable-payload" {
			t.Fatalf("payload=%q", payload)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !borrowed {
		t.Fatal("visitor received an unexpected detached payload allocation")
	}

	wantErr := errors.New("stop decoding")
	if err := VisitPayload(actorrouting.DomainFarm, "plots", encoded, func([]byte) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("visitor error=%v", err)
	}
	if err := VisitPayload(actorrouting.DomainFarm, "plots", encoded, nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("nil visitor error=%v", err)
	}
}

func TestUnmarshalStillReturnsOwnedPayload(t *testing.T) {
	encoded, err := Marshal(actorrouting.DomainFarm, "plots", []byte("owned-payload"))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := Unmarshal(actorrouting.DomainFarm, "plots", encoded)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if got := string(encoded[envelopeHeaderBytes:]); got != "owned-payload" {
		t.Fatalf("public Unmarshal leaked borrowed storage: %q", got)
	}
}

func BenchmarkMarshalProtoEnvelope(b *testing.B) {
	message := wrapperspb.String(strings.Repeat("farm-profile-", 4096))
	b.Run("owned-payload", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkEnvelope, err = Marshal(actorrouting.DomainPlayer, "profile", payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("direct-envelope", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var err error
			benchmarkEnvelope, err = MarshalProto(actorrouting.DomainPlayer, "profile", message)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
