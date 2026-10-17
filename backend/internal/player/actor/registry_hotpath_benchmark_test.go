package actor

import (
	"fmt"
	"testing"

	playerpb "farmgame/api/gen/player"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

var benchmarkPlayerRecordSink *playerpb.PlayerRecord

func benchmarkResidentPlayerRecord(b *testing.B) (*playerpb.PlayerRecord, authoritystore.AuthoritySnapshot) {
	b.Helper()
	record := initialRecord(98_001, "benchmark-player")
	// A resident player quickly accumulates bounded receipts. Keeping them in
	// the fixture prevents a tiny bootstrap-only record from hiding the cost
	// seen in the journey-bot allocation profile.
	for index := 0; index < 64; index++ {
		record.RecentOperationReceipts = append(record.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
			OperationId:   fmt.Sprintf("benchmark-operation-%03d", index),
			PayloadHash:   make([]byte, 32),
			ResultPayload: make([]byte, 64),
		})
	}
	state, err := playeractormodelv2.Encode(record)
	if err != nil {
		b.Fatal(err)
	}
	return record, authoritystore.AuthoritySnapshot{
		ActorAddress:  authoritystore.ActorAddress{ActorID: record.GetPlayerId()},
		RecordVersion: 1,
		ModelLayout:   authoritystore.ModelLayoutV2,
		ModelFields:   state,
	}
}

func BenchmarkResidentPlayerV2Decode(b *testing.B) {
	_, snapshot := benchmarkResidentPlayerRecord(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record, err := decodePlayerAuthoritySnapshot(snapshot, snapshot.ActorAddress.ActorID)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkPlayerRecordSink = record
	}
}

func BenchmarkResidentPlayerProtoClone(b *testing.B) {
	record, _ := benchmarkResidentPlayerRecord(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkPlayerRecordSink = proto.Clone(record).(*playerpb.PlayerRecord)
	}
}
