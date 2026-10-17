package ownerstore

import (
	ownercorepb "farmgame/api/gen/ownercore"

	"google.golang.org/protobuf/proto"
)

func protoClone(rec *ownercorepb.OwnerCoreRecord) *ownercorepb.OwnerCoreRecord {
	return proto.Clone(rec).(*ownercorepb.OwnerCoreRecord)
}
