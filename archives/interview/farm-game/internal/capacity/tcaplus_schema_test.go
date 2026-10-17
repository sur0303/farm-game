package capacity

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	farmtcapluspb "farmgame/api/gen/farmtcaplus"
	tcapluspb "farmgame/api/gen/tcapluspb"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var tcaplusIndexPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*\(([a-z0-9_,]+)\)$`)

// TestTcaplusMigrationSchemaContract prevents a generated-but-unusable schema
// from being presented as migration evidence. It validates the exact five
// tables, their primary keys/indexes and every field referenced by an option
// from the generated descriptor that would be uploaded to TcaplusDB.
func TestTcaplusMigrationSchemaContract(t *testing.T) {
	t.Parallel()
	expected := []struct {
		name    protoreflect.Name
		primary string
		indexes []string
	}{
		{name: "PlayerRecordTable", primary: "player_id"},
		{name: "FarmRecordTable", primary: "farm_id"},
		{name: "WorkflowRecordTable", primary: "workflow_id", indexes: []string{"due_workflow(next_retry_at_ms,state)"}},
		{name: "FriendRelationTable", primary: "player_low_id,player_high_id", indexes: []string{
			"low_roster(player_low_id,status)",
			"high_roster(player_high_id,status)",
		}},
		{name: "FriendMutationReceiptTable", primary: "operation_id"},
	}

	messages := farmtcapluspb.File_tcaplus_farm_tables_proto.Messages()
	if messages.Len() != len(expected) {
		t.Fatalf("table count=%d want=%d", messages.Len(), len(expected))
	}
	for _, want := range expected {
		message := messages.ByName(want.name)
		if message == nil {
			t.Fatalf("missing table %s", want.name)
		}
		options, ok := message.Options().(*descriptorpb.MessageOptions)
		if !ok {
			t.Fatalf("%s has unexpected options type %T", want.name, message.Options())
		}
		primary, ok := proto.GetExtension(options, tcapluspb.E_TcaplusPrimaryKey).(string)
		if !ok || primary != want.primary {
			t.Fatalf("%s primary=%q want=%q", want.name, primary, want.primary)
		}
		indexes, _ := proto.GetExtension(options, tcapluspb.E_TcaplusIndex).([]string)
		if !reflect.DeepEqual(indexes, want.indexes) {
			t.Fatalf("%s indexes=%v want=%v", want.name, indexes, want.indexes)
		}
		assertOptionFieldsExist(t, message, strings.Split(primary, ","))
		for _, index := range indexes {
			match := tcaplusIndexPattern.FindStringSubmatch(index)
			if len(match) != 2 {
				t.Fatalf("%s invalid index declaration %q", want.name, index)
			}
			assertOptionFieldsExist(t, message, strings.Split(match[1], ","))
		}
	}
}

func assertOptionFieldsExist(t *testing.T, message protoreflect.MessageDescriptor, names []string) {
	t.Helper()
	for _, name := range names {
		field := message.Fields().ByName(protoreflect.Name(name))
		if field == nil {
			t.Fatalf("%s option references missing field %q", message.Name(), name)
		}
		if field.Cardinality() == protoreflect.Repeated {
			t.Fatalf("%s key/index field %q is repeated", message.Name(), name)
		}
		switch field.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind, protoreflect.BytesKind,
			protoreflect.FloatKind, protoreflect.DoubleKind:
			t.Fatalf("%s key/index field %q has unsupported kind %s", message.Name(), name, field.Kind())
		}
	}
}
