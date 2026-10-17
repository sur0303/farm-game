package fieldv2

import (
	"encoding/binary"
	"fmt"

	"farmgame/internal/actorrouting"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const envelopeHeaderBytes = 12

// Marshal wraps already-deterministic field payload bytes with a field-bound
// header. Cross-field substitution and trailing bytes therefore fail closed.
func Marshal(domain actorrouting.RouteDomain, name string, payload []byte) ([]byte, error) {
	field, ok := fieldFor(domain, name)
	if !ok || len(payload) > MaxFieldValueBytes-envelopeHeaderBytes {
		return nil, fmt.Errorf("%w: field %q", ErrMalformed, name)
	}
	domainID := byte(0)
	if domain == actorrouting.DomainPlayer {
		domainID = 1
	} else if domain == actorrouting.DomainFarm {
		domainID = 2
	}
	out := make([]byte, envelopeHeaderBytes, envelopeHeaderBytes+len(payload))
	copy(out[:4], "AMF2")
	out[4] = domainID
	out[5] = field.id
	binary.BigEndian.PutUint16(out[6:8], 1)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(payload)))
	out = append(out, payload...)
	return out, nil
}

// ValidateNoUnknown recursively rejects protobuf unknown fields. A converter
// must never strand unowned future bytes in an arbitrary v2 model.
func ValidateNoUnknown(message proto.Message) error {
	if message == nil {
		return fmt.Errorf("%w: nil protobuf", ErrMalformed)
	}
	return validateNoUnknownMessage(message.ProtoReflect())
}

func validateNoUnknownMessage(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("%w: protobuf unknown fields", ErrMalformed)
	}
	var found error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind && field.MapValue().Kind() != protoreflect.GroupKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				found = validateNoUnknownMessage(item.Message())
				return found == nil
			})
			return found == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
				return true
			}
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				if found = validateNoUnknownMessage(list.Get(i).Message()); found != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			found = validateNoUnknownMessage(value.Message())
			return found == nil
		}
		return true
	})
	return found
}

func Unmarshal(domain actorrouting.RouteDomain, name string, encoded []byte) ([]byte, error) {
	field, ok := fieldFor(domain, name)
	if !ok || len(encoded) < envelopeHeaderBytes || len(encoded) > MaxFieldValueBytes ||
		string(encoded[:4]) != "AMF2" || binary.BigEndian.Uint16(encoded[6:8]) != 1 {
		return nil, fmt.Errorf("%w: field %q envelope", ErrMalformed, name)
	}
	wantDomain := byte(1)
	if domain == actorrouting.DomainFarm {
		wantDomain = 2
	}
	length := int(binary.BigEndian.Uint32(encoded[8:12]))
	if encoded[4] != wantDomain || encoded[5] != field.id || length != len(encoded)-envelopeHeaderBytes {
		return nil, fmt.Errorf("%w: field %q identity", ErrMalformed, name)
	}
	return append([]byte(nil), encoded[envelopeHeaderBytes:]...), nil
}

// DisabledState is the single deterministic representation for gameplay
// carriers that are intentionally absent in R2. Empty bytes are not used so a
// missing field cannot be confused with a present disabled field.
func DisabledState(domain actorrouting.RouteDomain, name string) ([]byte, error) {
	return Marshal(domain, name, []byte("DISABLED"))
}

func ValidateDisabledState(domain actorrouting.RouteDomain, name string, encoded []byte) error {
	payload, err := Unmarshal(domain, name, encoded)
	if err != nil || string(payload) != "DISABLED" {
		return fmt.Errorf("%w: field %q is not disabled", ErrMalformed, name)
	}
	return nil
}
