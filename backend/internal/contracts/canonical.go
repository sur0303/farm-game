// Package contracts registers the shared cross-process contract surface
// (SCB-1): the ParticipantCommandCatalog, the WorkType registry and the
// canonical hash rules of design contract 29 §7.11. It contains no business
// adjudication: no crop rules, no wallet math, no Redis layout, no actor
// runtime decisions.
package contracts

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ParticipantCanonicalSchemaVersion is the frozen schema framing version for
// Participant Step payload/result hashes. It is deliberately separate from
// any individual typed message's semantic fields: several Participant
// payloads have no schema_version field, while the contract still requires a
// versioned preimage.
const ParticipantCanonicalSchemaVersion uint32 = 1

// CanonicalHash computes the frozen semantic hash of a typed message
// (contract 29 §7.11):
//
//   - fields are encoded in ascending field-number order, which for the
//     contracts protos equals the declared semantic field order;
//   - variable-length fields use LP(x) = uint32_big_endian(len) || bytes;
//   - integers use fixed-width big-endian encoding;
//   - fields with explicit presence (optional/message/oneof) are preceded
//     by a one-byte presence bit;
//   - repeated fields are preceded by a uint32 element count;
//   - map fields, extensions and unknown fields are rejected (fail closed).
//
// It never hashes raw protobuf marshal bytes, map iteration order, trace,
// deadline, lease, route revision or remaining budget.
func CanonicalHash(msg proto.Message) ([32]byte, error) {
	buf, err := CanonicalBytes(msg)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(buf), nil
}

// CanonicalParticipantBytes returns the frozen Participant Step preimage:
//
//	uint32_be(schema_version) || uint32_be(participant_command) ||
//	CanonicalBytes(typed semantic message)
//
// This is intentionally not CanonicalBytes(msg) alone. The command prefix
// prevents two registered schemas with overlapping semantic fields from
// sharing an idempotency hash, and the schema prefix permits a future
// versioned builder without silently changing the meaning of old receipts.
// Runtime context (route, deadline, lease and budget) is not included.
func CanonicalParticipantBytes(spec ParticipantCommandSpec, msg proto.Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("contracts: nil participant message")
	}
	if _, err := LookupParticipantCommand(spec.Command); err != nil {
		return nil, err
	}
	gotSchema := msg.ProtoReflect().Descriptor().FullName()
	payloadSchema := spec.NewPayload().ProtoReflect().Descriptor().FullName()
	resultSchema := spec.NewResult().ProtoReflect().Descriptor().FullName()
	if gotSchema != payloadSchema && gotSchema != resultSchema {
		return nil, fmt.Errorf("contracts: participant schema %s does not match command %s payload/result", gotSchema, spec.Command)
	}
	m := msg.ProtoReflect()
	if len(m.GetUnknown()) > 0 {
		return nil, fmt.Errorf("contracts: %s carries unknown fields", m.Descriptor().FullName())
	}
	preimage := make([]byte, 0, 8+canonicalInitialCapacity(msg))
	preimage = binary.BigEndian.AppendUint32(preimage, ParticipantCanonicalSchemaVersion)
	preimage = binary.BigEndian.AppendUint32(preimage, uint32(spec.Command))
	if err := encodeMessage(&preimage, m); err != nil {
		return nil, err
	}
	return preimage, nil
}

// CanonicalParticipantHash hashes the schema-versioned Participant Step
// payload/result preimage. The registered command is part of the hash even
// though the typed message descriptor is already checked by the ingress
// kernel.
func CanonicalParticipantHash(spec ParticipantCommandSpec, msg proto.Message) ([32]byte, error) {
	preimage, err := CanonicalParticipantBytes(spec, msg)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(preimage), nil
}

// CanonicalBytes returns the canonical byte representation hashed by
// CanonicalHash. Exported so golden vectors can pin the preimage.
func CanonicalBytes(msg proto.Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("contracts: nil message")
	}
	m := msg.ProtoReflect()
	if len(m.GetUnknown()) > 0 {
		return nil, fmt.Errorf("contracts: %s carries unknown fields", m.Descriptor().FullName())
	}
	buf := make([]byte, 0, canonicalInitialCapacity(msg))
	if err := encodeMessage(&buf, m); err != nil {
		return nil, err
	}
	return buf, nil
}

// canonicalInitialCapacity uses the generated protobuf size only as a cheap
// lower-bound hint. Canonical encoding writes fixed-width zero scalars too, so
// a small allowance avoids the common first growth without changing bytes.
func canonicalInitialCapacity(msg proto.Message) int {
	const allowance = 64
	size := proto.Size(msg)
	if size > int(^uint(0)>>1)-allowance {
		return size
	}
	return size + allowance
}

type canonicalMessagePlan struct {
	ordered []protoreflect.FieldDescriptor
	err     error
}

var canonicalPlanCache sync.Map // protoreflect.FullName -> canonicalMessagePlan

func planCanonicalMessage(descriptor protoreflect.MessageDescriptor) canonicalMessagePlan {
	name := descriptor.FullName()
	if cached, ok := canonicalPlanCache.Load(name); ok {
		return cached.(canonicalMessagePlan)
	}
	fields := descriptor.Fields()
	plan := canonicalMessagePlan{ordered: make([]protoreflect.FieldDescriptor, 0, fields.Len())}
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.IsMap() {
			plan.err = fmt.Errorf("contracts: %s.%s is a map field, forbidden in canonical schemas", name, fd.Name())
			break
		}
		if fd.IsExtension() {
			plan.err = fmt.Errorf("contracts: %s.%s is an extension, forbidden in canonical schemas", name, fd.Name())
			break
		}
		plan.ordered = append(plan.ordered, fd)
	}
	if plan.err == nil {
		sort.Slice(plan.ordered, func(i, j int) bool { return plan.ordered[i].Number() < plan.ordered[j].Number() })
	}
	actual, _ := canonicalPlanCache.LoadOrStore(name, plan)
	return actual.(canonicalMessagePlan)
}

func encodeMessage(buf *[]byte, m protoreflect.Message) error {
	plan := planCanonicalMessage(m.Descriptor())
	if plan.err != nil {
		return plan.err
	}
	for _, fd := range plan.ordered {
		if err := encodeField(buf, m, fd); err != nil {
			return err
		}
	}
	return nil
}

func encodeField(buf *[]byte, m protoreflect.Message, fd protoreflect.FieldDescriptor) error {
	if fd.IsList() {
		list := m.Get(fd).List()
		*buf = binary.BigEndian.AppendUint32(*buf, uint32(list.Len()))
		for i := 0; i < list.Len(); i++ {
			if err := encodeValue(buf, fd, list.Get(i)); err != nil {
				return err
			}
		}
		return nil
	}
	if fd.HasPresence() {
		if !m.Has(fd) {
			*buf = append(*buf, 0)
			return nil
		}
		*buf = append(*buf, 1)
	}
	// proto3 non-optional scalar: no presence bit, value always encoded.
	return encodeValue(buf, fd, m.Get(fd))
}

func encodeValue(buf *[]byte, fd protoreflect.FieldDescriptor, v protoreflect.Value) error {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v.Bool() {
			*buf = append(*buf, 1)
		} else {
			*buf = append(*buf, 0)
		}
	case protoreflect.EnumKind:
		*buf = binary.BigEndian.AppendUint32(*buf, uint32(v.Enum()))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		*buf = binary.BigEndian.AppendUint32(*buf, uint32(v.Int()))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		*buf = binary.BigEndian.AppendUint32(*buf, uint32(v.Uint()))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		*buf = binary.BigEndian.AppendUint64(*buf, uint64(v.Int()))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		*buf = binary.BigEndian.AppendUint64(*buf, v.Uint())
	case protoreflect.StringKind:
		*buf = appendLPString(*buf, v.String())
	case protoreflect.BytesKind:
		*buf = appendLP(*buf, v.Bytes())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		nested := v.Message()
		if len(nested.GetUnknown()) > 0 {
			return fmt.Errorf("contracts: %s carries unknown fields", nested.Descriptor().FullName())
		}
		lengthOffset := len(*buf)
		*buf = binary.BigEndian.AppendUint32(*buf, 0)
		valueOffset := len(*buf)
		if err := encodeMessage(buf, nested); err != nil {
			return err
		}
		binary.BigEndian.PutUint32((*buf)[lengthOffset:valueOffset], uint32(len(*buf)-valueOffset))
	default:
		return fmt.Errorf("contracts: unsupported kind %s on %s", fd.Kind(), fd.FullName())
	}
	return nil
}

func appendLP(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

func appendLPString(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}
