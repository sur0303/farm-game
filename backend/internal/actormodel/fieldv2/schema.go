// Package fieldv2 freezes the exact Actor Model Field V2 registries and the
// small deterministic envelope shared by Player/Farm converters. It contains
// no repository, Redis key, claim or gameplay logic.
package fieldv2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"farmgame/internal/actorrouting"
)

const (
	MaxFieldValueBytes = 512 << 10
	MaxRepeatedItems   = 4096
)

var ErrMalformed = errors.New("actor model field v2: malformed data")

type Role uint8

const (
	RoleUnknown Role = iota
	RoleAuthoritative
	RoleDerived
)

type Field struct {
	Name    string
	CodecID string
	Role    Role
	id      byte
}

type Schema struct {
	Domain actorrouting.RouteDomain
	Fields []Field
}

var playerSchema = Schema{
	Domain: actorrouting.DomainPlayer,
	Fields: []Field{
		{Name: "compendium", CodecID: "amf2.player.compendium.pb-subset.v1", Role: RoleAuthoritative, id: 1},
		{Name: "economy", CodecID: "amf2.player.economy.pb-subset.v1", Role: RoleAuthoritative, id: 2},
		{Name: "inventory", CodecID: "amf2.player.inventory.pb-subset.v1", Role: RoleAuthoritative, id: 3},
		{Name: "mail_claim_state", CodecID: "amf2.player.mail-claim.disabled.v1", Role: RoleAuthoritative, id: 4},
		{Name: "pet", CodecID: "amf2.player.pet.disabled.v1", Role: RoleAuthoritative, id: 5},
		{Name: "profile", CodecID: "amf2.player.profile.pb-subset.v1", Role: RoleAuthoritative, id: 6},
		{Name: "progression", CodecID: "amf2.player.progression.pb-subset.v1", Role: RoleAuthoritative, id: 7},
		{Name: "task", CodecID: "amf2.player.task-and-legacy-receipts.pb-subset.v1", Role: RoleAuthoritative, id: 8},
	},
}

var farmSchema = Schema{
	Domain: actorrouting.DomainFarm,
	Fields: []Field{
		{Name: "activity_summary", CodecID: "amf2.farm.activity-summary.pb-subset.v1", Role: RoleAuthoritative, id: 1},
		{Name: "farm_meta", CodecID: "amf2.farm.meta.pb-subset.v1", Role: RoleAuthoritative, id: 2},
		{Name: "guard", CodecID: "amf2.farm.guard.disabled.v1", Role: RoleAuthoritative, id: 3},
		{Name: "opportunity_facts", CodecID: "amf2.farm.opportunity-facts.v1", Role: RoleAuthoritative, id: 4},
		{Name: "opportunity_header", CodecID: "amf2.farm.opportunity-header.v2", Role: RoleAuthoritative, id: 5},
		{Name: "plots", CodecID: "amf2.farm.plots.pb-subset.v1", Role: RoleAuthoritative, id: 6},
	},
}

func schemaFor(domain actorrouting.RouteDomain) (Schema, bool) {
	switch domain {
	case actorrouting.DomainPlayer:
		return playerSchema, true
	case actorrouting.DomainFarm:
		return farmSchema, true
	default:
		return Schema{}, false
	}
}

// SchemaFor returns a defensive copy for callers that may retain or mutate the
// field slice. Hot codec paths use schemaFor directly to avoid copying the
// immutable registry on every field marshal/unmarshal.
func SchemaFor(domain actorrouting.RouteDomain) (Schema, bool) {
	schema, ok := schemaFor(domain)
	if !ok {
		return Schema{}, false
	}
	schema.Fields = append([]Field(nil), schema.Fields...)
	return schema, true
}

func ExactNames(domain actorrouting.RouteDomain) ([]string, error) {
	schema, ok := schemaFor(domain)
	if !ok {
		return nil, fmt.Errorf("%w: unknown domain", ErrMalformed)
	}
	names := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		names[i] = field.Name
	}
	return names, nil
}

func ValidateExactNames(domain actorrouting.RouteDomain, names []string) error {
	want, err := ExactNames(domain)
	if err != nil {
		return err
	}
	if len(names) != len(want) {
		return fmt.Errorf("%w: %s model count", ErrMalformed, domain)
	}
	for i := range want {
		if names[i] != want[i] {
			return fmt.Errorf("%w: %s manifest entry %d", ErrMalformed, domain, i)
		}
	}
	return nil
}

// ValidateExactFields checks both the complete registry shape and every
// field-bound envelope. Domain codecs add their protobuf/subset validation on
// top; this common check is sufficient for the repository to reject corrupt,
// cross-field, truncated, or trailing bytes before exposing a v2 snapshot.
func ValidateExactFields(domain actorrouting.RouteDomain, fields map[string][]byte) error {
	if fields == nil {
		return fmt.Errorf("%w: nil model fields", ErrMalformed)
	}
	names := SortedNames(fields)
	if err := ValidateExactNames(domain, names); err != nil {
		return err
	}
	for _, name := range names {
		// Validation only needs to inspect the immutable envelope. Avoid making
		// an owned payload copy for every model field on repository hot paths.
		if _, err := borrowedPayload(domain, name, fields[name]); err != nil {
			return err
		}
	}
	return nil
}

// ManifestBytes freezes codec identity and role separately from the physical
// _meta:model_manifest. It is a compile-time registry/golden and does not
// create another persisted truth set.
func ManifestBytes(domain actorrouting.RouteDomain) ([]byte, error) {
	schema, ok := schemaFor(domain)
	if !ok {
		return nil, fmt.Errorf("%w: unknown domain", ErrMalformed)
	}
	out := []byte{'A', 'M', 'F', '2'}
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(schema.Fields)))
	out = append(out, count[:]...)
	for _, field := range schema.Fields {
		out = appendLP(out, []byte(field.Name))
		out = appendLP(out, []byte(field.CodecID))
		out = append(out, byte(field.Role))
	}
	return out, nil
}

func SortedNames(fields map[string][]byte) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func fieldFor(domain actorrouting.RouteDomain, name string) (Field, bool) {
	schema, ok := schemaFor(domain)
	if !ok {
		return Field{}, false
	}
	for _, field := range schema.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return Field{}, false
}

func appendLP(dst, value []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}
