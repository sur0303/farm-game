package contracts

import (
	"strings"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"
)

// TestParticipantCatalogIntegrity proves the frozen 1:1 mapping between the
// wire ParticipantCommandType enum, the registered specs and the stable
// identity names (contract 29 §7.8).
func TestParticipantCatalogIntegrity(t *testing.T) {
	specs := ParticipantCatalog()
	if len(specs) != 15 {
		t.Fatalf("catalog size = %d, want 15", len(specs))
	}
	seenCommands := map[contractspb.ParticipantCommandType]bool{}
	seenNames := map[string]bool{}
	seenSchemas := map[string]bool{}
	for _, spec := range specs {
		if seenCommands[spec.Command] {
			t.Fatalf("duplicate command %v", spec.Command)
		}
		seenCommands[spec.Command] = true

		name, err := StableCommandName(spec.Command)
		if err != nil {
			t.Fatalf("stable name: %v", err)
		}
		if name != string(spec.Step) {
			t.Fatalf("command %v stable name %q != step %q", spec.Command, name, spec.Step)
		}
		if seenNames[name] {
			t.Fatalf("duplicate stable name %q", name)
		}
		seenNames[name] = true

		if spec.MaxPayloadBytes <= 0 {
			t.Fatalf("command %v missing MaxPayloadBytes", spec.Command)
		}
		if spec.NewPayload == nil || spec.NewResult == nil {
			t.Fatalf("command %v missing schema prototypes", spec.Command)
		}
		payload := spec.NewPayload()
		result := spec.NewResult()
		if payload == nil || result == nil {
			t.Fatalf("command %v prototypes return nil", spec.Command)
		}
		fullName := string(payload.ProtoReflect().Descriptor().FullName())
		if seenSchemas[fullName] {
			t.Fatalf("payload schema %s registered twice", fullName)
		}
		seenSchemas[fullName] = true
		if _, err := CanonicalHash(payload); err != nil {
			t.Fatalf("payload schema %s not hashable: %v", fullName, err)
		}
		if _, err := CanonicalHash(result); err != nil {
			t.Fatalf("result schema for %s not hashable: %v", fullName, err)
		}

		workSpec, err := LookupWorkType(spec.WorkType)
		if err != nil {
			t.Fatalf("command %v: %v", spec.Command, err)
		}
		stepAllowed := false
		for _, s := range workSpec.Steps {
			if s == spec.Step {
				stepAllowed = true
			}
		}
		if !stepAllowed {
			t.Fatalf("step %s not registered under work type %s", spec.Step, spec.WorkType)
		}
		if spec.TargetRouteDomain != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER &&
			spec.TargetRouteDomain != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
			t.Fatalf("command %v has invalid target route domain %v", spec.Command, spec.TargetRouteDomain)
		}
	}
	// Every non-zero enum value must be registered exactly once.
	for v := range contractspb.ParticipantCommandType_name {
		cmd := contractspb.ParticipantCommandType(v)
		if cmd == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED {
			continue
		}
		if !seenCommands[cmd] {
			t.Fatalf("enum %v not registered in catalog", cmd)
		}
	}
}

// TestParticipantTargetDomains pins the frozen target authorities of
// contract 29 §7.8.
func TestParticipantTargetDomains(t *testing.T) {
	want := map[contractspb.ParticipantCommandType]logicrpcpb.RouteDomain{
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN:             logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:              logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION:  logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED:                    logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:                    logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION:         logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION:        logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:                  logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:            logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT:                    logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR:            logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:             logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD:               logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT:            logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
	}
	for cmd, domain := range want {
		spec, err := LookupParticipantCommand(cmd)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if spec.TargetRouteDomain != domain {
			t.Fatalf("command %v target = %v, want %v", cmd, spec.TargetRouteDomain, domain)
		}
	}
}

// TestWorkTypeRegistryIntegrity proves the placement/source registry of
// contract 27 §7.8.2 and 29 §6.2: every identity.WorkType is registered
// exactly once, source orders declare their source kinds, and the wire
// WorkType enum names match the stable identity names.
func TestWorkTypeRegistryIntegrity(t *testing.T) {
	registry := WorkTypeRegistry()
	seen := map[identity.WorkType]bool{}
	for _, spec := range registry {
		if seen[spec.WorkType] {
			t.Fatalf("duplicate work type %s", spec.WorkType)
		}
		seen[spec.WorkType] = true
		switch spec.Placement {
		case contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET:
			if len(spec.AllowedSourceKinds) != 0 {
				t.Fatalf("independent work %s must not declare source kinds", spec.WorkType)
			}
		case contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET:
			if len(spec.AllowedSourceKinds) == 0 {
				t.Fatalf("source order %s must declare source kinds", spec.WorkType)
			}
		default:
			t.Fatalf("work type %s has invalid placement %v", spec.WorkType, spec.Placement)
		}
	}
	for _, wt := range identity.WorkTypes {
		if !seen[wt] {
			t.Fatalf("identity work type %s not registered", wt)
		}
	}
	if len(registry) != len(identity.WorkTypes) {
		t.Fatalf("registry size = %d, want %d", len(registry), len(identity.WorkTypes))
	}
	for v := range contractspb.WorkType_name {
		wt := contractspb.WorkType(v)
		if wt == contractspb.WorkType_WORK_TYPE_UNSPECIFIED {
			continue
		}
		name, err := StableWorkTypeName(wt)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !seen[identity.WorkType(name)] {
			t.Fatalf("wire work type %v (%s) missing from registry", wt, name)
		}
	}
}

// TestStableNamesMatchIdentityRegistry pins every identity.ParticipantStep
// to a wire ParticipantStep enum value and vice versa.
func TestStableNamesMatchIdentityRegistry(t *testing.T) {
	wireNames := map[string]bool{}
	for v, name := range contractspb.ParticipantStep_name {
		if v == 0 {
			continue
		}
		wireNames[strings.TrimPrefix(name, "PARTICIPANT_STEP_")] = true
	}
	for _, step := range identity.ParticipantSteps {
		if !wireNames[string(step)] {
			t.Fatalf("identity step %s has no wire enum", step)
		}
	}
	if len(wireNames) != len(identity.ParticipantSteps) {
		t.Fatalf("wire steps = %d, identity steps = %d", len(wireNames), len(identity.ParticipantSteps))
	}
}
