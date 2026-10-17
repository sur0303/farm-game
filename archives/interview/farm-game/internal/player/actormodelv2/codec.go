// Package actormodelv2 contains the deterministic PlayerRecord-to-field
// converter. It is intentionally not wired into normal PlayerActor runtime in
// M1-A; callers must pass the repository policy/cutover gate first.
package actormodelv2

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

var ErrMalformed = errors.New("player actor model v2: malformed state")

func ConvertV1Bytes(recordBytes []byte) (authoritystore.ModelState, error) {
	var record playerpb.PlayerRecord
	if err := unmarshalCanonical(recordBytes, &record); err != nil {
		return nil, err
	}
	return Encode(&record)
}

func Encode(record *playerpb.PlayerRecord) (authoritystore.ModelState, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	subsets := map[string]*playerpb.PlayerRecord{
		"profile": {
			PlayerId: record.GetPlayerId(), BootstrapId: record.GetBootstrapId(),
			PublicProfile: cloneMessage(record.GetPublicProfile()),
		},
		"economy": {
			EconomyVersion: record.GetEconomyVersion(), Wallet: cloneMessage(record.GetWallet()),
			UnlockCoinReservations: cloneMessages(record.GetUnlockCoinReservations()),
		},
		"inventory": {
			Inventory:        cloneMessage(record.GetInventory()),
			ItemReservations: cloneMessages(record.GetItemReservations()),
		},
		"progression": {Progression: cloneMessage(record.GetProgression())},
		// The embedded legacy operation receipts remain here as one compatibility
		// carrier. They are not copied into outer Authority receipts and never
		// become a second truth.
		"task": {
			RecentOperationReceipts: cloneMessages(record.GetRecentOperationReceipts()),
			Tasks:                   cloneMessages(record.GetTasks()),
			TaskDomainState:         cloneMessage(record.GetTaskDomainState()),
		},
		"compendium": {CodexEntries: cloneMessages(record.GetCodexEntries())},
	}
	state := make(authoritystore.ModelState, 8)
	for name, subset := range subsets {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(subset)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal %s: %v", ErrMalformed, name, err)
		}
		state[name], err = fieldv2.Marshal(actorrouting.DomainPlayer, name, payload)
		if err != nil {
			return nil, err
		}
	}
	var err error
	state["pet"], err = fieldv2.DisabledState(actorrouting.DomainPlayer, "pet")
	if err != nil {
		return nil, err
	}
	state["mail_claim_state"], err = fieldv2.DisabledState(actorrouting.DomainPlayer, "mail_claim_state")
	if err != nil {
		return nil, err
	}
	return state, nil
}

func Decode(state authoritystore.ModelState) (*playerpb.PlayerRecord, error) {
	if err := validateExactState(state); err != nil {
		return nil, err
	}
	if err := fieldv2.ValidateDisabledState(actorrouting.DomainPlayer, "pet", state["pet"]); err != nil {
		return nil, err
	}
	if err := fieldv2.ValidateDisabledState(actorrouting.DomainPlayer, "mail_claim_state", state["mail_claim_state"]); err != nil {
		return nil, err
	}
	parts := make(map[string]*playerpb.PlayerRecord, 6)
	for _, name := range []string{"profile", "economy", "inventory", "progression", "task", "compendium"} {
		payload, err := fieldv2.Unmarshal(actorrouting.DomainPlayer, name, state[name])
		if err != nil {
			return nil, err
		}
		part := &playerpb.PlayerRecord{}
		if err := unmarshalCanonical(payload, part); err != nil {
			return nil, fmt.Errorf("%w: decode %s: %v", ErrMalformed, name, err)
		}
		if err := validateSubset(name, part); err != nil {
			return nil, err
		}
		parts[name] = part
	}
	profile, economy, inventory := parts["profile"], parts["economy"], parts["inventory"]
	progression, task, compendium := parts["progression"], parts["task"], parts["compendium"]
	record := &playerpb.PlayerRecord{
		PlayerId: profile.GetPlayerId(), BootstrapId: profile.GetBootstrapId(), PublicProfile: profile.GetPublicProfile(),
		EconomyVersion: economy.GetEconomyVersion(), Wallet: economy.GetWallet(), UnlockCoinReservations: economy.GetUnlockCoinReservations(),
		Inventory: inventory.GetInventory(), ItemReservations: inventory.GetItemReservations(),
		Progression:             progression.GetProgression(),
		RecentOperationReceipts: task.GetRecentOperationReceipts(), Tasks: task.GetTasks(), TaskDomainState: task.GetTaskDomainState(),
		CodexEntries: compendium.GetCodexEntries(),
	}
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	return record, nil
}

// DecodePublicProfileFields decodes the only two v2 fields needed by the
// mailbox-free public-profile read path. It does not accept public_profile or
// record as rescue truth; repository readers are responsible for validating
// the complete v2 record before returning these bounded projections.
func DecodePublicProfileFields(profileField, progressionField []byte) (*playerpb.PlayerRecord, error) {
	parts := make(map[string]*playerpb.PlayerRecord, 2)
	for name, value := range map[string][]byte{"profile": profileField, "progression": progressionField} {
		payload, err := fieldv2.Unmarshal(actorrouting.DomainPlayer, name, value)
		if err != nil {
			return nil, err
		}
		part := &playerpb.PlayerRecord{}
		if err := unmarshalCanonical(payload, part); err != nil {
			return nil, fmt.Errorf("%w: decode %s: %v", ErrMalformed, name, err)
		}
		if err := validateSubset(name, part); err != nil {
			return nil, err
		}
		parts[name] = part
	}
	record := &playerpb.PlayerRecord{
		PlayerId:      parts["profile"].GetPlayerId(),
		BootstrapId:   parts["profile"].GetBootstrapId(),
		PublicProfile: parts["profile"].GetPublicProfile(),
		Progression:   parts["progression"].GetProgression(),
	}
	if record.GetPlayerId() == 0 || record.GetPublicProfile() == nil || record.GetProgression() == nil {
		return nil, fmt.Errorf("%w: public profile fields", ErrMalformed)
	}
	return record, nil
}

func validateExactState(state authoritystore.ModelState) error {
	if state == nil {
		return fmt.Errorf("%w: nil state", ErrMalformed)
	}
	names := fieldv2.SortedNames(state)
	if err := fieldv2.ValidateExactNames(actorrouting.DomainPlayer, names); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return nil
}

func validateRecord(record *playerpb.PlayerRecord) error {
	if record == nil || record.GetPlayerId() == 0 {
		return fmt.Errorf("%w: player identity", ErrMalformed)
	}
	if err := fieldv2.ValidateNoUnknown(record); err != nil {
		return fmt.Errorf("%w: unknown fields", ErrMalformed)
	}
	counts := []int{len(record.GetRecentOperationReceipts()), len(record.GetItemReservations()),
		len(record.GetCodexEntries()), len(record.GetTasks()), len(record.GetUnlockCoinReservations())}
	for _, count := range counts {
		if count > fieldv2.MaxRepeatedItems {
			return fmt.Errorf("%w: repeated carrier capacity", ErrMalformed)
		}
	}
	return nil
}

func validateSubset(name string, part *playerpb.PlayerRecord) error {
	check, ok := proto.Clone(part).(*playerpb.PlayerRecord)
	if !ok {
		return fmt.Errorf("%w: clone subset", ErrMalformed)
	}
	// Explicit field ownership: any populated field outside the named subset
	// is malformed instead of being silently ignored or duplicated.
	switch name {
	case "profile":
		check.PlayerId, check.BootstrapId, check.PublicProfile = 0, "", nil
	case "economy":
		check.EconomyVersion, check.Wallet, check.UnlockCoinReservations = 0, nil, nil
	case "inventory":
		check.Inventory, check.ItemReservations = nil, nil
	case "progression":
		check.Progression = nil
	case "task":
		check.RecentOperationReceipts, check.Tasks, check.TaskDomainState = nil, nil, nil
	case "compendium":
		check.CodexEntries = nil
	default:
		return fmt.Errorf("%w: unknown subset", ErrMalformed)
	}
	remaining, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(check)
	if marshalErr != nil || len(remaining) != 0 {
		return fmt.Errorf("%w: %s contains foreign fields", ErrMalformed, name)
	}
	return nil
}

func unmarshalCanonical(data []byte, message proto.Message) error {
	if len(data) > fieldv2.MaxFieldValueBytes {
		return fmt.Errorf("%w: bytes capacity", ErrMalformed)
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return fmt.Errorf("%w: protobuf", ErrMalformed)
	}
	if err := fieldv2.ValidateNoUnknown(message); err != nil {
		return fmt.Errorf("%w: protobuf unknown fields", ErrMalformed)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil || !bytes.Equal(canonical, data) {
		return fmt.Errorf("%w: non-canonical protobuf", ErrMalformed)
	}
	return nil
}

func cloneMessage[T proto.Message](in T) T {
	if reflect.ValueOf(in).IsNil() {
		var zero T
		return zero
	}
	return proto.Clone(in).(T)
}

func cloneMessages[T proto.Message](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	for i, item := range in {
		if !reflect.ValueOf(item).IsNil() {
			out[i] = proto.Clone(item).(T)
		}
	}
	return out
}
