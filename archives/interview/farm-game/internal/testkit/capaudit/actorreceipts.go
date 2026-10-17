package capaudit

import (
	"fmt"

	commonpb "farmgame/api/gen/common"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"

	"google.golang.org/protobuf/proto"
)

// Gates decided by reading what an actor's own record says was applied to it.
const (
	// DuplicateGrantGate counts operations that succeeded more than once on
	// the same actor. There is no grant ledger to compare against, so this is
	// the durable evidence a duplicate leaves: the same operation id carrying
	// two successful receipts. A replay that the guard caught stores
	// NO_CHANGE instead, and does not count.
	DuplicateGrantGate = "duplicate_grant_total"
	// DuplicateDebitGate counts a charge held twice under one reservation id.
	// Debits reserve before they consume, so a second live row for the same
	// reservation is a second hold on the same funds or items.
	DuplicateDebitGate = "duplicate_debit_total"
)

// Model fields the receipts live in. Which one to read depends on how the row
// was written, so the layout version is read alongside them.
const (
	metaModelLayoutVersion = "_meta:model_layout_version"
	modelRecordField       = "model:record"
	modelTaskField         = "model:task"
	modelInventoryField    = "model:inventory"
	modelEconomyField      = "model:economy"
)

// playerReceipts is what one player's record says about itself.
type playerReceipts struct {
	receipts     []*playerpb.PlayerOperationReceipt
	items        []*playerpb.ItemReservation
	unlockCoins  []*playerpb.UnlockCoinReservation
	layoutIsKnwn bool
}

// decodePlayer reads the receipts and reservations out of an actor row.
//
// A row is written either as one record or as separate model fields, and which
// one it is has to be taken from the row itself: guessing would read a v2 row
// as an empty v1 record and report it clean.
func decodePlayer(layout string, fields map[string][]byte) (playerReceipts, error) {
	switch layout {
	case "1":
		record := &playerpb.PlayerRecord{}
		if err := proto.Unmarshal(fields[modelRecordField], record); err != nil {
			return playerReceipts{}, fmt.Errorf("decode %s: %w", modelRecordField, err)
		}
		return playerReceipts{
			receipts: record.GetRecentOperationReceipts(), items: record.GetItemReservations(),
			unlockCoins: record.GetUnlockCoinReservations(), layoutIsKnwn: true,
		}, nil
	case "2":
		task, err := decodeField(modelTaskField, "task", fields)
		if err != nil {
			return playerReceipts{}, err
		}
		inventory, err := decodeField(modelInventoryField, "inventory", fields)
		if err != nil {
			return playerReceipts{}, err
		}
		economy, err := decodeField(modelEconomyField, "economy", fields)
		if err != nil {
			return playerReceipts{}, err
		}
		return playerReceipts{
			receipts: task.GetRecentOperationReceipts(), items: inventory.GetItemReservations(),
			unlockCoins: economy.GetUnlockCoinReservations(), layoutIsKnwn: true,
		}, nil
	default:
		return playerReceipts{}, fmt.Errorf("%s is %q, which this audit cannot read", metaModelLayoutVersion, layout)
	}
}

func decodeField(field, name string, fields map[string][]byte) (*playerpb.PlayerRecord, error) {
	payload, err := fieldv2.Unmarshal(actorrouting.DomainPlayer, name, fields[field])
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	part := &playerpb.PlayerRecord{}
	if err := proto.Unmarshal(payload, part); err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	return part, nil
}

// duplicateGrants counts operation ids that succeeded more than once.
func duplicateGrants(receipts []*playerpb.PlayerOperationReceipt) int64 {
	succeeded := map[string]int{}
	for _, receipt := range receipts {
		if receipt.GetStatus() == commonpb.CommandStatus_SUCCEEDED {
			succeeded[receipt.GetOperationId()]++
		}
	}
	var duplicates int64
	for _, count := range succeeded {
		if count > 1 {
			duplicates++
		}
	}
	return duplicates
}

// duplicateDebits counts reservation ids held more than once. A released row is
// not a hold, so it cannot be a second one.
func duplicateDebits(record playerReceipts) int64 {
	held := map[string]int{}
	for _, item := range record.items {
		if holding(item.GetState()) {
			held[item.GetReservationId()]++
		}
	}
	for _, coin := range record.unlockCoins {
		if holding(coin.GetState()) {
			held[coin.GetReservationId()]++
		}
	}
	var duplicates int64
	for _, count := range held {
		if count > 1 {
			duplicates++
		}
	}
	return duplicates
}

func holding(state playerpb.ReservationState) bool {
	return state == playerpb.ReservationState_PREPARED || state == playerpb.ReservationState_COMMITTED
}
