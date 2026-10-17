package playermail

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"farmgame/internal/player/mail"
)

// TestConcurrentMutationsSameMailbox hammers one mailbox with concurrent
// MarkRead/Delete/Projection from many goroutines and asserts counter
// consistency. Run with -race.
func TestConcurrentMutationsSameMailbox(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	const mails = 12
	entries := make([]mail.MailEntry, 0, mails)
	for i := 0; i < mails; i++ {
		entry := rewardEntry(t, player, fmt.Sprintf("C:%d", i), rig.now-int64(mails-i))
		mustInsert(t, rig.store, entry)
		entries = append(entries, entry)
	}

	var wg sync.WaitGroup
	for i, entry := range entries {
		entry := entry
		i := i
		wg.Add(3)
		go func() {
			defer wg.Done()
			if _, err := rig.store.MarkMailRead(ctx, player, entry.MailID, fmt.Sprintf("r-%d", i)); err != nil {
				t.Errorf("mark read: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			order := mail.ClaimProjectionOrder{
				RecipientPlayerID: player, WorkID: fmt.Sprintf("w-%d", i), WorkVersion: 1,
				MailID: entry.MailID, ClaimReceiptRef: "rcpt",
				ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: rig.now,
			}
			if _, err := rig.store.ApplyMailClaimProjection(ctx, order); err != nil {
				t.Errorf("projection: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			// Deletion is only legal after the projection lands; this races
			// on purpose — REJECT_UNCLAIMED and DELETED are both acceptable,
			// but counters must never go negative.
			_, _ = rig.store.DeleteMail(ctx, player, entry.MailID, fmt.Sprintf("d-%d", i), mail.AllowDeleteWithProof(mail.ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt"}))
		}()
	}
	wg.Wait()

	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.UnreadCount < 0 || summary.VisibleCount < 0 || summary.ClaimableCount < 0 ||
		summary.RetainedCount < 0 || summary.RetainedBytes < 0 {
		t.Fatalf("negative counters: %+v", summary)
	}
	if summary.RetainedCount != mails {
		t.Fatalf("retained must keep every mail: %+v", summary)
	}
	if summary.UnreadCount != 0 {
		t.Fatalf("all mails read: %+v", summary)
	}
	// Every projection applied exactly once: claimable is zero unless a
	// delete won the race before the projection (then the projection still
	// applied monotonically).
	if summary.ClaimableCount != 0 {
		t.Fatalf("claimable must drain: %+v", summary)
	}
	for _, entry := range entries {
		_, state, err := rig.store.GetEntry(ctx, player, entry.MailID)
		if err != nil {
			t.Fatal(err)
		}
		if !state.ClaimedProjection {
			t.Fatalf("projection missing on %s", entry.MailID)
		}
	}
}

// TestConcurrentInsertsSameDelivery proves that racing identical deliveries
// commit exactly once.
func TestConcurrentInsertsSameDelivery(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := textEntry(t, player, "SAME:1", rig.now)

	const racers = 16
	statuses := make([]mail.InsertStatus, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := rig.store.InsertMailIfAbsent(ctx, entry)
			if err != nil {
				t.Errorf("insert: %v", err)
				return
			}
			statuses[i] = res.Status
		}(i)
	}
	wg.Wait()
	inserted := 0
	for _, st := range statuses {
		switch st {
		case mail.InsertInserted:
			inserted++
		case mail.InsertAlreadyDelivered:
		default:
			t.Fatalf("unexpected status %s", st)
		}
	}
	if inserted != 1 {
		t.Fatalf("exactly one insert may commit, got %d", inserted)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 1 {
		t.Fatalf("one visible mail: %+v", summary)
	}
}
