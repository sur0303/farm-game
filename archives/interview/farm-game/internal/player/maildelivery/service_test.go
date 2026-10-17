package maildelivery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"farmgame/internal/player/mail"
)

const testNowMs = int64(1754200000000)

func newTestService(t *testing.T, now *int64) (*DeliveryService, *mail.MemoryStore) {
	t.Helper()
	store, err := mail.NewMemoryStore(func() int64 { return *now }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewDeliveryService(store, func() int64 { return *now }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return service, store
}

func baseRequest() DeliveryRequest {
	return DeliveryRequest{
		RecipientPlayerID:   10001,
		DeliveryID:          "SYSTEM:10001:welcome:1",
		SourceID:            "welcome-v1",
		Template:            "welcome-v1",
		RewardConfigVersion: "mail-reward-v1",
		Title:               "Welcome",
		Body:                "Thanks for joining.",
		RetentionMs:         2 * 60 * 60 * 1000,
	}
}

func TestDeliverFirstAndDeterministicMailID(t *testing.T) {
	now := testNowMs
	service, store := newTestService(t, &now)
	request := baseRequest()

	result, err := service.Deliver(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != mail.InsertInserted {
		t.Fatalf("status: got %s want %s", result.Status, mail.InsertInserted)
	}
	wantMailID, err := mail.MailID(request.RecipientPlayerID, request.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if result.MailID != wantMailID {
		t.Fatalf("mail id: got %s want %s", result.MailID, wantMailID)
	}

	entry, state, err := store.GetEntry(context.Background(), request.RecipientPlayerID, result.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if entry.SourceType != mail.SourceTypeSystem || entry.SourceID != request.SourceID ||
		entry.TemplateVersion != request.Template || entry.RewardConfigVersion != request.RewardConfigVersion {
		t.Fatalf("entry source/template: %+v", entry)
	}
	if entry.CreatedAtMs != now || entry.VisibleAtMs != now || entry.ExpireAtMs != now+request.RetentionMs {
		t.Fatalf("entry times: %+v", entry)
	}
	if state.ReadAtMs != 0 {
		t.Fatalf("new mail must be unread: %+v", state)
	}
}

func TestDeliverSamePayloadReplayIsIdempotent(t *testing.T) {
	now := testNowMs
	service, store := newTestService(t, &now)
	request := baseRequest()

	first, err := service.Deliver(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := service.Deliver(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.InsertAlreadyDelivered || again.MailID != first.MailID {
		t.Fatalf("replay: got %+v want already delivered for %s", again, first.MailID)
	}
	summary, err := store.GetSummary(context.Background(), request.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 1 || summary.RetainedCount != 1 || summary.MailboxRevision != 1 {
		t.Fatalf("replay duplicated mail: %+v", summary)
	}
}

func TestDeliverChangedPayloadFailsClosed(t *testing.T) {
	now := testNowMs
	service, store := newTestService(t, &now)
	request := baseRequest()
	if _, err := service.Deliver(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	conflictRequest := request
	conflictRequest.Body = "tampered body"
	result, err := service.Deliver(context.Background(), conflictRequest)
	if !errors.Is(err, ErrDeliveryConflict) {
		t.Fatalf("conflict error: got %v", err)
	}
	if result.Status != mail.InsertDeliveryConflict {
		t.Fatalf("conflict status: got %s", result.Status)
	}
	summary, err := store.GetSummary(context.Background(), request.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 1 || summary.MailboxRevision != 1 {
		t.Fatalf("conflict changed mailbox: %+v", summary)
	}
}

func TestDeliverRejectsExpiredOrInvalidInputBeforeInsert(t *testing.T) {
	tests := []struct {
		name string
		edit func(*DeliveryRequest)
	}{
		{name: "zero recipient", edit: func(r *DeliveryRequest) { r.RecipientPlayerID = 0 }},
		{name: "empty delivery id", edit: func(r *DeliveryRequest) { r.DeliveryID = "" }},
		{name: "expired retention", edit: func(r *DeliveryRequest) { r.RetentionMs = -1 }},
		{name: "retention too long", edit: func(r *DeliveryRequest) { r.RetentionMs = mail.LimitsV1().MaxRetentionMs + 1 }},
		{name: "title too large", edit: func(r *DeliveryRequest) { r.Title = strings.Repeat("t", mail.LimitsV1().MaxTitleBytes+1) }},
		{name: "body too large", edit: func(r *DeliveryRequest) { r.Body = strings.Repeat("b", mail.LimitsV1().MaxBodyBytes+1) }},
		{name: "invalid attachment", edit: func(r *DeliveryRequest) {
			r.Attachment = &mail.RewardBundle{Coin: -1}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := testNowMs
			service, store := newTestService(t, &now)
			request := baseRequest()
			tc.edit(&request)
			if _, err := service.Deliver(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error: got %v", err)
			}
			summary, err := store.GetSummary(context.Background(), request.RecipientPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			if summary.RetainedCount != 0 {
				t.Fatalf("invalid request inserted mail: %+v", summary)
			}
		})
	}
}

func TestDeliverSnapshotsAttachment(t *testing.T) {
	now := testNowMs
	service, store := newTestService(t, &now)
	attachment := &mail.RewardBundle{
		Coin: 42,
		Exp:  7,
		Items: []mail.ItemGrant{
			{ItemID: 11, Count: 2},
		},
	}
	request := baseRequest()
	request.DeliveryID = "SYSTEM:10001:reward:1"
	request.Attachment = attachment

	result, err := service.Deliver(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != mail.InsertInserted {
		t.Fatalf("status: got %s", result.Status)
	}
	attachment.Coin = 999
	attachment.Items[0].Count = 999

	entry, state, err := store.GetEntry(context.Background(), request.RecipientPlayerID, result.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Reward.Coin != 42 || entry.Reward.Exp != 7 || len(entry.Reward.Items) != 1 || entry.Reward.Items[0].Count != 2 {
		t.Fatalf("attachment was not frozen: %+v", entry.Reward)
	}
	if entry.ClaimMaterialHash == "" || !entry.HasAttachment() || state.ClaimMaterialHash == "" {
		t.Fatalf("attachment claim evidence missing: entry=%+v state=%+v", entry, state)
	}
}

func TestDeliverUsesTrustedNow(t *testing.T) {
	now := testNowMs
	store, err := mail.NewMemoryStore(func() int64 { return now }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	service, err := NewDeliveryService(store, func() int64 {
		calls++
		return now
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deliver(context.Background(), baseRequest()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("trusted clock calls: got %d want 1", calls)
	}
}
