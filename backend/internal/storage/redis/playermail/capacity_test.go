package playermail

import (
	"strings"
	"testing"

	"farmgame/internal/player/mail"
)

// TestCapacityEncodingSizes measures real encoded sizes for representative
// mails. These numbers feed the preliminary capacity model in the MA0
// handoff; they are encoding facts, not production capacity evidence.
func TestCapacityEncodingSizes(t *testing.T) {
	l := mail.LimitsV1()
	cases := []struct {
		name  string
		entry mail.MailEntry
	}{
		{
			name: "minimal_text",
			entry: mustBuildForSize(t, mail.DeliveryParams{
				RecipientPlayerID: 10001, DeliveryID: "SYS:1",
				SourceType: mail.SourceTypeSystem, TitleSnapshot: "t",
				CreatedAtMs: testNowMs, VisibleAtMs: testNowMs,
			}),
		},
		{
			name: "typical_reward",
			entry: mustBuildForSize(t, mail.DeliveryParams{
				RecipientPlayerID: 10001, DeliveryID: "WELCOME:10001:1",
				SourceType: mail.SourceTypeSystem, SourceID: "welcome-v1",
				TitleSnapshot: "Welcome!", BodySnapshot: "Hello farmer",
				Reward:              mail.RewardBundle{Coin: 100, Exp: 5, Items: []mail.ItemGrant{{ItemID: 7, Count: 3}}},
				TemplateVersion:     "tpl-v1",
				RewardConfigVersion: "rcfg-v1",
				CreatedAtMs:         testNowMs, VisibleAtMs: testNowMs,
			}),
		},
		{
			name: "max_bounds",
			entry: mustBuildForSize(t, mail.DeliveryParams{
				RecipientPlayerID: 10001, DeliveryID: strings.Repeat("D", 128),
				SourceType:    mail.SourceTypeSystem,
				TitleSnapshot: strings.Repeat("T", 256),
				BodySnapshot:  strings.Repeat("B", 4096),
				Reward: mail.RewardBundle{Coin: 1, Exp: 1, Items: []mail.ItemGrant{
					{ItemID: 1, Count: 1}, {ItemID: 2, Count: 1}, {ItemID: 3, Count: 1},
					{ItemID: 4, Count: 1}, {ItemID: 5, Count: 1}, {ItemID: 6, Count: 1},
				}},
				CreatedAtMs: testNowMs, VisibleAtMs: testNowMs,
			}),
		},
	}
	for _, tc := range cases {
		blob, err := mail.EncodeMailEntry(tc.entry, l)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		state := mail.NewUserState(tc.entry)
		stateBlob, err := mail.EncodeMailUserState(state, l)
		if err != nil {
			t.Fatalf("%s state: %v", tc.name, err)
		}
		member, err := mail.EncodeSortMember(tc.entry.CreatedAtMs, tc.entry.MailID)
		if err != nil {
			t.Fatalf("%s member: %v", tc.name, err)
		}
		t.Logf("CAPACITY %s: entry=%dB state=%dB member=%dB total_per_mail≈%dB",
			tc.name, len(blob), len(stateBlob), len(member), len(blob)+len(stateBlob)+2*len(member))
		if len(blob) > l.MaxMailEntryBytes {
			t.Fatalf("%s entry exceeds hard bound", tc.name)
		}
	}
}

func mustBuildForSize(t *testing.T, p mail.DeliveryParams) mail.MailEntry {
	t.Helper()
	entry, err := mail.BuildMailEntry(p, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
