package fieldv2

import (
	"encoding/hex"
	"reflect"
	"testing"

	"farmgame/internal/actorrouting"
)

func TestExactSchemaManifestGoldens(t *testing.T) {
	cases := []struct {
		domain actorrouting.RouteDomain
		names  []string
		hex    string
	}{
		{actorrouting.DomainPlayer, []string{"compendium", "economy", "inventory", "mail_claim_state", "pet", "profile", "progression", "task"}, "414d463200080000000a636f6d70656e6469756d00000023616d66322e706c617965722e636f6d70656e6469756d2e70622d7375627365742e7631010000000765636f6e6f6d7900000020616d66322e706c617965722e65636f6e6f6d792e70622d7375627365742e76310100000009696e76656e746f727900000022616d66322e706c617965722e696e76656e746f72792e70622d7375627365742e763101000000106d61696c5f636c61696d5f737461746500000022616d66322e706c617965722e6d61696c2d636c61696d2e64697361626c65642e763101000000037065740000001b616d66322e706c617965722e7065742e64697361626c65642e7631010000000770726f66696c6500000020616d66322e706c617965722e70726f66696c652e70622d7375627365742e7631010000000b70726f6772657373696f6e00000024616d66322e706c617965722e70726f6772657373696f6e2e70622d7375627365742e763101000000047461736b00000031616d66322e706c617965722e7461736b2d616e642d6c65676163792d72656365697074732e70622d7375627365742e763101"},
		{actorrouting.DomainFarm, []string{"activity_summary", "farm_meta", "guard", "opportunity_facts", "opportunity_header", "plots"}, "414d463200060000001061637469766974795f73756d6d61727900000027616d66322e6661726d2e61637469766974792d73756d6d6172792e70622d7375627365742e763101000000096661726d5f6d6574610000001b616d66322e6661726d2e6d6574612e70622d7375627365742e7631010000000567756172640000001b616d66322e6661726d2e67756172642e64697361626c65642e763101000000116f70706f7274756e6974795f66616374730000001e616d66322e6661726d2e6f70706f7274756e6974792d66616374732e763101000000126f70706f7274756e6974795f6865616465720000001f616d66322e6661726d2e6f70706f7274756e6974792d6865616465722e76320100000005706c6f74730000001c616d66322e6661726d2e706c6f74732e70622d7375627365742e763101"},
	}
	for _, tc := range cases {
		t.Run(string(tc.domain), func(t *testing.T) {
			names, err := ExactNames(tc.domain)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(names, tc.names) {
				t.Fatalf("names=%v", names)
			}
			manifest, err := ManifestBytes(tc.domain)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(manifest); got != tc.hex {
				t.Errorf("manifest=%s", got)
			}
		})
	}
}

func TestConversionPolicyDefaultOffAndV2Sticky(t *testing.T) {
	policy := DefaultConversionPolicy()
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		eligible, err := policy.Eligible(domain, 12345)
		if err != nil || eligible {
			t.Fatalf("default eligibility domain=%s eligible=%v err=%v", domain, eligible, err)
		}
		layout, err := policy.WriteLayout(domain, 12345, 1)
		if err != nil || layout != 1 {
			t.Fatalf("default v1 write layout=%d err=%v", layout, err)
		}
		layout, err = policy.WriteLayout(domain, 12345, 2)
		if err != nil || layout != 2 {
			t.Fatalf("existing v2 reverted layout=%d err=%v", layout, err)
		}
	}

	policy.Player = DomainPolicy{ConversionEnabled: true, CohortPermyriad: CohortBasis}
	eligible, err := policy.Eligible(actorrouting.DomainPlayer, 12345)
	if err != nil || !eligible {
		t.Fatalf("full player cohort eligible=%v err=%v", eligible, err)
	}
	eligible, err = policy.Eligible(actorrouting.DomainFarm, 12345)
	if err != nil || eligible {
		t.Fatalf("farm domain policy leaked eligible=%v err=%v", eligible, err)
	}
}
