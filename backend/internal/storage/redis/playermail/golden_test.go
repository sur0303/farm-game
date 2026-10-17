package playermail

import "testing"

// TestGoldenScriptSHA pins the exact SHA-1 of every registered script. Any
// source change requires a new script version name (31 §6.1, 35A §8). The
// V1 entries are frozen byte-level history; the V2 entries are the live
// family after the I0 review fixes (FIX-1/2/3).
func TestGoldenScriptSHA(t *testing.T) {
	want := map[string]string{
		ScriptInsertMailIfAbsent:         "39cd975d48e84e562341519e782555a3517c5f72",
		ScriptMarkMailRead:               "5cb1b7af104538fd785ee68a4b9f10f50222d1ac",
		ScriptDeleteMail:                 "2e734c75d1bc0c8e421dbec5ec25d42ff6754ad8",
		ScriptApplyMailClaimProjection:   "9a9dddc2b37c2633be07dfd9f7ade9451e96124b",
		ScriptExpireMailVisibility:       "02c91cba863d76beb2c2ff6c7fda1e7749dfd8e8",
		ScriptGcMail:                     "cd7d7072ad0fa07aac607e44acb083d5eb992298",
		ScriptClaimMailScanLease:         "dac6516406006a1e0ef1c9a3b508d8b0f093a125",
		ScriptMarkMailReadV2:             "3b5c0f63d2e99d013bcb81a2ca0910f3957c8fb7",
		ScriptDeleteMailV2:               "7baea31040c885a3a8df4b68637ac061b12ffdb9",
		ScriptApplyMailClaimProjectionV2: "b2627cdea8c3fe67896ef90a630dde3f1515e169",
		ScriptGcMailV2:                   "2275d6f7939d026108bb44a6e8a4c2ab6ad49859",
	}
	rig := newTestRig(t)
	for name, sha := range want {
		got, ok := rig.store.ScriptSHA(name)
		if !ok {
			t.Fatalf("script %s not registered", name)
		}
		if got != sha {
			t.Fatalf("script %s sha: got %s want %s (bump the version name instead)", name, got, sha)
		}
	}
}
