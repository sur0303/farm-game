package task

import "testing"

func TestP0RepositoryIsVersionedAndServerTimezoneBound(t *testing.T) {
	repo, err := NewP0Repository()
	if err != nil {
		t.Fatal(err)
	}
	assignment, ok := repo.AssignmentForPeriod("20260805")
	if !ok || assignment.ConfigVersion != P0TaskConfigVersion || assignment.ConfigPayloadHash == "" {
		t.Fatalf("assignment=%+v ok=%v", assignment, ok)
	}
	cfg, ok := repo.DailySnapshot(assignment.ConfigVersion)
	if !ok || cfg.Timezone != DefaultGameTimezone || cfg.PayloadHash != assignment.ConfigPayloadHash {
		t.Fatalf("cfg=%+v ok=%v", cfg, ok)
	}
	if cfg.Daily[0].TaskID != P0DailyLoginID || cfg.Daily[1].TaskID != P0DailyFriendVisitID || cfg.Daily[2].TaskID != P0DailyFriendInteractID {
		t.Fatalf("daily IDs=%+v", cfg.Daily)
	}
	growth, ok := repo.LatestGrowthChain()
	if !ok || len(growth.Definitions) != 1 || growth.Definitions[0].TaskID != P0GrowthHarvestID {
		t.Fatalf("growth=%+v ok=%v", growth, ok)
	}
	cfg.Daily[0].TaskID = "caller-mutation"
	cfgAgain, _ := repo.DailySnapshot(assignment.ConfigVersion)
	if cfgAgain.Daily[0].TaskID == "caller-mutation" {
		t.Fatal("repository returned a mutable internal snapshot")
	}
}
