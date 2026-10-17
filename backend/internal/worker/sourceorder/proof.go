// Package sourceorder is the shared CompleteWork proof registry used by CARE
// and the migrated HARVEST/PLANT/UNLOCK/THEFT SourceOrder families.
package sourceorder

import (
	"fmt"

	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
)

// ProofSpec is the Lua CompleteWorkV2 allow-list for one SourceOrder type:
// which participant step may complete it, and which terminal outcomes count.
type ProofSpec struct {
	WorkType string
	Step     string
	Outcomes map[string]struct{}
}

var defaultOutcomes = map[string]struct{}{
	"APPLIED":   {},
	"NO_CHANGE": {},
}

var specs = map[string]ProofSpec{
	string(identity.WorkTypeCareReward): {
		WorkType: string(identity.WorkTypeCareReward),
		Step:     string(identity.StepApplyCareReward),
		Outcomes: defaultOutcomes,
	},
	string(identity.WorkTypeHarvest): {
		WorkType: string(identity.WorkTypeHarvest),
		Step:     string(identity.StepGrantHarvestReward),
		Outcomes: defaultOutcomes,
	},
	string(identity.WorkTypePlant): {
		WorkType: string(identity.WorkTypePlant),
		Step:     string(identity.StepCommitPlant),
		Outcomes: defaultOutcomes,
	},
	string(identity.WorkTypeUnlockPlot): {
		WorkType: string(identity.WorkTypeUnlockPlot),
		Step:     string(identity.StepCommitPlotUnlock),
		Outcomes: defaultOutcomes,
	},
	string(identity.WorkTypeTheftVisitor): {
		WorkType: string(identity.WorkTypeTheftVisitor),
		Step:     string(identity.StepSettleTheftVisitor),
		Outcomes: defaultOutcomes,
	},
	string(identity.WorkTypeTheftGuard): {
		WorkType: string(identity.WorkTypeTheftGuard),
		Step:     string(identity.StepCreditGuardIncome),
		Outcomes: defaultOutcomes,
	},
}

func Lookup(workType string) (ProofSpec, bool) {
	spec, ok := specs[workType]
	return spec, ok
}

func AcceptsDirect(address workidentity.WorkAddress) error {
	if err := address.ValidateShape(); err != nil {
		return err
	}
	if _, ok := specs[address.WorkType]; !ok {
		return fmt.Errorf("sourceorder: unsupported work type %q", address.WorkType)
	}
	if address.PlacementKind != workidentity.SourcePersistenceBucket {
		return fmt.Errorf("sourceorder: %s is not a source-persistence order", address.WorkType)
	}
	return nil
}
