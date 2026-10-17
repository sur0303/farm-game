package scaleplan

import "context"

type PlanRecord struct {
	Plan Plan
}

func (r PlanRecord) Clone() PlanRecord {
	r.Plan = r.Plan.Clone()
	return r
}

type IdempotencySubmission struct {
	Key           string
	PayloadDigest string
	WorkloadKey   string
}

type CreateStatus string

const (
	CreateCommitted           CreateStatus = "CREATED"
	CreateExistingSamePayload CreateStatus = "EXISTING_SAME_PAYLOAD"
	CreateIdempotencyConflict CreateStatus = "IDEMPOTENCY_CONFLICT"
	CreateWorkloadBusy        CreateStatus = "WORKLOAD_BUSY"
	CreateCapacityExceeded    CreateStatus = "CAPACITY_EXCEEDED"
)

type CreateResult struct {
	Status CreateStatus
	Record PlanRecord
}

type CASStatus string

const (
	CASCommitted        CASStatus = "COMMITTED"
	CASAlreadyCommitted CASStatus = "ALREADY_COMMITTED"
	CASRevisionConflict CASStatus = "REVISION_CONFLICT"
	CASCapacityExceeded CASStatus = "CAPACITY_EXCEEDED"
)

type CASResult struct {
	Status CASStatus
	Record PlanRecord
}

type ScalePlanRepository interface {
	CreateOrGet(context.Context, IdempotencySubmission, Plan) (CreateResult, error)
	Load(context.Context, string) (PlanRecord, bool, error)
	CompareAndSwap(context.Context, uint64, Plan) (CASResult, error)
	ListRecoverable(context.Context) ([]PlanRecord, error)
}
