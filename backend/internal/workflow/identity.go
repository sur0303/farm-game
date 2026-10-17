package workflow

import (
	"fmt"

	workflowpb "farmgame/api/gen/workflow"
	"farmgame/internal/workidentity"
)

// StableID separates the recoverable workflow identity from the public
// operation_id while preserving deterministic CreateIfAbsent behavior.
func StableID(subjectPlayerID uint64, kind workflowpb.WorkflowKind, operationID string) (string, error) {
	name, err := StableKindName(kind)
	if err != nil {
		return "", err
	}
	return workidentity.WorkflowID(subjectPlayerID, name, operationID)
}

func StableKindName(kind workflowpb.WorkflowKind) (string, error) {
	switch kind {
	case workflowpb.WorkflowKind_PLANT:
		return "PLANT", nil
	case workflowpb.WorkflowKind_HARVEST:
		return "HARVEST", nil
	case workflowpb.WorkflowKind_THEFT:
		return "THEFT", nil
	case workflowpb.WorkflowKind_CARE:
		return "CARE", nil
	default:
		return "", fmt.Errorf("workflow: unstable kind %s", kind)
	}
}

// 业务意图的权威 payload hash 由 FirstHop 按 28 §3.5 唯一生成
// (commandcatalog CanonicalPayloadBuilder + command_type 前缀,见
// internal/logic/trusted.ValidateCommand 的 VerifiedCommand.PayloadHash);
// Workflow 持久记录只消费该值,不再自行哈希 ClientCommandEnvelope。
