package scaleplan

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	core "farmgame/internal/scaleplan"
)

// SafeDesiredReplicasAnnotation is a control intent, not a mirror of
// spec.replicas. Scale-out may also be adopted after a direct +1, but safe
// scale-in must be requested here so ScalePlan can drain the highest ordinal
// before it patches the StatefulSet scale subresource.
const SafeDesiredReplicasAnnotation = "scaleplan.farm/safe-desired-replicas"

type DesiredReplicaReader struct {
	client     kubernetes.Interface
	annotation string
}

func NewDesiredReplicaReader(client kubernetes.Interface, annotation string) (*DesiredReplicaReader, error) {
	annotation = strings.TrimSpace(annotation)
	if client == nil || annotation == "" {
		return nil, fmt.Errorf("%w: desired-replica reader", core.ErrInvalidPlan)
	}
	return &DesiredReplicaReader{client: client, annotation: annotation}, nil
}

func (r *DesiredReplicaReader) DesiredReplicas(ctx context.Context, ref core.WorkloadRef) (int32, bool, error) {
	if r == nil || r.client == nil || ctx == nil {
		return 0, false, core.ErrInvalidPlan
	}
	if err := ref.Validate(); err != nil {
		return 0, false, err
	}
	set, err := r.client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return 0, false, err
	}
	if set == nil || set.UID == "" || set.ResourceVersion == "" {
		return 0, false, fmt.Errorf("%w: incomplete StatefulSet", core.ErrConflict)
	}
	raw, found := set.Annotations[r.annotation]
	if !found {
		return 0, false, nil
	}
	if raw == "" || strings.TrimSpace(raw) != raw {
		return 0, false, fmt.Errorf("%w: non-canonical desired replicas", core.ErrConflict)
	}
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != raw {
		return 0, false, fmt.Errorf("%w: invalid desired replicas annotation", core.ErrConflict)
	}
	return int32(parsed), true, nil
}

// AcknowledgeDesiredReplicas conditionally removes a reached one-shot intent.
// JSON Patch tests prevent an old controller observation from deleting a new
// operator request that reused the same annotation key.
func (r *DesiredReplicaReader) AcknowledgeDesiredReplicas(ctx context.Context, ref core.WorkloadRef, observed core.WorkloadState, desired int32) error {
	if r == nil || r.client == nil || ctx == nil || desired < 1 {
		return core.ErrInvalidPlan
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if observed.Ref != ref || observed.UID == "" || observed.ResourceVersion == "" || observed.Replicas != desired {
		return fmt.Errorf("%w: desired-replica acknowledgement observation", core.ErrConflict)
	}
	annotationPath := "/metadata/annotations/" + strings.NewReplacer("~", "~0", "/", "~1").Replace(r.annotation)
	patch, err := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": observed.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": observed.ResourceVersion},
		{"op": "test", "path": annotationPath, "value": strconv.FormatInt(int64(desired), 10)},
		{"op": "remove", "path": annotationPath},
	})
	if err != nil {
		return err
	}
	_, err = r.client.AppsV1().StatefulSets(ref.Namespace).Patch(ctx, ref.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	return err
}
