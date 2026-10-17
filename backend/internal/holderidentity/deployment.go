// Package holderidentity defines the immutable deployment and process
// identity shared by holder bootstrap, registration, and ScalePlan adapters.
package holderidentity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	IdentityModeEnvironment = "ACTOR_HOLDER_IDENTITY_MODE"
	IdentityModeStatic      = "static-frozen-v1"
	IdentityModeKubernetes  = "kubernetes-process-v1"

	// MaxIdentityBytes matches the current bounded HolderRegistry identity
	// contract. Kubernetes UIDs are opaque values; they are not parsed as UUIDs.
	MaxIdentityBytes = 128
	bootEntropyBytes = 16
)

var ErrInvalidDeploymentIdentity = errors.New("holderidentity: invalid deployment identity")

// DeploymentIdentity is the immutable Kubernetes join for one holder process.
// The zero value is retained only for the frozen static compatibility path.
// Any non-zero value must contain the complete Pod/Workload/ordinal tuple.
type DeploymentIdentity struct {
	PodUID      string `json:"pod_uid,omitempty"`
	WorkloadUID string `json:"workload_uid,omitempty"`
	Ordinal     int32  `json:"ordinal"`
}

func (i DeploymentIdentity) IsZero() bool {
	return i.PodUID == "" && i.WorkloadUID == "" && i.Ordinal == 0
}

func (i DeploymentIdentity) Exact() bool {
	return i.PodUID != "" && i.WorkloadUID != "" && i.Ordinal >= 0
}

// ValidateOptional accepts the zero legacy tuple or a complete exact tuple.
func (i DeploymentIdentity) ValidateOptional() error {
	if i.IsZero() {
		return nil
	}
	return i.ValidateExact()
}

func (i DeploymentIdentity) ValidateExact() error {
	if err := validateOpaque("pod uid", i.PodUID); err != nil {
		return err
	}
	if err := validateOpaque("workload uid", i.WorkloadUID); err != nil {
		return err
	}
	if i.Ordinal < 0 {
		return fmt.Errorf("%w: ordinal must be non-negative", ErrInvalidDeploymentIdentity)
	}
	return nil
}

// ParseEnvironment reads the complete Kubernetes tuple exactly once. All
// fields absent selects the frozen static compatibility tuple; partial or
// malformed input fails closed. An explicit mode is recommended for new
// manifests; omission is retained only for the frozen static compatibility
// train.
func ParseEnvironment(getenv func(string) string) (DeploymentIdentity, error) {
	if getenv == nil {
		return DeploymentIdentity{}, fmt.Errorf("%w: environment reader required", ErrInvalidDeploymentIdentity)
	}
	mode := getenv(IdentityModeEnvironment)
	if mode != "" && mode != IdentityModeStatic && mode != IdentityModeKubernetes {
		return DeploymentIdentity{}, fmt.Errorf("%w: unsupported identity mode %q", ErrInvalidDeploymentIdentity, mode)
	}
	podUID := getenv("ACTOR_HOLDER_POD_UID")
	workloadUID := getenv("ACTOR_HOLDER_WORKLOAD_UID")
	ordinalRaw := getenv("ACTOR_HOLDER_ORDINAL")
	if podUID == "" && workloadUID == "" && ordinalRaw == "" {
		if mode == IdentityModeKubernetes {
			return DeploymentIdentity{}, fmt.Errorf("%w: Kubernetes identity mode requires the complete tuple", ErrInvalidDeploymentIdentity)
		}
		return DeploymentIdentity{}, nil
	}
	if mode != IdentityModeKubernetes {
		return DeploymentIdentity{}, fmt.Errorf("%w: complete Kubernetes tuple requires explicit %s=%s", ErrInvalidDeploymentIdentity, IdentityModeEnvironment, IdentityModeKubernetes)
	}
	if podUID == "" || workloadUID == "" || ordinalRaw == "" {
		return DeploymentIdentity{}, fmt.Errorf("%w: deployment identity must be all absent or complete", ErrInvalidDeploymentIdentity)
	}
	if strings.TrimSpace(ordinalRaw) != ordinalRaw || (len(ordinalRaw) > 1 && ordinalRaw[0] == '0') {
		return DeploymentIdentity{}, fmt.Errorf("%w: ordinal must be canonical decimal", ErrInvalidDeploymentIdentity)
	}
	ordinal, err := strconv.ParseInt(ordinalRaw, 10, 32)
	if err != nil || ordinal < 0 || strconv.FormatInt(ordinal, 10) != ordinalRaw {
		return DeploymentIdentity{}, fmt.Errorf("%w: ordinal must be canonical non-negative int32", ErrInvalidDeploymentIdentity)
	}
	identity := DeploymentIdentity{PodUID: podUID, WorkloadUID: workloadUID, Ordinal: int32(ordinal)}
	if err := identity.ValidateExact(); err != nil {
		return DeploymentIdentity{}, err
	}
	return identity, nil
}

// RandomBootID returns a cryptographically random 128-bit process generation.
// It must be called once before constructing authority, HolderAgent, or
// HolderControl so every subsystem receives the same immutable BootID.
func RandomBootID() (string, error) {
	return randomBootID(rand.Reader)
}

func randomBootID(reader io.Reader) (string, error) {
	if reader == nil {
		return "", errors.New("holderidentity: random reader required")
	}
	var value [bootEntropyBytes]byte
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validateOpaque(name, value string) error {
	if value == "" || len(value) > MaxIdentityBytes {
		return fmt.Errorf("%w: %s must be non-empty and at most %d bytes", ErrInvalidDeploymentIdentity, name, MaxIdentityBytes)
	}
	// Kubernetes UIDs are transported as opaque claims, but this protocol uses
	// one deliberately narrow byte grammar so Go and Redis Lua can validate it
	// identically before mutation. Visible ASCII excludes all whitespace,
	// malformed UTF-8, Unicode normalization aliases, and control bytes.
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return fmt.Errorf("%w: %s must contain visible ASCII without whitespace", ErrInvalidDeploymentIdentity, name)
		}
	}
	return nil
}
