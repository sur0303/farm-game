package holderdiscovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	podNameExtra = "authentication.kubernetes.io/pod-name"
	podUIDExtra  = "authentication.kubernetes.io/pod-uid"
)

type TokenReviewAttestor struct {
	client kubernetes.Interface
}

func NewTokenReviewAttestor(client kubernetes.Interface) (*TokenReviewAttestor, error) {
	if client == nil {
		return nil, ErrInvalidDiscovery
	}
	return &TokenReviewAttestor{client: client}, nil
}

func (a *TokenReviewAttestor) Attest(ctx context.Context, request AttestationRequest) (Attestation, error) {
	if a == nil || a.client == nil || ctx == nil || request.Token == "" || request.Audience == "" || request.Namespace == "" || request.ServiceAccountName == "" || request.PodName == "" || request.PodUID == "" {
		return Attestation{}, ErrWorkloadAttestation
	}
	review, err := a.client.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: request.Token, Audiences: []string{request.Audience}},
	}, metav1.CreateOptions{})
	if err != nil || review == nil || !review.Status.Authenticated || review.Status.Error != "" {
		return Attestation{}, fmt.Errorf("%w: token review rejected", ErrWorkloadAttestation)
	}
	if !contains(review.Status.Audiences, request.Audience) {
		return Attestation{}, fmt.Errorf("%w: audience mismatch", ErrWorkloadAttestation)
	}
	expectedUsername := "system:serviceaccount:" + request.Namespace + ":" + request.ServiceAccountName
	if review.Status.User.Username != expectedUsername || !extraContains(review.Status.User.Extra, podNameExtra, request.PodName) || !extraContains(review.Status.User.Extra, podUIDExtra, request.PodUID) {
		return Attestation{}, fmt.Errorf("%w: bound service account identity mismatch", ErrWorkloadAttestation)
	}
	groups := append([]string(nil), review.Status.User.Groups...)
	sort.Strings(groups)
	digest := sha256.Sum256([]byte(strings.Join([]string{expectedUsername, request.PodName, request.PodUID, request.Audience, strings.Join(groups, ",")}, "\x00")))
	return Attestation{SubjectHash: "sha256:" + hex.EncodeToString(digest[:])}, nil
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func extraContains(values map[string]authenticationv1.ExtraValue, key, expected string) bool {
	for _, value := range values[key] {
		if value == expected {
			return true
		}
	}
	return false
}
