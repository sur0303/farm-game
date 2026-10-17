package holderdiscovery

import (
	"context"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestTokenReviewAttestorBindsAudienceServiceAccountAndPod(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true, Audiences: []string{"farm-holder"},
			User: authenticationv1.UserInfo{Username: "system:serviceaccount:farm:farm-svr", Groups: []string{"system:serviceaccounts"}, Extra: map[string]authenticationv1.ExtraValue{
				podNameExtra: {"farm-svr-1"}, podUIDExtra: {"pod-1"},
			}},
		}}, nil
	})
	attestor, err := NewTokenReviewAttestor(client)
	if err != nil {
		t.Fatal(err)
	}
	result, err := attestor.Attest(context.Background(), AttestationRequest{Token: "token", Audience: "farm-holder", Namespace: "farm", ServiceAccountName: "farm-svr", PodName: "farm-svr-1", PodUID: "pod-1"})
	if err != nil || result.SubjectHash == "" {
		t.Fatalf("attestation failed: %+v err=%v", result, err)
	}
}

func TestTokenReviewAttestorRejectsWrongBoundPod(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Authenticated: true, Audiences: []string{"farm-holder"},
			User: authenticationv1.UserInfo{Username: "system:serviceaccount:farm:farm-svr", Extra: map[string]authenticationv1.ExtraValue{podNameExtra: {"other"}, podUIDExtra: {"other"}}}}}, nil
	})
	attestor, _ := NewTokenReviewAttestor(client)
	if _, err := attestor.Attest(context.Background(), AttestationRequest{Token: "token", Audience: "farm-holder", Namespace: "farm", ServiceAccountName: "farm-svr", PodName: "farm-svr-1", PodUID: "pod-1"}); err == nil {
		t.Fatal("expected bound-pod mismatch")
	}
}
