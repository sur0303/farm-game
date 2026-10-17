package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol/verifiedholder"
	scaleplancore "farmgame/internal/scaleplan"
)

func TestHolderDiscoveryModesKeepLegacyOffAndRequireMutationModeForKubernetes(t *testing.T) {
	legacy := config{holderDiscoveryMode: holderDiscoveryModeLegacy}
	if err := validateHolderDiscoveryConfig(legacy); err != nil {
		t.Fatalf("legacy discovery must stay inert: %v", err)
	}

	kube := validKubernetesHolderDiscoveryConfig()
	if err := validateHolderDiscoveryConfig(kube); err != nil {
		t.Fatalf("valid Kubernetes discovery: %v", err)
	}
	if !holderDiscoveryUsesK8sCandidates(holderDiscoveryModeK8sCanary) ||
		!holderDiscoveryUsesK8sCandidates(holderDiscoveryModeK8s) ||
		holderDiscoveryUsesK8sCandidates(holderDiscoveryModeShadow) {
		t.Fatal("holder candidate provider mode selection drifted")
	}

	kube.scalePlanMode = string(scaleplancore.RuntimeModeOff)
	if err := validateHolderDiscoveryConfig(kube); err == nil {
		t.Fatal("Kubernetes discovery without a mutation ScalePlan mode must fail closed")
	}
	kube.scalePlanMode = string(scaleplancore.RuntimeModeAuto)
	if err := validateHolderDiscoveryConfig(kube); err != nil {
		t.Fatalf("auto ScalePlan must reuse Kubernetes discovery: %v", err)
	}
}

func TestHolderDiscoveryVerificationWindowMustExceedResync(t *testing.T) {
	cfg := validKubernetesHolderDiscoveryConfig()
	cfg.holderDiscoveryVerifiedTTL = cfg.holderDiscoveryResync
	if err := validateHolderDiscoveryConfig(cfg); err == nil {
		t.Fatal("verification TTL equal to resync must fail closed")
	}
}

func TestHolderDiscoveryWaitInitialRequiresBothDomainSnapshots(t *testing.T) {
	player := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	farm := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	view := verifiedholder.NewStore(time.Now)
	runtime := &holderDiscoveryRuntime{view: view, workloads: []verifiedholder.WorkloadKey{player, farm}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.WaitInitial(ctx) }()

	mustReplaceEmptyWorkload(t, view, player, 1)
	select {
	case err := <-done:
		t.Fatalf("wait completed before FARM snapshot: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	mustReplaceEmptyWorkload(t, view, farm, 1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("initial holder discovery did not become ready")
	}
}

func TestHolderDiscoveryWaitInitialHonorsContext(t *testing.T) {
	runtime := &holderDiscoveryRuntime{
		view:      verifiedholder.NewStore(time.Now),
		workloads: []verifiedholder.WorkloadKey{{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := runtime.WaitInitial(ctx); err == nil {
		t.Fatal("missing initial snapshot must fail closed")
	}
}

func TestHolderDiscoveryRefreshDomainUsesOnlyRequestedReconciler(t *testing.T) {
	playerCalls, farmCalls := 0, 0
	runtime := &holderDiscoveryRuntime{refreshers: map[actorrouting.RouteDomain]func(context.Context) error{
		actorrouting.DomainPlayer: func(context.Context) error { playerCalls++; return nil },
		actorrouting.DomainFarm:   func(context.Context) error { farmCalls++; return nil },
	}}
	if err := runtime.RefreshDomain(context.Background(), actorrouting.DomainFarm); err != nil {
		t.Fatal(err)
	}
	if playerCalls != 0 || farmCalls != 1 {
		t.Fatalf("playerCalls=%d farmCalls=%d", playerCalls, farmCalls)
	}
	if err := runtime.RefreshDomain(context.Background(), actorrouting.RouteDomain("UNKNOWN")); err == nil {
		t.Fatal("unknown route domain refresh was accepted")
	}
}

func TestHolderDiscoveryRefreshDomainCoalescesConcurrentCalls(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	refreshCalls := 0
	runtime := &holderDiscoveryRuntime{refreshers: map[actorrouting.RouteDomain]func(context.Context) error{
		actorrouting.DomainFarm: func(ctx context.Context) error {
			refreshCalls++
			close(started)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}}

	results := make(chan error, 2)
	go func() { results <- runtime.RefreshDomain(context.Background(), actorrouting.DomainFarm) }()
	<-started
	go func() { results <- runtime.RefreshDomain(context.Background(), actorrouting.DomainFarm) }()

	deadline := time.Now().Add(time.Second)
	for {
		runtime.refreshMu.Lock()
		flight := runtime.refreshing[actorrouting.DomainFarm]
		joined := flight != nil && flight.waiters == 2
		runtime.refreshMu.Unlock()
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent refresh did not join the in-flight domain verification")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if refreshCalls != 1 {
		t.Fatalf("refreshCalls=%d want=1", refreshCalls)
	}
}

func TestHolderDiscoveryRefreshDomainWaiterHonorsContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runtime := &holderDiscoveryRuntime{refreshers: map[actorrouting.RouteDomain]func(context.Context) error{
		actorrouting.DomainFarm: func(context.Context) error {
			close(started)
			<-release
			return nil
		},
	}}
	leader := make(chan error, 1)
	go func() { leader <- runtime.RefreshDomain(context.Background(), actorrouting.DomainFarm) }()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := runtime.RefreshDomain(ctx, actorrouting.DomainFarm); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter err=%v want deadline exceeded", err)
	}
	close(release)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
}

func TestHolderDiscoveryRefreshDomainSharesFailure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	want := errors.New("verification failed")
	runtime := &holderDiscoveryRuntime{refreshers: map[actorrouting.RouteDomain]func(context.Context) error{
		actorrouting.DomainFarm: func(context.Context) error {
			close(started)
			<-release
			return want
		},
	}}

	var wait sync.WaitGroup
	wait.Add(2)
	errs := make([]error, 2)
	go func() {
		defer wait.Done()
		errs[0] = runtime.RefreshDomain(context.Background(), actorrouting.DomainFarm)
	}()
	<-started
	go func() {
		defer wait.Done()
		errs[1] = runtime.RefreshDomain(context.Background(), actorrouting.DomainFarm)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		runtime.refreshMu.Lock()
		flight := runtime.refreshing[actorrouting.DomainFarm]
		joined := flight != nil && flight.waiters == 2
		runtime.refreshMu.Unlock()
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failure waiter did not join the in-flight refresh")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	wait.Wait()
	for i, err := range errs {
		if !errors.Is(err, want) {
			t.Fatalf("errs[%d]=%v want shared failure", i, err)
		}
	}
}

func TestHolderControlClientTimeoutStaysWithinTransportBudget(t *testing.T) {
	for _, test := range []struct {
		name        string
		input, want time.Duration
	}{
		{name: "unset", input: 0, want: 30 * time.Second},
		{name: "negative", input: -time.Second, want: 30 * time.Second},
		{name: "short", input: 12 * time.Second, want: 12 * time.Second},
		{name: "maximum", input: 30 * time.Second, want: 30 * time.Second},
		{name: "routeplan-five-minutes", input: 5 * time.Minute, want: 30 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := holderControlClientTimeout(test.input); got != test.want {
				t.Fatalf("timeout=%s want=%s", got, test.want)
			}
		})
	}
}

func mustReplaceEmptyWorkload(t *testing.T, view *verifiedholder.Store, key verifiedholder.WorkloadKey, revision uint64) {
	t.Helper()
	if err := view.ReplaceWorkload(verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: "uid-" + key.Name, KubeGeneration: 1,
		KubeResourceVersion: "rv", KubeListResourceVersion: "list-rv",
		DiscoveryRevision: revision, CapturedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

func validKubernetesHolderDiscoveryConfig() config {
	return config{
		scalePlanMode:                    string(scaleplancore.RuntimeModeManual),
		holderDiscoveryMode:              holderDiscoveryModeK8sCanary,
		holderDiscoveryAudience:          defaultHolderDiscoveryAudience,
		holderDiscoveryResync:            30 * time.Second,
		holderDiscoveryVerifiedTTL:       45 * time.Second,
		holderDiscoveryPlayerLogicPort:   9100,
		holderDiscoveryPlayerControlPort: 19100,
		holderDiscoveryFarmLogicPort:     9090,
		holderDiscoveryFarmControlPort:   19101,
		holderClientCert:                 "client.crt",
		holderClientKey:                  "client.key",
		holderClientCA:                   "ca.crt",
	}
}
