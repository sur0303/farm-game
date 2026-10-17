package main

import (
	"context"
	"testing"
	"time"
)

func TestDeadlineProjectionGateSerializesConcurrentRefreshes(t *testing.T) {
	owner := &pendingProjectionRecoveryOwner{deadlineGate: make(chan struct{}, 1)}
	release, err := owner.acquireDeadlineGate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := owner.acquireDeadlineGate(ctx)
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for !owner.DeadlineRefreshPending() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !owner.DeadlineRefreshPending() {
		t.Fatal("blocked deadline projection was not recorded as pending")
	}
	if err := <-result; err == nil {
		t.Fatal("concurrent deadline projection acquired the gate")
	}
	if owner.DeadlineRefreshPending() {
		t.Fatal("cancelled trailing deadline projection remained pending")
	}
}

func TestDeadlineProjectionGateRunsTrailingWaiterAfterActiveWave(t *testing.T) {
	owner := &pendingProjectionRecoveryOwner{deadlineGate: make(chan struct{}, 1)}
	release, err := owner.acquireDeadlineGate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	go func() {
		trailingRelease, acquireErr := owner.acquireDeadlineGate(context.Background())
		if acquireErr != nil {
			return
		}
		acquired <- trailingRelease
	}()
	select {
	case <-acquired:
		t.Fatal("trailing request acquired while active wave was still running")
	case <-time.After(10 * time.Millisecond):
	}
	release()
	select {
	case trailingRelease := <-acquired:
		trailingRelease()
	case <-time.After(time.Second):
		t.Fatal("trailing deadline request was lost after active wave")
	}
}

func TestPlanRuntimeWakeExecutesScalePlanChildRoutePlan(t *testing.T) {
	called := make(chan struct{}, 1)
	runtime := &planRuntime{
		recoveryInterval: time.Hour,
		routePlanWake:    make(chan struct{}, 1),
		routePlanRecovery: func(context.Context) error {
			called <- struct{}{}
			return nil
		},
	}
	runtime.StartRecovery(context.Background())
	defer runtime.Close()

	runtime.WakeRoutePlanRecovery()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("ScalePlan-created child RoutePlan was not executed after wakeup")
	}
}

func TestPlanRuntimePeriodicallyRecoversRoutePlansWhenWakeupIsLost(t *testing.T) {
	called := make(chan struct{}, 1)
	runtime := &planRuntime{
		recoveryInterval: 5 * time.Millisecond,
		routePlanRecovery: func(context.Context) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return nil
		},
	}
	runtime.StartRecovery(context.Background())
	defer runtime.Close()

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("periodic RoutePlan recovery fallback did not run")
	}
}

func TestPlanRuntimeRefreshesDeadlinesWhileRoutePlanRecoveryIsBlocked(t *testing.T) {
	recoveryStarted := make(chan struct{})
	deadlineRefreshed := make(chan struct{}, 1)
	runtime := &planRuntime{
		recoveryInterval: 5 * time.Millisecond,
		routePlanWake:    make(chan struct{}, 1),
		routePlanRecovery: func(ctx context.Context) error {
			select {
			case <-recoveryStarted:
			default:
				close(recoveryStarted)
			}
			<-ctx.Done()
			return ctx.Err()
		},
		deadlineRefresh: func(context.Context) error {
			select {
			case deadlineRefreshed <- struct{}{}:
			default:
			}
			return nil
		},
	}
	runtime.StartRecovery(context.Background())
	defer runtime.Close()
	runtime.WakeRoutePlanRecovery()
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("RoutePlan recovery did not start")
	}
	select {
	case <-deadlineRefreshed:
	case <-time.After(time.Second):
		t.Fatal("deadline refresh was sequenced behind blocked RoutePlan recovery")
	}
}

func TestPlanRuntimeStopsBeforeGenericRecoveryWhenRoutePlanWaveIsPending(t *testing.T) {
	called := make(chan struct{}, 1)
	runtime := &planRuntime{
		recoveryInterval: time.Hour,
		routePlanWake:    make(chan struct{}, 1),
		routePlanRecovery: func(context.Context) error {
			called <- struct{}{}
			return context.DeadlineExceeded
		},
	}
	runtime.StartRecovery(context.Background())
	defer runtime.Close()

	runtime.WakeRoutePlanRecovery()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("RoutePlan wave recovery did not run before generic recovery")
	}
}

func TestRecoverRoutePlansUntilReadyRetriesTransientStartupState(t *testing.T) {
	attempts := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := recoverRoutePlansUntilReady(ctx, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v want 3/nil", attempts, err)
	}
}
