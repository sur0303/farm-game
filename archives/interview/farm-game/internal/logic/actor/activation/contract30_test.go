package activation

import (
	"errors"
	"testing"
)

func TestLoadUnknownHasDedicatedClaimUnknownState(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	if err := c.BeginLoad(100); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkClaimUnknown(100); err != nil {
		t.Fatal(err)
	}
	if got := c.StateOf(100); got != ClaimUnknown {
		t.Fatalf("state=%s", got)
	}
	if _, err := c.AdmitUserCommand(100, true); !errors.Is(err, ErrActorBusy) {
		t.Fatalf("err=%v", err)
	}
	if got := c.Stats(); got.ConcurrentLoads != 1 || got.StateClaimUnknown != 1 {
		t.Fatalf("stats=%+v", got)
	}
	if err := c.ResolveClaimUnknownToActive(100); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats(); got.ConcurrentLoads != 0 || got.StateActive != 1 {
		t.Fatalf("stats=%+v", got)
	}
}

func TestQuiescingCanCancelButPassivatingCannotResurrect(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	mustActivate(t, c, 101)
	if err := c.BeginPassivate(101); err != nil {
		t.Fatal(err)
	}
	if got := c.StateOf(101); got != Quiescing {
		t.Fatalf("state=%s", got)
	}
	if action, err := c.AdmitUserCommand(101, true); err != nil || action != AdmitProceed {
		t.Fatalf("admit=%v err=%v", action, err)
	}
	if got := c.StateOf(101); got != Active {
		t.Fatalf("state=%s", got)
	}

	if err := c.BeginPassivate(101); err != nil {
		t.Fatal(err)
	}
	if err := c.CommitPassivate(101); err != nil {
		t.Fatal(err)
	}
	if got := c.StateOf(101); got != Passivating {
		t.Fatalf("state=%s", got)
	}
	if _, err := c.AdmitUserCommand(101, true); !errors.Is(err, ErrActorBusy) {
		t.Fatalf("err=%v", err)
	}
	if err := c.CancelPassivate(101); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("cancel err=%v", err)
	}
	if got := c.StateOf(101); got != Passivating {
		t.Fatalf("state=%s", got)
	}
}

func TestClaimUnknownCanFailClosedToFencedOrReconcile(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	if err := c.BeginLoad(102); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkClaimUnknown(102); err != nil {
		t.Fatal(err)
	}
	if err := c.FenceClaimUnknown(102); err != nil {
		t.Fatal(err)
	}
	if got := c.StateOf(102); got != Fenced {
		t.Fatalf("state=%s", got)
	}
	if got := c.Stats().ConcurrentLoads; got != 0 {
		t.Fatalf("loads=%d", got)
	}

	if err := c.BeginLoad(103); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkClaimUnknown(103); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkLoadReconcileRequired(103); err != nil {
		t.Fatal(err)
	}
	if got := c.StateOf(103); got != ReconcileRequired {
		t.Fatalf("state=%s", got)
	}
}
