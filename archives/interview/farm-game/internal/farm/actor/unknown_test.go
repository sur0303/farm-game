package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/storage/blobstore"
)

type ambiguousCASStore struct {
	base      *blobstore.MemoryStore
	commit    bool
	triggered atomic.Bool
	once      sync.Once
	readGate  chan struct{}
}

func newAmbiguousCASStore(commit bool) *ambiguousCASStore {
	return &ambiguousCASStore{base: blobstore.NewMemoryStore(), commit: commit, readGate: make(chan struct{})}
}

func (s *ambiguousCASStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	if s.triggered.Load() {
		select {
		case <-s.readGate:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	return s.base.Get(ctx, id)
}

func (s *ambiguousCASStore) Insert(ctx context.Context, id uint64, data []byte) error {
	return s.base.Insert(ctx, id, data)
}

func (s *ambiguousCASStore) CAS(ctx context.Context, id uint64, version uint64, data []byte) error {
	fired := false
	s.once.Do(func() { fired = true })
	if !fired {
		return s.base.CAS(ctx, id, version, data)
	}
	if s.commit {
		if err := s.base.CAS(ctx, id, version, data); err != nil {
			return err
		}
	}
	s.triggered.Store(true)
	return blobstore.ErrTimeoutUnknown
}

func (s *ambiguousCASStore) releaseReads() { close(s.readGate) }

func TestTimeoutUnknownFreezesQueuedTurnsAndResolverReloadsAuthority(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "not-committed"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			store := newAmbiguousCASStore(committed)
			registry := NewRegistry(store, 8)
			registry.SetAutoResolveUnknown(false)
			defer registry.Stop()
			_, err := registry.Submit(context.Background(), 41, func(ctx context.Context, actor *FarmActor) (any, error) {
				return actor.Plant(ctx, "unknown-op", testPlantCropCycleID(actor.farmID, "unknown-op"), []byte("plant-hash"), &farmpb.PlantCropRequest{
					PlotIndex: 0, CropDefinitionId: 1,
				}, 0, 0, time.UnixMilli(1_800_000_000_000), time.Minute)
			})
			if !errors.Is(err, blobstore.ErrTimeoutUnknown) {
				t.Fatalf("plant err=%v", err)
			}
			if got := registry.Activation().StateOf(41); got != activation.ResolvingUnknown {
				t.Fatalf("activation=%s, want RESOLVING_UNKNOWN", got)
			}
			unknown, ok := registry.UnknownContext(41)
			if !ok || unknown.OperationID != "unknown-op" || unknown.BeforeRecordVersion != 1 ||
				unknown.ActivationEpoch == 0 || unknown.CommandType != commonpb.CommandType_PLANT {
				t.Fatalf("unknown context=%+v ok=%v", unknown, ok)
			}
			called := false
			_, err = registry.Submit(context.Background(), 41, func(context.Context, *FarmActor) (any, error) {
				called = true
				return nil, nil
			})
			if !errors.Is(err, activation.ErrActorBusy) || called {
				t.Fatalf("queued turn err=%v called=%v", err, called)
			}

			store.releaseReads()
			resolution, err := registry.ResolveUnknown(context.Background(), 41)
			if err != nil {
				t.Fatal(err)
			}
			if committed && resolution != UnknownCommitted {
				t.Fatalf("resolution=%v, want committed", resolution)
			}
			if !committed && resolution != UnknownNotCommitted {
				t.Fatalf("resolution=%v, want not committed", resolution)
			}
			if got := registry.Activation().StateOf(41); got != activation.Active {
				t.Fatalf("resolved activation=%s", got)
			}
			value, err := registry.Submit(context.Background(), 41, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.Operation("unknown-op")
			})
			if committed {
				if err != nil || value == nil {
					t.Fatalf("committed receipt value=%v err=%v", value, err)
				}
			} else if err == nil {
				t.Fatalf("not-committed receipt value=%v err=%v", value, err)
			}

			// Retrying the exact operation after authoritative resolution either
			// replays the committed receipt or performs the previously uncommitted
			// mutation once. It must never create a second CropCycle.
			value, err = registry.Submit(context.Background(), 41, func(ctx context.Context, actor *FarmActor) (any, error) {
				action, plantErr := actor.Plant(ctx, "unknown-op", testPlantCropCycleID(actor.farmID, "unknown-op"), []byte("plant-hash"), &farmpb.PlantCropRequest{
					PlotIndex: 0, CropDefinitionId: 1,
				}, 0, 0, time.UnixMilli(1_800_000_000_000), time.Minute)
				return struct {
					Action *ActionResult
					Cycles int
				}{Action: action, Cycles: len(actor.record.Core.CropCycles)}, plantErr
			})
			if err != nil {
				t.Fatal(err)
			}
			retried := value.(struct {
				Action *ActionResult
				Cycles int
			})
			if retried.Action == nil || retried.Cycles != 1 || retried.Action.Replayed != committed {
				t.Fatalf("retry after resolution=%+v committed=%v", retried, committed)
			}
		})
	}
}

func TestWaterCommittedButTimeoutResolvesAndReplaysOnce(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)
	store := newAmbiguousCASStore(true)
	record := InitialRecord(42)
	record.Core.Plots[0].ActiveCropCycleId = "water-cycle"
	record.Core.CropCycles = append(record.Core.CropCycles, &farmpb.CropCycle{
		CropCycleId: "water-cycle", PlotIndex: 0, CropDefinitionId: 1,
		PlantAt: now.Add(-time.Second).UnixMilli(), MatureAt: now.Add(time.Minute).UnixMilli(),
		BaseYield: 10, StealableLimit: 2,
	})
	data, err := marshalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.base.Insert(ctx, 42, data); err != nil {
		t.Fatal(err)
	}

	registry := NewRegistry(store, 8)
	registry.SetAutoResolveUnknown(false)
	defer registry.Stop()
	water := func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Water(ctx, "water-unknown-op", []byte("water-hash"), &farmpb.WaterCropRequest{
			PlotIndex: 0,
		}, 0, 0, "water-cycle", now)
	}
	if _, err := registry.Submit(ctx, 42, water); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("water err=%v", err)
	}
	unknown, ok := registry.UnknownContext(42)
	if !ok || unknown.OperationID != "water-unknown-op" ||
		unknown.CommandType != commonpb.CommandType_CARE || unknown.ActivationEpoch == 0 {
		t.Fatalf("unknown context=%+v ok=%v", unknown, ok)
	}

	store.releaseReads()
	resolution, err := registry.ResolveUnknown(ctx, 42)
	if err != nil || resolution != UnknownCommitted {
		t.Fatalf("resolution=%v err=%v", resolution, err)
	}
	value, err := registry.Submit(ctx, 42, func(ctx context.Context, actor *FarmActor) (any, error) {
		action, waterErr := water(ctx, actor)
		cycle := actor.findCycle("water-cycle")
		return struct {
			Action      *ActionResult
			Watered     bool
			FarmVersion uint64
		}{Action: action.(*ActionResult), Watered: cycle != nil && cycle.Watered, FarmVersion: actor.record.Core.FarmVersion}, waterErr
	})
	if err != nil {
		t.Fatal(err)
	}
	got := value.(struct {
		Action      *ActionResult
		Watered     bool
		FarmVersion uint64
	})
	if got.Action == nil || !got.Action.Replayed || !got.Watered || got.FarmVersion != 1 {
		t.Fatalf("water replay=%+v", got)
	}
}
