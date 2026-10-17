package blobstore

import (
	"context"
	"errors"
	"testing"
)

func BenchmarkMemoryCASEpoch(b *testing.B) {
	store := NewMemoryStore()
	ctx := context.Background()
	payload := make([]byte, 2<<10)
	if err := store.InsertEpoch(ctx, 1, 1, payload); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	version := uint64(1)
	for i := 0; i < b.N; i++ {
		if err := store.CASEpoch(ctx, 1, version, 1, payload); err != nil {
			if errors.Is(err, ErrCASConflict) {
				b.Fatal("unexpected conflict")
			}
			b.Fatal(err)
		}
		version++
	}
}
