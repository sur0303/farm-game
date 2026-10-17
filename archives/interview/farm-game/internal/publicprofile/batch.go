package publicprofile

import (
	"context"
	"errors"
	"fmt"
	"sync"

	publicpb "farmgame/api/gen/public"
)

const MaxBatchProfiles = 50

type batchSource interface {
	BatchGetPublicProfiles(context.Context, []uint64) (map[uint64]*publicpb.PublicProfileView, error)
}

type BatchReader struct {
	reader  Reader
	workers int
}

func NewBatchReader(reader Reader, workers int) (*BatchReader, error) {
	if reader == nil {
		return nil, errors.New("public profile batch reader is required")
	}
	if workers <= 0 {
		workers = 8
	}
	return &BatchReader{reader: reader, workers: workers}, nil
}

// BatchGetPublicProfiles performs bounded parallel actor reads and fails the
// whole page on any missing/malformed profile. Callers never serialize a
// partial roster or placeholder owner.
func (r *BatchReader) BatchGetPublicProfiles(ctx context.Context, playerIDs []uint64) (map[uint64]*publicpb.PublicProfileView, error) {
	if r == nil || len(playerIDs) == 0 || len(playerIDs) > MaxBatchProfiles {
		return nil, fmt.Errorf("public profile batch must contain 1..%d ids", MaxBatchProfiles)
	}
	seen := make(map[uint64]struct{}, len(playerIDs))
	for _, id := range playerIDs {
		if id == 0 {
			return nil, errors.New("public profile batch player id is required")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, errors.New("public profile batch contains duplicate player id")
		}
		seen[id] = struct{}{}
	}
	if optimized, ok := r.reader.(batchSource); ok {
		return optimized.BatchGetPublicProfiles(ctx, playerIDs)
	}
	workers := r.workers
	if workers > len(playerIDs) {
		workers = len(playerIDs)
	}
	profiles := make([]*publicpb.PublicProfileView, len(playerIDs))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				profile, err := r.reader.Read(ctx, playerIDs[index])
				if err == nil {
					err = Validate(profile, playerIDs[index])
				}
				if err != nil {
					once.Do(func() { firstErr = err })
					continue
				}
				profiles[index] = profile
			}
		}()
	}
	for index := range playerIDs {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	result := make(map[uint64]*publicpb.PublicProfileView, len(profiles))
	for index, profile := range profiles {
		if profile == nil {
			return nil, errors.New("public profile batch incomplete")
		}
		result[playerIDs[index]] = profile
	}
	return result, nil
}
