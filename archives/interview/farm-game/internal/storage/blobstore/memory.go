package blobstore

import (
	"context"
	"sync"
)

type memoryRecord struct {
	data        []byte
	version     uint64
	writerEpoch uint64
}

func (m *MemoryStore) InsertEpoch(ctx context.Context, id uint64, writerEpoch uint64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[id]; ok {
		return ErrExists
	}
	m.records[id] = memoryRecord{data: append([]byte(nil), data...), version: 1, writerEpoch: writerEpoch}
	return nil
}

func (m *MemoryStore) Fence(ctx context.Context, id uint64, writerEpoch uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		return ErrNotFound
	}
	if writerEpoch < rec.writerEpoch {
		return ErrWriterEpochMismatch
	}
	rec.writerEpoch = writerEpoch
	m.records[id] = rec
	return nil
}

func (m *MemoryStore) CASEpoch(ctx context.Context, id uint64, expectVersion, writerEpoch uint64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		return ErrNotFound
	}
	if rec.writerEpoch != writerEpoch {
		return ErrWriterEpochMismatch
	}
	if rec.version != expectVersion {
		return ErrCASConflict
	}
	rec.data = append([]byte(nil), data...)
	rec.version++
	m.records[id] = rec
	return nil
}

type MemoryStore struct {
	mu      sync.Mutex
	records map[uint64]memoryRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[uint64]memoryRecord)}
}

func (m *MemoryStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return append([]byte(nil), rec.data...), rec.version, nil
}

func (m *MemoryStore) Insert(ctx context.Context, id uint64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[id]; ok {
		return ErrExists
	}
	m.records[id] = memoryRecord{data: append([]byte(nil), data...), version: 1}
	return nil
}

func (m *MemoryStore) CAS(ctx context.Context, id uint64, expectVersion uint64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		return ErrNotFound
	}
	if rec.writerEpoch != 0 {
		return ErrWriterEpochMismatch
	}
	if rec.version != expectVersion {
		return ErrCASConflict
	}
	rec.data = append([]byte(nil), data...)
	rec.version++
	m.records[id] = rec
	return nil
}
