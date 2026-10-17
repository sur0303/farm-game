package ownerstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	ownercorepb "farmgame/api/gen/ownercore"
	tcaplus "git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api"
	"git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api/protocol/option"
)

// TcaplusStore is the production OwnerStore adapter. The client is injected so
// local tests can use MemoryStore and the real probe can use the official SDK.
type TcaplusStore struct {
	client *tcaplus.PBClient
	zone   uint32
}

func NewTcaplusStore(client *tcaplus.PBClient, zone uint32) (*TcaplusStore, error) {
	if client == nil || zone == 0 {
		return nil, errors.New("ownerstore: tcaplus client and non-zero zone required")
	}
	return &TcaplusStore{client: client, zone: zone}, nil
}

func (s *TcaplusStore) Get(ctx context.Context, ownerID uint64) (*ownercorepb.OwnerCoreRecord, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	rec := &ownercorepb.OwnerCoreRecord{OwnerId: ownerID}
	opt := &option.PBOpt{}
	if err := s.client.DoGet(rec, opt, s.zone); err != nil {
		return nil, 0, classifyTcaplus(err)
	}
	return rec, uint64(opt.Version), nil
}

func (s *TcaplusStore) Insert(ctx context.Context, rec *ownercorepb.OwnerCoreRecord) error {
	if rec == nil {
		return errors.New("ownerstore: nil record")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return classifyTcaplus(s.client.DoInsert(rec, &option.PBOpt{}, s.zone))
}

func (s *TcaplusStore) CAS(ctx context.Context, rec *ownercorepb.OwnerCoreRecord, expectVersion uint64) error {
	if rec == nil {
		return errors.New("ownerstore: nil record")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	opt := &option.PBOpt{Version: int32(expectVersion), VersionPolicy: option.CheckDataVersionAutoIncrease}
	return classifyTcaplus(s.client.DoUpdate(rec, opt, s.zone))
}

func (s *TcaplusStore) CompareAndSwap(ctx context.Context, ownerID uint64, expectVersion, expectOwnerEpoch uint64, next *ownercorepb.OwnerCoreRecord) error {
	current, version, err := s.Get(ctx, ownerID)
	if err != nil {
		return err
	}
	if version != expectVersion {
		return ErrCASConflict
	}
	if current.OwnerMeta != nil && current.OwnerMeta.OwnerEpoch != expectOwnerEpoch {
		return ErrOwnerEpochStale
	}
	return s.CAS(ctx, next, expectVersion)
}

func classifyTcaplus(err error) error {
	if err == nil {
		return nil
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "not exist"), strings.Contains(s, "not_exist"), strings.Contains(s, "not found"), strings.Contains(s, "no record"):
		return fmt.Errorf("%w: %v", ErrOwnerNotFound, err)
	case strings.Contains(s, "already"), strings.Contains(s, "exist"):
		return fmt.Errorf("%w: %v", ErrOwnerExists, err)
	case strings.Contains(s, "version"), strings.Contains(s, "old version"), strings.Contains(s, "record version"):
		return fmt.Errorf("%w: %v", ErrCASConflict, err)
	case strings.Contains(s, "timeout"), strings.Contains(s, "deadline"):
		return fmt.Errorf("%w: %v", ErrTimeoutUnknown, err)
	default:
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
}
