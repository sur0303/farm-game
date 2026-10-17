package maildelivery

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/player/mail"
)

const maxInt64 = int64(1<<63 - 1)

var (
	// ErrInvalidRequest means that the delivery was rejected before the
	// repository was called.
	ErrInvalidRequest = errors.New("maildelivery: invalid request")
	// ErrDeliveryConflict means that delivery_id is already bound to a
	// different immutable payload. The caller must not retry it with changed
	// content.
	ErrDeliveryConflict = errors.New("maildelivery: delivery id conflict")
	// ErrInvalidService means that the service was constructed without a
	// required trusted dependency or with an invalid limit profile.
	ErrInvalidService = errors.New("maildelivery: invalid service")
	// ErrRepositoryContract means that the repository returned a success
	// result which was not bound to the deterministic mail id we submitted.
	ErrRepositoryContract = errors.New("maildelivery: repository contract violation")
)

// TrustedNow returns the trusted server time in Unix milliseconds. The
// service calls it once per delivery and never accepts a client-supplied
// creation or visibility time.
type TrustedNow func() int64

// DeliveryRequest is the immutable input for one directed system-mail
// delivery. RecipientPlayerID is a single recipient; this type intentionally
// has no broadcast or sender/player fields.
//
// Template is stored as the mail template/version snapshot. Title and Body
// are measured in bytes by mail.BuildMailEntry. Attachment is optional; a
// nil pointer produces a text-only mail. RetentionMs is the visibility
// validity from the trusted creation time. Zero uses the mail package's
// configured default retention.
type DeliveryRequest struct {
	RecipientPlayerID   uint64
	DeliveryID          string
	SourceID            string
	RewardConfigVersion string
	Template            string
	Title               string
	Body                string
	Attachment          *mail.RewardBundle
	RetentionMs         int64
}

// DeliveryService is the trusted use case for inserting one system mail.
// Repository implementations own atomic deduplication; this service owns
// request validation, snapshot construction and result interpretation.
type DeliveryService struct {
	repository mail.MailEntryRepository
	trustedNow TrustedNow
	limits     mail.Limits
}

// NewDeliveryService constructs a service with explicit trusted time and
// mail limits. The repository is the only persistence port; no HTTP or player
// messaging transport is part of this package.
func NewDeliveryService(repository mail.MailEntryRepository, trustedNow TrustedNow, limits mail.Limits) (*DeliveryService, error) {
	if repository == nil {
		return nil, fmt.Errorf("%w: repository is nil", ErrInvalidService)
	}
	if trustedNow == nil {
		return nil, fmt.Errorf("%w: trusted clock is nil", ErrInvalidService)
	}
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidService, err)
	}
	return &DeliveryService{
		repository: repository,
		trustedNow: trustedNow,
		limits:     limits,
	}, nil
}

// Deliver validates and inserts one trusted system mail. Inserted and
// AlreadyDelivered are successful outcomes. A DeliveryConflict is returned
// as ErrDeliveryConflict so a changed payload can never be mistaken for a
// successful replay. Other typed repository outcomes are returned unchanged
// for the caller's retry/dead-letter policy.
func (s *DeliveryService) Deliver(ctx context.Context, request DeliveryRequest) (mail.InsertResult, error) {
	if s == nil || s.repository == nil || s.trustedNow == nil {
		return mail.InsertResult{}, fmt.Errorf("%w: service is nil", ErrInvalidService)
	}
	if ctx == nil {
		return mail.InsertResult{}, fmt.Errorf("%w: context is nil", ErrInvalidRequest)
	}

	nowMs := s.trustedNow()
	if nowMs <= 0 {
		return mail.InsertResult{}, fmt.Errorf("%w: trusted now must be positive", ErrInvalidRequest)
	}

	entry, err := s.buildEntry(request, nowMs)
	if err != nil {
		return mail.InsertResult{}, err
	}

	result, err := s.repository.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		return mail.InsertResult{}, err
	}

	switch result.Status {
	case mail.InsertInserted, mail.InsertAlreadyDelivered:
		if result.MailID != entry.MailID {
			return result, fmt.Errorf("%w: got mail id %q want %q", ErrRepositoryContract, result.MailID, entry.MailID)
		}
	case mail.InsertDeliveryConflict:
		return result, ErrDeliveryConflict
	}
	return result, nil
}

func (s *DeliveryService) buildEntry(request DeliveryRequest, nowMs int64) (mail.MailEntry, error) {
	retention := request.RetentionMs
	if retention == 0 {
		retention = s.limits.DefaultRetentionMs
	}
	if retention <= 0 || retention > s.limits.MaxRetentionMs {
		return mail.MailEntry{}, fmt.Errorf("%w: retention", ErrInvalidRequest)
	}
	if nowMs > maxInt64-retention {
		return mail.MailEntry{}, fmt.Errorf("%w: expiry overflows trusted time", ErrInvalidRequest)
	}
	expireAtMs := nowMs + retention
	if expireAtMs > maxInt64-s.limits.ClaimMaterialRetainAfterClaimEndMs {
		return mail.MailEntry{}, fmt.Errorf("%w: purge time overflows trusted time", ErrInvalidRequest)
	}

	reward := snapshotReward(request.Attachment)
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID:   request.RecipientPlayerID,
		DeliveryID:          request.DeliveryID,
		SourceType:          mail.SourceTypeSystem,
		SourceID:            request.SourceID,
		TitleSnapshot:       request.Title,
		BodySnapshot:        request.Body,
		Reward:              reward,
		TemplateVersion:     request.Template,
		RewardConfigVersion: request.RewardConfigVersion,
		CreatedAtMs:         nowMs,
		VisibleAtMs:         nowMs,
		RetentionMs:         request.RetentionMs,
	}, s.limits)
	if err != nil {
		return mail.MailEntry{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	// Keep the service's explicit expiry guard meaningful if the shared mail
	// builder changes its arithmetic in the future.
	if entry.ExpireAtMs != expireAtMs || entry.ExpireAtMs <= nowMs {
		return mail.MailEntry{}, fmt.Errorf("%w: invalid expiry", ErrInvalidRequest)
	}
	return entry, nil
}

func snapshotReward(attachment *mail.RewardBundle) mail.RewardBundle {
	if attachment == nil {
		return mail.RewardBundle{}
	}
	reward := *attachment
	if attachment.Items != nil {
		reward.Items = append([]mail.ItemGrant(nil), attachment.Items...)
	}
	return reward
}
