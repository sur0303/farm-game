package publicprofile

import (
	"errors"
	"fmt"

	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
)

// ModelField is the compact public profile committed beside PlayerRecord.
const ModelField = "public_profile"

// MaxEncodedProjectionBytes is a deliberately conservative fixed charge for
// the validated projection (128 UTF-8 runes plus bounded scalar fields). The
// writer enforces it and Player participant capacity admission reserves it.
const MaxEncodedProjectionBytes = 1024

// MaxModelFieldBytes includes the model-field key charged by AuthorityStore.
const MaxModelFieldBytes = len(ModelField) + MaxEncodedProjectionBytes

func MarshalProjection(view *publicpb.PublicProfileView) ([]byte, error) {
	if view == nil || view.GetPlayerId() == 0 {
		return nil, errors.New("public profile projection is incomplete")
	}
	if err := Validate(view, view.GetPlayerId()); err != nil {
		return nil, err
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(view)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxEncodedProjectionBytes {
		return nil, errors.New("public profile projection exceeds bounded capacity")
	}
	return data, nil
}

func UnmarshalProjection(data []byte, playerID uint64) (*publicpb.PublicProfileView, error) {
	if len(data) == 0 || playerID == 0 {
		return nil, errors.New("public profile projection is unavailable")
	}
	var view publicpb.PublicProfileView
	if err := proto.Unmarshal(data, &view); err != nil {
		return nil, fmt.Errorf("public profile projection decode: %w", err)
	}
	if err := Validate(&view, playerID); err != nil {
		return nil, err
	}
	return &view, nil
}
