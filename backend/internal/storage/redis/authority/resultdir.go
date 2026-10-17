package authority

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// ResultDirectory is the contract-31 §8.3 query copy (`proj:result:v2` hash,
// field = root-scoped operation digest). It is a rebuildable projection, never
// an authority: AuthorityReceipt/StoredOutcome/ResolutionReceipt remain the
// only terminal truth. Publishing is idempotent per digest; a different entry
// under the same digest fails closed. Writes go only to the frozen v2 key;
// v1 never became a published production format, so there is no v1 read
// fallback and no dual write.
type ResultDirectoryStore struct {
	client        redis.UniversalClient
	registry      *ScriptRegistry
	proj          rediskey.ProjectionKeys
	maxEntryBytes int
}

const defaultMaxResultEntryBytes = 2048

func NewResultDirectoryStore(client redis.UniversalClient, registry *ScriptRegistry, namespace rediskey.Namespace, maxEntryBytes int) (*ResultDirectoryStore, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client required", authoritystore.ErrInvalidRequest)
	}
	proj, err := rediskey.NewProjectionKeys(namespace)
	if err != nil {
		return nil, err
	}
	if registry == nil {
		if registry, err = NewScriptRegistry(client); err != nil {
			return nil, err
		}
	}
	if maxEntryBytes <= 0 {
		maxEntryBytes = defaultMaxResultEntryBytes
	}
	return &ResultDirectoryStore{client: client, registry: registry, proj: proj, maxEntryBytes: maxEntryBytes}, nil
}

type ResultPublishStatus uint8

const (
	ResultPublishUnknown ResultPublishStatus = iota
	ResultPublishPublished
	ResultPublishAlreadyPublished
	ResultPublishConflict
	ResultPublishUnavailableNotSent
	ResultPublishTimeoutUnknown
)

func (s ResultPublishStatus) String() string {
	switch s {
	case ResultPublishPublished:
		return "PUBLISHED"
	case ResultPublishAlreadyPublished:
		return "ALREADY_PUBLISHED"
	case ResultPublishConflict:
		return "CONFLICT"
	case ResultPublishUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case ResultPublishTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

func (d *ResultDirectoryStore) validateEntry(entry ResultEntry) error {
	if err := validateStableID("command_type", entry.CommandType); err != nil {
		return err
	}
	if len(entry.OperationDigest) != 64 {
		return fmt.Errorf("%w: operation digest", authoritystore.ErrInvalidRequest)
	}
	if entry.RootKind == "FRIEND_PAIR" {
		if workidentity.ValidateStableOpaqueID(entry.RootID) != nil {
			return fmt.Errorf("%w: Pair root id", authoritystore.ErrInvalidRequest)
		}
	} else if _, err := strconv.ParseUint(entry.RootID, 10, 64); entry.RootID == "" || err != nil {
		return fmt.Errorf("%w: root id", authoritystore.ErrInvalidRequest)
	}
	if err := validateStableID("root_kind", entry.RootKind); err != nil {
		return err
	}
	if err := validateStableID("final_status_class", entry.FinalStatusClass); err != nil {
		return err
	}
	if len(entry.SourceRef) > 256 {
		return fmt.Errorf("%w: source ref", authoritystore.ErrInvalidRequest)
	}
	return nil
}

// PublishResult upserts one directory entry. Same digest + same entry is an
// idempotent replay; same digest + different entry is CONFLICT (fail closed).
func (d *ResultDirectoryStore) PublishResult(ctx context.Context, playerID uint64, entry ResultEntry) (ResultPublishStatus, error) {
	if err := ctx.Err(); err != nil {
		return ResultPublishUnknown, err
	}
	if playerID == 0 {
		return ResultPublishUnknown, fmt.Errorf("%w: player id", authoritystore.ErrInvalidRequest)
	}
	if err := d.validateEntry(entry); err != nil {
		return ResultPublishUnknown, err
	}
	key, err := d.proj.ResultDirectory(playerID)
	if err != nil {
		return ResultPublishUnknown, err
	}
	value := encodeResultEntry(entry)
	reply, err := d.registry.Run(ctx, ScriptPublishResultEntry, []string{key.String()},
		entry.OperationDigest, value, strconv.Itoa(d.maxEntryBytes))
	if err != nil {
		if isContractViolation(err) {
			return ResultPublishUnknown, fmt.Errorf("%w: %v", errScriptContract, err)
		}
		if classifyWriteError(err) == fateNotSent {
			return ResultPublishUnavailableNotSent, nil
		}
		return ResultPublishTimeoutUnknown, nil
	}
	if len(reply) == 0 {
		return ResultPublishUnknown, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "PUBLISHED":
		return ResultPublishPublished, nil
	case "ALREADY_PUBLISHED":
		return ResultPublishAlreadyPublished, nil
	case "CONFLICT":
		return ResultPublishConflict, nil
	case "CAPACITY_EXCEEDED":
		return ResultPublishUnknown, authoritystore.ErrCapacityExceeded
	default:
		return ResultPublishUnknown, fmt.Errorf("%w: publish status %q", errScriptContract, reply[0])
	}
}

// LookupResult reads one entry by digest. A miss is NOT_FOUND per contract:
// directory absence never proves an operation did not run.
func (d *ResultDirectoryStore) LookupResult(ctx context.Context, playerID uint64, operationDigest string) (ResultEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return ResultEntry{}, false, err
	}
	if playerID == 0 || len(operationDigest) != 64 {
		return ResultEntry{}, false, fmt.Errorf("%w: player id/operation digest", authoritystore.ErrInvalidRequest)
	}
	key, err := d.proj.ResultDirectory(playerID)
	if err != nil {
		return ResultEntry{}, false, err
	}
	value, err := d.client.HGet(ctx, key.String(), operationDigest).Result()
	if errors.Is(err, redis.Nil) {
		return ResultEntry{}, false, nil
	}
	if err != nil {
		return ResultEntry{}, false, fmt.Errorf("redisauthority: result directory read unavailable: %w", err)
	}
	entry, err := parseResultEntry(value)
	if err != nil {
		return ResultEntry{}, false, err
	}
	return entry, true, nil
}
