package capstaticroute

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
)

var (
	transitionPattern = regexp.MustCompile(`^capinit-[0-9a-f]{32}$`)
	digestPattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func MarshalReceipt(receipt Receipt) ([]byte, error) {
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func ValidateBundle(snapshotBytes, ledgerBytes, receiptBytes []byte, current time.Time) error {
	return validateBundle(snapshotBytes, ledgerBytes, receiptBytes, current, true)
}

func ValidateBundleEvidence(snapshotBytes, ledgerBytes, receiptBytes []byte) error {
	return validateBundle(snapshotBytes, ledgerBytes, receiptBytes, time.Time{}, false)
}

func validateBundle(snapshotBytes, ledgerBytes, receiptBytes []byte, current time.Time, requireFresh bool) error {
	var snapshot actorrouting.RouteSnapshot
	var ledger Ledger
	var receipt Receipt
	if err := strictDecode(snapshotBytes, &snapshot); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := strictDecode(ledgerBytes, &ledger); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	if err := strictDecode(receiptBytes, &receipt); err != nil {
		return fmt.Errorf("receipt: %w", err)
	}
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return err
	}
	if ledger.SchemaVersion != LedgerSchema || receipt.SchemaVersion != ReceiptSchema ||
		receipt.Status != "INITIALIZED_LOCAL_PROFILE_B_NOT_QUALIFIED" || !receipt.Complete || !receipt.BootstrapMaterializable || receipt.Activatable ||
		!ledger.Complete || !ledger.BootstrapMaterializable || ledger.Activatable || receipt.CompanyRedisContacted || ledger.CompanyRedisContacted ||
		receipt.ActiveBindingAuthorized || ledger.ActiveBindingAuthorized || receipt.CapacityConstantEligible || ledger.CapacityConstantEligible ||
		receipt.Supports30mDAU || ledger.Supports30mDAU || receipt.FormalL2Authorized ||
		receipt.BindingScope != "LOCAL_FUNCTIONAL_ONLY" || ledger.BindingScope != "LOCAL_FUNCTIONAL_ONLY" {
		return ErrIncomplete
	}
	if receipt.RunID != ledger.RunID || receipt.ProfileID != ledger.ProfileID || receipt.Generation != ledger.Generation ||
		receipt.PlayerShardCount != snapshot.Player.ShardCount || receipt.FarmShardCount != snapshot.Farm.ShardCount ||
		snapshot.Player.ShardCount != snapshot.Farm.ShardCount || snapshot.Player.ShardCount != 4 && snapshot.Player.ShardCount != 1024 {
		return ErrIdentityConflict
	}
	if receipt.HashVersion != snapshot.HashVersion || receipt.HashSeed != snapshot.HashSeed || receipt.Realm != snapshot.Realm ||
		receipt.HashVersion != actorrouting.HashVersionXXH64V2 || snapshot.GlobalRevision == 0 || receipt.ManagerWorkingRevision < 1 ||
		len(snapshot.Holders) != 4 || len(receipt.Holders) != 4 {
		return ErrIdentityConflict
	}
	if receipt.DurableSnapshotCommitted {
		canonical, err := routecontrol.CanonicalizeRouteSnapshot(snapshot)
		if err != nil || receipt.DurableSnapshotRevision != snapshot.GlobalRevision ||
			receipt.DurableSnapshotChecksumSHA256 != canonical.ChecksumSHA256 ||
			receipt.DurableSnapshotTopologyFingerprint != canonical.TopologyFingerprint {
			return ErrIncomplete
		}
	} else if snapshot.GlobalRevision != 1 {
		return ErrIncomplete
	}
	for index, holder := range receipt.Holders {
		actual := snapshot.Holders[index]
		expectedDomain := "PLAYER"
		if index >= 2 {
			expectedDomain = "FARM"
		}
		if holder.Domain != expectedDomain || holder.ID != actual.InstanceID || holder.BootID != actual.BootID || holder.Endpoint != actual.Endpoint || actual.State != actorrouting.HolderStateActive {
			return ErrIdentityConflict
		}
		prefix := fmt.Sprintf("%s-%s-g%d-", holder.ID, receipt.ProfileID, receipt.Generation)
		if !strings.HasPrefix(holder.BootID, prefix) || sha256Text(strings.TrimPrefix(holder.BootID, prefix)) != receipt.ControllerNonceSHA256 {
			return ErrIdentityConflict
		}
	}
	snapshotDigest := sha256.Sum256(snapshotBytes)
	ledgerDigest := sha256.Sum256(ledgerBytes)
	if receipt.RouteSnapshotSHA256 != hex.EncodeToString(snapshotDigest[:]) || ledger.RouteSnapshotSHA256 != receipt.RouteSnapshotSHA256 ||
		receipt.ExactLedgerSHA256 != hex.EncodeToString(ledgerDigest[:]) {
		return ErrIncomplete
	}
	expected := int(snapshot.Player.ShardCount) * 2
	claimed := receipt.Counters.ExistingExact + receipt.Counters.ClaimApplied + receipt.Counters.ClaimReplay
	renewed := receipt.Counters.RenewApplied + receipt.Counters.RenewReplay
	if ledger.ExpectedRecordCount != expected || len(ledger.Records) != expected || receipt.Counters.Verified != expected ||
		claimed != expected || renewed != expected {
		return ErrIncomplete
	}
	minimum, maximum := int64(1<<63-1), int64(0)
	seenKeys := make(map[string]bool, expected)
	seenTags := make(map[string]bool, expected)
	for index, record := range ledger.Records {
		domain := actorrouting.DomainPlayer
		shard := uint32(index)
		routes := snapshot.Player.Routes
		if index >= int(snapshot.Player.ShardCount) {
			domain = actorrouting.DomainFarm
			shard = uint32(index - int(snapshot.Player.ShardCount))
			routes = snapshot.Farm.Routes
		}
		route := routes[shard]
		holder := snapshot.Holders[route.HolderIndex]
		wantHash, err := ledgerRecordHash(record)
		if err != nil || wantHash != record.RecordSHA256 || record.Domain != domain || record.VirtualShard != shard ||
			record.AuthorityLayoutVersion == "" || record.HashVersion != snapshot.HashVersion || record.HashSeed != snapshot.HashSeed ||
			record.HolderID != holder.InstanceID || record.HolderBootID != holder.BootID ||
			record.AssignmentRevision != route.ShardRevision || record.WriterEpoch != route.ExpectedWriterEpoch ||
			record.LeaseEpoch != route.ExpectedLeaseEpoch || record.LeaseUntilMs != route.LeaseUntilMs ||
			record.LeaseState != "ACTIVE" || route.State != actorrouting.ShardStateActive || !transitionPattern.MatchString(record.TransitionID) ||
			!digestPattern.MatchString(record.TransitionDigest) || record.TransitionAtMs <= 0 || seenKeys[record.RedisKey] {
			return fmt.Errorf("%w: ledger record %d", ErrIncomplete, index)
		}
		tag := redisHashTag(record.RedisKey)
		if tag == "" || seenTags[tag] || !strings.Contains(tag, ":auth:") ||
			!strings.HasSuffix(record.RedisKey, "}:shard-authority") {
			return fmt.Errorf("%w: Redis key %d", ErrIncomplete, index)
		}
		seenKeys[record.RedisKey] = true
		seenTags[tag] = true
		if record.LeaseUntilMs < minimum {
			minimum = record.LeaseUntilMs
		}
		if record.LeaseUntilMs > maximum {
			maximum = record.LeaseUntilMs
		}
	}
	if minimum != receipt.MinimumLeaseUntilMs || maximum != receipt.MaximumLeaseUntilMs || receipt.MinimumRemainingLeaseMs <= 0 ||
		receipt.RequestedTTLms <= receipt.MinimumRemainingLeaseMs || maximum-minimum >= receipt.RequestedTTLms-receipt.MinimumRemainingLeaseMs ||
		requireFresh && minimum-current.UnixMilli() < receipt.MinimumRemainingLeaseMs {
		return ErrIncomplete
	}
	return nil
}

func strictDecode(data []byte, target any) error {
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func redisHashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return ""
	}
	close := strings.IndexByte(key[open+1:], '}')
	if close <= 0 {
		return ""
	}
	return key[open+1 : open+1+close]
}
