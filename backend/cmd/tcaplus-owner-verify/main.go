// Command tcaplus-owner-verify verifies the production OwnerCoreRecord PB
// table with the official Tcaplus SDK. It is intentionally separate from the
// owner_probe latency tool: the two schemas and table names must not be mixed.
//
// The command inserts one explicitly selected test owner, reads it back,
// performs one RecordVersion CAS, verifies a stale CAS is rejected, and
// deletes the test record unless -keep is supplied.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	ownercorepb "farmgame/api/gen/ownercore"
	"farmgame/internal/storage/ownerstore"
	tcaplus "git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api"
	"git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api/protocol/option"
	"git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api/terror"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "198.18.0.5:9999", "tcaplus dir address host:port")
	app := flag.Uint64("app", 221, "tcaplus app/接入 ID")
	zone := flag.Uint("zone", 0, "tcaplus zone/表格组 ID")
	sig := flag.String("sig", "", "tcaplus cluster signature")
	table := flag.String("table", "OwnerCoreRecord", "OwnerCoreRecord PB table name")
	owner := flag.Uint64("owner", 880000000000002, "dedicated test owner_id")
	timeout := flag.Uint("timeout", 30, "dial timeout seconds")
	keep := flag.Bool("keep", false, "keep the test record instead of deleting it")
	flag.Parse()

	if *sig == "" || *zone == 0 || *owner == 0 {
		fmt.Fprintln(os.Stderr, "example: go run ./cmd/tcaplus-owner-verify -dir 198.18.0.5:9999 -app 221 -zone 1 -sig '<signature>'")
		return fmt.Errorf("usage requires -sig, -zone and non-zero -owner")
	}
	if *table != "OwnerCoreRecord" {
		return fmt.Errorf("refusing table %q: this verifier is for OwnerCoreRecord only", *table)
	}

	dirURL := *dir
	if !strings.Contains(dirURL, "://") {
		dirURL = "tcp://" + dirURL
	}
	zoneID := uint32(*zone)
	client := tcaplus.NewPBClient()
	fmt.Printf("dialing tcaplus dir=%s app=%d zone=%d table=%s owner=%d ...\n", dirURL, *app, zoneID, *table, *owner)
	if err := client.Dial(*app, []uint32{zoneID}, dirURL, *sig, uint32(*timeout), map[uint32][]string{zoneID: {*table}}); err != nil {
		return fmt.Errorf("DIAL FAILED: %w", err)
	}
	client.SetDefaultZoneId(zoneID)
	fmt.Println("DIAL OK")

	record := &ownercorepb.OwnerCoreRecord{OwnerId: *owner}
	cleanup := func() {
		if !*keep {
			_ = client.DoDelete(&ownercorepb.OwnerCoreRecord{OwnerId: *owner}, &option.PBOpt{}, zoneID)
		}
	}
	defer cleanup()
	_ = client.DoDelete(record, &option.PBOpt{}, zoneID)

	initial := ownerstore.BuildInitialRecord(*owner, "tcaplus-owner-verify", ownerstore.DefaultSchemaVersion)
	if err := client.DoInsert(initial, &option.PBOpt{}, zoneID); err != nil {
		return fmt.Errorf("INSERT OwnerCoreRecord: %w", err)
	}
	fmt.Println("[OK] Insert OwnerCoreRecord")

	got := &ownercorepb.OwnerCoreRecord{OwnerId: *owner}
	readOpt := &option.PBOpt{}
	if err := client.DoGet(got, readOpt, zoneID); err != nil {
		return fmt.Errorf("GET OwnerCoreRecord: %w", err)
	}
	if got.OwnerMeta == nil || got.OwnerMeta.BootstrapId != "tcaplus-owner-verify" {
		return fmt.Errorf("GET returned an unexpected bootstrap_id/owner_meta")
	}
	fmt.Printf("[OK] Get OwnerCoreRecord version=%d plots=%d\n", readOpt.Version, len(got.GetFarm().GetPlots()))

	expectedVersion := readOpt.Version
	if got.Wallet == nil {
		// Wallet is always populated by BuildInitialRecord. Reject a malformed
		// table/schema instead of turning a partial record into a false success.
		return fmt.Errorf("OwnerCoreRecord wallet field is absent")
	}
	got.Wallet.Coin++
	if err := client.DoUpdate(got, &option.PBOpt{Version: expectedVersion, VersionPolicy: option.CheckDataVersionAutoIncrease}, zoneID); err != nil {
		return fmt.Errorf("CAS OwnerCoreRecord version=%d: %w", expectedVersion, err)
	}
	fmt.Printf("[OK] CAS OwnerCoreRecord version=%d succeeded\n", expectedVersion)

	got.Wallet.Coin++
	if err := client.DoUpdate(got, &option.PBOpt{Version: expectedVersion, VersionPolicy: option.CheckDataVersionAutoIncrease}, zoneID); err == nil {
		return fmt.Errorf("stale CAS unexpectedly succeeded")
	} else {
		fmt.Printf("[OK] stale CAS rejected: %s\n", terror.GetErrMsg(errCode(err)))
	}
	if *keep {
		fmt.Println("kept test record because -keep was supplied")
	}
	return nil
}

func errCode(err error) int {
	if te, ok := err.(*terror.ErrorCode); ok {
		return te.Code
	}
	if te, ok := err.(terror.ErrorCode); ok {
		return te.Code
	}
	return -1
}
