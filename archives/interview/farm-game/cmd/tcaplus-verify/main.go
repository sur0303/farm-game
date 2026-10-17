// Command tcaplus-verify 连真实 TcaplusDB 验证 D7 关注的 SDK 语义,并采集
// 写放大/延迟实测数据回填容量模型(见 docs/design/51-tcaplus-verify.md)。
//
// 前置:需在集群控制台先建表 owner_probe(主键 owner_id),结构见
// cmd/tcaplus-verify/verifypb/probe.proto。
//
// 运行(凭据见 deploy/secrets/tcaplus.conf.example):
//
//	go run ./cmd/tcaplus-verify \
//	  -dir 198.18.0.5:9999 -app 221 -zone <ZONE> -sig <SIGNATURE> -table owner_probe
//
// 若未提供 -sig / -zone,程序仅做连通性 dial 尝试并打印指引后退出。
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"farmgame/cmd/tcaplus-verify/verifypb"

	tcaplus "git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api"
	"git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api/protocol/option"
	"git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api/terror"
)

func main() {
	var (
		dir     = flag.String("dir", "198.18.0.5:9999", "tcaplus dir addr host:port")
		app     = flag.Uint64("app", 221, "tcaplus app id (接入ID)")
		zone    = flag.Uint("zone", 0, "tcaplus zone id (表格组 id)")
		sig     = flag.String("sig", "", "tcaplus signature (集群密码)")
		table   = flag.String("table", "owner_probe", "probe table name")
		timeout = flag.Uint("timeout", 30, "dial timeout seconds")
	)
	flag.Parse()

	if *sig == "" || *zone == 0 {
		fmt.Println("[指引] 未提供 -sig / -zone。")
		fmt.Println("  1) 在集群控制台创建表格组(zone)与表 owner_probe(结构见 verifypb/probe.proto);")
		fmt.Println("  2) 取得该表格组 zone id 与集群密码 signature;")
		fmt.Printf("  3) 重新运行:go run ./cmd/tcaplus-verify -dir %s -app %d -zone <ZONE> -sig <SIGNATURE> -table %s\n", *dir, *app, *table)
		fmt.Println("  当前仅尝试 TCP 连通性(dial 需要 signature,故到此为止)。")
		os.Exit(0)
	}

	client := tcaplus.NewPBClient()
	zoneList := []uint32{uint32(*zone)}
	zoneTable := map[uint32][]string{uint32(*zone): {*table}}

	// SDK 要求 dir url 带 tcp:// scheme(dir.go 解析 "tcp://host:port")。
	dirURL := *dir
	if !strings.Contains(dirURL, "://") {
		dirURL = "tcp://" + dirURL
	}

	fmt.Printf("dialing tcaplus dir=%s app=%d zone=%d table=%s ...\n", dirURL, *app, *zone, *table)
	if err := client.Dial(*app, zoneList, dirURL, *sig, uint32(*timeout), zoneTable); err != nil {
		fmt.Printf("DIAL FAILED: %v\n", err)
		os.Exit(1)
	}
	client.SetDefaultZoneId(uint32(*zone))
	fmt.Println("DIAL OK")

	runChecks(client, uint32(*zone))
}

func runChecks(client *tcaplus.PBClient, zone uint32) {
	const probeOwner = int64(880000000000001)

	// 清理遗留
	_ = client.DoDelete(&verifypb.OwnerProbe{OwnerId: probeOwner}, &option.PBOpt{}, zone)

	// 1) 条件 Insert 并发：同一个 owner 只有一个创建者可以成功。
	const creators = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	var insertErrors []error
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := &verifypb.OwnerProbe{OwnerId: probeOwner, Counter: 0, Blob: make([]byte, 1024)}
			err := client.DoInsert(rec, &option.PBOpt{}, zone)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
			} else {
				insertErrors = append(insertErrors, err)
			}
		}()
	}
	wg.Wait()
	if successes != 1 {
		fmt.Printf("[FAIL] concurrent insert successes=%d errors=%d\n", successes, len(insertErrors))
		return
	}
	fmt.Printf("[OK] concurrent insert: %d creators, exactly one success (%d rejected)\n", creators, len(insertErrors))

	// 2) Get + 读回版本
	got := &verifypb.OwnerProbe{OwnerId: probeOwner}
	opt := &option.PBOpt{}
	if err := client.DoGet(got, opt, zone); err != nil {
		fmt.Printf("[FAIL] get: %v\n", err)
		return
	}
	fmt.Printf("[OK] get: counter=%d version=%d\n", got.Counter, opt.Version)

	// 3) CAS:用读回的版本做条件 Update,应成功;用旧版本再次应失败。
	ver := opt.Version
	got.Counter = 1
	if err := client.DoUpdate(got, &option.PBOpt{Version: ver, VersionPolicy: option.CheckDataVersionAutoIncrease}, zone); err != nil {
		fmt.Printf("[FAIL] CAS update with correct version: %v\n", err)
	} else {
		fmt.Printf("[OK] CAS update with version=%d succeeded\n", ver)
	}
	got.Counter = 2
	err := client.DoUpdate(got, &option.PBOpt{Version: ver, VersionPolicy: option.CheckDataVersionAutoIncrease}, zone) // 旧版本,应冲突
	if err != nil {
		fmt.Printf("[OK] CAS with stale version rejected as expected: %s\n", terror.GetErrMsg(errCode(err)))
	} else {
		fmt.Println("[WARN] stale-version update NOT rejected — 版本语义需复核!")
	}

	// 4) 写放大/延迟:16/32/64/128KB blob 各 replace N 次,measure P50/P99。
	for _, kb := range []int{16, 32, 64, 128} {
		measureLatency(client, zone, probeOwner, kb, 50)
	}

	// 清理
	_ = client.DoDelete(&verifypb.OwnerProbe{OwnerId: probeOwner}, &option.PBOpt{}, zone)
	fmt.Println("done.")
}

func measureLatency(client *tcaplus.PBClient, zone uint32, owner int64, kb, n int) {
	blob := make([]byte, kb*1024)
	var ds []time.Duration
	for i := 0; i < n; i++ {
		rec := &verifypb.OwnerProbe{OwnerId: owner, Counter: int64(i), Blob: blob}
		t0 := time.Now()
		if err := client.DoReplace(rec, &option.PBOpt{}, zone); err != nil {
			fmt.Printf("[FAIL] replace %dKB: %v\n", kb, err)
			return
		}
		ds = append(ds, time.Since(t0))
	}
	p50, p99 := percentile(ds, 50), percentile(ds, 99)
	fmt.Printf("[LAT] %3dKB replace x%d: P50=%v P99=%v\n", kb, n, p50, p99)
}

func percentile(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	// 简单排序
	for i := 1; i < len(ds); i++ {
		for j := i; j > 0 && ds[j] < ds[j-1]; j-- {
			ds[j], ds[j-1] = ds[j-1], ds[j]
		}
	}
	idx := (p * len(ds)) / 100
	if idx >= len(ds) {
		idx = len(ds) - 1
	}
	return ds[idx]
}

// errCode 从 tcaplus error 提取错误码(SDK error 携带 code)。
func errCode(err error) int {
	if te, ok := err.(*terror.ErrorCode); ok {
		return te.Code
	}
	if te, ok := err.(terror.ErrorCode); ok {
		return te.Code
	}
	return -1
}
