package loadtest

import "time"

// EvidenceRule 声明一个场景要申报 CAPACITY_PASS 之前必须真实采集的证据。
// 未真实运行并采集齐 RequiredEvidence 的场景,结论只能是 INCONCLUSIVE。
type EvidenceRule struct {
	// Tool 是被复用的既有工具(cmd/lease-stress、cmd/ws-stress、
	// scripts/test-multigate-drain),不允许另起第二套实现。
	Tool string
	// RequiredEvidence 列出必须落盘/抓取的证据形态。
	RequiredEvidence []string
	// PassCriteria 是判定阈值的可读描述(精确数值由后续 Owner 按
	// docs/design/31 §17.2 SLOProfile 与 60 §2.3 verdict 语义冻结)。
	PassCriteria string
}

// BroadcastSlowConn 扩展接缝:200 订阅同农场广播 + 慢连接背压。
// 复用 ws-stress hot 场景(共享农场)与 lease-stress 的 199 viewers;
// 慢连接比例由未来 ws-stress 参数化(当前缺口见 ICR-Q0-1,只能记录)。
func BroadcastSlowConn() (WSStressParams, EvidenceRule) {
	return WSStressParams{
			Scenario:   "hot",
			Conns:      200,
			SharedFarm: 1, // 调用方必须替换为真实 owner farm_id
			HotLoops:   10,
			HotMixed:   true,
		}, EvidenceRule{
			Tool: "cmd/ws-stress",
			RequiredEvidence: []string{
				"-json-out 原始结果",
				"Gate /metrics 抓取(broadcast 队列深度、丢弃计数、慢连接标记)",
				"慢连接客户端收到帧的 seq 连续性证明(NEVER_SILENT_DROP,合同 28 §L2-PROTO-49)",
			},
			PassCriteria: "广播不阻塞健康 Lane;慢连接只影响自身队列;无静默丢帧",
		}
}

// FullGameplay 扩展接缝:多种植/收获/浇水混合全玩法压力。
func FullGameplay() (WSStressParams, EvidenceRule) {
	return WSStressParams{
			Scenario: "plant",
			Conns:    100,
			Rate:     10,
			Duration: 60 * time.Second,
		}, EvidenceRule{
			Tool: "cmd/ws-stress",
			RequiredEvidence: []string{
				"-json-out 原始结果",
				"OperationResult 抽样核对(SUCCEEDED/REJECTED 无假阳性)",
				"farm-svr/player-svr 进程指标与错误日志",
			},
			PassCriteria: "全玩法命令在目标并发下无 5xx 放大、无幂等冲突误报",
		}
}

// Soak 扩展接缝:长时间续租 + 重连风暴组合 soak。
func Soak(duration time.Duration) (LeaseStressParams, EvidenceRule) {
	return LeaseStressParams{
			Viewers:         199,
			JoinConcurrency: 32,
			Duration:        duration,
		}, EvidenceRule{
			Tool: "cmd/lease-stress (+ scripts/test-lease-renew-stress 环境)",
			RequiredEvidence: []string{
				"全程 renewed/expired/fail_closed 计数(lease-stress 汇总输出)",
				"Gate /metrics 时序抓取(内存、队列、goroutine 泄漏曲线)",
				"客户端 0 次非预期 REJOIN_REQUIRED 的证明",
			},
			PassCriteria: "expired=0、fail_closed=0、无资源增长趋势;时长达到 docs/design/31 §20 的 2h/24h 档位才允许申报对应容量等级",
		}
}

// Drain 扩展接缝:多 Gate 排干证据(复用 W5B 脚本,不重新实现)。
func Drain() (DrainScriptEnv, EvidenceRule) {
	return DrainScriptEnv{}, EvidenceRule{
		Tool: "scripts/test-multigate-drain",
		RequiredEvidence: []string{
			"DrainNotice/1012 关闭码客户端观察记录",
			"RecoveryBarrier → InFarm 状态转换日志",
			"双 Gate 进程日志(proxy 切换前后)",
		},
		PassCriteria: "SIGTERM 后排干完成、客户端经 Gate B 恢复、无重复提交",
	}
}
