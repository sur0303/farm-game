package websocket

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics 是 WebSocket transport 的 Prometheus 指标集。
// 全部为可选:EnableMetrics=false 时完全不注册,零开销。
type Metrics struct {
	// 连接生命周期
	ConnActive    prometheus.Gauge   // 当前活跃连接数
	ConnTotal     prometheus.Counter // 累计接入连接数
	ConnRejected  prometheus.Counter // 因限流/超限被拒绝的连接数
	SlowClient    prometheus.Counter // 因发送队列满被踢出的慢客户端数
	DrainTotal    prometheus.Counter // 进入 drain 的次数
	Lifecycle     prometheus.Gauge   // 1=serving, 0=draining
	DrainSessions prometheus.Gauge   // drain 中剩余连接数

	// 消息流量
	MsgReceived   prometheus.Counter // 累计接收业务帧数
	MsgSent       prometheus.Counter // 累计发送业务帧数
	BytesReceived prometheus.Counter // 累计接收字节
	BytesSent     prometheus.Counter // 累计发送字节

	// 性能分布
	FrameSizeRecv prometheus.Histogram // 接收帧大小分布
	FrameSizeSent prometheus.Histogram // 发送帧大小分布
	WriteBatchSz  prometheus.Histogram // 单次 downlink 唤醒排空的帧数
	HandleLatency prometheus.Histogram // HandleBinary 处理延迟
}

// NewMetrics 注册所有指标到指定 Registerer。
// namespace 推荐 "farmgate",subsystem 推荐 "ws"。
// reg 为 nil 时使用 prometheus.DefaultRegisterer。
func NewMetrics(namespace, subsystem string, reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	constBuckets := []float64{64, 256, 1024, 4096, 16384, 65536, 262144}
	latencyBuckets := []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1}

	m := &Metrics{
		ConnActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "conn_active",
			Help: "Current active websocket connections.",
		}),
		ConnTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "conn_total",
			Help: "Total accepted websocket connections.",
		}),
		ConnRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "conn_rejected_total",
			Help: "Rejected connections (rate limit / invalid).",
		}),
		SlowClient: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "slow_client_kick_total",
			Help: "Slow clients kicked due to full send queue.",
		}),
		DrainTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "drain_total",
			Help: "Total graceful drain transitions.",
		}),
		Lifecycle: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "lifecycle_serving",
			Help: "1 while accepting traffic, 0 while draining.",
		}),
		DrainSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "drain_sessions_remaining",
			Help: "Connections remaining after drain has started.",
		}),
		MsgReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "msg_received_total",
			Help: "Total business frames received.",
		}),
		MsgSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "msg_sent_total",
			Help: "Total business frames sent.",
		}),
		BytesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "bytes_received_total",
			Help: "Total bytes received from clients.",
		}),
		BytesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "bytes_sent_total",
			Help: "Total bytes sent to clients.",
		}),
		FrameSizeRecv: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "frame_recv_bytes",
			Help: "Received frame size distribution.", Buckets: constBuckets,
		}),
		FrameSizeSent: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "frame_sent_bytes",
			Help: "Sent frame size distribution.", Buckets: constBuckets,
		}),
		WriteBatchSz: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "write_batch_size",
			Help:    "Downlink frames drained per wakeup.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		}),
		HandleLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "handle_latency_seconds",
			Help: "HandleBinary processing latency.", Buckets: latencyBuckets,
		}),
	}

	reg.MustRegister(
		m.ConnActive, m.ConnTotal, m.ConnRejected, m.SlowClient,
		m.DrainTotal, m.Lifecycle, m.DrainSessions,
		m.MsgReceived, m.MsgSent, m.BytesReceived, m.BytesSent,
		m.FrameSizeRecv, m.FrameSizeSent, m.WriteBatchSz, m.HandleLatency,
	)
	m.Lifecycle.Set(1)
	return m
}
