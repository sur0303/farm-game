// Command ws-driver is a minimal WebSocket client used only to drive a T1
// TCONND BY_WS probe run. It connects to ws://<tconnd>:<port>, prints every
// frame TCONND sends back (the text control notifications queue/close/pass as
// well as any binary data), and optionally sends one small binary frame so the
// gateproxy-side tconnd-probe can observe an INPROC event.
//
// It is deliberately dumb: it never speaks the farm-gate application protocol
// and never carries a connection_ticket. Its only job is to answer one
// question for a specific TCONND image: does BY_WS actually reach TBUS?
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/websocket"
)

func main() {
	url := getenv("WS_URL", "ws://tconnd:9980/")
	origin := getenv("WS_ORIGIN", "http://localhost/")
	dialTimeout := parseDuration("WS_DIAL_TIMEOUT", "10s")
	runFor := parseDuration("WS_RUN_DURATION", "20s")
	sendHello := getenv("WS_SEND_HELLO", "1") != "0"
	helloDelay := parseDuration("WS_SEND_DELAY", "2s")

	logf("dialing %s (origin=%s, timeout=%s)", url, origin, dialTimeout)

	ws, err := dialWithRetry(url, origin, dialTimeout)
	if err != nil {
		log.Fatalf("ws dial failed: %v", err)
	}
	defer ws.Close()
	logf("connected: WebSocket 101 upgrade OK")

	deadline := time.Now().Add(runFor)
	_ = ws.SetDeadline(deadline)

	if sendHello {
		go func() {
			time.Sleep(helloDelay)
			// A 4-byte binary probe frame. In non-transparent BY_WS mode TCONND
			// forwards this to GameSvr, so the tconnd-probe should log an INPROC.
			payload := []byte{0xF0, 0x0D, 0xBE, 0xEF}
			if _, werr := ws.Write(payload); werr != nil {
				logf("send probe frame error: %v", werr)
				return
			}
			logf("sent 4-byte binary probe frame % x", payload)
		}()
	}

	buf := make([]byte, 65536)
	frames := 0
	for time.Now().Before(deadline) {
		n, rerr := ws.Read(buf)
		if n > 0 {
			frames++
			data := buf[:n]
			logf("frame#%d recv %d bytes | text=%q | hex=% x", frames, n, safeText(data), data)
		}
		if rerr != nil {
			logf("read stopped: %v", rerr)
			break
		}
	}
	logf("done: received %d frame(s) in %s", frames, runFor)
}

func dialWithRetry(url, origin string, timeout time.Duration) (*websocket.Conn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ws, err := websocket.Dial(url, "", origin)
		if err == nil {
			return ws, nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, lastErr
}

func safeText(b []byte) string {
	// Only render as text if it looks printable; otherwise return empty so the
	// hex dump is the source of truth for binary frames.
	for _, c := range b {
		if c < 0x09 || (c > 0x0d && c < 0x20) || c > 0x7e {
			return ""
		}
	}
	return string(b)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseDuration(key, fallback string) time.Duration {
	v, err := time.ParseDuration(getenv(key, fallback))
	if err != nil || v <= 0 {
		v, _ = time.ParseDuration(fallback)
	}
	return v
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "["+strconv.FormatInt(time.Now().UnixMilli(), 10)+"] ws-driver: "+format+"\n", args...)
}
