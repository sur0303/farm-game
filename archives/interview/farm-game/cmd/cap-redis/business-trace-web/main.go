// Command business-trace-web serves a prebuilt H5 bundle and proxies its
// same-origin API/WebSocket paths to the isolated trace stack. It has no HMR,
// dependency installation, or build work in the measurement process.
package main

import (
	"flag"
	"fmt"
	"log"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:5194", "loopback listen address")
	dist := flag.String("dist", "", "prebuilt H5 dist directory")
	api := flag.String("api", "", "Gate HTTP origin")
	ws := flag.String("ws", "", "Gate WebSocket origin")
	flag.Parse()
	if *dist == "" || *api == "" || *ws == "" {
		log.Fatal("dist, api, and ws are required")
	}
	if host := strings.Split(*listen, ":")[0]; host != "127.0.0.1" && host != "localhost" {
		log.Fatal("listen must be loopback")
	}
	root, err := filepath.Abs(*dist)
	if err != nil {
		log.Fatal(err)
	}
	if info, statErr := os.Stat(filepath.Join(root, "index.html")); statErr != nil || info.IsDir() {
		log.Fatal("prebuilt dist/index.html is required")
	}
	apiURL, err := url.Parse(*api)
	if err != nil || (apiURL.Scheme != "http" && apiURL.Scheme != "https") {
		log.Fatal("invalid API origin")
	}
	wsURL, err := url.Parse(*ws)
	if err != nil || (wsURL.Scheme != "ws" && wsURL.Scheme != "wss") {
		log.Fatal("invalid WebSocket origin")
	}
	wsHTTP := *wsURL
	if wsHTTP.Scheme == "ws" {
		wsHTTP.Scheme = "http"
	} else {
		wsHTTP.Scheme = "https"
	}
	apiProxy := httputil.NewSingleHostReverseProxy(apiURL)
	wsProxy := httputil.NewSingleHostReverseProxy(&wsHTTP)
	mux := http.NewServeMux()
	mux.Handle("/api/", apiProxy)
	mux.Handle("/ws", wsProxy)
	mux.HandleFunc("/", spaHandler(root))
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("BUSINESS_TRACE_WEB_READY listen=%s dist=prebuilt\n", *listen)
	log.Fatal(server.ListenAndServe())
}

func spaHandler(root string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		clean := filepath.Clean("/" + r.URL.Path)
		candidate := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
		if !strings.HasPrefix(candidate, root+string(filepath.Separator)) && candidate != root {
			http.NotFound(w, r)
			return
		}
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			candidate = filepath.Join(root, "index.html")
		}
		if contentType := mime.TypeByExtension(filepath.Ext(candidate)); contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		http.ServeFile(w, r, candidate)
	}
}
