package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSPAHandlerServesAssetAndFallback(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("index"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "app.js"), []byte("asset"), 0o600); err != nil {
		t.Fatal(err)
	}
	handler := spaHandler(root)
	for path, wanted := range map[string]string{"/app.js": "asset", "/route": "index"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler(response, request)
		if response.Code != http.StatusOK || response.Body.String() != wanted {
			t.Fatalf("path=%s code=%d body=%q", path, response.Code, response.Body.String())
		}
	}
}

func TestSPAHandlerRejectsMutation(t *testing.T) {
	root := t.TempDir()
	_ = os.WriteFile(filepath.Join(root, "index.html"), []byte("index"), 0o600)
	response := httptest.NewRecorder()
	spaHandler(root)(response, httptest.NewRequest(http.MethodPost, "/", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d", response.Code)
	}
}
