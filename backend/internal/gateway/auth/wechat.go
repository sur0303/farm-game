package auth

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
)

// WeChatMiniProgramLoginRequest is the request from wx.login() in a WeChat mini-program.
// In production, the code is exchanged with the WeChat API for openid + session_key.
// In dev mode, the code is accepted as-is and mapped to a deterministic player ID.
type WeChatMiniProgramLoginRequest struct {
	Code        string `json:"code"`
	ClientBuild string `json:"client_build"`
}

// WeChatDevVerifier accepts any WeChat code and derives a deterministic player ID.
// This is for dev/staging only. Production uses the WeChat API to resolve openid.
type WeChatDevVerifier struct{}

func (v *WeChatDevVerifier) Verify(_ context.Context, code string) (playerID uint64, err error) {
	// Deterministic player ID from code hash
	hash := sha256.Sum256([]byte("wechat-dev-" + code))
	playerID = binary.BigEndian.Uint64(hash[:8]) % 90000000 // keep in 0..90M range
	if playerID == 0 {
		playerID = 100001 // fallback
	}
	return playerID, nil
}

// WeChatHandler exposes the WeChat mini-program login endpoint.
// In dev mode it accepts any code; in production it would exchange
// the code with WeChat's API for openid + session_key.
type WeChatHandler struct {
	svc          *Service
	verifier     *WeChatDevVerifier
	cookieSecure bool
}

// NewWeChatHandler creates the WeChat mini-program auth handler.
// In dev mode, pass a WeChatDevVerifier; in production, use a real
// WeChat API-backed verifier.
func NewWeChatHandler(svc *Service, cookieSecure ...bool) *WeChatHandler {
	return &WeChatHandler{
		svc:          svc,
		verifier:     &WeChatDevVerifier{},
		cookieSecure: len(cookieSecure) > 0 && cookieSecure[0],
	}
}

// ServeHTTP handles POST /api/v1/auth/wx-miniprogram-login.
func (h *WeChatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	var req WeChatMiniProgramLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "bad request body"})
		return
	}
	if req.Code == "" {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "missing code"})
		return
	}

	playerID, err := h.verifier.Verify(r.Context(), req.Code)
	if err != nil {
		log.Printf("wx-miniprogram-login verify failed: %v", err)
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized"})
		return
	}

	// Use the shared demo platform token so the existing demoVerifier in
	// svc.Login succeeds. The WeChat code is used for tracing/bootstrap only.
	bootstrapID := "wx-bootstrap-" + string(rune(playerID%10000))
	res, err := h.svc.Login(r.Context(), LoginRequest{
		PlatformToken: "demo-platform-token",
		BootstrapID:   bootstrapID,
		ClientBuild:   req.ClientBuild,
	})
	if err != nil {
		log.Printf("wx-miniprogram-login failed err=%v", err)
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized"})
		return
	}
	if err := setHTTPSessionCookie(w, h.svc, res, h.cookieSecure); err != nil {
		log.Printf("wx-miniprogram-login session failed err=%v", err)
		writeJSON(w, http.StatusInternalServerError, httpError{Error: "failed to establish session"})
		return
	}
	writeJSON(w, http.StatusOK, res)
}
