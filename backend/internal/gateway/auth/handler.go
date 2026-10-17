package auth

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
)

// Handler 暴露 HTTPS 登录接口(/v1/auth/login),返回一次性连接票据与 WSS 地址。
// 这是 S2 最小登录入口;生产可叠加真实平台 SSO、风控与限流。
type Handler struct {
	svc          *Service
	cookieSecure bool
}

// RenewHandler exchanges the signed HttpOnly session cookie for a new
// single-use WebSocket ticket without replaying a platform/launch ticket.
type RenewHandler struct {
	svc          *Service
	cookieSecure bool
}

// RevokeAllHandler invalidates every HTTP/WSS session generation for the
// authenticated player and clears the browser's current Cookie.
type RevokeAllHandler struct {
	svc          *Service
	cookieSecure bool
}

func NewRenewHandler(svc *Service, cookieSecure ...bool) *RenewHandler {
	secure := len(cookieSecure) > 0 && cookieSecure[0]
	return &RenewHandler{svc: svc, cookieSecure: secure}
}

func NewRevokeAllHandler(svc *Service, cookieSecure ...bool) *RevokeAllHandler {
	secure := len(cookieSecure) > 0 && cookieSecure[0]
	return &RevokeAllHandler{svc: svc, cookieSecure: secure}
}

func (h *RevokeAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	if h.svc == nil {
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "session service unavailable"})
		return
	}
	cookie, err := r.Cookie(HTTPSessionCookieName)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	binding, err := h.svc.AuthenticateHTTPSession(r.Context(), cookie.Value)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	if err := h.svc.RevokeAllSessions(r.Context(), binding); err != nil {
		if errors.Is(err, ErrHTTPSessionInvalid) {
			writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
			return
		}
		log.Printf("revoke all sessions failed player_id=%d err=%v", binding.PlayerID, err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "failed to revoke sessions"})
		return
	}
	clearHTTPSessionCookie(w, h.cookieSecure)
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

func (h *RenewHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	cookie, err := r.Cookie(HTTPSessionCookieName)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	binding, err := h.svc.AuthenticateHTTPSession(r.Context(), cookie.Value)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	res, err := h.svc.Renew(r.Context(), binding)
	if err != nil {
		log.Printf("connection ticket renew failed player_id=%d err=%v", binding.PlayerID, err)
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	if err := setHTTPSessionCookie(w, h.svc, res, h.cookieSecure); err != nil {
		log.Printf("http session refresh failed player_id=%d err=%v", binding.PlayerID, err)
		writeJSON(w, http.StatusInternalServerError, httpError{Error: "failed to refresh session"})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// NewHandler 创建登录 HTTP handler。
func NewHandler(svc *Service, cookieSecure ...bool) *Handler {
	secure := len(cookieSecure) > 0 && cookieSecure[0]
	return &Handler{svc: svc, cookieSecure: secure}
}

type httpLoginRequest struct {
	PlatformToken string `json:"platform_token"`
	BootstrapID   string `json:"bootstrap_id"`
	ClientBuild   string `json:"client_build"`
}

type httpError struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code,omitempty"`
}

// ServeHTTP 处理登录请求。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	var req httpLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "bad request body"})
		return
	}
	res, err := h.svc.Login(r.Context(), LoginRequest{
		PlatformToken: req.PlatformToken,
		BootstrapID:   req.BootstrapID,
		ClientBuild:   req.ClientBuild,
	})
	if err != nil {
		// Keep the public response uniform, but retain a redacted server-side
		// cause so cross-process bootstrap/Tcaplus failures are distinguishable
		// from an invalid platform token during staging verification. The
		// request body and connection ticket are never logged.
		log.Printf("login failed path=%s err=%v", r.URL.Path, err)
		// 统一映射为 401 + ERR_CONNECTION_TICKET_INVALID 之外,这里平台层用 401 unauthorized。
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	if err := setHTTPSessionCookie(w, h.svc, res, h.cookieSecure); err != nil {
		log.Printf("issue http session failed player_id=%d err=%v", res.PlayerID, err)
		writeJSON(w, http.StatusInternalServerError, httpError{Error: "failed to establish session"})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func setHTTPSessionCookie(w http.ResponseWriter, svc *Service, result *LoginResult, secure bool) error {
	token, expiresAt, err := svc.IssueHTTPSession(result)
	if err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{
		Name: HTTPSessionCookieName, Value: token, Path: "/",
		HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode,
		Expires: expiresAt, MaxAge: int(time.Until(expiresAt).Seconds()),
	})
	return nil
}

func clearHTTPSessionCookie(w http.ResponseWriter, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name: HTTPSessionCookieName, Value: "", Path: "/",
		HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode,
		Expires: time.Unix(1, 0), MaxAge: -1,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Auth, share and friend payloads are identity-specific and can change as a
	// side effect of another connection. Never let a browser or reverse proxy
	// satisfy a roster invalidation refresh with an older successful GET.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Add("Vary", "Cookie")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
