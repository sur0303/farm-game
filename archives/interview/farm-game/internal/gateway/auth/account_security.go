package auth

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	"farmgame/internal/gateway/account"
)

type changePasswordRequest struct {
	Username        string `json:"username"`
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
	BootstrapID     string `json:"bootstrap_id,omitempty"`
	DeviceID        string `json:"device_id,omitempty"`
}

type adminDisableRequest struct {
	Username string `json:"username"`
}

// ChangePasswordHandler replaces a verified password and revokes every session.
type ChangePasswordHandler struct {
	accounts     *account.Service
	sessions     *Service
	cookieSecure bool
	limiter      account.AttemptLimiter
	clientIP     func(*http.Request) string
	metrics      *account.AttemptMetrics
}

func NewChangePasswordHandler(accounts *account.Service, sessions *Service, cookieSecure ...bool) *ChangePasswordHandler {
	secure := len(cookieSecure) > 0 && cookieSecure[0]
	return &ChangePasswordHandler{accounts: accounts, sessions: sessions, cookieSecure: secure}
}

func (h *ChangePasswordHandler) SetAttemptLimiter(limiter account.AttemptLimiter, clientIP func(*http.Request) string) {
	h.limiter = limiter
	h.clientIP = clientIP
}

func (h *ChangePasswordHandler) SetAttemptMetrics(metrics *account.AttemptMetrics) {
	h.metrics = metrics
}

func (h *ChangePasswordHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	if h.accounts == nil || h.sessions == nil {
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "account service unavailable"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAccountRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request changePasswordRequest
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "bad request body"})
		return
	}
	request.Username = strings.TrimSpace(request.Username)
	request.BootstrapID = strings.TrimSpace(request.BootstrapID)
	request.DeviceID = strings.TrimSpace(request.DeviceID)
	deviceID := request.DeviceID
	if deviceID == "" {
		deviceID = request.BootstrapID
	}
	if h.limiter != nil {
		clientIP := ""
		if h.clientIP != nil {
			clientIP = h.clientIP(r)
		}
		decision, err := h.limiter.Allow(r.Context(), account.LoginAttempt{
			Username: request.Username, ClientIP: clientIP, DeviceID: deviceID,
		})
		h.metrics.Observe("change_password", decision, err)
		if err != nil {
			log.Printf("change-password attempt limiter unavailable err=%v", err)
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "account service unavailable"})
			return
		}
		if !decision.Allowed {
			retrySeconds := int(math.Ceil(decision.RetryAfter.Seconds()))
			if retrySeconds < 1 {
				retrySeconds = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			writeJSON(w, http.StatusTooManyRequests, httpError{
				Error: "too many attempts", ErrorCode: "ERR_RATE_LIMITED",
			})
			return
		}
	}
	playerID, err := h.accounts.ChangePassword(r.Context(), request.Username, request.CurrentPassword, request.NewPassword)
	if err != nil {
		writeChangePasswordError(w, err)
		return
	}
	if h.limiter != nil {
		if resetErr := h.limiter.ResetAccount(r.Context(), request.Username); resetErr != nil {
			log.Printf("change-password attempt limiter reset failed err=%v", resetErr)
		}
	}
	if err := h.sessions.RevokePlayerAuthority(r.Context(), playerID); err != nil {
		log.Printf("change-password revoke failed player_id=%d err=%v", playerID, err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "failed to revoke sessions"})
		return
	}
	clearHTTPSessionCookie(w, h.cookieSecure)
	writeJSON(w, http.StatusOK, map[string]string{"status": "password_changed"})
}

func writeChangePasswordError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, account.ErrInvalidPassword), errors.Is(err, account.ErrSamePassword):
		writeJSON(w, http.StatusBadRequest, httpError{Error: err.Error(), ErrorCode: "ERR_INVALID_ACCOUNT_INPUT"})
	case errors.Is(err, account.ErrRateLimited):
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, httpError{Error: "too many failed attempts", ErrorCode: "ERR_RATE_LIMITED"})
	case errors.Is(err, account.ErrAccountConflict):
		writeJSON(w, http.StatusConflict, httpError{Error: "credential changed concurrently", ErrorCode: "ERR_ACCOUNT_CONFLICT"})
	case errors.Is(err, account.ErrInvalidCredential), errors.Is(err, account.ErrAccountNotFound),
		errors.Is(err, account.ErrInvalidUsername):
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "invalid username or password", ErrorCode: "ERR_UNAUTHENTICATED"})
	default:
		log.Printf("change-password failed err=%v", err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "account service unavailable"})
	}
}

// AdminDisableHandler bans a username and unconditionally revokes its sessions.
// It is authorized by a shared admin token, never by a player Cookie.
type AdminDisableHandler struct {
	accounts   *account.Service
	sessions   *Service
	adminToken string
}

func NewAdminDisableHandler(accounts *account.Service, sessions *Service, adminToken string) *AdminDisableHandler {
	return &AdminDisableHandler{
		accounts: accounts, sessions: sessions, adminToken: strings.TrimSpace(adminToken),
	}
}

func (h *AdminDisableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	if h.accounts == nil || h.sessions == nil || h.adminToken == "" {
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "admin disable unavailable"})
		return
	}
	if !adminTokenAuthorized(r, h.adminToken) {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthorized", ErrorCode: "ERR_UNAUTHENTICATED"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAccountRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request adminDisableRequest
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "bad request body"})
		return
	}
	playerID, err := h.accounts.Disable(r.Context(), request.Username)
	if err != nil {
		switch {
		case errors.Is(err, account.ErrInvalidUsername):
			writeJSON(w, http.StatusBadRequest, httpError{Error: err.Error(), ErrorCode: "ERR_INVALID_ACCOUNT_INPUT"})
		case errors.Is(err, account.ErrAccountNotFound):
			writeJSON(w, http.StatusNotFound, httpError{Error: "account not found", ErrorCode: "ERR_ACCOUNT_NOT_FOUND"})
		default:
			log.Printf("admin disable failed err=%v", err)
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "account service unavailable"})
		}
		return
	}
	if err := h.sessions.RevokePlayerAuthority(r.Context(), playerID); err != nil {
		log.Printf("admin disable revoke failed player_id=%d err=%v", playerID, err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "failed to revoke sessions"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "disabled", "player_id": playerID})
}

func adminTokenAuthorized(r *http.Request, expected string) bool {
	provided := strings.TrimSpace(r.Header.Get("X-Farm-Admin-Token"))
	if provided == "" {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
			provided = strings.TrimSpace(authHeader[7:])
		}
	}
	if provided == "" || len(provided) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}
