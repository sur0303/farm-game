package auth

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	"farmgame/internal/gateway/account"
)

const maxAccountRequestBytes = 4 << 10

type AccountHandler struct {
	accounts     *account.Service
	sessions     *Service
	register     bool
	cookieSecure bool
	limiter      account.AttemptLimiter
	clientIP     func(*http.Request) string
	metrics      *account.AttemptMetrics
}

func NewAccountLoginHandler(accounts *account.Service, sessions *Service, cookieSecure ...bool) *AccountHandler {
	return newAccountHandler(accounts, sessions, false, cookieSecure...)
}

func NewAccountRegisterHandler(accounts *account.Service, sessions *Service, cookieSecure ...bool) *AccountHandler {
	return newAccountHandler(accounts, sessions, true, cookieSecure...)
}

func newAccountHandler(accounts *account.Service, sessions *Service, register bool, cookieSecure ...bool) *AccountHandler {
	secure := len(cookieSecure) > 0 && cookieSecure[0]
	return &AccountHandler{accounts: accounts, sessions: sessions, register: register, cookieSecure: secure}
}

type accountRequest struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	BootstrapID string `json:"bootstrap_id"`
	// DeviceID is a rate-limit risk signal only. It is client controlled and
	// never participates in authentication, player identity or owner routing.
	DeviceID    string `json:"device_id,omitempty"`
	ClientBuild string `json:"client_build"`
}

// SetAttemptLimiter configures the shared account/IP/device admission boundary.
// It must be called during process construction, before the handler is served.
func (h *AccountHandler) SetAttemptLimiter(limiter account.AttemptLimiter, clientIP func(*http.Request) string) {
	h.limiter = limiter
	h.clientIP = clientIP
}

func (h *AccountHandler) SetAttemptMetrics(metrics *account.AttemptMetrics) {
	h.metrics = metrics
}

func (h *AccountHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	if h.accounts == nil || h.sessions == nil {
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "account login unavailable"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAccountRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request accountRequest
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "bad request body"})
		return
	}
	request.Username = strings.TrimSpace(request.Username)
	request.BootstrapID = strings.TrimSpace(request.BootstrapID)
	request.DeviceID = strings.TrimSpace(request.DeviceID)
	request.ClientBuild = strings.TrimSpace(request.ClientBuild)
	normalizedUsername, normalizeErr := account.NormalizeUsername(request.Username)
	if normalizeErr != nil {
		h.writeAccountError(w, normalizeErr, h.register)
		return
	}
	request.Username = normalizedUsername
	deviceID := request.DeviceID
	if deviceID == "" {
		// Existing H5 clients already persist bootstrap_id per browser. Reuse
		// it as a backward-compatible risk signal, never as trusted identity.
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
		operation := "login"
		if h.register {
			operation = "register"
		}
		h.metrics.Observe(operation, decision, err)
		if err != nil {
			log.Printf("account attempt limiter unavailable register=%v err=%v", h.register, err)
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

	var playerID uint64
	if h.register {
		credential, err := h.accounts.Register(r.Context(), request.Username, request.Password)
		if err != nil {
			h.writeAccountError(w, err, true)
			return
		}
		playerID = credential.PlayerID
	} else {
		var err error
		playerID, err = h.accounts.Authenticate(r.Context(), request.Username, request.Password)
		if err != nil {
			h.writeAccountError(w, err, false)
			return
		}
	}
	if h.limiter != nil {
		if err := h.limiter.ResetAccount(r.Context(), request.Username); err != nil {
			// Authentication already succeeded. A failed cleanup must not turn
			// correct credentials into a false negative; the remaining bucket
			// is conservative and will expire/refill automatically.
			log.Printf("account attempt limiter reset failed register=%v err=%v", h.register, err)
		}
	}

	displayName, normalizeErr := account.NormalizeUsername(request.Username)
	if normalizeErr != nil {
		// Register/Authenticate already accepted this identity, so a failure here
		// is an internal invariant breach rather than a client-visible success.
		log.Printf("account profile normalization failed player_id=%d err=%v", playerID, normalizeErr)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "failed to establish game session"})
		return
	}
	result, err := h.sessions.LoginPlayerWithProfile(r.Context(), playerID, request.BootstrapID, request.ClientBuild, displayName, 1)
	if err != nil {
		log.Printf("account session failed player_id=%d register=%v err=%v", playerID, h.register, err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "failed to establish game session"})
		return
	}
	if err := setHTTPSessionCookie(w, h.sessions, result, h.cookieSecure); err != nil {
		log.Printf("account http session failed player_id=%d err=%v", playerID, err)
		writeJSON(w, http.StatusInternalServerError, httpError{Error: "failed to establish session"})
		return
	}
	status := http.StatusOK
	if h.register {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (h *AccountHandler) writeAccountError(w http.ResponseWriter, err error, registering bool) {
	switch {
	case errors.Is(err, account.ErrAccountExists):
		writeJSON(w, http.StatusConflict, httpError{Error: "username already exists", ErrorCode: "ERR_ACCOUNT_EXISTS"})
	case errors.Is(err, account.ErrInvalidUsername), errors.Is(err, account.ErrInvalidPassword):
		writeJSON(w, http.StatusBadRequest, httpError{Error: err.Error(), ErrorCode: "ERR_INVALID_ACCOUNT_INPUT"})
	case errors.Is(err, account.ErrRateLimited):
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, httpError{Error: "too many failed attempts", ErrorCode: "ERR_RATE_LIMITED"})
	case errors.Is(err, account.ErrInvalidCredential), errors.Is(err, account.ErrAccountNotFound):
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "invalid username or password", ErrorCode: "ERR_UNAUTHENTICATED"})
	default:
		log.Printf("account credential store failed register=%v err=%v", registering, err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "account service unavailable"})
	}
}
