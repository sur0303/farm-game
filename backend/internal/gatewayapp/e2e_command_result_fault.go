package gatewayapp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"

	commonpb "farmgame/api/gen/common"
	wstransport "farmgame/internal/gateway/transport/websocket"

	"google.golang.org/protobuf/proto"
)

var errE2ECommandResultDropped = errors.New("e2e fault: command result dropped after authority execution")

// e2eCommandResultDropper is constructed only when the explicit smoke-only
// environment switch is enabled. It executes the real Gate first, then closes
// the WSS before one selected CommandResult reaches H5. Production has no
// route or configured instance of this wrapper.
type e2eCommandResultDropper struct {
	inner wstransport.FrameHandler
	mu    sync.Mutex
	armed commonpb.CommandType
	fired bool
}

func (d *e2eCommandResultDropper) HandleBinary(ctx context.Context, connectionID string, generation uint64, raw []byte) ([]byte, error) {
	var input commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &input); err != nil {
		return d.inner.HandleBinary(ctx, connectionID, generation, raw)
	}
	commandType := commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED
	if command := input.GetClientCommand(); command != nil {
		commandType = command.CommandType
	}
	response, err := d.inner.HandleBinary(ctx, connectionID, generation, raw)
	if err != nil || commandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
		return response, err
	}
	d.mu.Lock()
	shouldDrop := d.armed == commandType && !d.fired
	if shouldDrop {
		d.fired = true
		d.armed = commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED
	}
	d.mu.Unlock()
	if shouldDrop {
		return nil, errE2ECommandResultDropped
	}
	return response, nil
}

func (d *e2eCommandResultDropper) Disconnect(connectionID string, generation uint64) {
	d.inner.Disconnect(connectionID, generation)
}

func (d *e2eCommandResultDropper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodPost:
		var request struct {
			CommandType string `json:"command_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid fault request", http.StatusBadRequest)
			return
		}
		value, ok := commonpb.CommandType_value[strings.TrimSpace(request.CommandType)]
		commandType := commonpb.CommandType(value)
		if !ok || (commandType != commonpb.CommandType_CARE && commandType != commonpb.CommandType_THEFT && commandType != commonpb.CommandType_CLAIM_TASK) {
			http.Error(w, "command_type must be CARE, THEFT, or CLAIM_TASK", http.StatusBadRequest)
			return
		}
		d.mu.Lock()
		d.armed, d.fired = commandType, false
		d.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "armed", "command_type": commandType.String()})
	case http.MethodGet:
		d.mu.Lock()
		armed, fired := d.armed, d.fired
		d.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"armed": armed.String(), "fired": fired})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
