package gatewayapp

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	commonpb "farmgame/api/gen/common"

	"google.golang.org/protobuf/proto"
)

type faultFrameHandler struct{ calls int }

func (h *faultFrameHandler) HandleBinary(context.Context, string, uint64, []byte) ([]byte, error) {
	h.calls++
	return []byte("real-result"), nil
}
func (*faultFrameHandler) Disconnect(string, uint64) {}

func TestE2ECommandResultDropperAllowsTaskClaimFault(t *testing.T) {
	dropper := &e2eCommandResultDropper{inner: &faultFrameHandler{}}
	arm := httptest.NewRecorder()
	dropper.ServeHTTP(arm, httptest.NewRequest(http.MethodPost, "/api/v1/e2e/fault/drop-command-result",
		bytes.NewBufferString(`{"command_type":"CLAIM_TASK"}`)))
	if arm.Code != http.StatusOK || !bytes.Contains(arm.Body.Bytes(), []byte(`"command_type":"CLAIM_TASK"`)) {
		t.Fatalf("arm=%d %s", arm.Code, arm.Body.String())
	}
	raw, err := proto.Marshal(&commonpb.FarmFrame{FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{
		RequestId: "r-task", OperationId: "op-task", CommandType: commonpb.CommandType_CLAIM_TASK,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := dropper.HandleBinary(context.Background(), "c", 1, raw); response != nil || !errors.Is(err, errE2ECommandResultDropped) {
		t.Fatalf("claim response=%q err=%v", response, err)
	}
}

func TestE2ECommandResultDropperExecutesAuthorityThenDropsExactlyOnce(t *testing.T) {
	inner := &faultFrameHandler{}
	dropper := &e2eCommandResultDropper{inner: inner}
	arm := httptest.NewRecorder()
	dropper.ServeHTTP(arm, httptest.NewRequest(http.MethodPost, "/api/v1/e2e/fault/drop-command-result",
		bytes.NewBufferString(`{"command_type":"CARE"}`)))
	if arm.Code != http.StatusOK {
		t.Fatalf("arm=%d %s", arm.Code, arm.Body.String())
	}
	raw, err := proto.Marshal(&commonpb.FarmFrame{FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{
		RequestId: "r", OperationId: "op", CommandType: commonpb.CommandType_CARE,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := dropper.HandleBinary(context.Background(), "c", 1, raw); response != nil || !errors.Is(err, errE2ECommandResultDropped) || inner.calls != 1 {
		t.Fatalf("first response=%q calls=%d err=%v", response, inner.calls, err)
	}
	if response, err := dropper.HandleBinary(context.Background(), "c", 1, raw); err != nil || string(response) != "real-result" || inner.calls != 2 {
		t.Fatalf("second response=%q calls=%d err=%v", response, inner.calls, err)
	}
	status := httptest.NewRecorder()
	dropper.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/v1/e2e/fault/drop-command-result", nil))
	if !bytes.Contains(status.Body.Bytes(), []byte(`"fired":true`)) {
		t.Fatalf("status=%s", status.Body.String())
	}
}
