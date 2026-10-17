// Package session 实现网关侧 SessionAuthority 与短时 SessionPermit。
//
// 职责见 docs/design/05-system-skeleton.md §7 与 10-protocol.md:
//   - 登录/顶号时提升 active_session_epoch;
//   - session_authority_epoch 用于整体作废旧会话世代(灾难切换预留);
//   - 为本次 request_id + action + connection/session 生成短时 SessionPermit,
//     强写/可靠流程额外绑定 operation_id;旧 epoch 不能取得新写 Permit;
//   - Logic 校验 Permit(签名/player_id/action/epoch/过期),不接受客户端自报 Permit。
package session

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"
)

// Action 是 Permit 绑定的动作类别,用于最小化权限范围。
type Action string

const (
	ActionJoinFarm   Action = "join_farm"
	ActionOwnerWrite Action = "owner_write" // 强写/可靠流程:需 operation_id
	ActionReadOnly   Action = "read_only"
)

// ErrPermitInvalid 表示 Permit 格式/签名非法。
var ErrPermitInvalid = errors.New("session: permit invalid")

// ErrPermitExpired 表示 Permit 已过期。
var ErrPermitExpired = errors.New("session: permit expired")

// ErrPermitEpochStale 表示 Permit 的 authority/active epoch 已被新登录作废。
var ErrPermitEpochStale = errors.New("session: permit epoch stale")

// Permit 是网关签发的短时可信凭证,经 gRPC 透传给 Logic 校验。
// Permit 绑定完整 SessionRoute fence；任意健康 Logic 实例都能校验，
// 但旧 Gate、旧进程、复用后的 connection_id 或旧 route_revision 都不能重放。
type Permit struct {
	PlayerID             uint64
	SessionID            string
	AuthorityEpoch       uint64
	ActiveSessionEpoch   uint64
	GatewayID            string
	GatewayBootID        string
	ConnectionID         string
	ConnectionGeneration uint64
	RouteRevision        uint64
	RequestID            string
	Action               Action
	OperationID          string // 仅强写/可靠流程绑定,运行态命令为空
	ExpiresAt            time.Time
	Signature            []byte
}

// Sign 用 HMAC-SHA256(secret) 对 Permit 关键字段签名(不含 Signature 自身)。
func (p *Permit) Sign(secret []byte) {
	p.Signature = signPermit(p, secret)
}

// Token 返回可序列化的不透明 token(base64 编码的 protobuf-free 紧凑字节)。
func (p *Permit) Token(secret []byte) (string, error) {
	buf, err := marshalPermit(p)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// ParseToken 解析并校验签名;签名非法返回 ErrPermitInvalid。
func ParseToken(token string, secret []byte) (*Permit, error) {
	buf, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrPermitInvalid
	}
	p, err := unmarshalPermit(buf)
	if err != nil {
		return nil, ErrPermitInvalid
	}
	if !hmac.Equal(p.Signature, signPermit(p, secret)) {
		return nil, ErrPermitInvalid
	}
	return p, nil
}

// Validate 校验签名、过期与 epoch。两个 epoch 都必须与可信上下文精确相等；
// “大于当前值”同样不是合法权限，避免调用方把尚未激活的候选代次当成当前会话。
func (p *Permit) Validate(now time.Time, secret []byte, authorityEpoch, activeEpoch uint64) error {
	if !hmac.Equal(p.Signature, signPermit(p, secret)) {
		return ErrPermitInvalid
	}
	if now.After(p.ExpiresAt) {
		return ErrPermitExpired
	}
	if p.AuthorityEpoch != authorityEpoch || p.ActiveSessionEpoch != activeEpoch {
		return ErrPermitEpochStale
	}
	return nil
}

// NeedsOperationBinding 报告该动作是否必须绑定 operation_id(强写/可靠流程)。
func (a Action) NeedsOperationBinding() bool {
	return a == ActionOwnerWrite
}

func signPermit(p *Permit, secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, p.PlayerID)
	b.WriteString(p.SessionID)
	binary.Write(&b, binary.BigEndian, p.AuthorityEpoch)
	binary.Write(&b, binary.BigEndian, p.ActiveSessionEpoch)
	b.WriteString(p.GatewayID)
	b.WriteString(p.GatewayBootID)
	b.WriteString(p.ConnectionID)
	binary.Write(&b, binary.BigEndian, p.ConnectionGeneration)
	binary.Write(&b, binary.BigEndian, p.RouteRevision)
	b.WriteString(p.RequestID)
	b.WriteString(string(p.Action))
	b.WriteString(p.OperationID)
	binary.Write(&b, binary.BigEndian, p.ExpiresAt.UnixNano())
	mac.Write(b.Bytes())
	return mac.Sum(nil)
}
