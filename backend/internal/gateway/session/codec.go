package session

import (
	"bytes"
	"encoding/binary"
	"errors"

	"time"
)

// 紧凑二进制编码(非 protobuf):长度前缀字符串数组 + 定长数值,足以在网关↔Logic 间
// 传递并校验 Permit。字段顺序变更属于不兼容改动,需提升 Permit 结构版本。
func marshalPermit(p *Permit) ([]byte, error) {
	var b bytes.Buffer
	writeStr(&b, p.SessionID)
	writeStr(&b, p.GatewayID)
	writeStr(&b, p.GatewayBootID)
	writeStr(&b, p.ConnectionID)
	writeStr(&b, p.RequestID)
	writeStr(&b, string(p.Action))
	writeStr(&b, p.OperationID)
	var hdr [7]uint64
	hdr[0] = uint64(p.PlayerID)
	hdr[1] = p.AuthorityEpoch
	hdr[2] = p.ActiveSessionEpoch
	hdr[3] = p.ConnectionGeneration
	hdr[4] = p.RouteRevision
	hdr[5] = uint64(p.ExpiresAt.UnixNano())
	hdr[6] = uint64(len(p.Signature))
	for _, v := range hdr {
		_ = binary.Write(&b, binary.BigEndian, v)
	}
	b.Write(p.Signature)
	return b.Bytes(), nil
}

func unmarshalPermit(buf []byte) (*Permit, error) {
	r := bytes.NewReader(buf)
	p := &Permit{}
	var err error
	if p.SessionID, err = readStr(r); err != nil {
		return nil, err
	}
	if p.GatewayID, err = readStr(r); err != nil {
		return nil, err
	}
	if p.GatewayBootID, err = readStr(r); err != nil {
		return nil, err
	}
	if p.ConnectionID, err = readStr(r); err != nil {
		return nil, err
	}
	if p.RequestID, err = readStr(r); err != nil {
		return nil, err
	}
	act, err := readStr(r)
	if err != nil {
		return nil, err
	}
	p.Action = Action(act)
	if p.OperationID, err = readStr(r); err != nil {
		return nil, err
	}
	var hdr [7]uint64
	for i := range hdr {
		if err = binary.Read(r, binary.BigEndian, &hdr[i]); err != nil {
			return nil, errors.New("session: permit truncated")
		}
	}
	p.PlayerID = hdr[0]
	p.AuthorityEpoch = hdr[1]
	p.ActiveSessionEpoch = hdr[2]
	p.ConnectionGeneration = hdr[3]
	p.RouteRevision = hdr[4]
	p.ExpiresAt = unixNano(hdr[5])
	sigLen := int(hdr[6])
	if sigLen < 0 || sigLen > 1<<20 {
		return nil, errors.New("session: permit bad signature length")
	}
	sig := make([]byte, sigLen)
	if _, err = r.Read(sig); err != nil {
		return nil, errors.New("session: permit truncated signature")
	}
	p.Signature = sig
	return p, nil
}

func writeStr(b *bytes.Buffer, s string) {
	_ = binary.Write(b, binary.BigEndian, uint32(len(s)))
	b.WriteString(s)
}

func readStr(r *bytes.Reader) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", errors.New("session: permit truncated string")
	}
	buf := make([]byte, n)
	if _, err := r.Read(buf); err != nil {
		return "", errors.New("session: permit truncated string body")
	}
	return string(buf), nil
}

func unixNano(ns uint64) time.Time {
	return time.Unix(0, int64(ns))
}
