package rediskey

import "fmt"

// HashTag applies the Redis Cluster key-tag rule. If no non-empty {...} pair
// exists, the whole key is hashed.
func HashTag(key RedisKey) string {
	raw := string(key)
	for i := 0; i < len(raw); i++ {
		if raw[i] != '{' {
			continue
		}
		for j := i + 1; j < len(raw); j++ {
			if raw[j] == '}' {
				if j > i+1 {
					return raw[i+1 : j]
				}
				break
			}
		}
		break
	}
	return raw
}

// ClusterSlot returns CRC16-XMODEM(hash-tag) mod 16384, matching Redis.
func ClusterSlot(key RedisKey) uint16 {
	return redisCRC16([]byte(HashTag(key))) & 0x3fff
}

// AssertSameSlot is intentionally stricter than comparing numeric slots:
// authority Lua keys must carry byte-identical hash tags, because distinct tags
// can collide onto one physical slot and later move independently by contract.
func AssertSameSlot(keys ...RedisKey) error {
	if len(keys) == 0 {
		return fmt.Errorf("%w: no keys", ErrCrossSlot)
	}
	wantTag := HashTag(keys[0])
	wantSlot := ClusterSlot(keys[0])
	for _, key := range keys[1:] {
		if HashTag(key) != wantTag || ClusterSlot(key) != wantSlot {
			return fmt.Errorf("%w: %q(slot=%d) vs %q(slot=%d)", ErrCrossSlot,
				wantTag, wantSlot, HashTag(key), ClusterSlot(key))
		}
	}
	return nil
}

func redisCRC16(data []byte) uint16 {
	var crc uint16
	for _, value := range data {
		crc ^= uint16(value) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
