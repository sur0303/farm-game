package ownerstore

import "time"

// nowMs 返回当前 Unix 毫秒,用于记录时间戳字段。
func nowMs() int64 {
	return time.Now().UnixMilli()
}
