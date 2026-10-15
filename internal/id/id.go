// Package id 生成跨进程、高并发下仍唯一的时间序标识。
//
// 历史实现用 time.Now().UnixNano() 拼前缀：同一纳秒内多 goroutine / 多实例
// 会撞主键，且插入失败没有重试。UUIDv7 在毫秒时间戳后带随机位与单调计数器，
// 适合作为 Run / 事件 / DLQ 等 VARCHAR(64) 主键。
package id

import "github.com/google/uuid"

// New 返回 "{prefix}-{uuidv7}"。prefix 为空时只返回 uuidv7 本身。
// 常用前缀（run / event / dlq / worker 等）总长不超过 50，落在 VARCHAR(64) 内；
// 不要把 node_id 等可变长字段再拼进 ID，以免撑爆主键列。
func New(prefix string) string {
	u := UUID()
	if prefix == "" {
		return u
	}
	return prefix + "-" + u
}

// UUID 返回标准 36 字符 UUIDv7。CSPRNG 失败时回退到 UUIDv4，仍保证唯一性。
func UUID() string {
	u, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return u.String()
}
