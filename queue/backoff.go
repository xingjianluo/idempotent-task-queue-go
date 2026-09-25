package queue

import "time"

// Backoff 根据当前投递次数计算下一次重试前的等待时间。
// attempt 为已发生的投递次数（从 1 开始）。
type Backoff interface {
	Next(attempt int) time.Duration
}

// ExponentialBackoff 有上限的指数退避：
// 第 attempt 次失败后等待 min(Base * Multiplier^(attempt-1), Max)。
// 上限保证退避不会无限增长而长期占用/饿死处理资源。
type ExponentialBackoff struct {
	Base       time.Duration
	Multiplier float64
	Max        time.Duration
}

// Next 返回第 attempt 次投递失败后的退避时长。
func (b ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * b.Multiplier)
		if d >= b.Max || d <= 0 {
			return b.Max
		}
	}
	if d > b.Max {
		return b.Max
	}
	return d
}
