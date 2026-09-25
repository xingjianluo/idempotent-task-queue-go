package queue

// Backoff 退避策略：给定第 attempt 次失败（attempt 从 1 起），返回延迟毫秒数。
// 实现必须保证有上限（MaxMillis），避免失败任务长期占用处理资源。
type Backoff interface {
	DelayMillis(attempt int) int64
}

// ExponentialBackoff 有上限的指数退避：
// delay = min(BaseMillis * Multiplier^(attempt-1), MaxMillis)。
type ExponentialBackoff struct {
	BaseMillis int64
	Multiplier int64
	MaxMillis  int64
}

func (b *ExponentialBackoff) DelayMillis(attempt int) int64 {
	if attempt < 1 {
		attempt = 1
	}
	base := b.BaseMillis
	if base < 0 {
		base = 0
	}
	mult := b.Multiplier
	if mult < 1 {
		mult = 1
	}
	max := b.MaxMillis
	if max < 0 {
		max = 0
	}
	d := base
	for i := 1; i < attempt; i++ {
		// 先与上限比较，避免溢出。
		if d > max/mult {
			return max
		}
		d *= mult
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

// DefaultBackoff 返回默认退避策略（100ms 起，翻倍，上限 10s）。
func DefaultBackoff() Backoff {
	return &ExponentialBackoff{BaseMillis: 100, Multiplier: 2, MaxMillis: 10_000}
}
