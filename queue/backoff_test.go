package queue

import "testing"

func TestExponentialBackoffCapped(t *testing.T) {
	b := &ExponentialBackoff{BaseMillis: 100, Multiplier: 2, MaxMillis: 1000}
	want := []int64{100, 200, 400, 800, 1000, 1000, 1000}
	for i, w := range want {
		if got := b.DelayMillis(i + 1); got != w {
			t.Fatalf("attempt=%d delay=%d, want %d", i+1, got, w)
		}
	}
	// 上限保证退避不会无限增长而长期占用资源。
	if d := b.DelayMillis(1_000_000); d != 1000 {
		t.Fatalf("huge attempt delay=%d, want cap 1000", d)
	}
}

func TestExponentialBackoffDegenerate(t *testing.T) {
	b := &ExponentialBackoff{BaseMillis: -1, Multiplier: 0, MaxMillis: -5}
	if d := b.DelayMillis(3); d != 0 {
		t.Fatalf("degenerate config delay=%d, want 0", d)
	}
}
