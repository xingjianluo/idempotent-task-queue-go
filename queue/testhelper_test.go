package queue

import (
	"path/filepath"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)} }
func (c *fakeClock) now() time.Time {
	return c.t
}
func (c *fakeClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func newTestQueue(t *testing.T, clock *fakeClock, maxAttempts int, lease, baseBackoff, maxBackoff time.Duration) (*Queue, Config) {
	t.Helper()
	cfg := Config{
		VisibilityTimeout: lease,
		MaxAttempts:       maxAttempts,
		Backoff:           ExponentialBackoff{Base: baseBackoff, Multiplier: 2, Max: maxBackoff},
		Now:               clock.now,
	}
	q := New(cfg, NewMemoryStore())
	if err := q.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q, cfg
}

func newFileTestQueue(t *testing.T, cfg Config) (*Queue, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	q := New(cfg, NewFileStore(path))
	if err := q.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	return q, path
}

// mustReject 断言 err 是携带指定 Reason 的 RejectError。
func mustReject(t *testing.T, err error, want RejectReason, op string) *RejectError {
	t.Helper()
	re, ok := AsReject(err)
	if !ok {
		t.Fatalf("%s: expected RejectError with reason %q, got %v", op, want, err)
	}
	if re.Reason != want {
		t.Fatalf("%s: expected reason %q, got %q", op, want, re.Reason)
	}
	return re
}
