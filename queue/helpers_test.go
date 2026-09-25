package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// testLogger 同时输出到测试日志并保留副本，供断言日志内容。
type testLogger struct {
	t   testing.TB
	mu  sync.Mutex
	buf []string
}

func newTestLogger(t testing.TB) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) Printf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	ts := time.Now().Format("15:04:05.000")
	l.mu.Lock()
	l.buf = append(l.buf, line)
	l.mu.Unlock()
	l.t.Logf("[%s] %s", ts, line)
}

func (l *testLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.buf, "\n")
}

func (l *testLogger) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.buf {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// fetchReady 在超时内等待并取走一个投递，避免测试永久阻塞。
func fetchReady(t testing.TB, q *Queue, timeout time.Duration) *Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	d, err := q.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	return d
}

// assertFetchNone 断言超时内没有可投递任务（退避尚未到期）。
func assertFetchNone(t testing.TB, q *Queue, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if d, err := q.Fetch(ctx); err == nil {
		t.Fatalf("unexpected delivery while none should be ready: id=%s", d.Task.ID)
	}
}

// newTestQueue 构造内存队列：可控时钟、极短轮询下的短租约、指定重试上限。
func newTestQueue(t testing.TB, log Logger, clock Clock, maxAttempts int, leaseTTL time.Duration) *Queue {
	t.Helper()
	q, err := NewQueue(context.Background(), NewMemoryStore(), Config{
		LeaseTTL:    leaseTTL,
		MaxAttempts: maxAttempts,
		Backoff:     &ExponentialBackoff{BaseMillis: 50, Multiplier: 2, MaxMillis: 500},
		Clock:       clock,
		Logger:      log,
	})
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
}

// stateSnapshotCount 返回内部任务总数（白盒测试辅助）。
func (q *Queue) stateSnapshotCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.state.Tasks)
}
