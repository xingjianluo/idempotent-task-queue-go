package queue

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentConsumersNoDuplicateAck 多 worker 并发下每个任务只被确认一次、
// 处理并发度生效，且所有任务最终成功。
func TestConcurrentConsumersNoDuplicateAck(t *testing.T) {
	log := newTestLogger(t)
	q, _ := NewQueue(context.Background(), NewMemoryStore(), Config{
		LeaseTTL: time.Hour, MaxAttempts: 3, Backoff: DefaultBackoff(), Logger: log,
	})
	ctx := context.Background()
	const n = 40
	for i := 0; i < n; i++ {
		_, _, err := q.Submit(ctx, TaskSpec{ID: idFor(i), Type: "t"})
		if err != nil {
			t.Fatal(err)
		}
	}

	var acks int64
	var inFlight, maxInFlight int64
	var mu sync.Mutex
	success := map[string]int{}

	handler := func(ctx context.Context, d *Delivery) error {
		cur := atomic.AddInt64(&inFlight, 1)
		for {
			mx := atomic.LoadInt64(&maxInFlight)
			if cur <= mx || atomic.CompareAndSwapInt64(&maxInFlight, mx, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
		mu.Lock()
		success[d.Task.ID]++
		mu.Unlock()
		atomic.AddInt64(&acks, 1)
		return nil
	}
	c := NewConsumer(q, handler, ConsumerConfig{
		Name: "concurrent", Concurrency: 8, ReapInterval: 10 * time.Millisecond,
		ShutdownWait: 5 * time.Second, Logger: log,
	})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allDone := func() bool {
			for i := 0; i < n; i++ {
				tt, ok := q.Get(idFor(i))
				if !ok || tt.Status != StatusSucceeded {
					return false
				}
			}
			return true
		}()
		if allDone {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	for i := 0; i < n; i++ {
		tt, ok := q.Get(idFor(i))
		if !ok || tt.Status != StatusSucceeded {
			t.Fatalf("task %s status=%v, want succeeded", idFor(i), tt)
		}
		if success[idFor(i)] != 1 {
			t.Fatalf("task %s handled %d times", idFor(i), success[idFor(i)])
		}
	}
	if acks != n {
		t.Fatalf("handler invocations=%d, want %d", acks, n)
	}
	if maxInFlight < 2 {
		t.Fatalf("expected concurrent processing, maxInFlight=%d", maxInFlight)
	}
}

// TestCrashRecoveryWithFileStore 模拟进程崩溃（直接丢弃未关闭的队列实例），
// 重启后未确认任务回到 pending，已确认/死信状态保持。
func TestCrashRecoveryWithFileStore(t *testing.T) {
	log := newTestLogger(t)
	path := filepath.Join(t.TempDir(), "crash.json")
	ctx := context.Background()

	q1, _ := NewQueue(ctx, NewFileStore(path), Config{
		LeaseTTL: time.Hour, MaxAttempts: 1, Backoff: &ExponentialBackoff{BaseMillis: 1, MaxMillis: 1}, Logger: log,
	})
	_, _, _ = q1.Submit(ctx, TaskSpec{ID: "acked", Type: "t"})
	_, _, _ = q1.Submit(ctx, TaskSpec{ID: "flying", Type: "t"})
	_, _, _ = q1.Submit(ctx, TaskSpec{ID: "willdie", Type: "t"})
	a := fetchReady(t, q1, time.Second)
	_, _ = q1.Ack(ctx, a.Task.ID, a.Tag)
	_ = fetchReady(t, q1, time.Second) // flying 被取走后"进程崩溃"（不 Close）
	w := fetchReady(t, q1, time.Second)
	_, _ = q1.Nack(ctx, w.Task.ID, w.Tag, Failure{Kind: FailRetryable, Message: "boom"}) // maxAttempts=1 -> dead
	// 不显式 Close，模拟崩溃；最近一次持久化之后的状态依赖已落盘记录。

	log2 := newTestLogger(t)
	q2, _ := NewQueue(ctx, NewFileStore(path), Config{
		LeaseTTL: time.Hour, MaxAttempts: 1,
		Backoff: &ExponentialBackoff{BaseMillis: 1, MaxMillis: 1}, Logger: log2,
	})
	defer func() { _ = q2.Close(ctx) }()

	tt, _ := q2.Get("acked")
	if tt.Status != StatusSucceeded {
		t.Fatalf("acked after crash status=%s", tt.Status)
	}
	fl, _ := q2.Get("flying")
	if fl.Status != StatusPending || fl.PendingReason != "restart_recovery" {
		t.Fatalf("flying after crash status=%s reason=%s, want pending/restart_recovery", fl.Status, fl.PendingReason)
	}
	wd, _ := q2.Get("willdie")
	if wd.Status != StatusDead || wd.DeadReason != DeadRetryExhausted {
		t.Fatalf("willdie after crash status=%s reason=%s", wd.Status, wd.DeadReason)
	}
	// 只有 flying 可投递。
	d := fetchReady(t, q2, time.Second)
	if d.Task.ID != "flying" || d.Task.Delivery != 2 {
		t.Fatalf("recovered delivery id=%s delivery=%d", d.Task.ID, d.Task.Delivery)
	}
}

func idFor(i int) string {
	return "task-" + pad2(i)
}

func pad2(i int) string {
	if i < 10 {
		return "0" + itoa(i)
	}
	return itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
