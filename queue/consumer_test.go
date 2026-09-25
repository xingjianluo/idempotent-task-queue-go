package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConsumerGroupConcurrent 多个消费者并发处理一批任务：
// 每个任务恰好完成一次，无重复确认丢失。
func TestConsumerGroupConcurrent(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 10*time.Millisecond)

	const n = 50
	for i := 0; i < n; i++ {
		if _, err := q.Submit(idFor(i), []byte("p")); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var completed int64
	handler := func(ctx context.Context, task *Task) error {
		mu.Lock()
		seen[task.ID]++
		mu.Unlock()
		atomic.AddInt64(&completed, 1)
		return nil
	}
	g := NewConsumerGroup(q, "grp", 4, handler)
	g.Start()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&completed) >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	shctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Shutdown(shctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if atomic.LoadInt64(&completed) != n {
		t.Fatalf("expected %d completions, got %d", n, completed)
	}
	for i := 0; i < n; i++ {
		id := idFor(i)
		task, ok := q.Task(id)
		if !ok || task.Status != StatusCompleted {
			t.Fatalf("task %s not completed: ok=%v", id, ok)
		}
		if seen[id] != 1 {
			t.Fatalf("task %s handled %d times, want exactly 1", id, seen[id])
		}
	}
	t.Logf("verdict: %d tasks each handled exactly once by 4 consumers, all completed", n)
}

// TestConsumerGroupGracefulShutdownInFlight 停机时有在途任务：
// 给足时间让在途任务正常完成并确认（确定语义：完成）。
func TestConsumerGroupGracefulShutdownInFlight(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, 50*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	_, _ = q.Submit("slow", []byte("p"))

	var started int64
	handler := func(ctx context.Context, task *Task) error {
		atomic.AddInt64(&started, 1)
		time.Sleep(80 * time.Millisecond) // 比测试墙钟快，fake clock 不影响 sleep
		return nil
	}
	g := NewConsumerGroup(q, "grp", 1, handler)
	g.Start()
	time.Sleep(30 * time.Millisecond) // 确保已开始处理

	shctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Shutdown(shctx); err != nil {
		t.Fatalf("graceful shutdown should wait, got %v", err)
	}
	task, _ := q.Task("slow")
	if task.Status != StatusCompleted {
		t.Fatalf("in-flight task should complete during graceful shutdown, got %s", task.Status)
	}
	t.Logf("task=slow attempts=%d verdict=completed basis=finished_within_shutdown_grace", task.Attempts)
}

// TestShutdownTimeoutRequeue 停机超时、在途任务来不及完成：
// 任务以 in_flight 状态落盘；进程退出后重启，租约过期重新投递，最终完成且不丢任务。
func TestShutdownTimeoutRequeue(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: 100 * time.Millisecond,
		MaxAttempts:       3,
		Backoff:           ExponentialBackoff{Base: 5 * time.Millisecond, Multiplier: 2, Max: 50 * time.Millisecond},
		Now:               clk.now,
	}
	q, path := newFileTestQueue(t, cfg)
	_, _ = q.Submit("hung", []byte("p"))

	// 模拟消费者取走任务后处理器卡死：Fetch 后不 Ack/Nack。
	d1, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d1.Task.Attempts != 1 || d1.Task.Status != StatusInFlight {
		t.Fatalf("expected in_flight attempt 1, got %+v", d1.Task)
	}
	t.Logf("task=hung attempts=1 token=1 verdict=left_in_flight basis=shutdown_grace_exceeded")
	if err := q.Close(); err != nil { // 模拟进程退出
		t.Fatal(err)
	}

	// 重启恢复：租约过期 -> 回收重投 -> 新处理器完成
	clk.advance(200 * time.Millisecond)
	q2 := New(cfg, NewFileStore(path))
	if err := q2.Open(); err != nil {
		t.Fatal(err)
	}
	if got := q2.ReapExpired(); got != 1 {
		t.Fatalf("expected 1 reclaimed lease after restart, got %d", got)
	}
	clk.advance(6 * time.Millisecond) // 越过重投退避

	var handled int64
	g2 := NewConsumerGroup(q2, "grp", 1, func(ctx context.Context, task *Task) error {
		atomic.AddInt64(&handled, 1)
		return nil
	})
	g2.Start()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q2.Task("hung"); s != nil && s.Status == StatusCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	defer scancel()
	_ = g2.Shutdown(sctx)

	task, _ := q2.Task("hung")
	if task.Status != StatusCompleted || task.Attempts != 2 {
		t.Fatalf("expected completed with attempts=2 after restart, got status=%s attempts=%d",
			task.Status, task.Attempts)
	}
	if atomic.LoadInt64(&handled) != 1 {
		t.Fatalf("expected exactly 1 post-restart handling, got %d", handled)
	}
	t.Logf("task=hung attempts=2 verdict=completed_after_restart basis=in_flight_lease_reclaimed")
	_ = q2.Close()
}

func idFor(i int) string {
	return "task-" + pad(i)
}

func pad(i int) string {
	if i < 10 {
		return "0" + itoa(i)
	}
	return itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
