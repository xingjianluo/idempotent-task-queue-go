package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestLeaseExpiryRedelivery 消费者中断（未确认）后任务必须可重新投递；
// 旧持有者用旧令牌确认必须被 lease_mismatch 拒绝；新持有者可正常确认。
func TestLeaseExpiryRedelivery(t *testing.T) {
	log := newTestLogger(t)
	clock := NewFakeClock(time.Now().UnixMilli())
	lease := 200 * time.Millisecond
	q := newTestQueue(t, log, clock, 3, lease)
	ctx := context.Background()

	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-crash", Type: "t"})
	d1 := fetchReady(t, q, time.Second) // 消费者 1 取走后"崩溃"，从不 Ack/Nack

	// 租约未到期：不重投。
	clock.Advance(100 * time.Millisecond)
	assertFetchNone(t, q, 60*time.Millisecond)

	// 租约到期，reaper 等价调用回收。
	clock.Advance(150 * time.Millisecond)
	ids := q.RequeueExpired(ctx)
	if len(ids) != 1 || ids[0] != "job-crash" {
		t.Fatalf("requeued ids=%v", ids)
	}
	task, _ := q.Get("job-crash")
	if task.Status != StatusPending || task.Interruptions != 1 {
		t.Fatalf("after reclaim status=%s interruptions=%d", task.Status, task.Interruptions)
	}

	// 重新投递：投递次数 +1、新令牌。
	d2 := fetchReady(t, q, time.Second)
	if d2.Task.Delivery != 2 || d2.Tag == d1.Tag {
		t.Fatalf("redelivery delivery=%d tag mismatch old=%d new=%d", d2.Task.Delivery, d1.Tag, d2.Tag)
	}

	// "崩溃"的消费者醒来用旧令牌 Ack：必须拒绝。
	if code := rejectCode(mustErrAck(q, ctx, "job-crash", d1.Tag)); code != RejectLeaseMismatch {
		t.Fatalf("stale ack code=%v, want lease_mismatch", code)
	}
	// 新持有者确认成功，之后不再投递。
	if _, err := q.Ack(ctx, "job-crash", d2.Tag); err != nil {
		t.Fatalf("new owner ack: %v", err)
	}
	assertFetchNone(t, q, 60*time.Millisecond)

	if !log.contains("basis=lease_expired") {
		t.Fatalf("log must explain lease_expired redelivery\n%s", log.String())
	}
}

// TestReaperRunsInConsumer 通过 Consumer 的 reaper 自动完成中断恢复（真实时钟快速验证）。
func TestReaperRunsInConsumer(t *testing.T) {
	log := newTestLogger(t)
	q, err := NewQueue(context.Background(), NewMemoryStore(), Config{
		LeaseTTL:    80 * time.Millisecond,
		MaxAttempts: 3,
		Backoff:     DefaultBackoff(),
		Logger:      log,
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	deliveries := map[int64]int{}
	handler := func(ctx context.Context, d *Delivery) error {
		mu.Lock()
		deliveries[d.Tag]++
		n := len(deliveries)
		mu.Unlock()
		if n == 1 {
			// 第一次处理直接返回但不确认？Handler 返回 nil 会自动 Ack。
			// 因此第一次模拟"进程崩溃"：返回一个特殊错误让其走重投不合适，
			// 这里直接不返回，通过丢弃投递来模拟：利用 goroutine 卡住直到租约过期。
			time.Sleep(400 * time.Millisecond)
		}
		return nil
	}
	c := NewConsumer(q, handler, ConsumerConfig{
		Name: "crash-consumer", Concurrency: 1, ReapInterval: 20 * time.Millisecond, Logger: log,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	_, _, _ = q.Submit(context.Background(), TaskSpec{ID: "job-auto", Type: "t"})
	// 等待任务最终成功（重投后第二次处理 Ack）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if task, ok := q.Get("job-auto"); ok && task.Status == StatusSucceeded {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not stop")
	}
	task, _ := q.Get("job-auto")
	if task.Status != StatusSucceeded || task.Delivery < 2 {
		t.Fatalf("task status=%s delivery=%d, want succeeded with redelivery", task.Status, task.Delivery)
	}
}
