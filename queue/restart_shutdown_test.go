package queue

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownInFlightRequeued 停机时未完成任务被重新入队；
// 重启后可继续消费；停机期间已完成确认不丢、旧令牌确认被拒。
func TestShutdownInFlightRequeued(t *testing.T) {
	log := newTestLogger(t)
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)

	q1, err := NewQueue(context.Background(), store, Config{
		LeaseTTL: time.Hour, MaxAttempts: 3, Backoff: DefaultBackoff(), Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _, _ = q1.Submit(ctx, TaskSpec{ID: "job-done", Type: "t"})
	_, _, _ = q1.Submit(ctx, TaskSpec{ID: "job-flying", Type: "t"})
	_, _, _ = q1.Submit(ctx, TaskSpec{ID: "job-waiting", Type: "t"})

	dDone := fetchReady(t, q1, time.Second)
	if _, err := q1.Ack(ctx, dDone.Task.ID, dDone.Tag); err != nil {
		t.Fatalf("ack done: %v", err)
	}
	dFlying := fetchReady(t, q1, time.Second) // 取走后停机：未完成
	// job-waiting 保持 pending。

	if err := q1.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 停机后旧令牌确认必须被拒绝（queue_closed）。
	if _, err := q1.Ack(ctx, dFlying.Task.ID, dFlying.Tag); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("ack after close err=%v, want queue_closed", err)
	}

	// 重启：从同一文件恢复。
	log2 := newTestLogger(t)
	q2, err := NewQueue(context.Background(), NewFileStore(path), Config{
		LeaseTTL: time.Hour, MaxAttempts: 3, Backoff: DefaultBackoff(), Logger: log2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close(context.Background()) }()

	done, _ := q2.Get("job-done")
	if done.Status != StatusSucceeded {
		t.Fatalf("restart: acked task status=%s, want succeeded (ack not lost)", done.Status)
	}
	flying, _ := q2.Get("job-flying")
	// 停机时已确定性地回收并持久化为 pending；重启后保持 pending，不再重复回收。
	if flying.Status != StatusPending || flying.Interruptions != 1 {
		t.Fatalf("restart: flying task=%+v, want pending with interruption count 1", flying)
	}
	// 旧令牌在重启后也无法确认。
	if code := rejectCode(mustErrAck(q2, ctx, "job-flying", dFlying.Tag)); code != RejectNotLeased {
		t.Fatalf("stale tag after restart code=%v, want not_leased", code)
	}
	// 两个 pending 任务（flying + waiting）都可被投递并成功。
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		d := fetchReady(t, q2, time.Second)
		seen[d.Task.ID] = true
		if _, err := q2.Ack(ctx, d.Task.ID, d.Tag); err != nil {
			t.Fatalf("ack %s: %v", d.Task.ID, err)
		}
	}
	if !seen["job-flying"] || !seen["job-waiting"] {
		t.Fatalf("restart delivered=%v", seen)
	}
	// 说明：优雅停机已把 job-flying 确定性回收并落盘，重启时无需再次回收；
	// 崩溃遗留的 leased 任务则会打印 basis=restart_recovery（见 TestCrashRecoveryWithFileStore）。
	_ = log2
}

// TestDeadReasonsPersistAcrossRestart 两类死信原因在重启后仍一致且可区分。
func TestDeadReasonsPersistAcrossRestart(t *testing.T) {
	log := newTestLogger(t)
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)
	ctx := context.Background()

	func() {
		q, _ := NewQueue(ctx, store, Config{LeaseTTL: time.Hour, MaxAttempts: 2,
			Backoff: &ExponentialBackoff{BaseMillis: 1, MaxMillis: 1}, Logger: log})
		_, _, _ = q.Submit(ctx, TaskSpec{ID: "fatal", Type: "t"})
		d1 := fetchReady(t, q, time.Second)
		_, _ = q.Nack(ctx, d1.Task.ID, d1.Tag, Failure{Kind: FailNonRetryable, Message: "x"})

		_, _, _ = q.Submit(ctx, TaskSpec{ID: "exhaust", Type: "t"})
		e1 := fetchReady(t, q, time.Second)
		_, _ = q.Nack(ctx, e1.Task.ID, e1.Tag, Failure{Kind: FailRetryable, Message: "x"})
		e2 := fetchReady(t, q, time.Second)
		_, _ = q.Nack(ctx, e2.Task.ID, e2.Tag, Failure{Kind: FailRetryable, Message: "x"})
		_ = q.Close(ctx)
	}()

	// 重启后死信原因不变。
	q2, err := NewQueue(ctx, NewFileStore(path), Config{MaxAttempts: 2,
		Backoff: &ExponentialBackoff{BaseMillis: 1, MaxMillis: 1}, Logger: newTestLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close(ctx) }()
	byReason := map[DeadReason]string{}
	for _, dl := range q2.DeadLetters() {
		byReason[dl.DeadReason] = dl.ID
		if dl.Status != StatusDead {
			t.Fatalf("restarted dead letter status=%s", dl.Status)
		}
	}
	if byReason[DeadNonRetryable] != "fatal" || byReason[DeadRetryExhausted] != "exhaust" {
		t.Fatalf("dead reasons after restart=%v", byReason)
	}
	// 死信任务重启后仍不会被投递。
	assertFetchNone(t, q2, 80*time.Millisecond)
}

// TestGracefulShutdownWithConsumer 多 worker 停机：在途任务要么完成确认，要么被回收。
func TestGracefulShutdownWithConsumer(t *testing.T) {
	log := newTestLogger(t)
	path := filepath.Join(t.TempDir(), "state.json")
	q, _ := NewQueue(context.Background(), NewFileStore(path), Config{
		LeaseTTL: time.Hour, MaxAttempts: 3, Backoff: DefaultBackoff(), Logger: log,
	})

	var processed int64
	block := make(chan struct{})
	handler := func(ctx context.Context, d *Delivery) error {
		n := atomic.AddInt64(&processed, 1)
		if n == 1 {
			<-block // 第一个任务一直挂起，模拟在途长任务，触发停机回收
		}
		return nil
	}
	c := NewConsumer(q, handler, ConsumerConfig{
		Name: "g", Concurrency: 2, ReapInterval: 10 * time.Millisecond,
		ShutdownWait: 100 * time.Millisecond, Logger: log,
	})
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()

	_, _, _ = q.Submit(context.Background(), TaskSpec{ID: "slow", Type: "t"})
	// 确保 slow 已被领取后再停机。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tt, ok := q.Get("slow"); ok && tt.Status == StatusLeased {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	// 宽限到期后任务被确定性地回收（block 仍未放行，模拟任务来不及完成）。
	reclaimDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(reclaimDeadline) {
		if tt, ok := q.Get("slow"); ok && tt.Status == StatusPending {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(block) // 放行挂起的处理；其旧令牌确认会被安全拒绝
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not shut down")
	}

	// 重启验证：slow 必须是 pending（被停机回收），可重新消费成功。
	q2, _ := NewQueue(context.Background(), NewFileStore(path), Config{
		LeaseTTL: time.Hour, MaxAttempts: 3, Backoff: DefaultBackoff(), Logger: newTestLogger(t),
	})
	defer func() { _ = q2.Close(context.Background()) }()
	tt, ok := q2.Get("slow")
	if !ok || tt.Status != StatusPending {
		t.Fatalf("after shutdown+restart slow status=%v ok=%v, want pending", tt, ok)
	}
	d := fetchReady(t, q2, time.Second)
	if _, err := q2.Ack(context.Background(), d.Task.ID, d.Tag); err != nil {
		t.Fatalf("recovered ack: %v", err)
	}
}
