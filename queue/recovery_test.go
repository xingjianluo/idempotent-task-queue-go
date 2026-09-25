package queue

import (
	"context"
	"testing"
	"time"
)

// TestConsumerInterruptionRequeue 模拟消费者拿到任务后中断（不 Ack）：
// 租约到期后 ReapExpired 必须把任务重新投递；已 Ack 的任务不受影响。
func TestConsumerInterruptionRequeue(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, 100*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	_, _ = q.Submit("interrupted", []byte("p"))
	_, _ = q.Submit("done", []byte("p"))

	d1, _ := q.Fetch(context.Background())
	dDone, _ := q.Fetch(context.Background())
	byID := map[string]*Delivery{d1.TaskID(): d1, dDone.TaskID(): dDone}
	d1, dDone = byID["interrupted"], byID["done"]
	if d1 == nil || dDone == nil {
		t.Fatalf("expected interrupted and done deliveries, got %s %s", byID["interrupted"].TaskID(), byID["done"].TaskID())
	}
	t.Logf("task=interrupted attempts=1 token=1 verdict=in_flight (consumer stops without ack)")

	if err := q.Ack("done", dDone.Token); err != nil {
		t.Fatal(err)
	}

	// 租约未到期：不回收
	if n := q.ReapExpired(); n != 0 {
		t.Fatalf("expected 0 requeued before lease expiry, got %d", n)
	}
	// 租约到期：interrupted 被回收，done 不被回收
	clk.advance(101 * time.Millisecond)
	if n := q.ReapExpired(); n != 1 {
		t.Fatalf("expected 1 requeued after lease expiry, got %d", n)
	}
	t.Logf("task=interrupted attempts=1 verdict=requeued basis=lease_expired; task=done verdict=not_requeued basis=acked")

	clk.advance(6 * time.Millisecond) // 越过重投退避
	d2, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d2.TaskID() != "interrupted" || d2.Token != 2 || d2.Task.Attempts != 2 {
		t.Fatalf("expected redelivery of interrupted with token=2 attempts=2, got %+v", d2)
	}
	t.Logf("task=interrupted attempts=2 token=2 verdict=redelivered basis=lease_reclaim")
}

// TestRestartRecoveryInFlight 模拟进程在任务 in_flight 时退出并重启：
// 新队列从文件恢复，租约过期后任务重新投递；已完成/死信的判定与原因保持一致。
func TestRestartRecoveryInFlight(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: 100 * time.Millisecond,
		MaxAttempts:       3,
		Backoff:           ExponentialBackoff{Base: 5 * time.Millisecond, Multiplier: 2, Max: 50 * time.Millisecond},
		Now:               clk.now,
	}
	q, path := newFileTestQueue(t, cfg)

	_, _ = q.Submit("mid", []byte("p"))
	_, _ = q.Submit("ok", []byte("p"))
	_, _ = q.Submit("poison", []byte("p"))

	dMid, _ := q.Fetch(context.Background())
	dOk, _ := q.Fetch(context.Background())
	_ = q.Ack("ok", dOk.Token)
	dPoison, _ := q.Fetch(context.Background())
	_ = q.Nack("poison", dPoison.Token, FailureNonRetryable, "bad payload")
	t.Logf("before restart: task=mid attempts=1 in_flight; task=ok completed; task=poison dead_lettered(non_retryable)")
	_ = dMid
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：新实例、文件存储
	clk.advance(200 * time.Millisecond) // 跨越 mid 的租约
	q2 := New(cfg, NewFileStore(path))
	if err := q2.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	mid, _ := q2.Task("mid")
	ok, _ := q2.Task("ok")
	poison, _ := q2.Task("poison")
	if ok.Status != StatusCompleted {
		t.Fatalf("ok should remain completed, got %s", ok.Status)
	}
	if poison.Status != StatusDeadLettered || poison.DLQReason != DLQReasonNonRetryable {
		t.Fatalf("poison classification must survive restart, got %s/%s", poison.Status, poison.DLQReason)
	}
	t.Logf("after restart: task=mid status=%s; task=ok status=%s; task=poison status=%s dlq_reason=%s",
		mid.Status, ok.Status, poison.Status, poison.DLQReason)

	if n := q2.ReapExpired(); n != 1 {
		t.Fatalf("expected 1 lease reclaim after restart, got %d", n)
	}
	clk.advance(6 * time.Millisecond)
	d, err := q2.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "mid" || d.Task.Attempts != 2 {
		t.Fatalf("expected mid redelivered with attempts=2, got %+v", d)
	}
	t.Logf("task=mid attempts=2 verdict=redelivered_after_restart basis=recovered_in_flight_lease_expired")

	// 最终完成并再次重启，确认终态稳定
	if err := q2.Ack("mid", d.Token); err != nil {
		t.Fatal(err)
	}
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}
	q3 := New(cfg, NewFileStore(path))
	if err := q3.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q3.Close() })
	mid3, _ := q3.Task("mid")
	if mid3.Status != StatusCompleted || mid3.Attempts != 2 {
		t.Fatalf("final state mismatch: %+v", mid3)
	}
	t.Logf("task=mid attempts=2 verdict=completed_survives_restart")
}

// TestRestartDuplicateSubmitIdempotent 重启后重复提交同样被拒绝且原因可区分。
func TestRestartDuplicateSubmitIdempotent(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       2,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 10 * time.Millisecond},
		Now:               clk.now,
	}
	q, path := newFileTestQueue(t, cfg)
	_, _ = q.Submit("t1", []byte("hello"))
	_ = q.Close()

	q2 := New(cfg, NewFileStore(path))
	if err := q2.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	_, err := q2.Submit("t1", []byte("hello"))
	mustReject(t, err, ReasonDuplicateSubmission, "duplicate after restart")
	_, err = q2.Submit("t1", []byte("world"))
	mustReject(t, err, ReasonPayloadConflict, "conflict after restart")
	t.Logf("task=t1 verdict: duplicate_submission and payload_conflict remain distinguishable after restart")
}
