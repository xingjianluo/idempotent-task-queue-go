package queue

import (
	"context"
	"testing"
	"time"
)

// TestRetryWithBackoffThenSuccess 可重试失败按有上限退避延迟重投，退避期间不占用资源。
func TestRetryWithBackoffThenSuccess(t *testing.T) {
	log := newTestLogger(t)
	start := time.Now().UnixMilli()
	clock := NewFakeClock(start)
	q := newTestQueue(t, log, clock, 3, 10*time.Second) // 长租约，避免回收干扰
	ctx := context.Background()

	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-retry", Type: "t"})
	d1 := fetchReady(t, q, time.Second)
	if d1.Task.Delivery != 1 {
		t.Fatalf("delivery=%d, want 1", d1.Task.Delivery)
	}

	// 第一次可重试失败：attempt 1 < maxAttempts 3，退避 50ms。
	if _, err := q.Nack(ctx, d1.Task.ID, d1.Tag, Failure{Kind: FailRetryable, Message: "transient db error", At: clock.NowMillis()}); err != nil {
		t.Fatalf("nack1: %v", err)
	}
	// 退避未到期：不可投递（不长期占用处理资源）。
	assertFetchNone(t, q, 80*time.Millisecond)

	// 推进时钟超过 50ms 退避。
	clock.Advance(60 * time.Millisecond)
	d2 := fetchReady(t, q, time.Second)
	if d2.Task.Delivery != 2 || d2.Task.Attempts != 1 {
		t.Fatalf("redelivery: delivery=%d attempts=%d, want delivery=2 attempts=1", d2.Task.Delivery, d2.Task.Attempts)
	}
	if d2.Tag == d1.Tag {
		t.Fatalf("redelivery must issue a new tag")
	}
	// 第二次成功。
	if _, err := q.Ack(ctx, d2.Task.ID, d2.Tag); err != nil {
		t.Fatalf("ack after retry: %v", err)
	}
	got, _ := q.Get("job-retry")
	if got.Status != StatusSucceeded {
		t.Fatalf("final status=%s, want succeeded", got.Status)
	}
	if !log.contains("basis=retry_backoff") {
		t.Fatalf("log must explain retry backoff\n%s", log.String())
	}
}

// TestNonRetryableGoesDirectlyDead 不可重试失败直接进死信，原因可区分。
func TestNonRetryableGoesDirectlyDead(t *testing.T) {
	log := newTestLogger(t)
	clock := NewFakeClock(time.Now().UnixMilli())
	q := newTestQueue(t, log, clock, 5, 10*time.Second)
	ctx := context.Background()

	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-fatal", Type: "t"})
	d := fetchReady(t, q, time.Second)
	task, err := q.Nack(ctx, d.Task.ID, d.Tag, Failure{Kind: FailNonRetryable, Message: "invalid payload", At: clock.NowMillis()})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if task.Status != StatusDead || task.DeadReason != DeadNonRetryable || task.Attempts != 1 {
		t.Fatalf("task=%+v, want dead/non_retryable/attempts=1", task)
	}
	assertFetchNone(t, q, 80*time.Millisecond) // 死信不再投递
	dls := q.DeadLetters()
	if len(dls) != 1 || dls[0].DeadReason != DeadNonRetryable {
		t.Fatalf("dead letters=%+v", dls)
	}
	if !log.contains("basis=dead_non_retryable") {
		t.Fatalf("log must explain dead_non_retryable\n%s", log.String())
	}
}

// TestRetryExhaustedGoesDead 可重试失败累计达到上限进死信，与不可重试原因区分。
func TestRetryExhaustedGoesDead(t *testing.T) {
	log := newTestLogger(t)
	clock := NewFakeClock(time.Now().UnixMilli())
	q := newTestQueue(t, log, clock, 2, 10*time.Second)
	ctx := context.Background()

	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-exhaust", Type: "t"})
	d1 := fetchReady(t, q, time.Second)
	// attempt 1：退避重投。
	t1, err := q.Nack(ctx, d1.Task.ID, d1.Tag, Failure{Kind: FailRetryable, Message: "e1", At: clock.NowMillis()})
	if err != nil || t1.Status != StatusPending {
		t.Fatalf("nack1 status=%s err=%v", t1.Status, err)
	}
	clock.Advance(600 * time.Millisecond) // 超过退避上限 500ms
	d2 := fetchReady(t, q, time.Second)
	// attempt 2：达到上限，死信。
	t2, err := q.Nack(ctx, d2.Task.ID, d2.Tag, Failure{Kind: FailRetryable, Message: "e2", At: clock.NowMillis()})
	if err != nil || t2.Status != StatusDead || t2.DeadReason != DeadRetryExhausted {
		t.Fatalf("nack2 task=%+v err=%v", t2, err)
	}
	if len(t2.Failures) != 2 {
		t.Fatalf("failures recorded=%d, want 2", len(t2.Failures))
	}
	if !log.contains("basis=dead_retry_exhausted") {
		t.Fatalf("log must explain dead_retry_exhausted\n%s", log.String())
	}
	// 两类死信原因必须同时可区分地出现在死信列表中。
	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-fatal2", Type: "t"})
	d3 := fetchReady(t, q, time.Second)
	_, _ = q.Nack(ctx, d3.Task.ID, d3.Tag, Failure{Kind: FailNonRetryable, Message: "bad", At: clock.NowMillis()})
	reasons := map[DeadReason]int{}
	for _, dl := range q.DeadLetters() {
		reasons[dl.DeadReason]++
	}
	if reasons[DeadRetryExhausted] != 1 || reasons[DeadNonRetryable] != 1 {
		t.Fatalf("dead reasons=%v, want one of each kind", reasons)
	}
}
