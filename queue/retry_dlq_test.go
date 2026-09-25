package queue

import (
	"context"
	"testing"
	"time"
)

func TestClassifyFailure(t *testing.T) {
	if ClassifyFailure(nil) != FailureNone {
		t.Fatal("nil -> none")
	}
	if ClassifyFailure(NonRetryable("bad input")) != FailureNonRetryable {
		t.Fatal("NonRetryable -> non_retryable")
	}
	if ClassifyFailure(assertErr("boom")) != FailureRetryable {
		t.Fatal("plain error -> retryable")
	}
	t.Logf("verdict: nil=none, plain_error=retryable, NonRetryable=non_retryable")
}

// assertErr 是一个普通 error，用于与 NonRetryableError 区分。
type assertErr string

func (e assertErr) Error() string { return string(e) }

func TestNonRetryableGoesDirectlyToDLQ(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 5, time.Second, time.Millisecond, 10*time.Millisecond)
	_, _ = q.Submit("t1", []byte("p"))

	d, _ := q.Fetch(context.Background())
	if err := q.Nack("t1", d.Token, FailureNonRetryable, "invalid argument"); err != nil {
		t.Fatal(err)
	}
	task, _ := q.Task("t1")
	if task.Status != StatusDeadLettered || task.DLQReason != DLQReasonNonRetryable {
		t.Fatalf("expected dead_lettered/non_retryable, got %s/%s attempts=%d",
			task.Status, task.DLQReason, task.Attempts)
	}
	t.Logf("task=t1 attempts=%d verdict=dead_lettered dlq_reason=non_retryable basis=classified_failure", task.Attempts)

	if dlqs := q.DeadLetters(); len(dlqs) != 1 || dlqs[0].ID != "t1" {
		t.Fatalf("expected 1 dead letter, got %d", len(dlqs))
	}
	// 死信任务不可再确认、不再投递
	mustReject(t, q.Ack("t1", d.Token), ReasonDeadLettered, "ack dead letter")
	clk.advance(time.Hour)
	if q.ReapExpired() != 0 {
		t.Fatal("dead lettered task must not be requeued")
	}
}

func TestRetryableExhaustionGoesToDLQ(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 5*time.Millisecond)
	_, _ = q.Submit("t1", []byte("p"))

	// 前 3 次投递均以可重试失败结束：第 3 次失败即超限入死信。
	for attempt := 1; attempt <= 3; attempt++ {
		d, err := q.Fetch(context.Background())
		if err != nil {
			t.Fatalf("fetch attempt %d: %v", attempt, err)
		}
		if d.Task.Attempts != attempt {
			t.Fatalf("attempt %d: task attempts=%d", attempt, d.Task.Attempts)
		}
		err = q.Nack("t1", d.Token, FailureRetryable, "flaky")
		if attempt < 3 {
			if err != nil {
				t.Fatalf("nack attempt %d: %v", attempt, err)
			}
			t.Logf("task=t1 attempts=%d verdict=requeue class=retryable basis=attempts_below_max", attempt)
			clk.advance(10 * time.Millisecond) // 越过退避
			continue
		}
		if err != nil {
			t.Fatalf("nack final: %v", err)
		}
	}
	task, _ := q.Task("t1")
	if task.Status != StatusDeadLettered || task.DLQReason != DLQReasonMaxAttempts {
		t.Fatalf("expected dead_lettered/max_attempts_exceeded, got %s/%s", task.Status, task.DLQReason)
	}
	if task.Attempts != 3 {
		t.Fatalf("expected attempts=3, got %d", task.Attempts)
	}
	t.Logf("task=t1 attempts=3 verdict=dead_lettered dlq_reason=max_attempts_exceeded basis=attempts>=max(3)")

	// 两种死信原因必须同时存在时仍可区分
	_, _ = q.Submit("t2", []byte("p"))
	d2, _ := q.Fetch(context.Background())
	_ = q.Nack("t2", d2.Token, FailureNonRetryable, "bad")
	byReason := map[DLQReason]int{}
	for _, dl := range q.DeadLetters() {
		byReason[dl.DLQReason]++
	}
	if byReason[DLQReasonNonRetryable] != 1 || byReason[DLQReasonMaxAttempts] != 1 {
		t.Fatalf("dlq reasons not distinguishable: %+v", byReason)
	}
	t.Logf("verdict: dlq reasons distinguishable non_retryable=%d max_attempts_exceeded=%d",
		byReason[DLQReasonNonRetryable], byReason[DLQReasonMaxAttempts])
}

func TestBackoffCapped(t *testing.T) {
	bo := ExponentialBackoff{Base: 10 * time.Millisecond, Multiplier: 2, Max: 25 * time.Millisecond}
	got := []time.Duration{bo.Next(1), bo.Next(2), bo.Next(3), bo.Next(4)}
	want := []time.Duration{10, 20, 25, 25}
	for i := range got {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("Next(%d)=%s want %s", i+1, got[i], want[i]*time.Millisecond)
		}
	}
	t.Logf("backoff sequence=%v verdict=capped_at_max basis=max_backoff", got)
}
