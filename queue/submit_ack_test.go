package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSubmitIdempotency(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 10*time.Millisecond)

	t1, err := q.Submit("t1", []byte("payload-1"))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if t1.Status != StatusPending || t1.Attempts != 0 {
		t.Fatalf("unexpected new task: %+v", t1)
	}
	t.Logf("task=t1 attempts=0 verdict=accepted reason=none")

	// 重复提交，载荷一致 -> duplicate_submission，任务不变化、不重复生效。
	_, err = q.Submit("t1", []byte("payload-1"))
	mustReject(t, err, ReasonDuplicateSubmission, "submit duplicate")
	t.Logf("task=t1 attempts=0 verdict=rejected reason=duplicate_submission")

	// 相同 ID 不同载荷 -> payload_conflict。
	_, err = q.Submit("t1", []byte("different"))
	mustReject(t, err, ReasonPayloadConflict, "submit conflict")
	t.Logf("task=t1 verdict=rejected reason=payload_conflict")

	// 任务仍只有一个。
	if len(q.Snapshot()) != 1 {
		t.Fatalf("expected exactly 1 task, got %d", len(q.Snapshot()))
	}
}

func TestAckRejectReasons(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 10*time.Millisecond)

	if _, err := q.Submit("t1", []byte("p")); err != nil {
		t.Fatal(err)
	}

	// Ack 未知任务
	mustReject(t, q.Ack("nope", 1), ReasonUnknownDelivery, "ack unknown")

	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "t1" || d.Token != 1 {
		t.Fatalf("unexpected delivery: %+v", d)
	}
	t.Logf("task=t1 attempts=1 token=1 verdict=delivered")

	// 旧 token 的确认 -> stale_delivery
	mustReject(t, q.Ack("t1", 99), ReasonStaleDelivery, "ack stale")
	t.Logf("task=t1 attempts=1 verdict=rejected reason=stale_delivery")

	// 正确确认
	if err := q.Ack("t1", d.Token); err != nil {
		t.Fatalf("ack: %v", err)
	}
	t.Logf("task=t1 attempts=1 token=1 verdict=acked")

	// 重复确认 -> already_acked（已确认任务不得被重复生效）
	mustReject(t, q.Ack("t1", d.Token), ReasonAlreadyAcked, "ack twice")
	t.Logf("task=t1 attempts=1 verdict=rejected reason=already_acked")

	// 已确认任务不会被再次投递
	clk.advance(time.Hour)
	if got := q.ReapExpired(); got != 0 {
		t.Fatalf("completed task must not be requeued, got %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := q.Fetch(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no delivery for completed task, got err=%v", err)
	}
}

func TestNackRetryThenAck(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, 20*time.Millisecond, 100*time.Millisecond)
	if _, err := q.Submit("t1", []byte("p")); err != nil {
		t.Fatal(err)
	}

	d1, _ := q.Fetch(context.Background())
	if err := q.Nack("t1", d1.Token, FailureRetryable, "transient"); err != nil {
		t.Fatal(err)
	}
	snap, _ := q.Task("t1")
	if snap.Status != StatusPending || snap.Attempts != 1 {
		t.Fatalf("expected pending after retryable nack, got %+v", snap)
	}
	t.Logf("task=t1 attempts=1 verdict=nacked class=retryable next_visible_in=20ms")

	// 退避未到期：取不到
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := q.Fetch(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected backoff hold, got %v", err)
	}

	clk.advance(21 * time.Millisecond)
	d2, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d2.Token != 2 || d2.Task.Attempts != 2 {
		t.Fatalf("expected redelivery token=2 attempts=2, got %+v", d2)
	}
	t.Logf("task=t1 attempts=2 token=2 verdict=redelivered basis=backoff_elapsed")

	// 用旧 token 再 Nack -> stale_delivery
	mustReject(t, q.Nack("t1", d1.Token, FailureRetryable, "late"), ReasonStaleDelivery, "nack stale")

	if err := q.Ack("t1", d2.Token); err != nil {
		t.Fatal(err)
	}
	if done, _ := q.Task("t1"); done.Status != StatusCompleted {
		t.Fatalf("expected completed, got %s", done.Status)
	}
	t.Logf("task=t1 attempts=2 token=2 verdict=acked")
}
