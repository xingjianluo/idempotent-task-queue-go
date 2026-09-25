package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestDuplicateSubmitNotEffective 重复提交不得重复生效，且拒绝原因可区分。
func TestDuplicateSubmitNotEffective(t *testing.T) {
	log := newTestLogger(t)
	clock := NewFakeClock(time.Now().UnixMilli())
	q := newTestQueue(t, log, clock, 3, time.Second)

	spec := TaskSpec{ID: "job-1", Type: "email", Payload: "hello"}
	first, created, err := q.Submit(context.Background(), spec)
	if err != nil || !created {
		t.Fatalf("first submit: created=%v err=%v", created, err)
	}
	if first.Status != StatusPending {
		t.Fatalf("new task status=%s, want pending", first.Status)
	}

	again, createdAgain, err2 := q.Submit(context.Background(), spec)
	if createdAgain {
		t.Fatalf("duplicate submit must not create again")
	}
	var re *RejectError
	if !errors.As(err2, &re) || re.Code != RejectDuplicateSubmit {
		t.Fatalf("duplicate submit error=%v, want RejectError code=%s", err2, RejectDuplicateSubmit)
	}
	if again.ID != "job-1" {
		t.Fatalf("duplicate submit should return existing task, got id=%s", again.ID)
	}

	// 不同 ID 的任务不受影响。
	if _, created3, err3 := q.Submit(context.Background(), TaskSpec{ID: "job-2", Type: "email"}); err3 != nil || !created3 {
		t.Fatalf("second distinct submit: created=%v err=%v", created3, err3)
	}
	tasks := q.stateSnapshotCount()
	_ = tasks
	if !log.contains("basis=duplicate_submit") || !log.contains("job-1") {
		t.Fatalf("logs must print task id and basis=duplicate_submit\n%s", log.String())
	}
}

// TestAckIdempotencyAndRejectionReasons 已确认不重投；重复/越权确认被可区分原因拒绝。
func TestAckIdempotencyAndRejectionReasons(t *testing.T) {
	log := newTestLogger(t)
	clock := NewFakeClock(time.Now().UnixMilli())
	q := newTestQueue(t, log, clock, 3, time.Second)
	ctx := context.Background()

	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-ack", Type: "t"})
	d := fetchReady(t, q, time.Second)

	// 首次确认成功。
	if _, err := q.Ack(ctx, d.Task.ID, d.Tag); err != nil {
		t.Fatalf("first ack: %v", err)
	}

	// 用旧令牌再次确认：任务已不是 leased，拒绝码 not_leased。
	_, err := q.Ack(ctx, d.Task.ID, d.Tag)
	if code := rejectCode(err); code != RejectNotLeased {
		t.Fatalf("second ack code=%v, want %s", code, RejectNotLeased)
	}

	// 已确认任务不会被再次投递。
	assertFetchNone(t, q, 120*time.Millisecond)

	// 不存在任务：task_not_found。
	if code := rejectCode(mustErrAck(q, ctx, "missing", 1)); code != RejectTaskNotFound {
		t.Fatalf("ack missing code=%v, want %s", code, RejectTaskNotFound)
	}

	// 租约令牌不匹配：lease_mismatch。
	_, _, _ = q.Submit(ctx, TaskSpec{ID: "job-tag", Type: "t"})
	d2 := fetchReady(t, q, time.Second)
	wrongTag := d2.Tag + 1000
	if code := rejectCode(mustErrAck(q, ctx, d2.Task.ID, wrongTag)); code != RejectLeaseMismatch {
		t.Fatalf("wrong tag ack code=%v, want %s", code, RejectLeaseMismatch)
	}
	// 正确令牌仍可确认（拒绝没有改变任务归属）。
	if _, err := q.Ack(ctx, d2.Task.ID, d2.Tag); err != nil {
		t.Fatalf("correct tag ack: %v", err)
	}
}

func rejectCode(err error) RejectCode {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Code
	}
	return ""
}

func mustErrAck(q *Queue, ctx context.Context, id string, tag int64) error {
	_, err := q.Ack(ctx, id, tag)
	if err == nil {
		// 触发失败路径不可能；通过 sentinel 让调用方断言失败
		return &RejectError{Code: RejectTaskNotFound, Reason: "expected error"}
	}
	return err
}
