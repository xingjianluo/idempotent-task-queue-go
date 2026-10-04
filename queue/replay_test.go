package queue

import (
	"context"
	"testing"
	"time"
)

// failToDLQ 投递任务 maxAttempts 次且全部以可重试失败结束，任务进入死信。
// 每轮失败后推进墙钟 1h 越过退避；入死信发生在最后一次 Nack，最后一次推进在入死信之后。
func failToDLQ(t *testing.T, q *Queue, clk *fakeClock, id string, reason string) {
	t.Helper()
	if _, err := q.Submit(id, []byte("p")); err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	task, _ := q.Task(id)
	for attempt := 1; attempt <= task.MaxAttempts; attempt++ {
		d, err := q.Fetch(context.Background())
		if err != nil {
			t.Fatalf("fetch %s attempt %d: %v", id, attempt, err)
		}
		if err := q.Nack(d.TaskID(), d.Token, FailureRetryable, reason); err != nil {
			t.Fatalf("nack %s: %v", id, err)
		}
		clk.advance(time.Hour) // 越过退避
	}
	cur, _ := q.Task(id)
	if cur.Status != StatusDeadLettered || cur.DLQReason != DLQReasonMaxAttempts {
		t.Fatalf("task %s should be dead_lettered(max_attempts), got %s/%s", id, cur.Status, cur.DLQReason)
	}
}

// TestReplaySingleTask 点名单个死信任务重放：回到 pending，走正常投递/Ack；
// 重放次数、重放后投递次数与原始投递历史分开记录。
func TestReplaySingleTask(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 5*time.Millisecond)
	failToDLQ(t, q, clk, "t1", "flaky service")

	res, err := q.Replay(ReplayRequest{BatchID: "batch-1", TaskID: "t1"})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(res.Replayed) != 1 || res.Replayed[0] != "t1" || res.AlreadyApplied {
		t.Fatalf("unexpected replay result: %+v", res)
	}

	task, _ := q.Task("t1")
	if task.Status != StatusPending {
		t.Fatalf("replayed task should be pending, got %s", task.Status)
	}
	if task.ReplayCount != 1 || task.AttemptsAtReplay != 3 || task.ReplayAttempts != 0 || task.Attempts != 3 {
		t.Fatalf("replay counters wrong: %+v", task)
	}
	if len(task.ReplayBatches) != 1 || task.ReplayBatches[0] != "batch-1" {
		t.Fatalf("replay batches wrong: %v", task.ReplayBatches)
	}
	if task.DLQReason != "" {
		t.Fatalf("dlq reason should be cleared, got %s", task.DLQReason)
	}
	if task.VisibleAt.After(clk.now()) {
		t.Fatalf("replayed task should be immediately visible, visible_at=%s now=%s", task.VisibleAt, clk.now())
	}
	t.Logf("task=t1 attempts=%d replays=%d replay_attempts=0 verdict=pending basis=manual_replay",
		task.Attempts, task.ReplayCount)

	// 重放后走正常投递与确认
	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "t1" || d.Task.Attempts != 4 || d.Task.ReplayAttempts != 1 || d.Task.ReplayCount != 1 {
		t.Fatalf("post-replay delivery wrong: %+v", d.Task)
	}
	if err := q.Ack("t1", d.Token); err != nil {
		t.Fatal(err)
	}
	done, _ := q.Task("t1")
	if done.Status != StatusCompleted || done.Attempts != 4 || done.ReplayAttempts != 1 || done.ReplayCount != 1 {
		t.Fatalf("post-replay completion state wrong: %+v", done)
	}
	t.Logf("task=t1 attempts=4 replays=1 replay_attempts=1 verdict=completed basis=normal_ack_after_replay")
}

// TestReplayFilterBatch 按条件批量挑选：失败原因子串、死信原因、失败类别、
// 时间范围、Limit，并按进入死信先后稳定排序。各批次挑选互不重叠的任务集合。
func TestReplayFilterBatch(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)

	// 三个可重试超限死信（进入死信时间每小时一个），两个不可重试死信。
	failToDLQ(t, q, clk, "dl-a", "upstream timeout")
	enteredA := clk.now().Add(-time.Hour) // 入死信在最后一次推进之前 1h
	failToDLQ(t, q, clk, "dl-b", "connection reset")
	enteredB := clk.now().Add(-time.Hour)
	failToDLQ(t, q, clk, "dl-c", "timeout again")
	nonRetryDLQ := func(id, reason string) time.Time {
		t.Helper()
		if _, err := q.Submit(id, []byte("p")); err != nil {
			t.Fatal(err)
		}
		d, err := q.Fetch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Nack(id, d.Token, FailureNonRetryable, reason); err != nil {
			t.Fatal(err)
		}
		return clk.now()
	}
	enteredX := nonRetryDLQ("dl-x", "invalid payload")
	clk.advance(time.Hour)
	enteredY := nonRetryDLQ("dl-y", "invalid payload too")

	// 条件 1：max_attempts_exceeded + 原因含 "timeout" => dl-a, dl-c，按入死信先后
	res1, err := q.Replay(ReplayRequest{
		BatchID: "b-timeout",
		Filter:  ReplayFilter{DLQReason: DLQReasonMaxAttempts, ReasonSubstr: "timeout"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Replayed) != 2 || res1.Replayed[0] != "dl-a" || res1.Replayed[1] != "dl-c" {
		t.Fatalf("expected [dl-a dl-c] by entered-at order, got %v", res1.Replayed)
	}
	t.Logf("batch=b-timeout tasks=%v verdict=replayed basis=dlq_reason+reason_substr", res1.Replayed)

	// 条件 2：失败类别 non_retryable + Limit=1 => 只取进入死信较早的 dl-x
	res2, err := q.Replay(ReplayRequest{
		BatchID: "b-nonretry-limit",
		Filter:  ReplayFilter{FailureClass: FailureNonRetryable, Limit: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Replayed) != 1 || res2.Replayed[0] != "dl-x" {
		t.Fatalf("expected only [dl-x] with limit, got %v", res2.Replayed)
	}
	t.Logf("batch=b-nonretry-limit tasks=%v verdict=replayed basis=failure_class+limit", res2.Replayed)

	// 条件 3：时间范围只命中 dl-b
	res3, err := q.Replay(ReplayRequest{
		BatchID: "b-window",
		Filter:  ReplayFilter{EnteredFrom: enteredB, EnteredTo: enteredB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Replayed) != 1 || res3.Replayed[0] != "dl-b" {
		t.Fatalf("time window should only hit dl-b, got %v", res3.Replayed)
	}
	t.Logf("batch=b-window tasks=%v verdict=replayed basis=entered_at_window entered_b=%s", res3.Replayed, enteredB)

	// 剩余死信只有 dl-y；空过滤应只返回它
	res4, err := q.Replay(ReplayRequest{BatchID: "b-rest", Filter: ReplayFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Replayed) != 1 || res4.Replayed[0] != "dl-y" {
		t.Fatalf("expected remaining [dl-y], got %v", res4.Replayed)
	}
	if got := len(q.DeadLetters()); got != 0 {
		t.Fatalf("all dead letters should be replayed, %d remain", got)
	}
	t.Logf("entered_times dl-a=%s dl-b=%s dl-x=%s dl-y=%s verdict=filter_ordering_and_limit_ok",
		enteredA, enteredB, enteredX, enteredY)
}

// TestReplayBatchIdempotent 同一批次重复触发不重复投出任务：
// 即使任务重放后再次失败入死信，同一 BatchID 仍是幂等 no-op。
func TestReplayBatchIdempotent(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)
	failToDLQ(t, q, clk, "t1", "flaky")
	failToDLQ(t, q, clk, "t2", "flaky")

	res1, err := q.Replay(ReplayRequest{BatchID: "dup-batch", Filter: ReplayFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Replayed) != 2 || res1.AlreadyApplied {
		t.Fatalf("first replay should apply, got %+v", res1)
	}

	// 立即重复触发：幂等 no-op，任务仍为 pending，不会被再次计数
	res2, err := q.Replay(ReplayRequest{BatchID: "dup-batch", Filter: ReplayFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if !res2.AlreadyApplied || len(res2.Replayed) != 2 {
		t.Fatalf("duplicate trigger should return already_applied with same tasks, got %+v", res2)
	}
	for _, id := range []string{"t1", "t2"} {
		task, _ := q.Task(id)
		if task.ReplayCount != 1 || task.Status != StatusPending {
			t.Fatalf("task %s changed on duplicate batch: %+v", id, task)
		}
	}
	t.Logf("batch=dup-batch verdict=idempotent_noop basis=batch_already_applied (tasks still pending)")

	// 把重放的任务重新投递并判不可重试入死信，再用同一批次触发：仍不得重放。
	for range 2 {
		d, err := q.Fetch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Nack(d.TaskID(), d.Token, FailureNonRetryable, "still bad"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(q.DeadLetters()); got != 2 {
		t.Fatalf("expected 2 dead letters again, got %d", got)
	}
	res3, err := q.Replay(ReplayRequest{BatchID: "dup-batch", TaskID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if !res3.AlreadyApplied {
		t.Fatalf("same batch after re-dlq must stay idempotent, got %+v", res3)
	}
	t1, _ := q.Task("t1")
	if t1.Status != StatusDeadLettered || t1.ReplayCount != 1 {
		t.Fatalf("t1 must remain dead_lettered with replay_count=1, got %s count=%d",
			t1.Status, t1.ReplayCount)
	}
	t.Logf("batch=dup-batch verdict=idempotent_noop basis=batch_already_applied (tasks re-deadlettered, not replayed)")
}
