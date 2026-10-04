package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// makeDeadLetter 提交并把指定任务驱动到死信（每次失败前推进时钟越过退避）。
// 调用时队列中不应有其他待投递任务，保证 Fetch 确定性。
func makeDeadLetter(t *testing.T, q *Queue, clk *fakeClock, id string, class FailureClass, reason string) {
	t.Helper()
	if _, err := q.Submit(id, []byte("payload-"+id)); err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	for i := 0; i < 20; i++ {
		clk.advance(100 * time.Millisecond)
		d, err := q.Fetch(context.Background())
		if err != nil {
			t.Fatalf("fetch %s: %v", id, err)
		}
		if d.TaskID() != id {
			t.Fatalf("expected task %s, got %s", id, d.TaskID())
		}
		if err := q.Nack(id, d.Token, class, reason); err != nil {
			t.Fatalf("nack %s: %v", id, err)
		}
		if task, _ := q.Task(id); task.Status == StatusDeadLettered {
			t.Logf("task=%s verdict=dead_lettered dlq_reason=%s attempts=%d total_deliveries=%d",
				id, task.DLQReason, task.Attempts, task.TotalDeliveries)
			return
		}
	}
	t.Fatalf("task %s did not reach dead letter", id)
}

// TestReplayNamedAndFilteredSelection 覆盖点名重放与按条件（死信原因、失败原因文本、
// 进入死信时间范围）批量挑选；不满足条件的任务不被选中，点名的非死信任务记入 Skipped。
func TestReplayNamedAndFilteredSelection(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 10*time.Millisecond)

	// t1 时刻：non-retryable 死信两个，失败原因文本不同
	makeDeadLetter(t, q, clk, "nr-a", FailureNonRetryable, "bad payload: schema mismatch")
	makeDeadLetter(t, q, clk, "nr-b", FailureNonRetryable, "bad payload: unknown field")
	t1 := clk.now()
	// t2 时刻：超限死信一个
	clk.advance(time.Hour)
	makeDeadLetter(t, q, clk, "retry-c", FailureRetryable, "connection refused")
	t2 := clk.now()
	t.Logf("timeline: nr-a/nr-b dead at %s, retry-c dead at %s", t1, t2)

	// 1) 点名重放单个任务
	res, err := q.Replay("batch-named", ReplayFilter{TaskIDs: []string{"nr-a"}})
	if err != nil {
		t.Fatalf("replay named: %v", err)
	}
	if len(res.TaskIDs) != 1 || res.TaskIDs[0] != "nr-a" || res.Duplicate {
		t.Fatalf("unexpected named replay result: %+v", res)
	}
	task, _ := q.Task("nr-a")
	if task.Status != StatusPending || task.Attempts != 0 || task.ReplayCount != 1 ||
		task.TotalDeliveries != 1 || task.LastReplayBatch != "batch-named" || task.DLQReason != "" {
		t.Fatalf("unexpected replayed task state: %+v", task)
	}
	t.Logf("task=nr-a verdict=replayed replay_count=1 attempts=0 total_deliveries=1 basis=named")

	// 2) 按死信原因 + 失败原因文本筛选：只剩 nr-b 匹配（nr-a 已重放、retry-c 原因不符）
	res, err = q.Replay("batch-filter", ReplayFilter{
		DLQReason:      DLQReasonNonRetryable,
		ReasonContains: "unknown field",
	})
	if err != nil {
		t.Fatalf("replay filtered: %v", err)
	}
	if len(res.TaskIDs) != 1 || res.TaskIDs[0] != "nr-b" {
		t.Fatalf("unexpected filtered replay result: %+v", res)
	}
	t.Logf("task=nr-b verdict=replayed basis=filter(dlq_reason=non_retryable,reason~unknown field)")

	// 3) 按进入死信时间范围筛选：只匹配 t2 时刻的 retry-c
	res, err = q.Replay("batch-time", ReplayFilter{EnteredAfter: t1.Add(time.Minute), EnteredBefore: t2})
	if err != nil {
		t.Fatalf("replay time range: %v", err)
	}
	if len(res.TaskIDs) != 1 || res.TaskIDs[0] != "retry-c" {
		t.Fatalf("unexpected time-range replay result: %+v", res)
	}
	t.Logf("task=retry-c verdict=replayed basis=entered_after/before window")

	// 4) 点名一个非死信任务与一个未知任务：记入 Skipped，不误重放
	_, _ = q.Submit("alive", []byte("p"))
	res, err = q.Replay("batch-skip", ReplayFilter{TaskIDs: []string{"alive", "ghost"}})
	if err != nil {
		t.Fatalf("replay skip: %v", err)
	}
	if len(res.TaskIDs) != 0 || len(res.Skipped) != 2 {
		t.Fatalf("unexpected skip result: %+v", res)
	}
	if task, _ := q.Task("alive"); task.Status != StatusPending || task.ReplayCount != 0 {
		t.Fatalf("alive task must not be replayed: %+v", task)
	}
	t.Logf("verdict: alive/ghost skipped=%v basis=not_dead_lettered/unknown", res.Skipped)
}

// TestReplayIdempotentDuplicateBatch 同一批次重复触发：返回首次结果、不重复投放；
// 已完成任务不会因重复重放被复活；同批次 ID 换条件被拒绝。
func TestReplayIdempotentDuplicateBatch(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 10*time.Millisecond)
	makeDeadLetter(t, q, clk, "dead-1", FailureRetryable, "timeout")
	makeDeadLetter(t, q, clk, "dead-2", FailureRetryable, "timeout")

	filter := ReplayFilter{DLQReason: DLQReasonMaxAttempts}
	first, err := q.Replay("batch-1", filter)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if len(first.TaskIDs) != 2 || first.Duplicate {
		t.Fatalf("unexpected first result: %+v", first)
	}

	// 重放后正常走投递-确认流程：dead-1 被消费完成
	clk.advance(time.Millisecond)
	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(d.TaskID(), d.Token); err != nil {
		t.Fatalf("ack after replay: %v", err)
	}
	ackedID := d.TaskID()
	t.Logf("task=%s verdict=completed basis=ack_after_replay token=%d", ackedID, d.Token)

	// 重复触发同一批次：幂等命中，不重复生效
	for i := 0; i < 3; i++ {
		dup, err := q.Replay("batch-1", filter)
		if err != nil {
			t.Fatalf("duplicate replay %d: %v", i, err)
		}
		if !dup.Duplicate || len(dup.TaskIDs) != 2 || !dup.ReplayedAt.Equal(first.ReplayedAt) {
			t.Fatalf("duplicate replay %d mismatch: %+v", i, dup)
		}
	}
	// 已确认任务不被复活；待投递任务不被重复投放（ReplayCount 仍为 1）
	if task, _ := q.Task(ackedID); task.Status != StatusCompleted || task.ReplayCount != 1 {
		t.Fatalf("acked task must stay completed: %+v", task)
	}
	for _, id := range []string{"dead-1", "dead-2"} {
		if task, _ := q.Task(id); task.ReplayCount != 1 {
			t.Fatalf("task %s replay_count=%d, want 1 (no duplicate effect)", id, task.ReplayCount)
		}
	}
	t.Logf("verdict: duplicate triggers idempotent, replay_count stays 1, completed task not resurrected")

	// 同批次 ID 换用不同筛选条件：拒绝
	_, err = q.Replay("batch-1", ReplayFilter{DLQReason: DLQReasonNonRetryable})
	mustReject(t, err, ReasonReplayBatchConflict, "replay conflict")
	t.Logf("verdict: same batch id with different filter rejected basis=replay_batch_conflict")
}

// TestReplayRetrySemanticsAfterReplay 重放后的任务再次失败：仍按现有失败分类与
// 重试上限处理——可重试失败在新预算内退避重试、超限再入死信；不可重试失败直接死信。
func TestReplayRetrySemanticsAfterReplay(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 10*time.Millisecond)
	makeDeadLetter(t, q, clk, "flaky", FailureRetryable, "timeout") // 超限死信，attempts=2
	makeDeadLetter(t, q, clk, "broken", FailureNonRetryable, "bad") // 不可重试死信

	// 分批点名重放，保证 Fetch 顺序确定
	if _, err := q.Replay("batch-flaky", ReplayFilter{TaskIDs: []string{"flaky"}}); err != nil {
		t.Fatalf("replay flaky: %v", err)
	}

	// flaky：重放获得新预算（2 次），第一次可重试失败 -> 退避重投而非直接死信
	clk.advance(time.Millisecond)
	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "flaky" || d.Task.Attempts != 1 || d.Task.TotalDeliveries != 3 || d.Task.ReplayCount != 1 {
		t.Fatalf("unexpected redelivery after replay: %+v", d.Task)
	}
	if err := q.Nack("flaky", d.Token, FailureRetryable, "timeout again"); err != nil {
		t.Fatal(err)
	}
	task, _ := q.Task("flaky")
	if task.Status != StatusPending {
		t.Fatalf("retryable failure after replay must re-enter pending, got %s", task.Status)
	}
	t.Logf("task=flaky verdict=repending basis=retryable attempts=1<2 replay_count=1")

	// 第二次失败达到上限 -> 再次死信，原因仍是 max_attempts_exceeded
	clk.advance(100 * time.Millisecond)
	d, err = q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack("flaky", d.Token, FailureRetryable, "timeout again"); err != nil {
		t.Fatal(err)
	}
	task, _ = q.Task("flaky")
	if task.Status != StatusDeadLettered || task.DLQReason != DLQReasonMaxAttempts || task.TotalDeliveries != 4 {
		t.Fatalf("retry limit must still apply after replay: %+v", task)
	}
	t.Logf("task=flaky verdict=dead_lettered dlq_reason=max_attempts_exceeded total_deliveries=4")

	// broken：重放后再次不可重试失败 -> 直接死信，不被判成可重试
	if _, err := q.Replay("batch-broken", ReplayFilter{TaskIDs: []string{"broken"}}); err != nil {
		t.Fatalf("replay broken: %v", err)
	}
	clk.advance(time.Millisecond)
	d, err = q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "broken" {
		t.Fatalf("expected broken, got %s", d.TaskID())
	}
	if err := q.Nack("broken", d.Token, FailureNonRetryable, "still bad"); err != nil {
		t.Fatal(err)
	}
	task, _ = q.Task("broken")
	if task.Status != StatusDeadLettered || task.DLQReason != DLQReasonNonRetryable || task.Attempts != 1 {
		t.Fatalf("non-retryable must stay non-retryable after replay: %+v", task)
	}
	t.Logf("task=broken verdict=dead_lettered dlq_reason=non_retryable basis=classification_preserved")
}

// TestReplayCapacityLimits 单次与累计重放上限：超限明确拒绝且不产生任何投放；
// 调用方 Limit 在单次上限内截断挑选数量。
func TestReplayCapacityLimits(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       1,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 10 * time.Millisecond},
		Now:               clk.now,
		MaxReplayPerBatch: 2,
		MaxReplayTotal:    3,
	}
	q := New(cfg, NewMemoryStore())
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })

	for i := 0; i < 4; i++ {
		makeDeadLetter(t, q, clk, fmt.Sprintf("dead-%d", i), FailureNonRetryable, "bad")
	}

	// 单次上限：4 个候选 > 2，明确拒绝且不投放
	_, err := q.Replay("batch-big", ReplayFilter{})
	re := mustReject(t, err, ReasonReplayLimitExceeded, "per-batch limit")
	t.Logf("verdict: per-batch limit rejected msg=%q", re.Message)
	if dl := q.DeadLetters(); len(dl) != 4 {
		t.Fatalf("rejected replay must not enqueue anything, dead letters=%d", len(dl))
	}

	// Limit 截断：4 个候选限取 2（按进入死信先后），成功
	res, err := q.Replay("batch-limited", ReplayFilter{Limit: 2})
	if err != nil {
		t.Fatalf("limited replay: %v", err)
	}
	if len(res.TaskIDs) != 2 || res.TaskIDs[0] != "dead-0" || res.TaskIDs[1] != "dead-1" {
		t.Fatalf("unexpected limited result: %+v", res)
	}
	t.Logf("verdict: limit=2 truncated to %v basis=dead_lettered_at order", res.TaskIDs)

	// 累计上限：已重放 2，再重放 2 会超过总量 3，明确拒绝
	_, err = q.Replay("batch-total", ReplayFilter{Limit: 2})
	re = mustReject(t, err, ReasonReplayLimitExceeded, "total limit")
	t.Logf("verdict: total limit rejected msg=%q", re.Message)

	// 累计额度内（再重放 1 个，总计 3）允许
	res, err = q.Replay("batch-total-ok", ReplayFilter{Limit: 1})
	if err != nil {
		t.Fatalf("replay within total limit: %v", err)
	}
	if len(res.TaskIDs) != 1 || res.TaskIDs[0] != "dead-2" {
		t.Fatalf("unexpected result: %+v", res)
	}
	t.Logf("verdict: cumulative 3/3 replayed, dead-3 remains dead lettered")
	if dl := q.DeadLetters(); len(dl) != 1 || dl[0].ID != "dead-3" {
		t.Fatalf("expected only dead-3 remaining, got %v", dl)
	}
}

// TestReplayOnClosedQueue 队列关闭后重放被拒绝。
func TestReplayOnClosedQueue(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 1, time.Second, time.Millisecond, 10*time.Millisecond)
	makeDeadLetter(t, q, clk, "dead", FailureNonRetryable, "bad")
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := q.Replay("batch-closed", ReplayFilter{})
	mustReject(t, err, ReasonQueueClosed, "replay on closed queue")
	t.Logf("verdict: replay on closed queue rejected basis=queue_closed")
}
