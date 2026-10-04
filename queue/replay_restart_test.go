package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestReplayFailureRulesUnchanged 重放不绕过失败分类与重试上限：
//   - 不可重试失败：重放后首次投递即重新死信（non_retryable），不会被当成可重试；
//   - 可重试失败：本轮重放获得独立的 max_attempts 预算，超限仍入死信（max_attempts_exceeded）；
//   - 再次人工重放又获得新预算；历史 Attempts 与 ReplayAttempts 始终可区分。
func TestReplayFailureRulesUnchanged(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)
	failToDLQ(t, q, clk, "nr", "boom") // attempts=2 死信
	failToDLQ(t, q, clk, "rt", "boom") // attempts=2 死信

	// nr：重放后给不可重试失败 => 第一次重放投递就死信
	if _, err := q.Replay(ReplayRequest{BatchID: "re-nr", TaskID: "nr"}); err != nil {
		t.Fatal(err)
	}
	d, err := q.Fetch(context.Background()) // nr
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "nr" {
		t.Fatalf("expected nr delivered first, got %s", d.TaskID())
	}
	if err := q.Nack("nr", d.Token, FailureNonRetryable, "still invalid"); err != nil {
		t.Fatal(err)
	}
	nr, _ := q.Task("nr")
	if nr.Status != StatusDeadLettered || nr.DLQReason != DLQReasonNonRetryable {
		t.Fatalf("non-retryable after replay must go straight to DLQ, got %s/%s", nr.Status, nr.DLQReason)
	}
	if nr.Attempts != 3 || nr.ReplayAttempts != 1 {
		t.Fatalf("nr counters wrong: %+v", nr)
	}
	t.Logf("task=nr attempts=3 replays=1 replay_attempts=1 verdict=dead_lettered dlq_reason=non_retryable basis=classification_unchanged")

	// rt：重放后两次可重试失败 => 第 2 次超本轮上限死信
	if _, err := q.Replay(ReplayRequest{BatchID: "re-rt", TaskID: "rt"}); err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 2; round++ {
		// 每个轮次 Fetch 前保证 nr 仍死信，唯一 pending 是 rt
		d, err := q.Fetch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if d.TaskID() != "rt" {
			t.Fatalf("expected rt, got %s", d.TaskID())
		}
		if err := q.Nack("rt", d.Token, FailureRetryable, "flaky again"); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Hour)
	}
	rt, _ := q.Task("rt")
	if rt.Status != StatusDeadLettered || rt.DLQReason != DLQReasonMaxAttempts {
		t.Fatalf("retryable after replay must respect max_attempts, got %s/%s", rt.Status, rt.DLQReason)
	}
	if rt.Attempts != 4 || rt.ReplayAttempts != 2 || rt.AttemptsAtReplay != 2 {
		t.Fatalf("rt counters wrong: %+v", rt)
	}
	t.Logf("task=rt attempts=4 replays=1 replay_attempts=2 verdict=dead_lettered dlq_reason=max_attempts_exceeded basis=independent_round_budget")

	// 第二次人工重放：新一轮预算，再次成功
	if _, err := q.Replay(ReplayRequest{BatchID: "re-rt-2", TaskID: "rt"}); err != nil {
		t.Fatal(err)
	}
	d2, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d2.Task.Attempts != 5 || d2.Task.ReplayAttempts != 3 || d2.Task.ReplayCount != 2 || d2.Task.AttemptsAtReplay != 4 {
		t.Fatalf("second replay delivery counters wrong: %+v", d2.Task)
	}
	if err := q.Ack("rt", d2.Token); err != nil {
		t.Fatal(err)
	}
	t.Logf("task=rt attempts=5 replays=2 replay_attempts=3 verdict=completed basis=new_round_budget_history_preserved")
}

// TestReplayRestartRecovery 批次幂等记录与任务状态随同一快照落盘：
// 重启后已重放的批次重复触发仍是 no-op；待重放（批次记录已落盘）状态完整恢复。
func TestReplayRestartRecovery(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       2,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 5 * time.Millisecond},
		Now:               clk.now,
	}
	q, path := newFileTestQueue(t, cfg)
	failToDLQ(t, q, clk, "t1", "boom")
	failToDLQ(t, q, clk, "t2", "boom")

	res, err := q.Replay(ReplayRequest{BatchID: "persisted-batch", Filter: ReplayFilter{Limit: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Replayed) != 1 || res.Replayed[0] != "t1" {
		t.Fatalf("expected only t1 replayed, got %v", res.Replayed)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：t1 应恢复为 pending（重放已生效），t2 仍死信，批次记录仍在。
	q2 := New(cfg, NewFileStore(path))
	if err := q2.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	t1, _ := q2.Task("t1")
	t2, _ := q2.Task("t2")
	if t1.Status != StatusPending || t1.ReplayCount != 1 || t1.AttemptsAtReplay != 2 {
		t.Fatalf("t1 replay state must survive restart, got %+v", t1)
	}
	if t2.Status != StatusDeadLettered {
		t.Fatalf("t2 should remain dead_lettered, got %s", t2.Status)
	}
	if n := len(q2.Snapshot()); n != 2 {
		t.Fatalf("internal registry marker must not leak into Snapshot, got %d entries", n)
	}

	// 同一批次重复触发：重启后仍幂等，不会把 t2 也投出去。
	res2, err := q2.Replay(ReplayRequest{BatchID: "persisted-batch", Filter: ReplayFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if !res2.AlreadyApplied || len(res2.Replayed) != 1 || res2.Replayed[0] != "t1" {
		t.Fatalf("batch must stay idempotent after restart, got %+v", res2)
	}
	t2b, _ := q2.Task("t2")
	if t2b.Status != StatusDeadLettered || t2b.ReplayCount != 0 {
		t.Fatalf("t2 must not be replayed by duplicate batch after restart, got %+v", t2b)
	}
	t.Logf("batch=persisted-batch verdict=idempotent_after_restart basis=registry_persisted_with_snapshot")

	// 恢复的 t1 可正常投递完成；新批次可重放 t2。
	d, err := q2.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "t1" || d.Task.Attempts != 3 || d.Task.ReplayAttempts != 1 {
		t.Fatalf("t1 delivery after restart wrong: %+v", d.Task)
	}
	if err := q2.Ack("t1", d.Token); err != nil {
		t.Fatal(err)
	}
	res3, err := q2.Replay(ReplayRequest{BatchID: "new-batch", TaskID: "t2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Replayed) != 1 {
		t.Fatalf("new batch should replay t2, got %v", res3.Replayed)
	}
	t.Logf("task=t1 verdict=completed_after_restart; task=t2 batch=new-batch verdict=replayed_after_restart")
}

// failPersistStore 在 failCount 次 SaveAll 时返回错误，其余行为同内存存储。
type failPersistStore struct {
	failCount int
	mu        sync.Mutex
	tasks     map[string]*Task
}

func (s *failPersistStore) Load() (map[string]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Task, len(s.tasks))
	for id, t := range s.tasks {
		out[id] = t.Clone()
	}
	return out, nil
}

func (s *failPersistStore) SaveAll(tasks map[string]*Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCount > 0 {
		s.failCount--
		return errors.New("simulated disk failure")
	}
	out := make(map[string]*Task, len(tasks))
	for id, t := range tasks {
		out[id] = t.Clone()
	}
	s.tasks = out
	return nil
}

// TestReplayPersistFailureRollback 落盘失败时整批回滚：任务仍处死信、批次未登记，
// 同一批次随后可完整重放，不丢任务也不重复生效。
func TestReplayPersistFailureRollback(t *testing.T) {
	clk := newFakeClock()
	store := &failPersistStore{tasks: map[string]*Task{}}
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       2,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 5 * time.Millisecond},
		Now:               clk.now,
	}
	q := New(cfg, store)
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	failToDLQ(t, q, clk, "t1", "boom")
	failToDLQ(t, q, clk, "t2", "boom")

	// 让下一次 SaveAll（即 Replay 的落盘）失败。
	store.failCount = 1
	_, err := q.Replay(ReplayRequest{BatchID: "atomic-batch", Filter: ReplayFilter{}})
	if err == nil || err.Error() != "simulated disk failure" {
		t.Fatalf("expected simulated disk failure, got %v", err)
	}

	for _, id := range []string{"t1", "t2"} {
		task, _ := q.Task(id)
		if task.Status != StatusDeadLettered || task.ReplayCount != 0 {
			t.Fatalf("task %s must roll back to dead_lettered after failed persist, got %+v", id, task)
		}
	}
	t.Logf("verdict=rolled_back basis=persist_failed; dead letters untouched: t1,t2")

	// 落盘恢复后同批次重试：完整生效一次，不是部分重放。
	res, err := q.Replay(ReplayRequest{BatchID: "atomic-batch", Filter: ReplayFilter{}})
	if err != nil {
		t.Fatalf("retry after persist recovery: %v", err)
	}
	if res.AlreadyApplied || len(res.Replayed) != 2 {
		t.Fatalf("retry should fully apply once, got %+v", res)
	}
	// 再触发即幂等
	res2, err := q.Replay(ReplayRequest{BatchID: "atomic-batch", Filter: ReplayFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if !res2.AlreadyApplied {
		t.Fatalf("second trigger must be idempotent, got %+v", res2)
	}
	t.Logf("batch=atomic-batch verdict=fully_applied_after_retry_then_idempotent")
}
