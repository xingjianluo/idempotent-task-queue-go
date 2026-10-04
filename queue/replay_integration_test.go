package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestReplaySkipsInFlightAndCompleted 处理中与已确认的任务不能被误重放：
// 点名重放时记入 Skipped，任务状态与投递 token 不受影响。
func TestReplaySkipsInFlightAndCompleted(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 10*time.Millisecond)
	_, _ = q.Submit("busy", []byte("p"))

	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// in_flight：点名重放被跳过，租约与 token 不变
	res, err := q.Replay("batch-busy", ReplayFilter{TaskIDs: []string{"busy"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TaskIDs) != 0 || len(res.Skipped) != 1 || res.Skipped[0].TaskID != "busy" {
		t.Fatalf("in-flight task must be skipped: %+v", res)
	}
	task, _ := q.Task("busy")
	if task.Status != StatusInFlight || task.DeliveryToken != d.Token || task.ReplayCount != 0 {
		t.Fatalf("in-flight task disturbed by replay: %+v", task)
	}
	t.Logf("task=busy verdict=skipped basis=in_flight token=%d", d.Token)

	// completed：确认后点名重放仍被跳过
	if err := q.Ack("busy", d.Token); err != nil {
		t.Fatal(err)
	}
	res, err = q.Replay("batch-done", ReplayFilter{TaskIDs: []string{"busy"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TaskIDs) != 0 || len(res.Skipped) != 1 {
		t.Fatalf("completed task must be skipped: %+v", res)
	}
	if task, _ := q.Task("busy"); task.Status != StatusCompleted || task.ReplayCount != 0 {
		t.Fatalf("completed task resurrected by replay: %+v", task)
	}
	t.Logf("task=busy verdict=skipped basis=completed")
}

// TestReplayConcurrentWithConsumers 重放与运行中的多消费者并存：
// 死信任务被重放后由消费者正常处理完成；期间新提交与确认不受干扰；
// 每个任务的处理次数与重放次数可核对，无重复生效。
func TestReplayConcurrentWithConsumers(t *testing.T) {
	q := New(Config{
		VisibilityTimeout: 200 * time.Millisecond,
		MaxAttempts:       2,
		Backoff:           ExponentialBackoff{Base: 5 * time.Millisecond, Multiplier: 2, Max: 20 * time.Millisecond},
	}, NewMemoryStore())
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })

	var mu sync.Mutex
	handled := map[string]int{}
	handler := func(ctx context.Context, task *Task) error {
		mu.Lock()
		handled[task.ID]++
		mu.Unlock()
		// flop-* 首次处理（重放前）不可重试失败；重放后成功
		if strings.HasPrefix(task.ID, "flop-") && task.ReplayCount == 0 {
			return NonRetryable("permanent failure on first life")
		}
		return nil
	}
	g := NewConsumerGroup(q, "replay-grp", 3, handler)
	g.Start()

	const okN, flopN = 6, 4
	for i := 0; i < okN; i++ {
		if _, err := q.Submit(fmt.Sprintf("ok-%d", i), []byte("p")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < flopN; i++ {
		if _, err := q.Submit(fmt.Sprintf("flop-%d", i), []byte("p")); err != nil {
			t.Fatal(err)
		}
	}

	// 等待 flop-* 全部进入死信
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(q.DeadLetters()) == flopN {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if dl := q.DeadLetters(); len(dl) != flopN {
		t.Fatalf("expected %d dead letters, got %d", flopN, len(dl))
	}

	// 消费者运行中触发重放；同时继续提交新任务，验证互不干扰
	res, err := q.Replay("ops-batch-1", ReplayFilter{DLQReason: DLQReasonNonRetryable})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(res.TaskIDs) != flopN {
		t.Fatalf("expected %d replayed, got %v", flopN, res.TaskIDs)
	}
	for i := 0; i < 3; i++ {
		if _, err := q.Submit(fmt.Sprintf("late-%d", i), []byte("p")); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("verdict: replayed %v while 3 consumers running; new submissions accepted", res.TaskIDs)

	// 等待全部任务完成
	total := okN + flopN + 3
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		done := 0
		for _, task := range q.Snapshot() {
			if task.Status == StatusCompleted {
				done++
			}
		}
		if done == total {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	shctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Shutdown(shctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for id, task := range q.Snapshot() {
		if task.Status != StatusCompleted {
			t.Fatalf("task %s not completed: %s", id, task.Status)
		}
	}
	for i := 0; i < flopN; i++ {
		id := fmt.Sprintf("flop-%d", i)
		task, _ := q.Task(id)
		if task.ReplayCount != 1 || task.TotalDeliveries != 2 || handled[id] != 2 {
			t.Fatalf("task %s: replay_count=%d total_deliveries=%d handled=%d, want 1/2/2",
				id, task.ReplayCount, task.TotalDeliveries, handled[id])
		}
	}
	for i := 0; i < okN; i++ {
		id := fmt.Sprintf("ok-%d", i)
		if handled[id] != 1 {
			t.Fatalf("task %s handled %d times, want 1", id, handled[id])
		}
	}
	t.Logf("verdict: %d tasks completed; flop-* replay_count=1 total_deliveries=2; ok-* handled once", total)
}

// TestReplayRecoveryAfterRestart 重放后停机重启：待重放任务以 pending 恢复、不丢；
// 批次记录与累计重放计数恢复，同一批次重复触发仍幂等、不重复生效；
// 累计容量上限跨重启继续生效。
func TestReplayRecoveryAfterRestart(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       1,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 10 * time.Millisecond},
		Now:               clk.now,
		MaxReplayTotal:    2,
	}
	q, path := newFileTestQueue(t, cfg)

	makeDeadLetter(t, q, clk, "dead-a", FailureNonRetryable, "bad a")
	makeDeadLetter(t, q, clk, "dead-b", FailureNonRetryable, "bad b")

	res, err := q.Replay("ops-restart", ReplayFilter{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(res.TaskIDs) != 2 {
		t.Fatalf("expected 2 replayed, got %v", res.TaskIDs)
	}
	// 重放后未消费就停机
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: replayed %v then closed before consumption", res.TaskIDs)

	// 重启：新队列从同一文件恢复
	clk.advance(time.Second)
	q2 := New(cfg, NewFileStore(path))
	if err := q2.Open(); err != nil {
		t.Fatal(err)
	}
	defer q2.Close()

	for _, id := range []string{"dead-a", "dead-b"} {
		task, ok := q2.Task(id)
		if !ok || task.Status != StatusPending || task.ReplayCount != 1 || task.LastReplayBatch != "ops-restart" {
			t.Fatalf("task %s not recovered as replayed-pending: %+v", id, task)
		}
	}
	t.Logf("verdict: dead-a/dead-b recovered as pending replay_count=1 (no loss)")

	// 同一批次重复触发：批次记录已持久化，幂等命中、不重复生效
	dup, err := q2.Replay("ops-restart", ReplayFilter{})
	if err != nil {
		t.Fatalf("duplicate replay after restart: %v", err)
	}
	if !dup.Duplicate || len(dup.TaskIDs) != 2 {
		t.Fatalf("expected idempotent duplicate after restart, got %+v", dup)
	}
	for _, id := range []string{"dead-a", "dead-b"} {
		if task, _ := q2.Task(id); task.ReplayCount != 1 {
			t.Fatalf("task %s replay_count=%d after duplicate, want 1", id, task.ReplayCount)
		}
	}
	t.Logf("verdict: duplicate batch after restart idempotent basis=persisted_batch_record")

	// 恢复的重放任务可以正常投递确认（先消费掉，避免干扰后续死信构造）
	for i := 0; i < 2; i++ {
		d, err := q2.Fetch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := q2.Ack(d.TaskID(), d.Token); err != nil {
			t.Fatalf("ack recovered replayed task: %v", err)
		}
		t.Logf("task=%s verdict=completed basis=ack_after_restart token=%d", d.TaskID(), d.Token)
	}

	// 累计上限跨重启生效：已重放 2/2，新批次再重放被拒绝
	makeDeadLetter(t, q2, clk, "dead-c", FailureNonRetryable, "bad c")
	_, err = q2.Replay("ops-extra", ReplayFilter{TaskIDs: []string{"dead-c"}})
	mustReject(t, err, ReasonReplayLimitExceeded, "total limit after restart")
	t.Logf("verdict: cumulative limit 2/2 enforced after restart")
}

// failingReplayStore 包装内存存储，可开关 SaveAllWithReplay 失败注入。
type failingReplayStore struct {
	inner Store
	mu    sync.Mutex
	fail  bool
}

func (s *failingReplayStore) Load() (map[string]*Task, error) { return s.inner.Load() }
func (s *failingReplayStore) SaveAll(tasks map[string]*Task) error {
	return s.inner.SaveAll(tasks)
}
func (s *failingReplayStore) LoadReplay() (map[string]*ReplayBatch, error) {
	return s.inner.(replayStore).LoadReplay()
}
func (s *failingReplayStore) SaveAllWithReplay(tasks map[string]*Task, batches map[string]*ReplayBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return fmt.Errorf("injected persist failure")
	}
	return s.inner.(replayStore).SaveAllWithReplay(tasks, batches)
}
func (s *failingReplayStore) setFail(f bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = f
}

// TestReplayPersistFailureRollback 重放落盘失败：整体回滚，不留半截状态；
// 故障恢复后同一批次可重试并成功。
func TestReplayPersistFailureRollback(t *testing.T) {
	clk := newFakeClock()
	store := &failingReplayStore{inner: NewMemoryStore()}
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       1,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 10 * time.Millisecond},
		Now:               clk.now,
	}
	q := New(cfg, store)
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })

	makeDeadLetter(t, q, clk, "dead-x", FailureNonRetryable, "bad x")
	makeDeadLetter(t, q, clk, "dead-y", FailureNonRetryable, "bad y")

	store.setFail(true)
	_, err := q.Replay("batch-fail", ReplayFilter{})
	if err == nil || !strings.Contains(err.Error(), "injected persist failure") {
		t.Fatalf("expected persist failure, got %v", err)
	}
	// 不留半截状态：任务仍死信、无批次记录、累计计数未变
	if dl := q.DeadLetters(); len(dl) != 2 {
		t.Fatalf("tasks must stay dead_lettered after failed replay, got %d", len(dl))
	}
	if batches := q.ReplayBatches(); len(batches) != 0 {
		t.Fatalf("no batch record must remain after failed replay, got %v", batches)
	}
	for _, id := range []string{"dead-x", "dead-y"} {
		if task, _ := q.Task(id); task.ReplayCount != 0 {
			t.Fatalf("task %s replay_count=%d after rollback, want 0", id, task.ReplayCount)
		}
	}
	t.Logf("verdict: persist failure rolled back, tasks still dead_lettered, no batch record")

	// 故障恢复后同一批次 ID 可重试（此前未生效，不算冲突）
	store.setFail(false)
	res, err := q.Replay("batch-fail", ReplayFilter{})
	if err != nil {
		t.Fatalf("retry replay after recovery: %v", err)
	}
	if len(res.TaskIDs) != 2 || res.Duplicate {
		t.Fatalf("unexpected retry result: %+v", res)
	}
	t.Logf("verdict: same batch id retried successfully after recovery, replayed %v", res.TaskIDs)
}

// tasksOnlyStore 只实现 Store 接口（不实现 replayStore），模拟既有自定义存储。
type tasksOnlyStore struct{ inner Store }

func (s *tasksOnlyStore) Load() (map[string]*Task, error)  { return s.inner.Load() }
func (s *tasksOnlyStore) SaveAll(t map[string]*Task) error { return s.inner.SaveAll(t) }

// TestReplayWithLegacyStore 既有自定义 Store（未实现重放持久化扩展）无需改动即可
// 使用重放功能：行为一致，批次记录保留在内存中。
func TestReplayWithLegacyStore(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       1,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 10 * time.Millisecond},
		Now:               clk.now,
	}
	q := New(cfg, &tasksOnlyStore{inner: NewMemoryStore()})
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })

	makeDeadLetter(t, q, clk, "dead-legacy", FailureNonRetryable, "bad")
	res, err := q.Replay("batch-legacy", ReplayFilter{})
	if err != nil {
		t.Fatalf("replay with legacy store: %v", err)
	}
	if len(res.TaskIDs) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	dup, err := q.Replay("batch-legacy", ReplayFilter{})
	if err != nil || !dup.Duplicate {
		t.Fatalf("idempotency must hold in-memory for legacy store: %v %+v", err, dup)
	}
	t.Logf("verdict: legacy store works unchanged; batch idempotency kept in memory")
}
