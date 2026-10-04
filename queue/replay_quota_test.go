package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestReplayBatchSizeLimit 单次重放数量超过 MaxReplayBatchSize 时明确拒绝，状态不变。
func TestReplayBatchSizeLimit(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout:  time.Second,
		MaxAttempts:        2,
		Backoff:            ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 5 * time.Millisecond},
		Now:                clk.now,
		MaxReplayBatchSize: 2,
	}
	q := New(cfg, NewMemoryStore())
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	for _, id := range []string{"a", "b", "c"} {
		failToDLQ(t, q, clk, id, "boom")
	}

	_, err := q.Replay(ReplayRequest{BatchID: "too-big", Filter: ReplayFilter{}})
	re := mustReject(t, err, ReasonReplayBatchTooLarge, "oversized replay")
	t.Logf("verdict=%s msg=%q basis=max_replay_batch_size(2)<3", re.Reason, re.Message)

	// 拒绝是原子的：没有任何任务被改动
	for _, id := range []string{"a", "b", "c"} {
		task, _ := q.Task(id)
		if task.Status != StatusDeadLettered || task.ReplayCount != 0 {
			t.Fatalf("task %s changed after rejected replay: %+v", id, task)
		}
	}
	// 同批次随后缩小范围可以成功（被拒绝的批次不留幂等记录）
	res, err := q.Replay(ReplayRequest{BatchID: "too-big", Filter: ReplayFilter{Limit: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Replayed) != 2 {
		t.Fatalf("expected 2 replayed within limit, got %v", res.Replayed)
	}
}

// TestReplayTotalQuota 累计重放数量超过 MaxReplayTotal 时拒绝，
// 且同一任务二次重放也继续消耗累计额度（不会无限制堆积）。
func TestReplayTotalQuota(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: time.Second,
		MaxAttempts:       2,
		Backoff:           ExponentialBackoff{Base: time.Millisecond, Multiplier: 2, Max: 5 * time.Millisecond},
		Now:               clk.now,
		MaxReplayTotal:    2,
	}
	q := New(cfg, NewMemoryStore())
	if err := q.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	failToDLQ(t, q, clk, "a", "boom")
	failToDLQ(t, q, clk, "b", "boom")

	if _, err := q.Replay(ReplayRequest{BatchID: "r1", Filter: ReplayFilter{Limit: 1}}); err != nil {
		t.Fatalf("r1: %v", err)
	}
	// 剩余 1 个死信；尝试同时重放 1 个死信 + 批次内不会超——先把 a 重新打入死信
	d, err := q.Fetch(context.Background()) // 重放的 a（b 已不在死信集合，pick 取 pending）
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(d.TaskID(), d.Token, FailureNonRetryable, "again"); err != nil {
		t.Fatal(err)
	}
	// 此时死信：b（未重放）与 a（重放后再次死信）。额度仅剩 1，批量 2 个应拒绝。
	_, err = q.Replay(ReplayRequest{BatchID: "r2", Filter: ReplayFilter{}})
	re := mustReject(t, err, ReasonReplayQuotaExceeded, "over quota replay")
	t.Logf("verdict=%s msg=%q basis=max_replay_total(2), used=1+2", re.Reason, re.Message)

	// 点名单个仍可成功（用掉最后额度）
	res, err := q.Replay(ReplayRequest{BatchID: "r3", TaskID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Replayed) != 1 {
		t.Fatalf("expected b replayed, got %v", res.Replayed)
	}
	// 额度用尽：即使只有 1 个也拒绝
	_, err = q.Replay(ReplayRequest{BatchID: "r4", TaskID: "a"})
	mustReject(t, err, ReasonReplayQuotaExceeded, "exhausted replay")
}

// TestReplayRejectReasons 校验重放的各类拒绝原因与批次参数校验。
func TestReplayRejectReasons(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)
	failToDLQ(t, q, clk, "dlq-1", "boom")
	if _, err := q.Submit("pending-1", []byte("p")); err != nil {
		t.Fatal(err)
	}

	// 缺少 BatchID
	_, err := q.Replay(ReplayRequest{TaskID: "dlq-1"})
	mustReject(t, err, ReasonInvalidReplay, "missing batch id")

	// 点名不存在的任务
	_, err = q.Replay(ReplayRequest{BatchID: "b1", TaskID: "ghost"})
	mustReject(t, err, ReasonNotDeadLettered, "unknown task")

	// 点名处理中的任务：Fetch 后 in_flight，不得重放
	clk.advance(time.Millisecond) // pending-1 立即可见
	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.TaskID() != "pending-1" {
		t.Fatalf("expected pending-1 delivered, got %s", d.TaskID())
	}
	_, err = q.Replay(ReplayRequest{BatchID: "b2", TaskID: "pending-1"})
	mustReject(t, err, ReasonNotDeadLettered, "in-flight task")
	if err := q.Ack("pending-1", d.Token); err != nil {
		t.Fatal(err)
	}

	// 点名已完成任务
	_, err = q.Replay(ReplayRequest{BatchID: "b3", TaskID: "pending-1"})
	mustReject(t, err, ReasonNotDeadLettered, "completed task")
	t.Logf("verdict: invalid_replay/not_dead_lettered distinguishable for unknown/in_flight/completed")

	// 关闭队列后拒绝
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = q.Replay(ReplayRequest{BatchID: "b4", TaskID: "dlq-1"})
	mustReject(t, err, ReasonQueueClosed, "closed queue")
}

// TestReplayResetsSideEffect 重放重置副作用认领：重放后的正常投递会重新执行
// RunIdempotent 包装的副作用（不被原始处理历史的认领挡住），但同一轮内重投仍跳过。
func TestReplayResetsSideEffect(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)
	failToDLQ(t, q, clk, "t1", "boom")
	// failToDLQ 中没有副作用；模拟“副作用已执行但最终死信”的遗留认领：
	// 先重放一次拿到投递，认领副作用后让其失败重死信。
	if _, err := q.Replay(ReplayRequest{BatchID: "prep", TaskID: "t1"}); err != nil {
		t.Fatal(err)
	}
	d, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first, err := q.MarkSideEffect("t1", d.Token); err != nil || !first {
		t.Fatalf("mark side effect: first=%v err=%v", first, err)
	}
	if err := q.Nack("t1", d.Token, FailureNonRetryable, "still broken"); err != nil {
		t.Fatal(err)
	}
	if !q.SideEffectDone("t1") {
		t.Fatal("side effect flag should survive nack (it only resets on replay)")
	}

	// 人工重放：认领被重置
	if _, err := q.Replay(ReplayRequest{BatchID: "recover", TaskID: "t1"}); err != nil {
		t.Fatal(err)
	}
	if q.SideEffectDone("t1") {
		t.Fatal("side effect flag must be reset by replay")
	}
	d2, err := q.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ran, err := q.RunIdempotent("t1", d2.Token, func(ctx context.Context) error { return nil })
	if err != nil || !ran {
		t.Fatalf("post-replay side effect should run once: ran=%v err=%v", ran, err)
	}
	if err := q.Ack("t1", d2.Token); err != nil {
		t.Fatal(err)
	}
	task, _ := q.Task("t1")
	if !task.SideEffectDone {
		t.Fatal("side effect should be done after replay round")
	}
	t.Logf("task=t1 replays=%d replay_attempts=%d verdict=side_effect_reset_and_ran basis=replay_clears_claim",
		task.ReplayCount, task.ReplayAttempts)
}

// TestReplayConcurrentWithConsumers 重放与正在运行的多个消费者安全并存：
// 一批死信被重放后由并发消费者正常处理；重复触发同一批次绝不产生额外投递。
func TestReplayConcurrentWithConsumers(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)
	const n = 20
	for i := 0; i < n; i++ {
		failToDLQ(t, q, clk, idFor(i), "boom")
	}

	var mu sync.Mutex
	handled := map[string]int{}
	var delivered int64
	g := NewConsumerGroup(q, "grp", 4, func(ctx context.Context, task *Task) error {
		mu.Lock()
		handled[task.ID]++
		mu.Unlock()
		atomic.AddInt64(&delivered, 1)
		return nil
	})
	g.Start()

	// 并发：多个 goroutine 用同一 BatchID 触发重放（幂等），同时消费者在跑。
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = q.Replay(ReplayRequest{BatchID: "concurrent-batch", Filter: ReplayFilter{}})
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&delivered) >= int64(n) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if atomic.LoadInt64(&delivered) != int64(n) {
		t.Fatalf("expected exactly %d post-replay deliveries, got %d", n, delivered)
	}
	for i := 0; i < n; i++ {
		id := idFor(i)
		task, ok := q.Task(id)
		if !ok || task.Status != StatusCompleted {
			t.Fatalf("task %s not completed: ok=%v", id, ok)
		}
		if task.ReplayCount != 1 || task.ReplayAttempts != 1 || task.Attempts != 3 {
			t.Fatalf("task %s counters wrong: %+v", id, task)
		}
		if handled[id] != 1 {
			t.Fatalf("task %s handled %d times, want 1", id, handled[id])
		}
	}
	t.Logf("verdict: %d dead letters replayed once despite 6 duplicate triggers and 4 consumers; each handled exactly once", n)
}

// TestReplayRacesWithSubmitAndAck 重放进行期间，正常的提交、投递与确认不受影响：
// 新提交的任务与重放的任务都恰好处理一次，重复批次不重复生效。
func TestReplayRacesWithSubmitAndAck(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 2, time.Second, time.Millisecond, 5*time.Millisecond)

	const dlqN = 15
	for i := 0; i < dlqN; i++ {
		failToDLQ(t, q, clk, "dlq-"+idFor(i), "boom")
	}

	var mu sync.Mutex
	handled := map[string]int{}
	var done int64
	g := NewConsumerGroup(q, "grp", 4, func(ctx context.Context, task *Task) error {
		mu.Lock()
		handled[task.ID]++
		mu.Unlock()
		atomic.AddInt64(&done, 1)
		return nil
	})
	g.Start()

	var wg sync.WaitGroup
	// 反复用同一批次重放（幂等），同时提交全新任务。
	for w := 0; w < 5; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = q.Replay(ReplayRequest{BatchID: "racing-batch", Filter: ReplayFilter{}})
		}()
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if _, err := q.Submit("fresh-"+itoa(w)+"-"+itoa(i), []byte("p")); err != nil {
					if re, ok := AsReject(err); !ok || re.Reason != ReasonDuplicateSubmission {
						t.Errorf("submit fresh: %v", err)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	total := dlqN + 25
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&done) >= int64(total) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if atomic.LoadInt64(&done) != int64(total) {
		t.Fatalf("expected %d deliveries (%d replayed + %d fresh), got %d",
			total, dlqN, 25, done)
	}
	for i := 0; i < dlqN; i++ {
		id := "dlq-" + idFor(i)
		task, ok := q.Task(id)
		if !ok || task.Status != StatusCompleted {
			t.Fatalf("%s not completed: ok=%v", id, ok)
		}
		if task.ReplayCount != 1 || task.ReplayAttempts != 1 || handled[id] != 1 {
			t.Fatalf("%s replay/handle counters wrong: %+v handled=%d", id, task, handled[id])
		}
	}
	for w := 0; w < 5; w++ {
		for i := 0; i < 5; i++ {
			id := "fresh-" + itoa(w) + "-" + itoa(i)
			task, ok := q.Task(id)
			if !ok || task.Status != StatusCompleted || task.ReplayCount != 0 || handled[id] != 1 {
				t.Fatalf("fresh %s wrong: ok=%v task=%+v handled=%d", id, ok, task, handled[id])
			}
		}
	}
	t.Logf("verdict: %d replayed + 25 fresh submissions each handled exactly once under racing replay/submit; ack path undisturbed", dlqN)
}
