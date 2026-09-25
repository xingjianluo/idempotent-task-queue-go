package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestConsumerFailureClassification 消费者自动把 handler 结果归类：
// 普通 error -> 可重试；*Failure(non_retryable) -> 直接死信。
func TestConsumerFailureClassification(t *testing.T) {
	log := newTestLogger(t)
	q, _ := NewQueue(context.Background(), NewMemoryStore(), Config{
		LeaseTTL: time.Hour, MaxAttempts: 3,
		Backoff: &ExponentialBackoff{BaseMillis: 1, MaxMillis: 1}, Logger: log,
	})
	ctx := context.Background()
	_, _, _ = q.Submit(ctx, TaskSpec{ID: "plain-err", Type: "t"})
	_, _, _ = q.Submit(ctx, TaskSpec{ID: "typed-fatal", Type: "t"})

	var calls int
	handler := func(ctx context.Context, d *Delivery) error {
		calls++
		if d.Task.ID == "plain-err" {
			return errors.New("boom: generic error is retryable")
		}
		return NewFailure(FailNonRetryable, "typed fatal")
	}
	c := NewConsumer(q, handler, ConsumerConfig{
		Name: "edge", Concurrency: 2, ReapInterval: 10 * time.Millisecond,
		ShutdownWait: 2 * time.Second, Logger: log,
	})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()

	waitFor := func(id string, st Status) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if tt, ok := q.Get(id); ok && tt.Status == st {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("task %s did not reach %s", id, st)
	}
	waitFor("plain-err", StatusDead)   // 3 次普通 error -> retry_exhausted
	waitFor("typed-fatal", StatusDead) // 1 次 typed -> non_retryable
	cancel()
	<-done

	pe, _ := q.Get("plain-err")
	if pe.DeadReason != DeadRetryExhausted || pe.Attempts != 3 {
		t.Fatalf("plain-err reason=%s attempts=%d", pe.DeadReason, pe.Attempts)
	}
	tf, _ := q.Get("typed-fatal")
	if tf.DeadReason != DeadNonRetryable || tf.Attempts != 1 {
		t.Fatalf("typed-fatal reason=%s attempts=%d", tf.DeadReason, tf.Attempts)
	}
}

// TestFetchCancelAndClosed Fetch 在 ctx 取消时返回；队列关闭后返回 queue_closed。
func TestFetchCancelAndClosed(t *testing.T) {
	log := newTestLogger(t)
	q := newTestQueue(t, log, NewRealClock(), 3, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := q.Fetch(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty fetch err=%v, want context deadline", err)
	}

	_, _, _ = q.Submit(context.Background(), TaskSpec{ID: "closed-1", Type: "t"})
	_ = q.Close(context.Background())
	if _, err := q.Fetch(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("fetch after close err=%v, want queue_closed", err)
	}
	if _, _, err := q.Submit(context.Background(), TaskSpec{ID: "x", Type: "t"}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("submit after close err=%v", err)
	}
	// Close 幂等。
	if err := q.Close(context.Background()); err != nil {
		t.Fatalf("double close: %v", err)
	}
}

// TestSubmitEmptyID 空 ID 提交被拒绝，原因可区分。
func TestSubmitEmptyID(t *testing.T) {
	log := newTestLogger(t)
	q := newTestQueue(t, log, NewRealClock(), 3, time.Second)
	_, _, err := q.Submit(context.Background(), TaskSpec{ID: ""})
	if code := rejectCode(err); code != RejectTaskNotFound {
		t.Fatalf("empty id submit code=%v, want task_not_found", code)
	}
}

// TestReclaimLeasedForShutdown 停机回收全部 leased（含未过期），旧令牌作废。
func TestReclaimLeasedForShutdown(t *testing.T) {
	log := newTestLogger(t)
	q := newTestQueue(t, log, NewRealClock(), 3, time.Hour)
	ctx := context.Background()
	_, _, _ = q.Submit(ctx, TaskSpec{ID: "a", Type: "t"})
	_, _, _ = q.Submit(ctx, TaskSpec{ID: "b", Type: "t"})
	d1 := fetchReady(t, q, time.Second)
	d2 := fetchReady(t, q, time.Second)
	ids := q.ReclaimLeasedForShutdown(ctx)
	if len(ids) != 2 {
		t.Fatalf("reclaimed=%v, want 2", ids)
	}
	for _, d := range []*Delivery{d1, d2} {
		if code := rejectCode(mustErrAck(q, ctx, d.Task.ID, d.Tag)); code != RejectNotLeased {
			t.Fatalf("stale ack after reclaim code=%v", code)
		}
	}
	// 回收后两个任务都能重新投递。
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		d := fetchReady(t, q, time.Second)
		seen[d.Task.ID] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("redelivered=%v", seen)
	}
}

// TestFileStoreCorruptAndRoundTrip FileStore 损坏文件报错；正常文件可往返加载。
func TestFileStoreCorruptAndRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// 损坏文件 -> Load 返回错误。
	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(corrupt).Load(context.Background()); err == nil {
		t.Fatal("corrupt state must return error")
	}

	// 保存 -> 重新加载：状态往返一致。
	path := filepath.Join(dir, "nested", "state.json")
	store := NewFileStore(path)
	st, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	st.Seq = 7
	st.Tasks["x"] = &Task{ID: "x", Status: StatusDead, DeadReason: DeadRetryExhausted,
		Failures: []Failure{{Kind: FailRetryable, Message: "f"}}}
	if err := store.Save(context.Background(), st); err != nil {
		t.Fatalf("save: %v", err)
	}
	st3, err := NewFileStore(path).Load(context.Background())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if st3.Seq != 7 || st3.Tasks["x"].DeadReason != DeadRetryExhausted || len(st3.Tasks["x"].Failures) != 1 {
		t.Fatalf("round trip mismatch: %+v", st3)
	}
}
