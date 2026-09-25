package queue

import (
	"context"
	"testing"
	"time"
)

// TestRunIdempotentNoDuplicateOnRedelivery：副作用执行成功但确认前租约丢失，
// 重投后副作用不得再次执行，但任务最终仍被确认。
func TestRunIdempotentNoDuplicateOnRedelivery(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, 100*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	_, _ = q.Submit("t1", []byte("p"))

	execs := 0
	effect := func(ctx context.Context) error {
		execs++
		t.Logf("task=t1 side-effect EXECUTED exec_count=%d", execs)
		return nil
	}

	d1, _ := q.Fetch(context.Background())
	ran, err := q.RunIdempotent("t1", d1.Token, effect)
	if err != nil || !ran {
		t.Fatalf("first run: ran=%v err=%v", ran, err)
	}
	if execs != 1 {
		t.Fatalf("expected 1 execution, got %d", execs)
	}
	// 模拟：副作用已完成、SideEffectDone 已落盘，但 Ack 前进程/消费者中断。
	// 旧 token 的迟到 Ack 也应被拒绝（若它“恰好”到达）。
	mustReject(t, q.Ack("t1", d1.Token+1), ReasonStaleDelivery, "late ack with wrong token")
	clk.advance(101 * time.Millisecond)
	q.ReapExpired()
	clk.advance(6 * time.Millisecond)

	d2, _ := q.Fetch(context.Background())
	if d2.Task.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", d2.Task.Attempts)
	}
	// 重投：RunIdempotent 必须跳过副作用
	ran, err = q.RunIdempotent("t1", d2.Token, effect)
	if err != nil || ran {
		t.Fatalf("redelivery run: ran=%v err=%v (want skipped)", ran, err)
	}
	if execs != 1 {
		t.Fatalf("side effect duplicated! execs=%d", execs)
	}
	t.Logf("task=t1 attempts=2 side-effect SKIPPED basis=SideEffectDone_persisted exec_count=1")
	if err := q.Ack("t1", d2.Token); err != nil {
		t.Fatal(err)
	}
	t.Logf("task=t1 attempts=2 verdict=completed exec_count=1")
}

// TestRunIdempotentFailureReleasesClaim：副作用失败后认领被撤销，下次重试仍可执行。
func TestRunIdempotentFailureReleasesClaim(t *testing.T) {
	clk := newFakeClock()
	q, _ := newTestQueue(t, clk, 3, time.Second, time.Millisecond, 10*time.Millisecond)
	_, _ = q.Submit("t1", []byte("p"))

	execs := 0
	d1, _ := q.Fetch(context.Background())
	ran, err := q.RunIdempotent("t1", d1.Token, func(ctx context.Context) error {
		execs++
		return assertErr("effect failed")
	})
	if !ran || err == nil {
		t.Fatalf("expected ran=true with error, got ran=%v err=%v", ran, err)
	}
	if q.SideEffectDone("t1") {
		t.Fatal("claim must be released after effect failure")
	}
	_ = q.Nack("t1", d1.Token, FailureRetryable, err.Error())
	clk.advance(5 * time.Millisecond)

	d2, _ := q.Fetch(context.Background())
	ran, err = q.RunIdempotent("t1", d2.Token, func(ctx context.Context) error { execs++; return nil })
	if err != nil || !ran {
		t.Fatalf("retry run: ran=%v err=%v", ran, err)
	}
	if execs != 2 {
		t.Fatalf("expected effect to run again, execs=%d", execs)
	}
	_ = q.Ack("t1", d2.Token)
	t.Logf("task=t1 attempts=2 verdict=completed exec_count=%d basis=claim_released_on_failure", execs)
}

// TestRunIdempotentStateSurvivesRestart：认领状态持久化，重启后重投仍跳过副作用。
func TestRunIdempotentStateSurvivesRestart(t *testing.T) {
	clk := newFakeClock()
	cfg := Config{
		VisibilityTimeout: 100 * time.Millisecond,
		MaxAttempts:       3,
		Backoff:           ExponentialBackoff{Base: 5 * time.Millisecond, Multiplier: 2, Max: 50 * time.Millisecond},
		Now:               clk.now,
	}
	q, path := newFileTestQueue(t, cfg)
	_, _ = q.Submit("t1", []byte("p"))
	d1, _ := q.Fetch(context.Background())
	if _, err := q.RunIdempotent("t1", d1.Token, func(ctx context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	t.Logf("task=t1 attempts=1 side-effect executed and claimed, then process exits before ack")
	_ = q.Close()

	clk.advance(200 * time.Millisecond)
	q2 := New(cfg, NewFileStore(path))
	if err := q2.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q2.Close() })
	if !q2.SideEffectDone("t1") {
		t.Fatal("side effect claim must survive restart")
	}
	q2.ReapExpired()
	clk.advance(6 * time.Millisecond)
	d2, _ := q2.Fetch(context.Background())
	ran, err := q2.RunIdempotent("t1", d2.Token, func(ctx context.Context) error {
		t.Fatal("side effect must not run again after restart")
		return nil
	})
	if err != nil || ran {
		t.Fatalf("expected skip after restart, ran=%v err=%v", ran, err)
	}
	_ = q2.Ack("t1", d2.Token)
	t.Logf("task=t1 attempts=2 verdict=completed after restart, side-effect skipped basis=persisted_claim")
}
