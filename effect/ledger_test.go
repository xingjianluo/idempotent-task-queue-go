package effect

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// TestApplyOnceInProcess 同进程内重复调用只生效一次。
func TestApplyOnceInProcess(t *testing.T) {
	l, err := NewLedger("")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var calls int
	for i := 0; i < 5; i++ {
		applied, err := l.ApplyOnce(context.Background(), "job-1", func() error {
			calls++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && !applied {
			t.Fatalf("first call must report applied=true")
		}
		if i > 0 && applied {
			t.Fatalf("repeat call %d must report applied=false", i)
		}
	}
	if calls != 1 {
		t.Fatalf("effect executed %d times, want 1", calls)
	}
	if !l.Seen("job-1") {
		t.Fatal("Seen must be true after apply")
	}
}

// TestApplyOnceConcurrent 并发重复投递同一任务：副作用只执行一次。
func TestApplyOnceConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, err := NewLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var calls int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = l.ApplyOnce(context.Background(), "same-job", func() error {
				mu.Lock()
				calls++
				mu.Unlock()
				return nil
			})
		}()
	}
	close(start)
	wg.Wait()
	if calls != 1 {
		t.Fatalf("concurrent effect executed %d times, want 1", calls)
	}
}

// TestApplyOncePersistAcrossRestart 重启后台账判定一致：已生效任务不重复执行；
// 新任务仍可执行；失败的 effect 不落盘、允许重试。
func TestApplyOncePersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")

	l1, _ := NewLedger(path)
	calls1 := 0
	applied, err := l1.ApplyOnce(context.Background(), "persisted-job", func() error {
		calls1++
		return nil
	})
	if err != nil || !applied {
		t.Fatalf("first lifecycle applied=%v err=%v", applied, err)
	}
	// 失败的副作用不记录。
	wantErr := errors.New("side effect failed")
	if _, err := l1.ApplyOnce(context.Background(), "failed-job", func() error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("want propagated error, got %v", err)
	}
	_ = l1.Close()

	// 重新打开（模拟重启）。
	l2, err := NewLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	calls2 := 0
	applied2, err := l2.ApplyOnce(context.Background(), "persisted-job", func() error {
		calls2++
		return nil
	})
	if err != nil || applied2 {
		t.Fatalf("after restart persisted-job applied=%v err=%v, want false/nil", applied2, err)
	}
	if calls2 != 0 {
		t.Fatalf("after restart effect ran %d times, want 0", calls2)
	}
	// 之前失败、未记录的任务在重启后仍允许执行。
	applied3, err := l2.ApplyOnce(context.Background(), "failed-job", func() error { return nil })
	if err != nil || !applied3 {
		t.Fatalf("failed-job should be allowed after restart: applied=%v err=%v", applied3, err)
	}
}
