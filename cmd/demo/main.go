// 命令行演示：提交 -> 消费 -> 失败分类与重试 -> 死信 -> 停机 -> 重启恢复。
// 用法：
//
//	go run ./cmd/demo -dir ./tmpdemo submit    # 提交演示任务并退出（未消费）
//	go run ./cmd/demo -dir ./tmpdemo consume   # 启动消费者处理一轮后优雅停机
//	go run ./cmd/demo -dir ./tmpdemo consume   # 再次运行即演示重启恢复
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/idempotent-task-queue-go/effect"
	"github.com/xingjianluo/idempotent-task-queue-go/queue"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

func main() {
	dir := flag.String("dir", "./tmpdemo", "state directory")
	flag.Parse()
	mode := "consume"
	if flag.NArg() > 0 {
		mode = flag.Arg(0)
	}
	if err := run(*dir, mode); err != nil {
		log.Fatalf("demo error: %v", err)
	}
}

func run(dir, mode string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	statePath := filepath.Join(dir, "queue-state.json")
	ledgerPath := filepath.Join(dir, "effects.jsonl")

	ctx := context.Background()
	q, err := queue.NewQueue(ctx, queue.NewFileStore(statePath), queue.Config{
		LeaseTTL:    2 * time.Second,
		MaxAttempts: 3,
		Backoff:     &queue.ExponentialBackoff{BaseMillis: 200, Multiplier: 2, MaxMillis: 2000},
		Logger:      stdLogger{},
	})
	if err != nil {
		return err
	}

	if mode == "submit" {
		return submitTasks(ctx, q)
	}

	ledger, err := effect.NewLedger(ledgerPath)
	if err != nil {
		return err
	}
	defer ledger.Close()

	return consume(ctx, q, ledger)
}

func submitTasks(ctx context.Context, q *queue.Queue) error {
	// 一个必失败的重试任务、一个不可重试任务、一个正常任务。
	specs := []queue.TaskSpec{
		{ID: "task-retry", Type: "demo", Payload: "retry-me"},
		{ID: "task-fatal", Type: "demo", Payload: "bad-payload"},
		{ID: "task-ok", Type: "demo", Payload: "hello"},
	}
	for _, s := range specs {
		t, created, err := q.Submit(ctx, s)
		if err != nil {
			var re *queue.RejectError
			if errors.As(err, &re) && re.Code == queue.RejectDuplicateSubmit {
				fmt.Printf("submit id=%s -> duplicate, no effect (existing status=%s)\n", s.ID, t.Status)
				continue
			}
			return err
		}
		fmt.Printf("submit id=%s created=%v\n", s.ID, created)
	}
	return nil
}

func consume(ctx context.Context, q *queue.Queue, ledger *effect.Ledger) error {
	var transientAttempt int64
	handler := func(ctx context.Context, d *queue.Delivery) error {
		// 以任务 ID 为幂等键，保证重复投递不产生重复副作用。
		applied, err := ledger.ApplyOnce(ctx, d.Task.ID, func() error {
			fmt.Printf("side-effect performed for task=%s delivery=%d attempts=%d\n",
				d.Task.ID, d.Task.Delivery, d.Task.Attempts)
			return nil
		})
		if err != nil {
			return err
		}
		if !applied {
			fmt.Printf("side-effect skipped (already applied) task=%s delivery=%d\n", d.Task.ID, d.Task.Delivery)
		}

		switch d.Task.ID {
		case "task-fatal":
			return queue.NewFailure(queue.FailNonRetryable, "payload is invalid, cannot process")
		case "task-retry":
			n := atomic.AddInt64(&transientAttempt, 1)
			if n <= 2 {
				return queue.NewFailure(queue.FailRetryable, "transient failure #"+strconv.FormatInt(n, 10))
			}
		}
		return nil // 成功，Consumer 自动 Ack
	}

	c := queue.NewConsumer(q, handler, queue.ConsumerConfig{
		Name:         "demo-consumer",
		Concurrency:  2,
		ReapInterval: 200 * time.Millisecond,
		ShutdownWait: 3 * time.Second,
		Logger:       stdLogger{},
	})

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()

	// 演示运行 8 秒后优雅停机：未完成任务确定性回收，重启可继续。
	fmt.Println("consumer running for 8s; submit tasks in another shell or pre-seed via 'submit' ...")
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
wait:
	for {
		select {
		case <-timer.C:
			break wait
		case <-time.After(500 * time.Millisecond):
			if allTerminal(q) {
				fmt.Printf("all tasks terminal, dead=%d; shutting down\n", len(q.DeadLetters()))
				break wait
			}
		}
	}
	cancel()
	if err := <-done; err != nil {
		return err
	}

	fmt.Println("dead letters:")
	for _, dl := range q.DeadLetters() {
		fmt.Printf("  id=%s reason=%s attempts=%d\n", dl.ID, dl.DeadReason, dl.Attempts)
	}
	fmt.Println("applied side-effects:", ledger.Keys())
	return nil
}

func allTerminal(q *queue.Queue) bool {
	// 仅检查演示使用的三个已知任务。
	for _, id := range []string{"task-retry", "task-fatal", "task-ok"} {
		t, ok := q.Get(id)
		if !ok {
			return false
		}
		if t.Status != queue.StatusSucceeded && t.Status != queue.StatusDead {
			return false
		}
	}
	return true
}
