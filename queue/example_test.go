package queue_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/idempotent-task-queue-go/queue"
)

func Example() {
	_ = os.RemoveAll("_example")
	dir := filepath.Join("_example", "state.json")
	cfg := queue.Config{
		VisibilityTimeout: 200 * time.Millisecond,
		MaxAttempts:       3,
		Backoff: queue.ExponentialBackoff{
			Base: 5 * time.Millisecond, Multiplier: 2, Max: 50 * time.Millisecond,
		},
	}
	q := queue.New(cfg, queue.NewFileStore(dir))
	if err := q.Open(); err != nil {
		log.Fatal(err)
	}

	// 幂等提交：重复提交同 ID 同载荷得到 duplicate_submission，不重复生效。
	if _, err := q.Submit("order-1", []byte(`{"amount":100}`)); err != nil {
		log.Fatal(err)
	}
	if _, err := q.Submit("order-1", []byte(`{"amount":100}`)); err != nil {
		if re, ok := queue.AsReject(err); ok {
			fmt.Println("resubmit rejected:", re.Reason)
		}
	}

	var sideEffects int32
	h := func(ctx context.Context, t *queue.Task) error {
		// 用 RunIdempotent 包装外部副作用：重投/重启后至多执行一次。
		ran, err := q.RunIdempotent(t.ID, t.DeliveryToken, func(ctx context.Context) error {
			atomic.AddInt32(&sideEffects, 1)
			return nil
		})
		if err != nil {
			return err // 普通 error => 可重试；queue.NonRetryable("...") => 直接死信
		}
		fmt.Printf("processed order-1, side-effect executed=%v\n", ran)
		return nil
	}

	g := queue.NewConsumerGroup(q, "orders", 2, h)
	g.Start()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = g.Shutdown(ctx)
	_ = q.Close()
	_ = os.RemoveAll("_example")

	// Output:
	// resubmit rejected: duplicate_submission
	// processed order-1, side-effect executed=true
}
