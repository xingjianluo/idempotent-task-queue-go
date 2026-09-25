# idempotent-task-queue-go

本地任务分发与异步处理流程：用进程内 / 本地 JSON 文件持久化存储模拟消息中间件，
提供提交幂等、投递确认、失败分类、有限退避重试、死信、多消费者并发、
优雅停机与重启恢复能力。详细设计见 [docs/DESIGN.md](docs/DESIGN.md)。

## 快速上手

```go
q := queue.New(queue.Config{
    VisibilityTimeout: 200 * time.Millisecond,
    MaxAttempts:       3,
    Backoff: queue.ExponentialBackoff{Base: 5 * time.Millisecond, Multiplier: 2, Max: 50 * time.Millisecond},
}, queue.NewFileStore("data/state.json"))
_ = q.Open()

_, err := q.Submit("order-1", payload) // 重复提交同 ID 同载荷 -> duplicate_submission

g := queue.NewConsumerGroup(q, "orders", 4, func(ctx context.Context, t *queue.Task) error {
    // 外部副作用用 RunIdempotent 包装：重投/重启后至多执行一次
    ran, err := q.RunIdempotent(t.ID, t.DeliveryToken, func(ctx context.Context) error {
        return doSideEffect(t)
    })
    if err != nil {
        // 返回普通 error：可重试；queue.NonRetryable("...")：直接死信
        return err
    }
    _ = ran
    return nil
})
g.Start()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
_ = g.Shutdown(ctx) // 宽限期内完成在途任务；超时遗留的任务重启后按租约重投
_ = q.Close()
```

拒绝原因（`queue.AsReject(err)` 判别）：`duplicate_submission`、`payload_conflict`、
`already_acked`、`stale_delivery`、`dead_lettered`、`unknown_delivery`、
`not_in_flight`、`queue_closed`。

死信原因：`non_retryable` 与 `max_attempts_exceeded`（持久化、可区分）。

## 验证

```bash
go test ./... -race -count=1 -v
```
