# idempotent-task-queue-go

本地任务分发与异步处理流程：用进程内 / 本地 JSON 文件持久化存储模拟消息中间件，
提供提交幂等、投递确认、失败分类、有限退避重试、死信、**死信人工重放（批次幂等、
容量背压）**、多消费者并发、优雅停机与重启恢复能力。详细设计见
[docs/DESIGN.md](docs/DESIGN.md)。

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
`not_in_flight`、`not_dead_lettered`、`replay_batch_too_large`、
`replay_quota_exceeded`、`invalid_replay`、`queue_closed`。

死信原因：`non_retryable` 与 `max_attempts_exceeded`（持久化、可区分）。

## 死信人工重放

死信任务可人工捞回正常处理流程，支持点名与按条件批量挑选，操作本身以
`BatchID` 为幂等键（同一批次重复触发不会重复投出）：

```go
// 点名单个任务
res, err := q.Replay(queue.ReplayRequest{BatchID: "ops-001", TaskID: "order-1"})

// 按条件批量：死信原因 / 最后失败原因子串 / 进入死信时间范围 / 数量上限，
// 结果按进入死信先后稳定排序
res, err = q.Replay(queue.ReplayRequest{
    BatchID: "ops-002",
    Filter: queue.ReplayFilter{
        DLQReason:    queue.DLQReasonMaxAttempts,
        ReasonSubstr: "timeout",
        EnteredFrom:  yesterday,
        EnteredTo:    time.Now(),
        Limit:        100,
    },
})
res.AlreadyApplied // 同批次重复触发时为 true，任务不会被再次投出
```

约定要点（详见 DESIGN.md 第 4 节）：

- 重放只把任务从 `dead_lettered` 原子置回 `pending`，之后仍走正常投递、确认和
  失败分类；不可重试失败仍直接死信，可重试失败仍受 `max_attempts` 约束（每轮
  重放有独立预算，历史投递次数不清零）。任务上可用 `Attempts`、`ReplayCount`、
  `ReplayAttempts` 区分原始处理历史与各轮重放。
- 处理中、已完成或不存在的任务不能重放（`not_dead_lettered`）；可与多个正在
  运行的消费者并存；批次记录与任务状态同快照原子落盘，重启后不丢任务、不重复
  生效；落盘失败整批回滚。
- 容量与背压（默认 0 = 不限，兼容现有用法）：`Config.MaxReplayBatchSize`
  （单次上限）、`Config.MaxReplayTotal`（累计上限），超限时分别以
  `replay_batch_too_large` / `replay_quota_exceeded` 拒绝并说明数量与上限。

## 验证

```bash
go test ./... -race -count=1 -v
go test ./... -race -count=10                     # 重复运行确认稳定可复现
go test ./queue/ -run TestReplay -race -count=1 -v
```
