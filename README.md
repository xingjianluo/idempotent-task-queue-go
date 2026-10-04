# idempotent-task-queue-go

本地任务分发与异步处理流程：用进程内 / 本地 JSON 文件持久化存储模拟消息中间件，
提供提交幂等、投递确认、失败分类、有限退避重试、死信、死信人工重放、
多消费者并发、优雅停机与重启恢复能力。详细设计见 [docs/DESIGN.md](docs/DESIGN.md)。

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
`not_in_flight`、`queue_closed`、`replay_batch_conflict`、`replay_limit_exceeded`。

死信原因：`non_retryable` 与 `max_attempts_exceeded`（持久化、可区分）。

## 死信重放（人工介入）

死信任务可以有选择地投回正常处理流程，用于排障后捞回某类任务：

```go
// 按条件批量挑选：死信原因 + 失败文本 + 进入死信的时间范围
res, err := q.Replay("ops-2026-10-04-001", queue.ReplayFilter{
    DLQReason:      queue.DLQReasonMaxAttempts,
    ReasonContains: "connection refused",
    EnteredAfter:   time.Now().Add(-24 * time.Hour),
    Limit:          50,
})

// 或点名单个/多个任务
res, err = q.Replay("ops-2026-10-04-002", queue.ReplayFilter{TaskIDs: []string{"order-7"}})
```

- 只有死信任务可被重放；处理中/已确认/待投递任务不会被误重放（点名命中时记入
  `res.Skipped`）。重放后任务重新走投递、确认与失败分类流程，重试上限与失败分类
  不被绕过：`attempts` 归零（新预算仍受 `max_attempts` 约束），`replay_count`
  记录重放次数，`total_deliveries` 累计全部投递，原始失败历史保留可区分。
- `batchID` 是幂等键：同一批次重复触发返回首次结果（`res.Duplicate=true`）不重复
  投放；同 ID 换条件被拒绝（`replay_batch_conflict`）。与运行中的消费者、并发
  提交/确认安全并存；状态与批次记录原子落盘，失败整体回滚，重启后不丢、不重复生效。
- 容量与背压：`Config.MaxReplayPerBatch`（默认 100）限制单次数量，
  `Config.MaxReplayTotal`（默认 1000）限制累计总量，超限明确拒绝
  （`replay_limit_exceeded`）并说明原因；设负数表示不限。既有调用方无需改动。

## 验证

```bash
go test ./... -race -count=1 -v
# 只看重放相关用例（日志含任务标识、投递/重放次数与判定依据）
go test ./queue/ -run 'TestReplay' -race -count=1 -v
```
