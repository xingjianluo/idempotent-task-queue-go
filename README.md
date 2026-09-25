# 本地任务分发与异步处理流程（幂等任务队列）

用**进程内 / 本地文件持久化存储模拟消息中间件**，实现任务提交、异步消费、
投递确认、失败重试、死信、多消费者并发与优雅停机/重启恢复。核心目标：

- 重复投递、重复提交、重复消费**不得重复生效**；
- 消费者中断后**未确认任务可重新投递**，**已确认任务绝不重投**；
- 拒绝提交/确认时返回**可区分的原因码**；
- 失败分**可重试 / 不可重试**两类，可重试失败按**有上限退避**重试，超限或不可重试进死信，
  两类死信原因在**重启后仍一致且可区分**；
- 并发与停机/重启期间**不丢确认、不重复确认**，在途任务按确定语义完成或重新入队。

## 目录结构

```
queue/            队列核心
  task.go         任务模型、状态、失败与死信原因、*Failure 错误
  errors.go       RejectError / RejectCode：可区分的拒绝原因
  queue.go        提交、投递(租约)、Ack/Nack、退避重试、死信、回收与停机
  consumer.go     多 worker 并发消费 + 租约回收 + 优雅停机
  backoff.go      有上限的指数退避
  store.go        Store 接口：MemoryStore（进程内）/ FileStore（本地 JSON 原子持久化）
  clock.go        时钟抽象（真实时钟 / 测试可控 FakeClock）
effect/           本地持久化的幂等副作用台账（演示“重复投递不重复生效”）
cmd/demo/         端到端命令行演示
```

## 任务状态流转

```
                         提交(Submit，ID 去重)
                                │
                                ▼
                         ┌───────────┐
          退避到期可投递  │  pending  │◀──────────────┐
          ┌────────────▶│ (待投递)   │               │
          │             └─────┬─────┘               │
          │                   │ Fetch：占用租约       │ 租约超时回收
          │                   ▼                     │ (RequeueExpired)
          │             ┌───────────┐               │ 优雅停机回收
          │   Nack      │  leased   │───────────────┘ (Close/Reclaim)
          │  (可重试且   │ (投递占用) │
          │   未到上限)  └───┬────┬──┘
          │                 │    │
          │      Ack 成功    │    │ Nack 不可重试
          │                 ▼    ▼ 或 Attempts>=MaxAttempts
          │           ┌───────────┐  ┌───────────┐
          └───────────│ succeeded │  │   dead    │
                      │  (终态)    │  │ (死信/终态) │
                      └───────────┘  └───────────┘
```

- `pending`：等待投递。首次提交立即可投递；重试任务的 `AvailableAt = 失败时间 + 退避`，
  退避未到期不会被领取，因此**失败任务不会长期占用 worker/处理资源**。
- `leased`：已投递给某个消费者，携带单调递增的**投递令牌 DeliveryTag** 与**租约到期时间 LeaseUntil**。
- `succeeded`：Ack 成功后的终态，**永不重投**。
- `dead`：死信终态，`DeadReason` 区分两类原因：
  - `non_retryable`：处理结果明确为不可重试失败（首次即进死信）；
  - `retry_exhausted`：可重试失败累计 `Attempts >= MaxAttempts`。

> 进程崩溃（未优雅停机）后遗留的 `leased` 任务，在 `NewQueue` 启动恢复时一律回到
> `pending`（`PendingReason=restart_recovery`，旧 DeliveryTag 作废）；已 `succeeded`/`dead`
> 的任务保持终态不变。

## 幂等与并发约定

### 提交幂等

- 任务 `ID` 是业务幂等键。相同 ID 重复提交**不会再创建任务、不会重复生效**；
  `Submit` 返回已存在任务、`created=false`，并返回
  `*RejectError{Code: RejectDuplicateSubmit}` 以便调用方区分“新建”与“重复”。

### 投递与确认（at-least-once + 令牌防重复确认）

- 投递语义为**至少一次**：消费者在 Ack 前崩溃/中断，任务会在租约超时后重新投递
  （`delivery` 次数 +1，产生新的 DeliveryTag）。
- **只有当前租约持有者能 Ack/Nack**。`Ack(id, tag)` / `Nack(id, tag, failure)` 会校验：
  - 任务存在？否则 `task_not_found`
  - 任务处于 `leased`？否则 `not_leased`（已确认任务再确认走此码）
  - 令牌是否为最新？否则 `lease_mismatch`（旧消费者在重投后醒来确认，被安全拒绝）
  - 队列是否已关闭？否则 `queue_closed`
- 状态变更遵循 **WAL 式顺序**：在互斥锁内修改内存状态 → **先原子持久化落盘** → 再对调用方返回。
  因此“确认返回成功”意味着已落盘，**停机/崩溃不会丢确认**；持久化失败会回滚占用，
  不会出现未持久化的投递。
- 选择任务按 `(AvailableAt, ID)` 确定顺序，多 worker 竞争下行为可预期。

### 副作用幂等（effect 台账）

至少一次语义下，同一任务可能被处理多次。要做到“**重复投递不产生重复副作用**”，
处理逻辑必须以任务 ID 为幂等键：`effect.Ledger.ApplyOnce(taskID, fn)` 保证 `fn`
只在首次执行并落盘，重复投递直接跳过；台账为本地 JSONL 追加文件，**重启后判定一致**。
生产系统中应将幂等键与副作用置于同一事务/条件写入中；本仓库的台账是这一约定的本地模拟实现。

### 并发与优雅停机

- `Consumer` 启动 `Concurrency` 个 worker + 一个租约回收器（reaper）。
- worker 的**处理过程不响应停机取消**（处理使用独立 context）：停机不会把一个副作用做一半。
  停机时每个在途任务只有两种确定结局：
  1. 在宽限 `ShutdownWait` 内完成 Ack/Nack → 以终态落盘；
  2. 宽限到期仍未确认 → `Queue.Close` 统一回收为 `pending` 并持久化，旧令牌立即作废；
     之后即使旧处理完成回调 Ack，也会被 `lease_mismatch`/`queue_closed` 拒绝，重启后重新投递。
- `Queue.Close` 幂等，可安全多次调用。

## 重试与死信规则

- `Handler` 返回值约定：
  - `nil`：成功，自动 Ack；
  - `*queue.Failure{Kind: FailNonRetryable}`：不可重试，**立即进死信**（`non_retryable`）；
  - `*queue.Failure{Kind: FailRetryable}` 或其它任意 `error`：计为一次可重试尝试；
- 每次可重试 Nack：`Attempts++`，当 `Attempts >= MaxAttempts` 时进死信（`retry_exhausted`），
  否则按退避回到 `pending`；
- 退避为**有上限指数退避**：
  `delay = min(BaseMillis * Multiplier^(attempt-1), MaxMillis)`，
  有乘法溢出保护，退避期间任务处于 pending、不占用 worker。
- 所有失败记录保存在 `Task.Failures` 中，作为判定依据随状态持久化。

## 拒绝原因码（可区分）

| Code                  | 场景                                                   |
|-----------------------|--------------------------------------------------------|
| `duplicate_submit`    | 相同任务 ID 重复提交（幂等返回已有任务，不重复生效）       |
| `queue_closed`        | 队列关闭后再 Submit/Ack/Nack                           |
| `task_not_found`      | Ack/Nack 不存在的任务（含空 ID 提交）                    |
| `not_leased`          | 对非 leased 任务（如已 succeeded）做 Ack/Nack           |
| `lease_mismatch`      | DeliveryTag 不匹配（租约失效/已重投，旧消费者无权确认）    |

## 持久化与重启恢复

- `FileStore`：整体状态序列化为 JSON，写入临时文件 → `fsync` → 原子 `rename`（+ 目录 fsync），
  状态文件始终是完整一致的上一个快照。
- `NewQueue` 加载后执行启动恢复：
  - 上次遗留 `leased` → `pending`（可立即重投），旧令牌作废，`Interruptions++`；
  - `succeeded` 保持成功（确认不丢、不重投）；
  - `dead` 保持死信，`DeadReason` 不变（两类原因重启后仍可区分）；
  - 退避中的 `pending` 保留 `AvailableAt`。

## 验证方法

运行全部单元测试（含竞态检测）：

```bash
go test ./... -race -count=1 -v
```

覆盖场景：

- `TestDuplicateSubmitNotEffective`：重复提交不重复生效，原因码 `duplicate_submit`；
- `TestAckIdempotencyAndRejectionReasons`：已确认不重投，重复/越权确认原因可区分；
- `TestLeaseExpiryRedelivery` / `TestReaperRunsInConsumer`：消费者中断后租约超时重投，旧令牌 Ack 被拒；
- `TestRetryWithBackoffThenSuccess`：有上限退避，退避期不投递，重投后成功；
- `TestNonRetryableGoesDirectlyDead`：不可重试失败直接死信；
- `TestRetryExhaustedGoesDead`：超过重试上限死信，两类死信原因可区分；
- `TestConcurrentConsumersNoDuplicateAck`：8 worker × 40 任务，每个任务只处理/确认一次；
- `TestGracefulShutdownWithConsumer` / `TestShutdownInFlightRequeued`：优雅停机在途任务确定回收，重启继续；
- `TestCrashRecoveryWithFileStore`：模拟崩溃后重启，终态保持、未确认任务恢复；
- `TestDeadReasonsPersistAcrossRestart`：两类死信原因重启后一致且可区分；
- `effect` 包：同进程/并发/重启三种情况下副作用只生效一次。

测试日志（`-v` 可见）统一打印 **任务标识 `id`、投递次数 `delivery`、判定依据 `basis=...`**，
关键 basis：`submit_accepted` / `duplicate_submit` / `claimed` / `ack_confirmed` /
`ack_rejected(code=...)` / `retry_backoff` / `dead_non_retryable` /
`dead_retry_exhausted` / `lease_expired` / `graceful_shutdown` / `restart_recovery`。

端到端演示：

```bash
go run ./cmd/demo -dir ./tmpdemo submit     # 预置任务（含重试型/不可重试型/正常型）
go run ./cmd/demo -dir ./tmpdemo consume    # 消费：重试、死信、成功，8s 后优雅停机
go run ./cmd/demo -dir ./tmpdemo consume    # 再次运行：从文件重启恢复，无重复副作用
go run ./cmd/demo -dir ./tmpdemo submit     # 重复提交：返回已有状态、不重复生效
```

## 语义边界（设计说明）

- 对**任意外部副作用**，崩溃恢复只能保证 at-least-once 投递；exactly-once 需副作用自身幂等
  （见 `effect.Ledger` 约定）。
- `FileStore` 为单进程本地存储；多实例部署时应替换为带租约/事务的外部存储并实现 `Store`。
