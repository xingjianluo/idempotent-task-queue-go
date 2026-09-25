# 本地任务分发与异步处理流程 — 设计说明

用进程内（`MemoryStore`）或本地 JSON 文件（`FileStore`，临时文件 + 原子 rename）
持久化存储模拟消息中间件，实现具备**投递确认、幂等提交、失败分类、有限退避重试、
死信、多消费者并发、优雅停机与重启恢复**能力的本地任务队列。

所有状态变更在同一把互斥锁内完成并**立即全量持久化**，因此崩溃/重启恢复后的状态
与最后一次成功落盘一致、可解释。

## 1. 任务状态流转

```
                 Submit(同ID同载荷: duplicate_submission / 不同载荷: payload_conflict)
   (不存在) ───────────────────────────────────────────────► pending
                                                                  │ Fetch（可见时间到期）
                                                                  │ Attempts++, Token++
                                                                  ▼
                                                              in_flight
                         ┌──────────────────┬───────────────────┼──────────────────────┐
                  Ack    │  Nack(retryable) │ Nack(non-retryable)│ 租约超时未确认        │ 已Ack/已死信
                  (token │ Attempts<Max     │                   │ (消费者中断/重启恢复)  │ 再 Ack:
                   匹配)  ▼                  ▼                   ▼                      │ already_acked/
                     completed        pending(退避等待)     判定分支                    │ dead_lettered
                                      VisibleAt=now+backoff ├ Attempts>=Max ─► dead_lettered
                                                            └ Attempts<Max   ─► pending(退避)
   non_retryable 或 Attempts>=Max 时的可重试失败 ──► dead_lettered
```

| 状态 | 含义 |
| --- | --- |
| `pending` | 已提交，等待投递；也包含退避等待中的任务（由 `visible_at` 控制） |
| `in_flight` | 已投递给消费者，租约（`lease_expires_at`）未过期 |
| `completed` | 已被当前有效投递 Ack 的终态，**永不重投** |
| `dead_lettered` | 死信终态，**永不重投**，记录可区分的 `dlq_reason` |

## 2. 幂等与并发约定

### 提交幂等
- 以业务任务 ID 为幂等键，并对载荷做 SHA-256 摘要（`payload_hash`）。
- 重复提交（同 ID 同载荷）返回 `RejectError{Reason: duplicate_submission}`，任务不变化、不重复生效。
- 同 ID 不同载荷返回 `payload_conflict`。两类原因可通过 `queue.AsReject` 精确区分。

### 投递与确认幂等（delivery token）
- 每次成功投递：`Attempts++` 且单调递增的 `DeliveryToken++`。
- `Ack/Nack` 必须携带投递时拿到的 token：
  - token 匹配且任务处于 `in_flight` 才生效；
  - 任务已确认 → `already_acked`；已死信 → `dead_lettered`；
  - token 过期（任务已被重投）的迟到确认 → `stale_delivery`；
  - 未知任务 → `unknown_delivery`；当前不在投递态 → `not_in_flight`。
- 因此**重复消费的迟到确认不会重复生效，未确认的任务可被安全重投，已确认的任务不会被重投**。

### 并发
- `ConsumerGroup` 启动 N 个 worker 并发 `Fetch`；队列内部用一把锁 + `sync.Cond` 调度，
  同一时刻同一任务只会被一个 worker 租约持有。
- 每次状态转移立即落盘；租约是唯一的“在途”所有权凭证，天然防止并发重复处理同一任务。

### 副作用幂等（at-most-once，可解释）
- 处理器可用 `q.RunIdempotent(taskID, token, effect)` 包装外部副作用：
  1. 先在持久化任务上**认领**（`side_effect_done=true` 立即落盘），仅当前有效投递可认领；
  2. 认领成功才执行 `effect`；成功后由消费者 Ack；
  3. 认领返回 `ran=false` 表示此前已执行/已认领（重投、重启恢复），必须跳过副作用直接 Ack；
  4. `effect` 失败时在当前投递仍有效的前提下撤销认领，再 Nack 重试。
- 这样“副作用已完成但 Ack 前崩溃”的任务在恢复重投时只会跳过，不产生重复副作用。
- 注意语义边界：崩溃若发生在“认领之后、effect 完成之前”，按 at-most-once 不重放
  （状态可由 `side_effect_done` 解释）。需要恰好一次的外部动作必须自身幂等，
  可结合 `RunIdempotent` 做双层防护。

## 3. 重试与死信规则

### 失败分类（`ClassifyFailure`）
- 处理器返回 `nil`：成功 → Ack。
- 返回 `queue.NonRetryable(reason)`（或被包装的 `*NonRetryableError`）：**不可重试**。
- 其他任意非 nil 错误：**可重试**。
- 分类结果与原因文本持久化到 `last_failure_class / last_failure_reason`，重启后一致。

### 有限退避重试
- 可重试失败后任务回到 `pending`，`visible_at = now + Backoff.Next(attempts)`。
- 默认实现为**有上限的指数退避**：`min(Base * Multiplier^(attempt-1), Max)`。
- 退避期间任务不可被 Fetch，因此失败任务不会长期占用 worker；上限避免退避无限增长。
- 投递次数 `attempts` 达到 `max_attempts` 后不再重试。

### 死信
进入 `dead_lettered` 的两种**可区分、重启后保持一致**的原因：
- `non_retryable`：处理器明确判定不可重试，首次投递即死信；
- `max_attempts_exceeded`：可重试失败累计投递次数达到上限
  （Nack 路径与租约超时回收路径用同一判定，结论一致）。

死信可通过 `q.DeadLetters()` 枚举，含最后失败类别/原因与入死信原因，供人工或补偿流程处理。

### 租约与重投（at-least-once）
- 每次投递设定 `visibility_timeout` 租约。后台 reaper（以及重启后的 `ReapExpired`）
  回收 `in_flight` 且租约过期的任务：计入重试上限判定，超限入死信，否则退避后重投。
- 正常 Nack 与“中断未确认”在重试计数上口径一致：**每次成功投递都消耗一次 attempt**。

## 4. 优雅停机与重启恢复

- `ConsumerGroup.Shutdown(ctx)`：关闭拉取并停止 reaper，等待所有在途 handler 结束并完成 Ack/Nack。
- 在宽限期内结束的任务 → 正常完成（确定语义：**完成**）。
- 宽限期仍未结束的任务 → 保持 `in_flight`（已落盘），进程退出；重启后由租约回收重投
  （确定语义：**重新入队**）。配合 `RunIdempotent`，副作用不重复；未确认的任务不丢。
- 重启 `Open()` 后：`completed/dead_lettered` 终态及死信原因原样恢复；
  遗留 `in_flight` 不自动改动，等租约过期再回收，行为与消费者崩溃完全一致。

## 5. 代码结构

| 文件 | 职责 |
| --- | --- |
| `queue/task.go` | 状态/失败/死信原因类型定义与任务快照 |
| `queue/errors.go` | 拒绝原因、`RejectError`、`NonRetryableError`、失败分类 |
| `queue/backoff.go` | 有上限的指数退避 |
| `queue/store.go` | 内存存储与原子落盘的 JSON 文件存储 |
| `queue/queue.go` | 提交/Fetch/Ack/Nack/租约回收/死信状态机 |
| `queue/executor.go` | 持久化认领的幂等副作用执行器 |
| `queue/consumer.go` | 多消费者并发组、reaper、优雅停机 |

## 6. 验证方法

```bash
go test ./... -race -count=1 -v
```

测试覆盖（日志均打印任务标识、投递次数 attempts 与判定依据）：
- `TestSubmitIdempotency` / `TestRestartDuplicateSubmitIdempotent`：重复提交、载荷冲突，重启后仍可区分；
- `TestAckRejectReasons` / `TestNackRetryThenAck`：重复/迟到 Ack 的各拒绝原因、旧 token Nack 拒绝；
- `TestClassifyFailure` / `TestNonRetryableGoesDirectlyToDLQ`：失败分类与不可重试直接死信；
- `TestRetryableExhaustionGoesToDLQ`：退避重试到上限入死信，两种死信原因并存仍可区分；
- `TestBackoffCapped`：退避上限；
- `TestConsumerInterruptionRequeue`：消费者中断、租约过期重投、已确认任务不重投；
- `TestRestartRecoveryInFlight`：跨重启恢复在途/完成/死信状态并重投完成；
- `TestRunIdempotent*`：重投与重启后副作用至多一次、失败后认领撤销可重试；
- `TestConsumerGroupConcurrent`：4 worker 并发 50 任务，每任务恰好处理一次、全部确认；
- `TestConsumerGroupGracefulShutdownInFlight`：宽限期内在途任务完成；
- `TestShutdownTimeoutRequeue`：停机超时遗留任务重启后租约回收重投并完成；
- `Example`：提交→幂等副作用→消费确认的端到端示例。
