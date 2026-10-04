package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Config 队列参数。
type Config struct {
	VisibilityTimeout time.Duration // 租约时长，超时未确认则重新投递
	MaxAttempts       int           // 最大投递次数
	Backoff           Backoff       // 重试退避策略
	MaxReplayPerBatch int           // 单次重放允许的最大任务数；负数表示不限
	MaxReplayTotal    int           // 累计允许重放的任务总数；负数表示不限
	Now               func() time.Time
}

func (c *Config) withDefaults() {
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.Backoff == nil {
		c.Backoff = ExponentialBackoff{Base: 10 * time.Millisecond, Multiplier: 2, Max: time.Second}
	}
	if c.MaxReplayPerBatch == 0 {
		c.MaxReplayPerBatch = 100
	}
	if c.MaxReplayTotal == 0 {
		c.MaxReplayTotal = 1000
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Queue 本地任务队列核心。所有状态变更在同一把锁内完成并立即持久化，
// 因此崩溃/重启恢复后状态与最后一次成功落盘一致。
type Queue struct {
	cfg   Config
	store Store

	mu            sync.Mutex
	tasks         map[string]*Task
	replayBatches map[string]*ReplayBatch
	replayedTotal int // 累计已重放任务数（容量背压判定用）
	closed        bool
	cond          *sync.Cond // 用于唤醒阻塞中的 Fetch
}

// New 创建队列。
func New(cfg Config, store Store) *Queue {
	cfg.withDefaults()
	q := &Queue{cfg: cfg, store: store, tasks: map[string]*Task{}, replayBatches: map[string]*ReplayBatch{}}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Open 加载持久化状态；重启后 in_flight 任务视为租约待回收。
// 存储支持 replayStore 扩展时一并恢复重放批次记录与累计重放计数。
func (q *Queue) Open() error {
	tasks, err := q.store.Load()
	if err != nil {
		return err
	}
	batches := map[string]*ReplayBatch{}
	if rs, ok := q.store.(replayStore); ok {
		if batches, err = rs.LoadReplay(); err != nil {
			return err
		}
	}
	total := 0
	for _, b := range batches {
		total += len(b.TaskIDs)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = tasks
	q.replayBatches = batches
	q.replayedTotal = total
	return nil
}

// persist 调用时须持有 q.mu。
// 存储支持 replayStore 扩展时，任务与重放批次记录在同一份快照里原子落盘。
func (q *Queue) persistLocked() error {
	if rs, ok := q.store.(replayStore); ok {
		return rs.SaveAllWithReplay(q.tasks, q.replayBatches)
	}
	return q.store.SaveAll(q.tasks)
}

// Close 持久化状态并关闭队列。重复关闭安全。
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	err := q.persistLocked()
	q.cond.Broadcast()
	q.mu.Unlock()
	return err
}

func hashPayload(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

// Submit 幂等提交任务。
//   - ID 首次出现：创建 pending 任务；
//   - ID 已存在且载荷一致：返回已有任务，拒绝原因为 duplicate_submission（不重复生效）；
//   - ID 已存在但载荷不一致：拒绝，原因为 payload_conflict。
func (q *Queue) Submit(id string, payload []byte) (*Task, error) {
	if id == "" {
		return nil, fmt.Errorf("queue: task id must not be empty")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, &RejectError{Op: "submit", Reason: ReasonQueueClosed, TaskID: id, Message: "queue is closed"}
	}
	h := hashPayload(payload)
	if t, ok := q.tasks[id]; ok {
		if t.PayloadHash == h {
			return t.Clone(), &RejectError{Op: "submit", Reason: ReasonDuplicateSubmission, TaskID: id, Message: "task already exists with identical payload"}
		}
		return nil, &RejectError{Op: "submit", Reason: ReasonPayloadConflict, TaskID: id, Message: "task id already exists with a different payload"}
	}
	now := q.cfg.Now()
	t := &Task{
		ID:          id,
		Payload:     append([]byte(nil), payload...),
		PayloadHash: h,
		Status:      StatusPending,
		MaxAttempts: q.cfg.MaxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
		VisibleAt:   now,
	}
	q.tasks[id] = t
	if err := q.persistLocked(); err != nil {
		delete(q.tasks, id)
		return nil, err
	}
	q.cond.Broadcast()
	return t.Clone(), nil
}

// pickDueLocked 返回一个当前可见的 pending 任务（visible_at 到期）。调用方持锁。
func (q *Queue) pickDueLocked(now time.Time) *Task {
	var best *Task
	for _, t := range q.tasks {
		if t.Status != StatusPending || t.VisibleAt.After(now) {
			continue
		}
		if best == nil || t.VisibleAt.Before(best.VisibleAt) ||
			(t.VisibleAt.Equal(best.VisibleAt) && t.ID < best.ID) {
			best = t
		}
	}
	return best
}

// Fetch 取出一个可见任务并加租约；无可见任务时阻塞等待。
// 每次成功投递 Attempts++ 且 DeliveryToken++，旧 token 的确认一律拒绝，
// 从而保证“同一任务被重复消费时不得重复生效”。
func (q *Queue) Fetch(ctx context.Context) (*Delivery, error) {
	done := make(chan struct{})
	defer close(done)

	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.closed {
			return nil, ErrQueueClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if t := q.pickDueLocked(q.cfg.Now()); t != nil {
			return q.deliverLocked(t)
		}
		q.cond.Wait()
	}
}

// deliverLocked 把任务置为 in_flight 并落盘。调用方持锁。
func (q *Queue) deliverLocked(t *Task) (*Delivery, error) {
	now := q.cfg.Now()
	t.Attempts++
	t.TotalDeliveries++
	t.DeliveryToken++
	t.Status = StatusInFlight
	t.LeaseExpiresAt = now.Add(q.cfg.VisibilityTimeout)
	t.UpdatedAt = now
	if err := q.persistLocked(); err != nil {
		// 落盘失败则本次投递不生效，状态回滚为待投递。
		t.Attempts--
		t.TotalDeliveries--
		t.DeliveryToken--
		t.Status = StatusPending
		t.LeaseExpiresAt = time.Time{}
		return nil, err
	}
	return &Delivery{Task: t.Clone(), Token: t.DeliveryToken}, nil
}

// Ack 确认任务完成。只有当前有效投递（in_flight 且 token 匹配）可以确认；
// 已确认、已死信、旧投递/未知投递都返回可区分的拒绝原因。
func (q *Queue) Ack(taskID string, token int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return &RejectError{Op: "ack", Reason: ReasonQueueClosed, TaskID: taskID, Message: "queue is closed"}
	}
	t, ok := q.tasks[taskID]
	if !ok {
		return &RejectError{Op: "ack", Reason: ReasonUnknownDelivery, TaskID: taskID, Message: "unknown task"}
	}
	switch t.Status {
	case StatusCompleted:
		return &RejectError{Op: "ack", Reason: ReasonAlreadyAcked, TaskID: taskID, Message: "task already acknowledged"}
	case StatusDeadLettered:
		return &RejectError{Op: "ack", Reason: ReasonDeadLettered, TaskID: taskID, Message: "task is dead-lettered"}
	case StatusInFlight:
		// 继续校验 token
	default:
		return &RejectError{Op: "ack", Reason: ReasonNotInFlight, TaskID: taskID, Message: "task is not currently in flight"}
	}
	if token != t.DeliveryToken {
		return &RejectError{Op: "ack", Reason: ReasonStaleDelivery, TaskID: taskID,
			Message: fmt.Sprintf("delivery token %d is stale, current token is %d", token, t.DeliveryToken)}
	}
	t.Status = StatusCompleted
	t.UpdatedAt = q.cfg.Now()
	t.LeaseExpiresAt = time.Time{}
	return q.persistLocked()
}

// Nack 报告任务处理失败。失败类别与入死信原因会持久化，重启后仍一致、可区分。
func (q *Queue) Nack(taskID string, token int64, class FailureClass, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return &RejectError{Op: "nack", Reason: ReasonQueueClosed, TaskID: taskID, Message: "queue is closed"}
	}
	t, ok := q.tasks[taskID]
	if !ok {
		return &RejectError{Op: "nack", Reason: ReasonUnknownDelivery, TaskID: taskID, Message: "unknown task"}
	}
	if t.Status == StatusCompleted {
		return &RejectError{Op: "nack", Reason: ReasonAlreadyAcked, TaskID: taskID, Message: "task already acknowledged"}
	}
	if t.Status == StatusDeadLettered {
		return &RejectError{Op: "nack", Reason: ReasonDeadLettered, TaskID: taskID, Message: "task is dead-lettered"}
	}
	if t.Status != StatusInFlight {
		return &RejectError{Op: "nack", Reason: ReasonNotInFlight, TaskID: taskID, Message: "task is not currently in flight"}
	}
	if token != t.DeliveryToken {
		return &RejectError{Op: "nack", Reason: ReasonStaleDelivery, TaskID: taskID,
			Message: fmt.Sprintf("delivery token %d is stale, current token is %d", token, t.DeliveryToken)}
	}
	return q.failLocked(t, class, reason)
}

// failLocked 应用失败判定：不可重试直接死信；可重试且投递次数超限死信，
// 否则按退避重新置为 pending。调用方持锁，状态已通过 token 校验。
func (q *Queue) failLocked(t *Task, class FailureClass, reason string) error {
	now := q.cfg.Now()
	t.LastFailureClass = class
	t.LastFailureReason = reason
	t.UpdatedAt = now
	t.LeaseExpiresAt = time.Time{}

	if class == FailureNonRetryable {
		t.Status = StatusDeadLettered
		t.DLQReason = DLQReasonNonRetryable
		t.DeadLetteredAt = now
		return q.persistLocked()
	}
	if t.Attempts >= t.MaxAttempts {
		t.Status = StatusDeadLettered
		t.DLQReason = DLQReasonMaxAttempts
		t.DeadLetteredAt = now
		return q.persistLocked()
	}
	t.Status = StatusPending
	t.VisibleAt = now.Add(q.cfg.Backoff.Next(t.Attempts))
	return q.persistLocked()
}

// ReapExpired 回收两类任务：
//  1. in_flight 且租约过期（消费者中断/崩溃未确认）—— 立即重新投递；
//  2. pending 且退避到期 —— 唤醒等待者（重新投递判定在 Fetch 完成）。
//
// 回收时再次校验重试上限，超限则进入死信（原因 max_attempts_exceeded），
// 保证“未确认可重投、已确认不重投、超限不死循环”。
func (q *Queue) ReapExpired() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.cfg.Now()
	n := 0
	for _, t := range q.tasks {
		if t.Status != StatusInFlight {
			continue
		}
		if t.LeaseExpiresAt.After(now) {
			continue
		}
		// 租约过期：当前投递尝试已计入 Attempts。
		t.LastFailureClass = FailureRetryable
		t.LastFailureReason = "visibility timeout: consumer interrupted without ack"
		t.UpdatedAt = now
		t.LeaseExpiresAt = time.Time{}
		if t.Attempts >= t.MaxAttempts {
			t.Status = StatusDeadLettered
			t.DLQReason = DLQReasonMaxAttempts
			t.DeadLetteredAt = now
		} else {
			t.Status = StatusPending
			t.VisibleAt = now.Add(q.cfg.Backoff.Next(t.Attempts))
		}
		n++
	}
	if n > 0 {
		_ = q.persistLocked()
		q.cond.Broadcast()
	}
	return n
}

// Task 返回任务快照。
func (q *Queue) Task(id string) (*Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return nil, false
	}
	return t.Clone(), true
}

// DeadLetters 返回死信任务快照，按任务 ID 排序。
func (q *Queue) DeadLetters() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []*Task
	for _, t := range q.tasks {
		if t.Status == StatusDeadLettered {
			out = append(out, t.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Snapshot 返回全部任务快照（测试/诊断用）。
func (q *Queue) Snapshot() map[string]*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]*Task, len(q.tasks))
	for id, t := range q.tasks {
		out[id] = t.Clone()
	}
	return out
}
