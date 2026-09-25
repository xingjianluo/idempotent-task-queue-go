package queue

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Delivery 是一次投递给消费者的任务句柄。
type Delivery struct {
	Task *Task
	Tag  int64
}

// Logger 最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Config 队列配置。
type Config struct {
	LeaseTTL    time.Duration // 投递租约时长，超时未确认则重新投递
	MaxAttempts int           // 最大处理尝试次数，超过进死信
	Backoff     Backoff       // 有上限退避策略
	Clock       Clock         // 时钟（默认系统时钟）
	Logger      Logger        // 日志（可选）
}

func (c *Config) withDefaults() {
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.Backoff == nil {
		c.Backoff = DefaultBackoff()
	}
	if c.Clock == nil {
		c.Clock = NewRealClock()
	}
	if c.Logger == nil {
		c.Logger = nopLogger{}
	}
}

// Queue 本地任务队列：在互斥锁内维护状态，并通过 Store 本地持久化。
type Queue struct {
	cfg   Config
	store Store
	clock Clock
	log   Logger

	mu     sync.Mutex
	state  *State
	closed bool
	wake   chan struct{} // 容量 1 的唤醒信号
}

// NewQueue 从 Store 加载状态构造队列；上次进程遗留的 leased 任务在重启时回收重投。
func NewQueue(ctx context.Context, store Store, cfg Config) (*Queue, error) {
	cfg.withDefaults()
	st, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if st.Tasks == nil {
		st.Tasks = make(map[string]*Task)
	}
	q := &Queue{
		cfg:   cfg,
		store: store,
		clock: cfg.Clock,
		log:   cfg.Logger,
		state: st,
		wake:  make(chan struct{}, 1),
	}
	q.recoverOnStartupLocked(ctx)
	return q, nil
}

// recoverOnStartupLocked 重启恢复：上次未确认（leased）的任务一律回到 pending，
// 旧投递令牌作废，保证"已确认不重投、未确认可重投"。
func (q *Queue) recoverOnStartupLocked(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.NowMillis()
	recovered := 0
	for _, t := range q.state.Tasks {
		if t.Status == StatusLeased {
			t.Status = StatusPending
			t.LeaseUntil = 0
			t.AvailableAt = now
			t.DeliveryTag = q.nextSeqLocked()
			t.Interruptions++
			t.PendingReason = "restart_recovery"
			recovered++
			q.log.Printf("task id=%s delivery=%d basis=restart_recovery: leased task from previous run requeued, old tag invalidated",
				t.ID, t.Delivery)
		}
	}
	if recovered > 0 {
		_ = q.persistLocked(ctx)
		q.signal()
	}
}

func (q *Queue) nextSeqLocked() int64 {
	q.state.Seq++
	return q.state.Seq
}

// persistLocked 必须在持有 mu 时调用：状态变更先落盘再对外可见，保证确认不丢。
func (q *Queue) persistLocked(ctx context.Context) error {
	return q.store.Save(ctx, cloneState(q.state))
}

// signal 唤醒一个阻塞的 Fetch（非阻塞，合并重复信号）。
func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func reject(code RejectCode, reason string) *RejectError {
	return &RejectError{Code: code, Reason: reason}
}

// Submit 提交任务。重复提交幂等：不重复生效，返回已存在任务，
// 同时返回 Code=duplicate_submit 的 RejectError 以便调用方区分"新建"与"重复"。
func (q *Queue) Submit(ctx context.Context, spec TaskSpec) (*Task, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, false, ErrQueueClosed
	}
	if spec.ID == "" {
		return nil, false, reject(RejectTaskNotFound, "task id must not be empty")
	}
	if t, ok := q.state.Tasks[spec.ID]; ok {
		q.log.Printf("task id=%s delivery=%d basis=duplicate_submit: task already exists in status=%s, submission has no effect",
			t.ID, t.Delivery, t.Status)
		return t.clone(), false, reject(RejectDuplicateSubmit, "task "+spec.ID+" already exists")
	}
	now := q.clock.NowMillis()
	t := &Task{
		ID:          spec.ID,
		Type:        spec.Type,
		Payload:     spec.Payload,
		Headers:     spec.Headers,
		Status:      StatusPending,
		AvailableAt: now,
		CreatedAt:   now,
	}
	q.state.Tasks[t.ID] = t
	if err := q.persistLocked(ctx); err != nil {
		delete(q.state.Tasks, t.ID)
		return nil, false, err
	}
	q.log.Printf("task id=%s delivery=%d basis=submit_accepted: task created status=pending", t.ID, t.Delivery)
	q.signal()
	return t.clone(), true, nil
}

// tryClaimLocked 选择一个到期的 pending 任务并占用；无可用任务返回 nil。
// 选择顺序按 (AvailableAt, ID) 确定，保证多消费者竞争下投递顺序可预期。
func (q *Queue) tryClaimLocked(ctx context.Context) *Delivery {
	now := q.clock.NowMillis()
	var best *Task
	for _, t := range q.state.Tasks {
		if t.Status != StatusPending || t.AvailableAt > now {
			continue
		}
		if best == nil || t.AvailableAt < best.AvailableAt ||
			(t.AvailableAt == best.AvailableAt && t.ID < best.ID) {
			best = t
		}
	}
	if best == nil {
		return nil
	}
	best.Status = StatusLeased
	best.Delivery++
	best.DeliveryTag = q.nextSeqLocked()
	best.LeaseUntil = now + q.cfg.LeaseTTL.Milliseconds()
	best.PendingReason = ""
	if err := q.persistLocked(ctx); err != nil {
		// 落盘失败则回滚占用，避免未持久化的投递产生重复副作用。
		best.Status = StatusPending
		best.Delivery--
		best.LeaseUntil = 0
		q.state.Seq--
		return nil
	}
	q.log.Printf("task id=%s delivery=%d basis=claimed: leased tag=%d lease_ttl=%s",
		best.ID, best.Delivery, best.DeliveryTag, q.cfg.LeaseTTL)
	return &Delivery{Task: best.clone(), Tag: best.DeliveryTag}
}

// Fetch 阻塞拉取一个可投递任务（成功或 ctx 取消/队列关闭时返回）。
func (q *Queue) Fetch(ctx context.Context) (*Delivery, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		if d := q.tryClaimLocked(ctx); d != nil {
			q.mu.Unlock()
			return d, nil
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.wake:
		case <-time.After(20 * time.Millisecond): // 兜底轮询，覆盖退避到期场景
		}
	}
}

// validateLeaseLocked 校验 Ack/Nack 的归属权，返回可区分的拒绝原因。
func (q *Queue) validateLeaseLocked(taskID string, tag int64) (*Task, error) {
	t, ok := q.state.Tasks[taskID]
	if !ok {
		return nil, reject(RejectTaskNotFound, "task "+taskID+" not found")
	}
	if t.Status != StatusLeased {
		return nil, reject(RejectNotLeased, "task "+taskID+" is not leased (status="+string(t.Status)+")")
	}
	if t.DeliveryTag != tag {
		return nil, reject(RejectLeaseMismatch,
			"delivery tag mismatch: lease was reclaimed and redelivered")
	}
	return t, nil
}

// Ack 确认成功。仅当前租约持有者可确认；成功是终态，已确认任务不会被重投。
func (q *Queue) Ack(ctx context.Context, taskID string, tag int64) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	t, err := q.validateLeaseLocked(taskID, tag)
	if err != nil {
		if t, ok := q.state.Tasks[taskID]; ok {
			q.log.Printf("task id=%s delivery=%d basis=ack_rejected code=%s", taskID, t.Delivery, err.(*RejectError).Code)
		} else {
			q.log.Printf("task id=%s delivery=%d basis=ack_rejected code=%s", taskID, 0, err.(*RejectError).Code)
		}
		return nil, err
	}
	t.Status = StatusSucceeded
	t.SucceededAt = q.clock.NowMillis()
	t.LeaseUntil = 0
	if err := q.persistLocked(ctx); err != nil {
		t.Status = StatusLeased
		t.SucceededAt = 0
		t.LeaseUntil = q.clock.NowMillis() + q.cfg.LeaseTTL.Milliseconds()
		return nil, err
	}
	q.log.Printf("task id=%s delivery=%d basis=ack_confirmed: terminal status=succeeded, will not be redelivered",
		t.ID, t.Delivery)
	return t.clone(), nil
}

// Nack 报告处理失败：可重试按有上限退避重新入队；不可重试或超过重试上限进死信。
func (q *Queue) Nack(ctx context.Context, taskID string, tag int64, f Failure) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	t, err := q.validateLeaseLocked(taskID, tag)
	if err != nil {
		if t, ok := q.state.Tasks[taskID]; ok {
			q.log.Printf("task id=%s delivery=%d basis=nack_rejected code=%s", taskID, t.Delivery, err.(*RejectError).Code)
		} else {
			q.log.Printf("task id=%s delivery=%d basis=nack_rejected code=%s", taskID, 0, err.(*RejectError).Code)
		}
		return nil, err
	}
	if f.At == 0 {
		f.At = q.clock.NowMillis()
	}
	t.Attempts++
	t.Failures = append(t.Failures, f)
	delivery := t.Delivery

	prevStatus := t.Status
	prevLease := t.LeaseUntil
	apply := func(rollback bool) {
		if !rollback {
			return
		}
		t.Status = prevStatus
		t.LeaseUntil = prevLease
		t.Attempts--
		t.Failures = t.Failures[:len(t.Failures)-1]
		t.DeadReason = ""
		t.DeadAt = 0
		t.AvailableAt = 0
		t.PendingReason = ""
	}

	switch {
	case f.Kind == FailNonRetryable:
		t.Status = StatusDead
		t.LeaseUntil = 0
		t.DeadReason = DeadNonRetryable
		t.DeadAt = f.At
		t.PendingReason = ""
		if perr := q.persistLocked(ctx); perr != nil {
			apply(true)
			return nil, perr
		}
		q.log.Printf("task id=%s delivery=%d attempts=%d basis=dead_non_retryable: reason=%q failure=%q",
			t.ID, delivery, t.Attempts, DeadNonRetryable, f.Message)
	case t.Attempts >= q.cfg.MaxAttempts:
		t.Status = StatusDead
		t.LeaseUntil = 0
		t.DeadReason = DeadRetryExhausted
		t.DeadAt = f.At
		t.PendingReason = ""
		if perr := q.persistLocked(ctx); perr != nil {
			apply(true)
			return nil, perr
		}
		q.log.Printf("task id=%s delivery=%d attempts=%d basis=dead_retry_exhausted: max_attempts=%d reason=%q last_failure=%q",
			t.ID, delivery, t.Attempts, q.cfg.MaxAttempts, DeadRetryExhausted, f.Message)
	default:
		delay := q.cfg.Backoff.DelayMillis(t.Attempts)
		t.Status = StatusPending
		t.LeaseUntil = 0
		t.AvailableAt = f.At + delay
		t.PendingReason = "retry_backoff"
		if perr := q.persistLocked(ctx); perr != nil {
			apply(true)
			return nil, perr
		}
		q.signal()
		q.log.Printf("task id=%s delivery=%d attempts=%d basis=retry_backoff: requeued delay_ms=%d (capped) max_attempts=%d failure=%q",
			t.ID, delivery, t.Attempts, delay, q.cfg.MaxAttempts, f.Message)
	}
	return t.clone(), nil
}

// requeueLocked 把指定占用任务回收为 pending（租约超时/停机复用），旧令牌作废。
func (q *Queue) requeueLocked(ids []string, onlyExpired bool, reason string) []string {
	now := q.clock.NowMillis()
	for _, t := range q.state.Tasks {
		if t.Status != StatusLeased {
			continue
		}
		if onlyExpired && t.LeaseUntil > now {
			continue
		}
		t.Status = StatusPending
		t.LeaseUntil = 0
		t.AvailableAt = now
		t.DeliveryTag = q.nextSeqLocked()
		t.Interruptions++
		t.PendingReason = reason
		ids = append(ids, t.ID)
		q.log.Printf("task id=%s delivery=%d basis=%s: unacked task requeued, interruptions=%d, old tag invalidated",
			t.ID, t.Delivery, reason, t.Interruptions)
	}
	return ids
}

// RequeueExpired 回收租约已过期的未确认任务并重新投递，返回被回收任务 ID。
func (q *Queue) RequeueExpired(ctx context.Context) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	var ids []string
	ids = q.requeueLocked(ids, true, "lease_expired")
	if len(ids) > 0 {
		if err := q.persistLocked(ctx); err == nil {
			q.signal()
		}
	}
	sort.Strings(ids)
	return ids
}

// ReclaimLeasedForShutdown 停机时把所有占用中任务回收为 pending（确定语义）。
func (q *Queue) ReclaimLeasedForShutdown(ctx context.Context) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	var ids []string
	ids = q.requeueLocked(ids, false, "graceful_shutdown")
	if len(ids) > 0 {
		_ = q.persistLocked(ctx)
		q.signal()
	}
	sort.Strings(ids)
	return ids
}

// Close 优雅关闭：回收占用任务并持久化最终状态；之后 Fetch/Ack/Nack 返回 closed。
func (q *Queue) Close(ctx context.Context) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	var ids []string
	ids = q.requeueLocked(ids, false, "graceful_shutdown")
	q.closed = true
	err := q.persistLocked(ctx)
	q.mu.Unlock()
	q.signal()
	q.log.Printf("queue closed basis=graceful_shutdown requeued=%d", len(ids))
	return err
}

// Get 查询任务快照（不存在返回 nil, false）。
func (q *Queue) Get(taskID string) (*Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.state.Tasks[taskID]
	if !ok {
		return nil, false
	}
	return t.clone(), true
}

// DeadLetters 返回死信任务快照，按 ID 排序，DeadReason 可区分两类原因。
func (q *Queue) DeadLetters() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []*Task
	for _, t := range q.state.Tasks {
		if t.Status == StatusDead {
			out = append(out, t.clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
