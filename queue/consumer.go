package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Handler 业务处理器。返回 nil 表示成功；用 NonRetryable 包装的错误直接进入死信；
// 其余错误按可重试失败处理（受最大投递次数与退避限制）。
type Handler func(ctx context.Context, t *Task) error

// ConsumerGroup 多消费者并发处理组，支持优雅停机。
type ConsumerGroup struct {
	q           *Queue
	name        string
	concurrency int
	handler     Handler
	reapEvery   time.Duration

	wg       sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}
	closed   bool
	mu       sync.Mutex
}

// NewConsumerGroup 创建消费组。
func NewConsumerGroup(q *Queue, name string, concurrency int, handler Handler) *ConsumerGroup {
	if concurrency < 1 {
		concurrency = 1
	}
	return &ConsumerGroup{
		q:           q,
		name:        name,
		concurrency: concurrency,
		handler:     handler,
		reapEvery:   maxDuration(50*time.Millisecond, q.cfg.VisibilityTimeout/4),
		stopCh:      make(chan struct{}),
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// MarkSideEffect 声明该任务的副作用已执行。仅当前有效投递、任务未完成/未死信时允许。
// 返回 false（无 error）表示副作用此前已记录，调用方应跳过副作用。
func (q *Queue) MarkSideEffect(taskID string, token int64) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[taskID]
	if !ok {
		return false, &RejectError{Op: "mark_side_effect", Reason: ReasonUnknownDelivery, TaskID: taskID, Message: "unknown task"}
	}
	if t.Status != StatusInFlight || token != t.DeliveryToken {
		return false, &RejectError{Op: "mark_side_effect", Reason: ReasonStaleDelivery, TaskID: taskID, Message: "stale or invalid delivery"}
	}
	if t.SideEffectDone {
		return false, nil
	}
	t.SideEffectDone = true
	t.UpdatedAt = q.cfg.Now()
	if err := q.persistLocked(); err != nil {
		t.SideEffectDone = false
		return false, err
	}
	return true, nil
}

// SideEffectDone 返回任务的副作用是否已执行（重启后保持一致）。
func (q *Queue) SideEffectDone(taskID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[taskID]
	return ok && t.SideEffectDone
}

// Start 启动消费者与租约回收循环。
func (g *ConsumerGroup) Start() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	for i := 0; i < g.concurrency; i++ {
		g.wg.Add(1)
		go g.worker(i)
	}
	g.wg.Add(1)
	go g.reaper()
	g.mu.Unlock()
}

func (g *ConsumerGroup) stopping() bool {
	select {
	case <-g.stopCh:
		return true
	default:
		return false
	}
}

func (g *ConsumerGroup) worker(id int) {
	defer g.wg.Done()
	for {
		if g.stopping() {
			return
		}
		// 停机后立即取消正在阻塞的 Fetch。
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-g.stopCh
			cancel()
		}()
		d, err := g.q.Fetch(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, ErrQueueClosed) || errors.Is(err, context.Canceled) || g.stopping() {
				return
			}
			log.Printf("[%s worker-%d] fetch error: %v", g.name, id, err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		g.process(id, d)
	}
}

func (g *ConsumerGroup) process(id int, d *Delivery) {
	t := d.Task
	replayCtx := ""
	if t.ReplayCount > 0 {
		replayCtx = fmt.Sprintf(" replays=%d replay_attempts=%d", t.ReplayCount, t.ReplayAttempts)
	}

	// 处理器负责业务副作用（建议用 q.RunIdempotent 包装以获得持久化的 at-most-once 保护）。
	err := g.handler(context.Background(), t)
	if err != nil {
		class := ClassifyFailure(err)
		log.Printf("[%s worker-%d] task=%s attempt=%d%s FAIL class=%s reason=%q",
			g.name, id, t.ID, t.Attempts, replayCtx, class, err.Error())
		if nerr := g.q.Nack(t.ID, d.Token, class, err.Error()); nerr != nil {
			log.Printf("[%s worker-%d] task=%s attempt=%d%s nack rejected: %v",
				g.name, id, t.ID, t.Attempts, replayCtx, nerr)
		}
		return
	}

	if err := g.q.Ack(t.ID, d.Token); err != nil {
		// 典型为停机/旧投递导致的迟到确认；任务会按租约重新投递。
		// 若副作用使用 RunIdempotent 包装，重投时认领会跳过，不会重复生效。
		log.Printf("[%s worker-%d] task=%s attempt=%d%s ack rejected: %v",
			g.name, id, t.ID, t.Attempts, replayCtx, err)
		return
	}
	log.Printf("[%s worker-%d] task=%s attempt=%d%s ACK ok side_effect_done=%v",
		g.name, id, t.ID, t.Attempts, replayCtx, t.SideEffectDone)
}

func (g *ConsumerGroup) reaper() {
	defer g.wg.Done()
	t := time.NewTicker(g.reapEvery)
	defer t.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-t.C:
			if n := g.q.ReapExpired(); n > 0 {
				log.Printf("[%s reaper] requeued %d expired/unacked delivery(s)", g.name, n)
			}
		}
	}
}

// Shutdown 优雅停机：停止拉取新任务并停止租约回收，等待在途任务结束。
// 若 ctx 超时仍有在途任务未结束，则放弃等待；这些任务保持 in_flight（已落盘），
// 重启后由租约回收重新投递——副作用是否发生以持久化的 SideEffectDone 为准，
// 因此最终语义确定：不重复副作用、不丢失确认机会。
func (g *ConsumerGroup) Shutdown(ctx context.Context) error {
	g.stopOnce.Do(func() {
		close(g.stopCh)
	})
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
