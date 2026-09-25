package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Handler 处理一次投递。返回 nil 表示成功（自动 Ack）；
// 返回 *Failure 表示业务失败（按分类重试/死信）；
// 返回其它 error 视为可重试失败。
type Handler func(ctx context.Context, d *Delivery) error

// ConsumerConfig 消费者配置。
type ConsumerConfig struct {
	Name         string        // 消费者名称（日志标识）
	Concurrency  int           // 并发 worker 数
	ReapInterval time.Duration // 租约回收扫描间隔
	ShutdownWait time.Duration // 停机宽限：在途任务最长等待时间
	Logger       Logger
}

func (c *ConsumerConfig) withDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 50 * time.Millisecond
	}
	if c.ShutdownWait <= 0 {
		c.ShutdownWait = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = nopLogger{}
	}
}

// Consumer 多消费者并发处理与优雅停机封装。
type Consumer struct {
	q   *Queue
	h   Handler
	cfg ConsumerConfig
	log Logger
}

// NewConsumer 创建消费者（不启动）。
func NewConsumer(q *Queue, h Handler, cfg ConsumerConfig) *Consumer {
	cfg.withDefaults()
	return &Consumer{q: q, h: h, cfg: cfg, log: cfg.Logger}
}

func asFailure(err error) Failure {
	var f *Failure
	if errors.As(err, &f) && f != nil {
		return *f
	}
	return Failure{Kind: FailRetryable, Message: err.Error()}
}

// Run 启动并发 worker 与租约回收循环，阻塞直到 ctx 取消；
// 随后优雅停机：等待在途任务到达确定结果；宽限到期仍未完成的任务
// 由 Queue.Close 统一回收重新入队（旧令牌作废，停机不会丢确认也不会重复确认）。
func (c *Consumer) Run(ctx context.Context) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go c.worker(workerCtx, i, &wg)
	}
	wg.Add(1)
	go c.reaper(workerCtx, &wg)

	c.log.Printf("consumer=%s started concurrency=%d reap_interval=%s",
		c.cfg.Name, c.cfg.Concurrency, c.cfg.ReapInterval)

	<-ctx.Done()
	c.log.Printf("consumer=%s basis=shutdown_begin: canceling workers, shutdown_wait=%s",
		c.cfg.Name, c.cfg.ShutdownWait)
	cancel()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		c.log.Printf("consumer=%s basis=shutdown_clean: all in-flight tasks reached terminal ack/nack", c.cfg.Name)
	case <-time.After(c.cfg.ShutdownWait):
		c.log.Printf("consumer=%s basis=shutdown_wait_exceeded: reclaiming unfinished tasks to queue", c.cfg.Name)
	}
	// Close 幂等且会回收所有 leased 任务（包括仍在运行的 worker 手里的任务），
	// 这些任务的旧令牌立即作废：即使 worker 稍后回调 Ack 也会被 lease_mismatch 拒绝。
	if err := c.q.Close(context.Background()); err != nil {
		return err
	}
	<-done // worker 观察到 closed 后必然退出
	c.log.Printf("consumer=%s basis=shutdown_complete: final state persisted", c.cfg.Name)
	return nil
}

// worker 循环拉取并处理任务；处理与确认携带独立 context，
// 使停机信号不会直接打断已开始的副作用（它们要么完成确认，要么被回收重投）。
func (c *Consumer) worker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		d, err := c.q.Fetch(ctx)
		if err != nil {
			if errors.Is(err, ErrQueueClosed) || ctx.Err() != nil {
				return
			}
			c.log.Printf("consumer=%s worker=%d fetch error: %v", c.cfg.Name, id, err)
			if !sleep(ctx, 10*time.Millisecond) {
				return
			}
			continue
		}
		c.process(ctx, id, d)
	}
}

// process 处理单次投递并按结果 Ack/Nack；所有拒绝原因都会被记录且可区分。
func (c *Consumer) process(ctx context.Context, id int, d *Delivery) {
	hctx := context.Background()
	err := c.h(hctx, d)
	if err == nil {
		if _, ackErr := c.q.Ack(context.Background(), d.Task.ID, d.Tag); ackErr != nil {
			var re *RejectError
			if errors.As(ackErr, &re) {
				c.log.Printf("consumer=%s worker=%d task id=%s delivery=%d basis=ack_rejected code=%s: %s",
					c.cfg.Name, id, d.Task.ID, d.Task.Delivery, re.Code, re.Reason)
			}
		}
		return
	}
	f := asFailure(err)
	if _, nackErr := c.q.Nack(context.Background(), d.Task.ID, d.Tag, f); nackErr != nil {
		var re *RejectError
		if errors.As(nackErr, &re) {
			c.log.Printf("consumer=%s worker=%d task id=%s delivery=%d basis=nack_rejected code=%s: %s",
				c.cfg.Name, id, d.Task.ID, d.Task.Delivery, re.Code, re.Reason)
		}
	}
}

// reaper 周期性回收租约过期任务，模拟消费者崩溃/中断后的重新投递。
func (c *Consumer) reaper(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.NewTicker(c.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.q.RequeueExpired(ctx)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
