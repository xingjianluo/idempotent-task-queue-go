package queue

// Status 任务状态。
type Status string

const (
	StatusPending   Status = "pending"   // 待投递（含等待退避后重试）
	StatusLeased    Status = "leased"    // 已投递、租约占用中
	StatusSucceeded Status = "succeeded" // 已确认成功（终态，不重投）
	StatusDead      Status = "dead"      // 死信（终态）
)

// FailKind 失败分类。
type FailKind string

const (
	FailRetryable    FailKind = "retryable"     // 可重试失败
	FailNonRetryable FailKind = "non_retryable" // 不可重试失败，直接进死信
)

// Failure 描述一次处理失败的分类与信息。
type Failure struct {
	Kind    FailKind `json:"kind"`
	Message string   `json:"message"`
	At      int64    `json:"at"` // unix 毫秒时间戳
}

// DeadReason 死信原因（两类原因在重启后仍可区分）。
type DeadReason string

const (
	DeadNonRetryable   DeadReason = "non_retryable"   // 不可重试失败
	DeadRetryExhausted DeadReason = "retry_exhausted" // 超过重试上限
)

// TaskSpec 是任务提交规约。
type TaskSpec struct {
	ID      string            `json:"id"`      // 业务幂等键
	Type    string            `json:"type"`    // 任务类型
	Payload string            `json:"payload"` // 任务负载
	Headers map[string]string `json:"headers"` // 可选元数据
}

// Task 是队列中持久化的任务记录。
type Task struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Payload string            `json:"payload"`
	Headers map[string]string `json:"headers,omitempty"`

	Status   Status `json:"status"`
	Delivery int    `json:"delivery"` // 投递次数（第几次投递给消费者）
	Attempts int    `json:"attempts"` // 处理尝试次数（每次 Nack +1）

	AvailableAt int64 `json:"available_at"` // unix 毫秒：何时可被投递（退避重试时延后）
	LeaseUntil  int64 `json:"lease_until"`  // unix 毫秒：租约到期时间；0 表示未占用
	DeliveryTag int64 `json:"delivery_tag"` // 单调投递令牌，每次重新投递 +1

	Failures []Failure `json:"failures,omitempty"` // 历次失败记录（判定依据）

	DeadReason DeadReason `json:"dead_reason,omitempty"` // 进入死信的原因
	DeadAt     int64      `json:"dead_at,omitempty"`

	CreatedAt   int64 `json:"created_at"`
	SucceededAt int64 `json:"succeeded_at,omitempty"`

	PendingReason string `json:"pending_reason,omitempty"` // 最近一次回到 pending 的原因，便于日志解释
	Interruptions int    `json:"interruptions"`            // 因租约超时/停机被回收重新投递的次数
}

// clone 返回任务的深拷贝，避免外部在锁外修改内部状态。
func (t *Task) clone() *Task {
	if t == nil {
		return nil
	}
	cp := *t
	if t.Headers != nil {
		cp.Headers = make(map[string]string, len(t.Headers))
		for k, v := range t.Headers {
			cp.Headers[k] = v
		}
	}
	if t.Failures != nil {
		cp.Failures = append([]Failure(nil), t.Failures...)
	}
	return &cp
}

// Error 使 *Failure 实现 error，可作为 Handler 的返回值表达业务失败。
func (f *Failure) Error() string {
	return string(f.Kind) + ": " + f.Message
}

// NewFailure 构造一个分类失败。
func NewFailure(kind FailKind, message string) *Failure {
	return &Failure{Kind: kind, Message: message}
}
