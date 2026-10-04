package queue

import "time"

// TaskStatus 任务状态。
type TaskStatus string

const (
	StatusPending      TaskStatus = "pending"       // 待投递（含退避等待中）
	StatusInFlight     TaskStatus = "in_flight"     // 已投递、租约未过期
	StatusCompleted    TaskStatus = "completed"     // 已确认
	StatusDeadLettered TaskStatus = "dead_lettered" // 已进入死信
)

// FailureClass 失败分类。
type FailureClass string

const (
	FailureNone         FailureClass = "none"
	FailureRetryable    FailureClass = "retryable"
	FailureNonRetryable FailureClass = "non_retryable"
)

// DLQReason 进入死信的可区分原因（持久化，重启后仍一致、可区分）。
type DLQReason string

const (
	DLQReasonNonRetryable DLQReason = "non_retryable"         // 不可重试失败
	DLQReasonMaxAttempts  DLQReason = "max_attempts_exceeded" // 超过最大投递次数
)

// Task 是任务的完整持久化状态。
type Task struct {
	ID                string       `json:"id"`
	Payload           []byte       `json:"payload"`
	PayloadHash       string       `json:"payload_hash"`
	Status            TaskStatus   `json:"status"`
	Attempts          int          `json:"attempts"` // 累计投递次数
	MaxAttempts       int          `json:"max_attempts"`
	DeliveryToken     int64        `json:"delivery_token"` // 每次投递递增，用于识别过期投递
	CreatedAt         time.Time    `json:"created_at"`
	UpdatedAt         time.Time    `json:"updated_at"`
	VisibleAt         time.Time    `json:"visible_at"`       // 退避结束后的可见时间
	LeaseExpiresAt    time.Time    `json:"lease_expires_at"` // 当前租约到期时间
	LastFailureClass  FailureClass `json:"last_failure_class"`
	LastFailureReason string       `json:"last_failure_reason"`
	DLQReason         DLQReason    `json:"dlq_reason"`
	DeadLetteredAt    time.Time    `json:"dead_lettered_at"` // 最最近一次进入死信的时间（按入死信先后排序用）
	SideEffectDone    bool         `json:"side_effect_done"` // 副作用是否已执行（幂等保护）

	// 人工死信重放相关（持久化，重启后仍可解释）：
	// ReplayCount 累计被人工重放的次数；ReplayAttempts 是所有重放轮次中累计发生的投递次数；
	// AttemptsAtReplay 是最近一次重放生效时的 Attempts，用于把“重放轮次”的重试上限与原始历史分开判定；
	// ReplayBatches 记录每次重放所属的批次 ID（与 ReplayCount 等长），是批次级幂等的任务侧凭证。
	ReplayCount      int       `json:"replay_count"`
	ReplayAttempts   int       `json:"replay_attempts"`
	AttemptsAtReplay int       `json:"attempts_at_replay"`
	LastReplayAt     time.Time `json:"last_replay_at"`
	ReplayBatches    []string  `json:"replay_batches,omitempty"`
}

// Clone 返回任务的深拷贝快照。
func (t *Task) Clone() *Task {
	cp := *t
	if t.Payload != nil {
		cp.Payload = append([]byte(nil), t.Payload...)
	}
	if t.ReplayBatches != nil {
		cp.ReplayBatches = append([]string(nil), t.ReplayBatches...)
	}
	return &cp
}

// Delivery 是一次投递给消费者的任务租约。
type Delivery struct {
	Task  *Task
	Token int64
}

// TaskID 返回投递对应的任务标识。
func (d *Delivery) TaskID() string {
	if d.Task == nil {
		return ""
	}
	return d.Task.ID
}
