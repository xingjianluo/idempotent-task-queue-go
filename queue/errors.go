package queue

// RejectCode 是提交或确认被拒绝的可区分原因码。
type RejectCode string

const (
	// RejectDuplicateSubmit 任务 ID 已存在（重复提交，幂等返回已有任务而非报错）。
	RejectDuplicateSubmit RejectCode = "duplicate_submit"
	// RejectQueueClosed 队列已关闭。
	RejectQueueClosed RejectCode = "queue_closed"
	// RejectTaskNotFound 确认/拒绝时任务不存在。
	RejectTaskNotFound RejectCode = "task_not_found"
	// RejectNotLeased 任务未处于投递占用状态（不能 Ack/Nack）。
	RejectNotLeased RejectCode = "not_leased"
	// RejectLeaseMismatch 投递令牌不匹配（租约已失效/已被重新投递，旧消费者无权确认）。
	RejectLeaseMismatch RejectCode = "lease_mismatch"
)

// RejectError 表示一次被拒绝的提交或确认，携带可区分的原因码。
type RejectError struct {
	Code   RejectCode
	Reason string
}

func (e *RejectError) Error() string {
	return string(e.Code) + ": " + e.Reason
}

// ErrQueueClosed 队列已关闭时返回的哨兵错误。
var ErrQueueClosed = &RejectError{Code: RejectQueueClosed, Reason: "queue is closed"}
