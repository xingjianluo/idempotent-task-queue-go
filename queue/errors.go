package queue

import (
	"errors"
	"fmt"
)

// RejectReason 描述提交或确认被拒绝的可区分原因。
type RejectReason string

const (
	ReasonDuplicateSubmission RejectReason = "duplicate_submission"   // 重复提交：任务已存在且载荷一致
	ReasonPayloadConflict     RejectReason = "payload_conflict"       // ID 相同但载荷不一致
	ReasonQueueClosed         RejectReason = "queue_closed"           // 队列已关闭
	ReasonAlreadyAcked        RejectReason = "already_acked"          // 任务已确认，重复确认
	ReasonDeadLettered        RejectReason = "dead_lettered"          // 任务已进入死信
	ReasonUnknownDelivery     RejectReason = "unknown_delivery"       // 确认了一个不存在的任务
	ReasonStaleDelivery       RejectReason = "stale_delivery"         // 旧投递（已重投）的迟到确认
	ReasonNotInFlight         RejectReason = "not_in_flight"          // 任务当前不在投递状态
	ReasonNotDeadLettered     RejectReason = "not_dead_lettered"      // 重放点名的任务不在死信中（处理中/已完成/不存在）
	ReasonReplayBatchTooLarge RejectReason = "replay_batch_too_large" // 单次重放数量超过 MaxReplayBatchSize
	ReasonReplayQuotaExceeded RejectReason = "replay_quota_exceeded"  // 累计重放数量超过 MaxReplayTotal
	ReasonInvalidReplay       RejectReason = "invalid_replay"         // 重放请求非法（如缺少批次 ID）
)

// RejectError 携带可区分的拒绝原因，调用方可用 AsReject 精确判定。
type RejectError struct {
	Op      string       // submit / ack / nack
	Reason  RejectReason // 机器可判别的拒绝原因
	TaskID  string
	Message string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("queue: %s rejected for task %q: %s (%s)", e.Op, e.TaskID, e.Reason, e.Message)
}

// AsReject 从 error 中提取 *RejectError。
func AsReject(err error) (*RejectError, bool) {
	var r *RejectError
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// ErrQueueClosed 队列关闭后的操作返回。
var ErrQueueClosed = errors.New("queue: queue is closed")

// NonRetryableError 由处理器返回，表示失败不可重试，需直接进入死信。
type NonRetryableError struct {
	Reason string
}

func (e *NonRetryableError) Error() string {
	return "non-retryable: " + e.Reason
}

// NonRetryable 包装一个不可重试失败。
func NonRetryable(reason string) error {
	return &NonRetryableError{Reason: reason}
}

// ClassifyFailure 判断处理器错误的失败类别。
// *NonRetryableError（含包装）判为不可重试，其余非 nil 错误判为可重试。
func ClassifyFailure(err error) FailureClass {
	if err == nil {
		return FailureNone
	}
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		return FailureNonRetryable
	}
	return FailureRetryable
}
