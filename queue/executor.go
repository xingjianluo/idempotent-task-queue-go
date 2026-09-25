package queue

import "context"

// SideEffect 是任务真正的外部副作用动作。
type SideEffect func(ctx context.Context) error

// RunIdempotent 在当前有效投递下至多执行一次副作用，用于配合处理器防止
// 重复投递/重启恢复造成的重复副作用。语义（at-most-once，状态持久化、可解释）：
//
//  1. 先在持久化任务上“认领”副作用（SideEffectDone=true 立即落盘），
//     只有持当前有效投递 token 且尚未认领时才认领成功；
//  2. 认领成功才执行 effect；effect 返回 nil 后由调用方 Ack；
//  3. 认领返回 skipped=true 表示副作用此前已执行/已认领（重投、重启恢复），
//     必须跳过 effect 直接 Ack；
//  4. effect 失败时撤销认领（要求 token 仍有效）并返回错误，交由 Nack 重试。
//
// 因此：副作用执行完但 Ack 前崩溃，恢复重投只会跳过；不会出现重复副作用。
// 崩溃发生在“认领之后、effect 完成之前”的极小窗口内，按 at-most-once 语义
// 不会重放（状态可通过 SideEffectDone 解释），需要恰好一次的外部动作应自身幂等。
func (q *Queue) RunIdempotent(taskID string, token int64, effect SideEffect) (ran bool, err error) {
	first, err := q.MarkSideEffect(taskID, token)
	if err != nil {
		return false, err
	}
	if !first {
		return false, nil
	}
	if err := effect(context.Background()); err != nil {
		// 副作用失败：在当前投递仍有效时撤销认领，使后续重试可以再执行。
		_ = q.ReleaseSideEffect(taskID, token)
		return true, err
	}
	return true, nil
}

// ReleaseSideEffect 撤销副作用认领（仅当前有效投递且已认领时）。
func (q *Queue) ReleaseSideEffect(taskID string, token int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[taskID]
	if !ok {
		return &RejectError{Op: "release_side_effect", Reason: ReasonUnknownDelivery, TaskID: taskID, Message: "unknown task"}
	}
	if t.Status != StatusInFlight || token != t.DeliveryToken {
		return &RejectError{Op: "release_side_effect", Reason: ReasonStaleDelivery, TaskID: taskID, Message: "stale or invalid delivery"}
	}
	if !t.SideEffectDone {
		return nil
	}
	t.SideEffectDone = false
	t.UpdatedAt = q.cfg.Now()
	return q.persistLocked()
}
