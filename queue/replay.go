package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ReplayFilter 描述一次死信重放的挑选条件。多个条件取交集；
// TaskIDs 非空时按名单点名（名单外条件仍生效）。
type ReplayFilter struct {
	TaskIDs        []string     // 点名指定任务；为空表示按条件批量挑选
	DLQReason      DLQReason    // 按进入死信的原因筛选
	FailureClass   FailureClass // 按最后一次失败类别筛选
	ReasonContains string       // 按最后失败原因文本包含筛选
	EnteredAfter   time.Time    // 进入死信时间下界（含），零值不限
	EnteredBefore  time.Time    // 进入死信时间上界（含），零值不限
	Limit          int          // 本次最多挑选数量；0 表示不额外限制（仍受单次上限约束）
}

// ReplaySkip 记录点名重放时被跳过的任务及原因。
type ReplaySkip struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

// ReplayResult 是一次重放操作的结果。
type ReplayResult struct {
	BatchID    string       `json:"batch_id"`
	TaskIDs    []string     `json:"task_ids"` // 实际被重放的任务
	Skipped    []ReplaySkip `json:"skipped"`  // 点名但未重放的任务及原因
	ReplayedAt time.Time    `json:"replayed_at"`
	Duplicate  bool         `json:"duplicate"` // 命中已有批次，本次未重复生效
}

// ReplayBatch 是持久化的重放批次记录，用于重放操作的幂等判定与审计。
type ReplayBatch struct {
	BatchID    string       `json:"batch_id"`
	FilterHash string       `json:"filter_hash"` // 规范化筛选条件摘要，防止同批次 ID 被换条件复用
	TaskIDs    []string     `json:"task_ids"`
	Skipped    []ReplaySkip `json:"skipped,omitempty"`
	CreatedAt  time.Time    `json:"created_at"`
}

// filterHash 计算筛选条件的规范化摘要：TaskIDs 排序后做 JSON + SHA-256，
// 保证同一批重用相同条件时摘要稳定，条件不同则摘要不同。
func filterHash(f ReplayFilter) string {
	norm := f
	norm.TaskIDs = append([]string(nil), f.TaskIDs...)
	sort.Strings(norm.TaskIDs)
	data, _ := json.Marshal(norm)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// matchesFilter 判断死信任务是否满足筛选条件（不含 TaskIDs 名单判定）。
func matchesFilter(t *Task, f ReplayFilter) bool {
	if f.DLQReason != "" && t.DLQReason != f.DLQReason {
		return false
	}
	if f.FailureClass != "" && t.LastFailureClass != f.FailureClass {
		return false
	}
	if f.ReasonContains != "" && !strings.Contains(t.LastFailureReason, f.ReasonContains) {
		return false
	}
	if !f.EnteredAfter.IsZero() && t.DeadLetteredAt.Before(f.EnteredAfter) {
		return false
	}
	if !f.EnteredBefore.IsZero() && t.DeadLetteredAt.After(f.EnteredBefore) {
		return false
	}
	return true
}

// Replay 把符合条件的死信任务重新投回正常处理流程（pending，立即可见）。
//
// 语义与约定：
//   - batchID 是本次重放操作的幂等键：同一 batchID + 相同筛选条件重复调用，
//     返回首次记录的结果（Duplicate=true），不会重复投放；同一 batchID 换用
//     不同筛选条件则拒绝（replay_batch_conflict）。
//   - 只有 dead_lettered 状态的任务可被重放；处理中（in_flight）、已确认
//     （completed）或待投递（pending）的任务不会被误重放。点名的任务不满足
//     条件时记入 Skipped 而非报错。
//   - 重放后 Attempts 归零（获得一个仍受 max_attempts 约束的新重试预算），
//     ReplayCount++，TotalDeliveries 持续累计，原始处理历史
//     （last_failure_class / last_failure_reason）保留可区分。
//   - 单次数量超过 MaxReplayPerBatch、或累计重放数超过 MaxReplayTotal 时
//     明确拒绝（replay_limit_exceeded），不做静默截断。
//   - 状态变更与批次记录在同一锁内完成并原子落盘；落盘失败整体回滚，
//     不留下半截状态。
func (q *Queue) Replay(batchID string, filter ReplayFilter) (*ReplayResult, error) {
	if batchID == "" {
		return nil, fmt.Errorf("queue: replay batch id must not be empty")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, &RejectError{Op: "replay", Reason: ReasonQueueClosed, TaskID: batchID, Message: "queue is closed"}
	}

	hash := filterHash(filter)
	if b, ok := q.replayBatches[batchID]; ok {
		if b.FilterHash != hash {
			return nil, &RejectError{Op: "replay", Reason: ReasonReplayBatchConflict, TaskID: batchID,
				Message: "batch id already used with a different filter"}
		}
		// 幂等命中：返回首次记录的结果，不重复生效。
		return &ReplayResult{
			BatchID:    batchID,
			TaskIDs:    append([]string(nil), b.TaskIDs...),
			Skipped:    append([]ReplaySkip(nil), b.Skipped...),
			ReplayedAt: b.CreatedAt,
			Duplicate:  true,
		}, nil
	}

	// 挑选候选：只考虑 dead_lettered 任务，按进入死信时间 + ID 排序保证结果确定。
	var named map[string]bool
	if len(filter.TaskIDs) > 0 {
		named = make(map[string]bool, len(filter.TaskIDs))
		for _, id := range filter.TaskIDs {
			named[id] = true
		}
	}
	var skipped []ReplaySkip
	if named != nil {
		for _, id := range filter.TaskIDs {
			t, ok := q.tasks[id]
			if !ok {
				skipped = append(skipped, ReplaySkip{TaskID: id, Reason: "unknown task"})
				continue
			}
			if t.Status != StatusDeadLettered {
				skipped = append(skipped, ReplaySkip{TaskID: id, Reason: fmt.Sprintf("task is %s, not dead_lettered", t.Status)})
			}
		}
	}
	var candidates []*Task
	for _, t := range q.tasks {
		if t.Status != StatusDeadLettered {
			continue
		}
		if named != nil && !named[t.ID] {
			continue
		}
		if !matchesFilter(t, filter) {
			continue
		}
		candidates = append(candidates, t)
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if !a.DeadLetteredAt.Equal(b.DeadLetteredAt) {
			return a.DeadLetteredAt.Before(b.DeadLetteredAt)
		}
		return a.ID < b.ID
	})

	// 容量与背压：先应用调用方 Limit，再校验单次与累计上限，超限明确拒绝。
	effective := len(candidates)
	if filter.Limit > 0 && effective > filter.Limit {
		effective = filter.Limit
	}
	if max := q.cfg.MaxReplayPerBatch; max >= 0 && effective > max {
		return nil, &RejectError{Op: "replay", Reason: ReasonReplayLimitExceeded, TaskID: batchID,
			Message: fmt.Sprintf("batch selects %d tasks, exceeding per-batch limit %d; narrow the filter or set Limit", effective, max)}
	}
	if max := q.cfg.MaxReplayTotal; max >= 0 && q.replayedTotal+effective > max {
		return nil, &RejectError{Op: "replay", Reason: ReasonReplayLimitExceeded, TaskID: batchID,
			Message: fmt.Sprintf("cumulative replayed %d + %d would exceed total limit %d", q.replayedTotal, effective, max)}
	}
	selected := candidates[:effective]

	// 应用重放：先备份以便落盘失败时整体回滚。
	type backup struct {
		t    *Task
		prev *Task
	}
	backups := make([]backup, 0, len(selected))
	now := q.cfg.Now()
	ids := make([]string, 0, len(selected))
	for _, t := range selected {
		backups = append(backups, backup{t: t, prev: t.Clone()})
		t.Status = StatusPending
		t.Attempts = 0 // 新重放周期：重试预算重置，但仍受 max_attempts 约束
		t.ReplayCount++
		t.LastReplayBatch = batchID
		t.DLQReason = ""
		t.DeadLetteredAt = time.Time{}
		t.VisibleAt = now
		t.LeaseExpiresAt = time.Time{}
		t.UpdatedAt = now
		ids = append(ids, t.ID)
	}
	q.replayBatches[batchID] = &ReplayBatch{
		BatchID:    batchID,
		FilterHash: hash,
		TaskIDs:    append([]string(nil), ids...),
		Skipped:    append([]ReplaySkip(nil), skipped...),
		CreatedAt:  now,
	}
	prevTotal := q.replayedTotal
	q.replayedTotal += len(ids)

	if err := q.persistLocked(); err != nil {
		// 落盘失败：整体回滚，不留下半截状态。
		for _, b := range backups {
			*b.t = *b.prev
		}
		delete(q.replayBatches, batchID)
		q.replayedTotal = prevTotal
		return nil, err
	}
	q.cond.Broadcast()
	return &ReplayResult{BatchID: batchID, TaskIDs: ids, Skipped: skipped, ReplayedAt: now}, nil
}

// ReplayBatches 返回已记录的重放批次，按批次 ID 排序。
func (q *Queue) ReplayBatches() []*ReplayBatch {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*ReplayBatch, 0, len(q.replayBatches))
	for _, b := range q.replayBatches {
		cp := *b
		cp.TaskIDs = append([]string(nil), b.TaskIDs...)
		cp.Skipped = append([]ReplaySkip(nil), b.Skipped...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BatchID < out[j].BatchID })
	return out
}
