package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// 内部保留 ID：重放批次幂等记录以一条“标记任务”的形式与任务快照同一次 SaveAll
// 原子落盘，因此崩溃/重启后“批次是否已生效”与任务状态永远一致，不会出现
// 批次生效了但任务没重放（或反之）的半截状态。
const (
	reservedTaskIDPrefix  = "__queue_internal__:"
	replayRegistryTaskID  = reservedTaskIDPrefix + "replay-registry"
	replayRegistryStatus  = TaskStatus("__replay_registry__")
	replayRegistryVersion = 1
)

func isReservedTaskID(id string) bool {
	return strings.HasPrefix(id, reservedTaskIDPrefix)
}

// replayBatchRecord 记录一个重放批次首次生效时的结果。
type replayBatchRecord struct {
	Replayed  []string  `json:"replayed"`
	AppliedAt time.Time `json:"applied_at"`
}

// replayRegistryData 是标记任务 Payload 的 JSON 结构。
type replayRegistryData struct {
	Version int                          `json:"version"`
	Batches map[string]replayBatchRecord `json:"batches"`
}

func (q *Queue) loadReplayRegistryLocked() *replayRegistryData {
	reg := &replayRegistryData{Version: replayRegistryVersion, Batches: map[string]replayBatchRecord{}}
	marker, ok := q.tasks[replayRegistryTaskID]
	if !ok || len(marker.Payload) == 0 {
		return reg
	}
	if err := json.Unmarshal(marker.Payload, reg); err != nil {
		// 落盘内容由本进程原子写入，损坏不应发生；保守地返回空登记表，
		// 拒绝重放而非带着错误状态继续。
		return &replayRegistryData{Version: replayRegistryVersion, Batches: map[string]replayBatchRecord{}}
	}
	if reg.Batches == nil {
		reg.Batches = map[string]replayBatchRecord{}
	}
	return reg
}

// saveReplayRegistryLocked 把登记表序列化到标记任务。调用方持锁。
func (q *Queue) saveReplayRegistryLocked(reg *replayRegistryData) error {
	data, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	now := q.cfg.Now()
	if marker, ok := q.tasks[replayRegistryTaskID]; ok {
		marker.Payload = data
		marker.UpdatedAt = now
		return nil
	}
	q.tasks[replayRegistryTaskID] = &Task{
		ID:        replayRegistryTaskID,
		Payload:   data,
		Status:    replayRegistryStatus,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return nil
}

// ReplayFilter 描述批量挑选死信任务的条件。各条件之间为“与”关系；
// 零值字段不参与过滤。
type ReplayFilter struct {
	DLQReason    DLQReason    // 仅重放指定入死信原因（non_retryable / max_attempts_exceeded）
	FailureClass FailureClass // 仅重放最后一次失败为指定类别的任务
	ReasonSubstr string       // 最后失败原因包含该子串（区分大小写）
	EnteredFrom  time.Time    // 进入死信时间下界（含）
	EnteredTo    time.Time    // 进入死信时间上界（含）
	Limit        int          // 最多挑选多少个（>0 生效），按进入死信先后、再按任务 ID 稳定排序
}

// ReplayRequest 是一次人工死信重放请求。
//   - TaskID 非空时只点名该任务（Filter 被忽略）；该任务当前不在死信中
//     （不存在/处理中/已完成/已被重放）会被拒绝，原因 not_dead_lettered；
//   - TaskID 为空时按 Filter 批量挑选，没有匹配任务时返回空 Replayed（不是错误）；
//   - BatchID 是调用方提供的批次幂等键：同一 BatchID 的重复触发直接返回首次结果，
//     不会把任务重复投出去。
type ReplayRequest struct {
	BatchID string
	TaskID  string
	Filter  ReplayFilter
}

// ReplayResult 是重放结果快照。
type ReplayResult struct {
	BatchID        string   // 批次幂等键
	AlreadyApplied bool     // true 表示该批次此前已生效，本次为幂等重放，未再次投递
	Replayed       []string // 首次生效时真正重放回正常流程的任务 ID
}

// Replay 把死信任务重新投回正常处理流程。
//
// 重放只负责把任务从 dead_lettered 置回 pending（立即可见），之后仍走正常的
// Fetch 投递、Ack/Nack 与失败分类流程；不可重试失败仍直接入死信，可重试失败
// 仍受 max_attempts 约束——每一轮重放都有独立的重试预算（AttemptsAtReplay
// 作为基线），但历史投递次数（Attempts）与重放后投递次数（ReplayAttempts）
// 都会累计保留，可与原始处理历史区分。
//
// 整个操作在队列锁内完成并与任务快照同次原子落盘：与并发的 Fetch/Ack/Nack
// 天然串行，处理中或已完成的任务不可能被误重放；落盘失败时全部回滚，
// 不会留下半截状态。
func (q *Queue) Replay(req ReplayRequest) (*ReplayResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, &RejectError{Op: "replay", Reason: ReasonQueueClosed, TaskID: req.TaskID, Message: "queue is closed"}
	}
	if req.BatchID == "" {
		return nil, &RejectError{Op: "replay", Reason: ReasonInvalidReplay, Message: "replay batch id must not be empty"}
	}
	if isReservedTaskID(req.TaskID) {
		return nil, &RejectError{Op: "replay", Reason: ReasonInvalidReplay, TaskID: req.TaskID, Message: "reserved task id"}
	}

	// 批次幂等：同一 BatchID 的重复触发返回首次结果，不做任何状态变更。
	reg := q.loadReplayRegistryLocked()
	if rec, ok := reg.Batches[req.BatchID]; ok {
		log.Printf("queue replay batch=%s tasks=%v count=%d verdict=idempotent_noop basis=batch_already_applied",
			req.BatchID, rec.Replayed, len(rec.Replayed))
		return &ReplayResult{
			BatchID:        req.BatchID,
			AlreadyApplied: true,
			Replayed:       append([]string(nil), rec.Replayed...),
		}, nil
	}

	targets, err := q.selectReplayTargetsLocked(req)
	if err != nil {
		return nil, err
	}

	// 容量与背压边界：超限在任何状态变更之前明确拒绝（原子拒绝，不留半截状态）。
	if q.cfg.MaxReplayBatchSize > 0 && len(targets) > q.cfg.MaxReplayBatchSize {
		return nil, &RejectError{Op: "replay", Reason: ReasonReplayBatchTooLarge,
			Message: fmt.Sprintf("replay batch size %d exceeds limit %d", len(targets), q.cfg.MaxReplayBatchSize)}
	}
	if q.cfg.MaxReplayTotal > 0 {
		used := q.totalReplaysLocked()
		if used+len(targets) > q.cfg.MaxReplayTotal {
			return nil, &RejectError{Op: "replay", Reason: ReasonReplayQuotaExceeded,
				Message: fmt.Sprintf("replay total %d+%d would exceed cumulative limit %d", used, len(targets), q.cfg.MaxReplayTotal)}
		}
	}

	// 备份以便落盘失败时回滚。
	markerExisted := false
	var markerBackup *Task
	if old, ok := q.tasks[replayRegistryTaskID]; ok {
		markerExisted = true
		markerBackup = old.Clone()
	}
	taskBackups := make(map[string]*Task, len(targets))
	replayed := make([]string, 0, len(targets))

	now := q.cfg.Now()
	for _, t := range targets {
		taskBackups[t.ID] = t.Clone()

		t.Status = StatusPending
		t.VisibleAt = now
		t.LeaseExpiresAt = time.Time{}
		t.DLQReason = ""
		t.DeadLetteredAt = time.Time{}
		// 重放开启新的处理轮次：基线取重放前的累计投递数，之后按
		// Attempts-AttemptsAtReplay 判定本轮重试上限；历史次数不清零。
		t.AttemptsAtReplay = t.Attempts
		t.ReplayCount++
		t.LastReplayAt = now
		t.ReplayBatches = append(t.ReplayBatches, req.BatchID)
		// 副作用认领随重放重置：重放后重新投递是一轮新的正常处理，
		// RunIdempotent 会重新认领并执行，不会被旧认领挡住。
		t.SideEffectDone = false
		t.UpdatedAt = now
		replayed = append(replayed, t.ID)
	}

	reg.Batches[req.BatchID] = replayBatchRecord{Replayed: append([]string(nil), replayed...), AppliedAt: now}
	if err := q.saveReplayRegistryLocked(reg); err != nil {
		q.rollbackReplayLocked(targets, taskBackups, markerExisted, markerBackup)
		return nil, err
	}
	if err := q.persistLocked(); err != nil {
		q.rollbackReplayLocked(targets, taskBackups, markerExisted, markerBackup)
		return nil, err
	}

	q.cond.Broadcast() // 立即唤醒 Fetch 等待者
	log.Printf("queue replay batch=%s tasks=%v count=%d verdict=replayed basis=manual_dead_letter_replay",
		req.BatchID, replayed, len(replayed))
	return &ReplayResult{BatchID: req.BatchID, Replayed: replayed}, nil
}

// rollbackReplayLocked 回滚未落盘成功的重放。调用方持锁。
func (q *Queue) rollbackReplayLocked(targets []*Task, backups map[string]*Task, markerExisted bool, markerBackup *Task) {
	for _, t := range targets {
		if old, ok := backups[t.ID]; ok {
			q.tasks[t.ID] = old
		}
	}
	if markerExisted {
		if markerBackup != nil {
			q.tasks[replayRegistryTaskID] = markerBackup
		}
	} else {
		delete(q.tasks, replayRegistryTaskID)
	}
}

// selectReplayTargetsLocked 按请求挑选当前确实处于 dead_lettered 的任务。调用方持锁。
func (q *Queue) selectReplayTargetsLocked(req ReplayRequest) ([]*Task, error) {
	if req.TaskID != "" {
		t, ok := q.tasks[req.TaskID]
		if !ok {
			return nil, &RejectError{Op: "replay", Reason: ReasonNotDeadLettered, TaskID: req.TaskID,
				Message: "unknown task: no such dead letter"}
		}
		if t.Status != StatusDeadLettered {
			return nil, &RejectError{Op: "replay", Reason: ReasonNotDeadLettered, TaskID: req.TaskID,
				Message: fmt.Sprintf("task is %s, not dead_lettered; only dead letters can be replayed", t.Status)}
		}
		return []*Task{t}, nil
	}

	f := req.Filter
	var out []*Task
	for _, t := range q.tasks {
		if t.Status != StatusDeadLettered {
			continue
		}
		if f.DLQReason != "" && t.DLQReason != f.DLQReason {
			continue
		}
		if f.FailureClass != "" && t.LastFailureClass != f.FailureClass {
			continue
		}
		if f.ReasonSubstr != "" && !strings.Contains(t.LastFailureReason, f.ReasonSubstr) {
			continue
		}
		if !f.EnteredFrom.IsZero() && t.DeadLetteredAt.Before(f.EnteredFrom) {
			continue
		}
		if !f.EnteredTo.IsZero() && t.DeadLetteredAt.After(f.EnteredTo) {
			continue
		}
		out = append(out, t)
	}
	// 稳定排序：进入死信先后，再按任务 ID。
	sort.Slice(out, func(i, j int) bool {
		if !out[i].DeadLetteredAt.Equal(out[j].DeadLetteredAt) {
			return out[i].DeadLetteredAt.Before(out[j].DeadLetteredAt)
		}
		return out[i].ID < out[j].ID
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// totalReplaysLocked 统计队列生命周期内累计重放的任务数量（所有任务的重放次数之和）。
func (q *Queue) totalReplaysLocked() int {
	total := 0
	for _, t := range q.tasks {
		total += t.ReplayCount
	}
	return total
}

// ReplayCount 返回某任务累计被人工重放的次数（快照诊断用）。
func (q *Queue) ReplayCount(taskID string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[taskID]
	if !ok {
		return 0
	}
	return t.ReplayCount
}
