package queue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Store 是任务状态的持久化接口，用本地存储模拟消息中间件。
// SaveAll 以全量快照原子落盘，保证重启恢复后任务状态一致。
type Store interface {
	Load() (map[string]*Task, error)
	SaveAll(tasks map[string]*Task) error
}

// replayStore 是 Store 的可选扩展：把重放批次记录与任务快照放在同一份
// 状态里原子落盘，避免“任务已重放但批次记录丢失”（或反之）的半截状态。
// 内置的 memoryStore / fileStore 均实现该接口；自定义 Store 未实现时，
// 重放批次记录只保留在内存中（重放的幂等性跨重启不保证，见文档）。
type replayStore interface {
	LoadReplay() (map[string]*ReplayBatch, error)
	SaveAllWithReplay(tasks map[string]*Task, batches map[string]*ReplayBatch) error
}

func cloneBatches(src map[string]*ReplayBatch) map[string]*ReplayBatch {
	out := make(map[string]*ReplayBatch, len(src))
	for id, b := range src {
		cp := *b
		cp.TaskIDs = append([]string(nil), b.TaskIDs...)
		cp.Skipped = append([]ReplaySkip(nil), b.Skipped...)
		out[id] = &cp
	}
	return out
}

// memoryStore 纯内存存储（进程重启不保留）。
type memoryStore struct {
	mu      sync.Mutex
	tasks   map[string]*Task
	batches map[string]*ReplayBatch
}

// NewMemoryStore 构造纯内存存储。
func NewMemoryStore() Store {
	return &memoryStore{tasks: map[string]*Task{}, batches: map[string]*ReplayBatch{}}
}

func (s *memoryStore) Load() (map[string]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Task, len(s.tasks))
	for id, t := range s.tasks {
		out[id] = t.Clone()
	}
	return out, nil
}

func (s *memoryStore) SaveAll(tasks map[string]*Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Task, len(tasks))
	for id, t := range tasks {
		out[id] = t.Clone()
	}
	s.tasks = out
	return nil
}

func (s *memoryStore) LoadReplay() (map[string]*ReplayBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBatches(s.batches), nil
}

func (s *memoryStore) SaveAllWithReplay(tasks map[string]*Task, batches map[string]*ReplayBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Task, len(tasks))
	for id, t := range tasks {
		out[id] = t.Clone()
	}
	s.tasks = out
	s.batches = cloneBatches(batches)
	return nil
}

// fileStore JSON 文件持久化存储（重启后恢复）。
type fileStore struct {
	mu      sync.Mutex
	path    string
	batches map[string]*ReplayBatch // Load/LoadReplay 时从磁盘读入的缓存
}

// NewFileStore 构造 JSON 文件持久化存储。
func NewFileStore(path string) Store {
	return &fileStore{path: path, batches: map[string]*ReplayBatch{}}
}

type persistedState struct {
	Tasks         map[string]*Task        `json:"tasks"`
	ReplayBatches map[string]*ReplayBatch `json:"replay_batches,omitempty"`
}

func (s *fileStore) Load() (map[string]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.readLocked()
	if err != nil {
		return nil, err
	}
	s.batches = cloneBatches(st.ReplayBatches)
	return st.Tasks, nil
}

func (s *fileStore) LoadReplay() (map[string]*ReplayBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.readLocked()
	if err != nil {
		return nil, err
	}
	s.batches = cloneBatches(st.ReplayBatches)
	return cloneBatches(st.ReplayBatches), nil
}

// readLocked 读取并解析状态文件；文件不存在或为空时返回空状态。调用方持锁。
func (s *fileStore) readLocked() (*persistedState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return &persistedState{Tasks: map[string]*Task{}, ReplayBatches: map[string]*ReplayBatch{}}, nil
		}
		return nil, err
	}
	var st persistedState
	if len(data) == 0 {
		return &persistedState{Tasks: map[string]*Task{}, ReplayBatches: map[string]*ReplayBatch{}}, nil
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Tasks == nil {
		st.Tasks = map[string]*Task{}
	}
	if st.ReplayBatches == nil {
		st.ReplayBatches = map[string]*ReplayBatch{}
	}
	return &st, nil
}

// SaveAll 保存任务快照，保留已加载的重放批次记录。
func (s *fileStore) SaveAll(tasks map[string]*Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(&persistedState{Tasks: tasks, ReplayBatches: s.batches})
}

// SaveAllWithReplay 把任务快照与重放批次记录放在同一份文件里原子落盘。
func (s *fileStore) SaveAllWithReplay(tasks map[string]*Task, batches map[string]*ReplayBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = cloneBatches(batches)
	return s.writeLocked(&persistedState{Tasks: tasks, ReplayBatches: batches})
}

// writeLocked 先写临时文件再原子 rename，避免进程中断产生半截状态文件。调用方持锁。
func (s *fileStore) writeLocked(st *persistedState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".queue-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}
