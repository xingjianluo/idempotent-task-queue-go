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

// memoryStore 纯内存存储（进程重启不保留）。
type memoryStore struct {
	mu    sync.Mutex
	tasks map[string]*Task
}

// NewMemoryStore 构造纯内存存储。
func NewMemoryStore() Store {
	return &memoryStore{tasks: map[string]*Task{}}
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

// fileStore JSON 文件持久化存储（重启后恢复）。
type fileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore 构造 JSON 文件持久化存储。
func NewFileStore(path string) Store {
	return &fileStore{path: path}
}

type persistedState struct {
	Tasks map[string]*Task `json:"tasks"`
}

func (s *fileStore) Load() (map[string]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]*Task{}, nil
		}
		return nil, err
	}
	var st persistedState
	if len(data) == 0 {
		return map[string]*Task{}, nil
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Tasks == nil {
		st.Tasks = map[string]*Task{}
	}
	return st.Tasks, nil
}

// SaveAll 先写临时文件再原子 rename，避免进程中断产生半截状态文件。
func (s *fileStore) SaveAll(tasks map[string]*Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := persistedState{Tasks: tasks}
	data, err := json.MarshalIndent(&st, "", "  ")
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
