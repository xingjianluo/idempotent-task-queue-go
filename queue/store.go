package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// State 是需要持久化的完整队列状态快照。
type State struct {
	Seq   int64            `json:"seq"` // 单调投递令牌水位
	Tasks map[string]*Task `json:"tasks"`
}

func newState() *State {
	return &State{Tasks: make(map[string]*Task)}
}

// Store 是本地持久化存储接口（用本地文件模拟消息中间件）。
type Store interface {
	Load(ctx context.Context) (*State, error)
	Save(ctx context.Context, state *State) error
}

// MemoryStore 进程内存储（用于测试与演示，不跨进程）。
type MemoryStore struct {
	mu sync.Mutex
	s  *State
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Load(ctx context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.s == nil {
		return newState(), nil
	}
	return cloneState(s.s), nil
}

func (s *MemoryStore) Save(ctx context.Context, state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.s = cloneState(state)
	return nil
}

// FileStore 本地 JSON 文件持久化存储（临时文件 + fsync + rename 原子写入）。
type FileStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

func (s *FileStore) Load(ctx context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return newState(), nil
		}
		return nil, err
	}
	var st State
	if len(data) == 0 {
		return newState(), nil
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Tasks == nil {
		st.Tasks = make(map[string]*Task)
	}
	return &st, nil
}

func (s *FileStore) Save(ctx context.Context, state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".queue-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil { // fsync 文件内容
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil { // 原子替换
		cleanup()
		return err
	}
	// fsync 目录，保证 rename 落盘。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// cloneState 返回状态的深拷贝。
func cloneState(s *State) *State {
	cp := &State{Seq: s.Seq, Tasks: make(map[string]*Task, len(s.Tasks))}
	for k, v := range s.Tasks {
		cp.Tasks[k] = v.clone()
	}
	return cp
}
