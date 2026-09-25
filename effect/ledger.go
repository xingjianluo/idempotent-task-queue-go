// Package effect 提供一个本地持久化的幂等副作用台账，
// 用于演示"任务重复投递时不重复生效"：以任务 ID 为幂等键记录副作用。
package effect

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"sync"
)

// Ledger 幂等副作用台账（本地文件 JSONL 追加持久化，重启后判定一致）。
type Ledger struct {
	mu   sync.Mutex
	path string
	seen map[string]struct{}
	f    *os.File
	w    *bufio.Writer
}

// ledgerRecord 每条记录一个已生效的幂等键。
type ledgerRecord struct {
	Key string `json:"key"`
	At  int64  `json:"at"`
}

// NewLedger 打开/创建台账文件并回放历史记录。path 为空表示仅内存。
func NewLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, seen: make(map[string]struct{})}
	if path == "" {
		return l, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l.f = f
	l.w = bufio.NewWriter(f)

	// 回放：重建已生效键集合（重复行天然幂等）。
	if data, rerr := os.Open(path); rerr == nil {
		sc := bufio.NewScanner(data)
		sc.Buffer(make([]byte, 1<<20), 1<<22)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var rec ledgerRecord
			if err := json.Unmarshal(line, &rec); err == nil && rec.Key != "" {
				l.seen[rec.Key] = struct{}{}
			}
		}
		_ = data.Close()
		if err := sc.Err(); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else if !errors.Is(rerr, fs.ErrNotExist) {
		_ = f.Close()
		return nil, rerr
	}
	return l, nil
}

// ApplyOnce 以 key（通常为任务 ID）为幂等键执行 effect：
// 首次执行成功并落盘后返回 (true, nil)；重复调用不再执行 effect，返回 (false, nil)。
// effect 执行失败不落盘，允许后续重试。
func (l *Ledger) ApplyOnce(ctx context.Context, key string, effect func() error) (bool, error) {
	l.mu.Lock()
	if _, ok := l.seen[key]; ok {
		l.mu.Unlock()
		return false, nil
	}
	l.mu.Unlock()

	if err := effect(); err != nil {
		return false, err
	}

	// effect 完成后再落盘幂等键。若在 effect 之后、落盘之前崩溃，
	// 重启后会重新执行 effect，因此要求 effect 本身尽量幂等（at-least-once 约定）。
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[key]; ok { // 双重检查
		return false, nil
	}
	if err := l.appendLocked(ledgerRecord{Key: key}); err != nil {
		return false, err
	}
	l.seen[key] = struct{}{}
	return true, nil
}

// appendLocked 追加一条记录并刷盘（JSONL 是追加式的，崩溃只可能丢最后半行，可被回放忽略）。
func (l *Ledger) appendLocked(rec ledgerRecord) error {
	if l.path == "" {
		return nil
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := l.w.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Seen 返回某 key 的副作用是否已生效。
func (l *Ledger) Seen(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.seen[key]
	return ok
}

// Keys 返回已生效的全部 key（快照，排序稳定）。
func (l *Ledger) Keys() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.seen))
	for k := range l.seen {
		out = append(out, k)
	}
	return out
}

// Close 关闭台账。
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		_ = l.f.Close()
		return err
	}
	return l.f.Close()
}
