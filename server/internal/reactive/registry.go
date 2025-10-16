package reactive

import (
	"sync"
)

type Registry struct {
	mu   sync.RWMutex
	data map[string]*LiveQuery
}

func NewRegistry() *Registry {
	return &Registry{data: make(map[string]*LiveQuery)}
}

func (r *Registry) Register(q *LiveQuery) {
	r.mu.Lock()
	r.data[q.ID] = q
	r.mu.Unlock()
}

func (r *Registry) Unregister(id string) {
	r.mu.Lock()
	delete(r.data, id)
	r.mu.Unlock()
}

func (r *Registry) Get(id string) (*LiveQuery, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	q, ok := r.data[id]
	return q, ok
}

func (r *Registry) Snapshot() []*LiveQuery {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*LiveQuery, 0, len(r.data))
	for _, q := range r.data {
		out = append(out, q)
	}
	return out
}

func (r *Registry) ForEach(fn func(*LiveQuery) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, q := range r.data {
		if !fn(q) {
			break
		}
	}
}

func (r *Registry) SnapshotView() []map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]map[string]any, 0, len(r.data))
	for _, q := range r.data {
		q.Mu.RLock()
		item := map[string]any{
			"id":        q.ID,
			"sql":       q.SQL,
			"rewritten": q.Rewritten,
			"tables":    append([]string(nil), q.Tables...), // copy slice
			"pkCols":    clonePKMap(q.PKCols),
			"clients":   len(q.Clients),
		}
		q.Mu.RUnlock()
		out = append(out, item)
	}
	return out
}

func clonePKMap(src map[string][]string) map[string][]string {
	dst := make(map[string][]string, len(src))
	for k, v := range src {
		dst[k] = append([]string(nil), v...)
	}
	return dst
}

func (r *Registry) CleanupOrphans() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	for id, q := range r.data {
		q.Mu.RLock()
		noClients := len(q.Clients) == 0
		q.Mu.RUnlock()
		if noClients {
			delete(r.data, id)
			count++
		}
	}
	return count
}
