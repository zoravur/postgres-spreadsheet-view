package protocol

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Subscription struct {
	ID    string
	Query string
	Conn  *websocket.Conn
}

type Registry struct {
	mu   sync.RWMutex
	subs map[string]*Subscription
}

func NewRegistry() *Registry {
	return &Registry{subs: make(map[string]*Subscription)}
}

func (r *Registry) Add(sub *Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs[sub.ID] = sub
}

func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subs, id)
}

func (r *Registry) Broadcast(update Update) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, sub := range r.subs {
		// naive broadcast to all subscribers
		sub.Conn.WriteJSON(update)
	}
}
