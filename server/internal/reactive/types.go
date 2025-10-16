package reactive

import (
	"database/sql"
	"sync"
)

type LiveQuery struct {
	ID        string
	SQL       string              // original
	Rewritten string              // with _pk_* injected
	Tables    []string            // ["public.actor", "public.film", ...]
	PKCols    map[string][]string // "public.actor" -> ["actor_id"]
	Clients   map[*Client]struct{}
	Mu        sync.RWMutex

	// 🔧 add this:
	ProvOrig      map[string][]string // from ResolveProvenance(origSQL)
	ProvRewritten map[string][]string // from ResolveProvenance(rewrittenSQL)
	PKMapByAlias  map[string][]string // direct from RewriteSelectInjectPKs
}

type Client struct {
	// abstract over ws.Conn to avoid import cycles
	Send func(msgType string, payload any) error
}

type WALEvent struct {
	Schema string
	Table  string
	Kind   string         // insert|update|delete
	Keys   map[string]any // primary key name -> value (from oldkeys/newkeys)
}

// lets you inject your DB + lineage deps without global singletons
type Deps struct {
	DB        *sql.DB
	Broadcast func(lq *LiveQuery, msgType string, payload any)
}
