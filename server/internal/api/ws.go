package api

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/pg_lineage"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WSHandler holds shared resources injected from app.Server
type WSHandler struct {
	DB       *sql.DB
	Registry *reactive.Registry
}

// HandleWS upgrades the connection and handles subscribe/unsubscribe messages
func (h *WSHandler) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	// small helper for sending messages to this connection
	wsSend := func(msgType string, payload any) error {
		out := map[string]any{"type": msgType, "data": payload}
		return conn.WriteJSON(out)
	}

	cl := &reactive.Client{Send: wsSend}
	activeQueries := []*reactive.LiveQuery{} // track what this client subscribed to

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("ws read error:", err)
			break
		}

		var req struct {
			Type string `json:"type"`
			SQL  string `json:"sql"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			wsSend("error", map[string]string{"error": "invalid JSON"})
			continue
		}

		switch strings.ToLower(req.Type) {
		case "subscribe":
			if req.SQL == "" {
				wsSend("error", map[string]string{"error": "missing SQL"})
				continue
			}

			lq, err := h.registerLiveQuery(req.SQL, cl)
			if err != nil {
				wsSend("error", map[string]string{"error": err.Error()})
				continue
			}

			activeQueries = append(activeQueries, lq)
			wsSend("subscribed", map[string]any{
				"id":      lq.ID,
				"tables":  lq.Tables,
				"pkCols":  lq.PKCols,
				"rewrote": lq.Rewritten,
			})

		case "unsubscribe":
			if len(activeQueries) == 0 {
				continue
			}
			for _, q := range activeQueries {
				h.Registry.Unregister(q.ID)
			}
			activeQueries = nil
			wsSend("unsubscribed", "ok")

		default:
			wsSend("error", map[string]string{"error": "unknown message type"})
		}
	}

	// cleanup on disconnect
	for _, q := range activeQueries {
		q.Mu.Lock()
		delete(q.Clients, cl)
		empty := len(q.Clients) == 0
		q.Mu.Unlock()

		if empty {
			h.Registry.Unregister(q.ID)
		}
	}
}

// registerLiveQuery parses, rewrites, and registers a new live query in the registry
func (h *WSHandler) registerLiveQuery(sql string, cl *reactive.Client) (*reactive.LiveQuery, error) {
	cat, err := pg_lineage.NewCatalogFromDB(h.DB, []string{"public"})
	if err != nil {
		return nil, err
	}

	// Run rewrite + provenance analysis
	rew, pkByAlias, _ := pg_lineage.RewriteSelectInjectPKs(sql, cat)
	prov, _ := pg_lineage.ResolveProvenance(rew, cat)

	// Map alias -> table (for dependency tracking)
	aliasToTable := make(map[string]string)
	tablesSet := map[string]struct{}{}

	for outCol, srcs := range prov {
		if len(srcs) == 0 {
			continue
		}
		src := srcs[0] // e.g., "actor.actor_id"
		parts := strings.SplitN(src, ".", 2)
		if len(parts) != 2 {
			continue
		}
		table := parts[0]
		tablesSet["public."+table] = struct{}{}
		aliasToTable[outCol] = table
	}

	var tables []string
	for t := range tablesSet {
		tables = append(tables, t)
	}

	// Preserve injected PK aliases directly for incremental WHERE filters
	pkAliasCols := make(map[string][]string)
	for alias, injectedCols := range pkByAlias {
		// Keep the injected columns exactly as the rewrite created them
		pkAliasCols[alias] = append([]string(nil), injectedCols...)
	}

	provOrig, _ := pg_lineage.ResolveProvenance(sql, cat)
	provRewritten, _ := pg_lineage.ResolveProvenance(rew, cat)

	lq := &reactive.LiveQuery{
		ID:            uuid.NewString(),
		SQL:           sql,
		Rewritten:     rew,
		Tables:        tables,
		PKCols:        pkAliasCols,
		Clients:       map[*reactive.Client]struct{}{cl: {}},
		ProvOrig:      provOrig,
		ProvRewritten: provRewritten,
		PKMapByAlias:  pkByAlias,
	}

	// lq := &reactive.LiveQuery{
	// 	ID:        uuid.NewString(),
	// 	SQL:       sql,
	// 	Rewritten: rew,
	// 	Tables:    tables,      // used for matching table changes from WAL
	// 	PKCols:    pkAliasCols, // used for building WHERE filters (_pk_a_actor_id, etc.)
	// 	Clients:   map[*reactive.Client]struct{}{cl: {}},
	// }

	h.Registry.Register(lq)
	return lq, nil
}
