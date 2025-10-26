package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"go.uber.org/zap"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/pg_lineage"
	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/richcatalog"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WSHandler holds shared resources injected from app.Server
type WSHandler struct {
	DB       *sql.DB
	Registry *reactive.Registry
	Catalog  *richcatalog.Catalog
	Log      *zap.Logger
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
			var ce *websocket.CloseError
			if errors.As(err, &ce) {
				if ce.Code == websocket.CloseNormalClosure || ce.Code == websocket.CloseGoingAway {
					zap.L().Info("ws closed", zap.Int("code", ce.Code))
				} else {
					zap.L().Warn("ws closed abnormally", zap.Int("code", ce.Code), zap.String("text", ce.Text))
				}
			} else {
				zap.L().Error("ws read error", zap.Error(err))
			}
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
	cat, err := richcatalog.New(h.DB, richcatalog.Options{
		Schemas:        []string{"public"},
		IncludeIndexes: true,
		IncludeFKs:     true,
	})
	if err != nil {
		return nil, err
	}

	// Critical: populate the catalog
	if err := cat.Refresh(context.TODO()); err != nil {
		return nil, fmt.Errorf("catalog refresh: %w", err)
	}

	// Run rewrite + provenance analysis
	rew, pkByAlias, err := pg_lineage.RewriteSelectInjectPKs(sql, cat)
	if err != nil {
		return nil, fmt.Errorf("rewrite: %w", err)
	}

	prov, err := pg_lineage.ResolveProvenance(rew, cat)
	if err != nil {
		zap.L().Warn("provenance_failed", zap.String("rew", rew), zap.Error(err))
		// Optional: fallback to FROM-clause extraction here
	}

	// Map alias -> table (for dependency tracking)
	// aliasToTable := make(map[string]string)
	tablesSet := map[string]struct{}{}

	for _, srcs := range prov {
		if len(srcs) == 0 {
			continue
		}
		for _, src := range srcs {
			parts := strings.SplitN(src, ".", 2)
			if len(parts) != 2 {
				continue
			}
			base := parts[0]
			tablesSet["public."+strings.ToLower(base)] = struct{}{}
		}

		if len(tablesSet) == 0 {
			zap.L().Error("No base tables in query")
			// bases, err := pg_lineage.ResolveBaseTables(rew, cat) // or walk AST
			// if err == nil {
			// 	for _, b := range bases {
			// 		tablesSet["public."+strings.ToLower(b)] = struct{}{}
			// 	}
			// }
		}

		// parts := strings.SplitN(src, ".", 2)
		// if len(parts) != 2 {
		// 	continue
		// }
		// table := parts[0]
		// tablesSet["public."+table] = struct{}{}
		// aliasToTable[outCol] = table
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

	h.Registry.Register(lq)
	return lq, nil
}
