package api

import (
	"database/sql"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
)

func SetupRoutes(reg *reactive.Registry, db *sql.DB) http.Handler {
	r := chi.NewRouter()

	// --- WebSocket routes: NO middleware allowed ---
	wsHandler := &WSHandler{DB: db, Registry: reg}
	r.Get("/api/ws", wsHandler.HandleWS)

	// --- All other routes grouped with middleware ---
	r.Group(func(r chi.Router) {
		r.Use(LoggingMiddleware)

		r.Route("/api", func(r chi.Router) {
			r.Post("/query", handleEditableQuery)
			r.Post("/edit", handleEdit)
			r.Get("/live", func(w http.ResponseWriter, req *http.Request) {
				handleLiveQueries(w, req, reg)
			})
		})
	})

	// --- Static file server ---
	fs := http.FileServer(http.Dir("web"))
	r.Handle("/*", fs)

	return r
}
