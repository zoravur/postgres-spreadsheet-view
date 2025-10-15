// routes.go
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()

	// Handle the WebSocket route before any global middleware that might wrap the response writer.
	r.Get("/api/ws", HandleWS)

	// Global middleware applied to all other routes.
	r.Group(func(r chi.Router) {
		r.Use(LoggingMiddleware)

		r.Route("/api", func(r chi.Router) {
			r.Post("/query", handleEditableQuery)
			r.Post("/edit", handleEdit)
		})
	})

	fs := http.FileServer(http.Dir("web"))
	r.Handle("/*", fs)

	return r
}
