package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	r.Use(LoggingMiddleware) // ← add this line

	r.Route("/api", func(r chi.Router) {
		r.Post("/query", handleEditableQuery)
		// r.Post("/provenance", handleProvenance)
		r.Post("/edit", handleEdit)
	})

	fs := http.FileServer(http.Dir("web"))
	r.Handle("/*", fs)

	return r
}
