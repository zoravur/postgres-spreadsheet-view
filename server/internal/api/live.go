package api

import (
	"encoding/json"
	"net/http"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
)

func handleLiveQueries(w http.ResponseWriter, r *http.Request, reg *reactive.Registry) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reg.SnapshotView())
}
